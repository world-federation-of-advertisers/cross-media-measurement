/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.service.api

import com.google.protobuf.Descriptors
import com.google.protobuf.TypeRegistry
import io.grpc.Status
import io.grpc.StatusException
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.coroutines.coroutineContext
import kotlinx.coroutines.CompletableDeferred
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import kotlinx.coroutines.time.delay
import org.jetbrains.annotations.NonBlockingExecutor
import org.jetbrains.annotations.VisibleForTesting
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.pb.Checked
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.common.ProtoReflection

private const val METADATA_FIELD = "metadata.metadata"
private const val MAX_PAGE_SIZE = 1000

interface CelEnvProvider {
  data class TypeRegistryAndEnv(val typeRegistry: TypeRegistry, val env: Env)

  suspend fun getTypeRegistryAndEnv(): TypeRegistryAndEnv
}

class CelEnvCacheProvider(
  private val eventGroupsMetadataDescriptorsStub:
    EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub,
  /** Protobuf descriptor of Reporting EventGroup message type. */
  private val reportingEventGroupDescriptor: Descriptors.Descriptor,
  private val cacheRefreshInterval: Duration,
  knownMetadataTypes: Iterable<Descriptors.FileDescriptor>,
  coroutineContext: @NonBlockingExecutor CoroutineContext = EmptyCoroutineContext,
  private val numRetriesInitialSync: Int = 3,
) : CelEnvProvider, AutoCloseable {
  private val allKnownMetadataTypes: Set<Descriptors.FileDescriptor> =
    knownMetadataTypes.asSequence().plus(ProtoReflection.WELL_KNOWN_TYPES).toSet()
  private lateinit var typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv
  private val coroutineScope = CoroutineScope(coroutineContext + SupervisorJob())
  private val initialSyncJob = CompletableDeferred<Unit>()
  private val syncMutex = Mutex()

  init {
    coroutineScope.launch {
      while (currentCoroutineContext().isActive) {
        syncMutex.withLock {
          if (initialSyncJob.isActive) {
            var updateFlow = flow<Unit> { setTypeRegistryAndEnv() }
            if (numRetriesInitialSync > 0) {
              updateFlow =
                updateFlow.retry(numRetriesInitialSync.toLong()) { e ->
                  (e is RetryableException).also { if (it) delay(RETRY_DELAY) }
                }
            }
            try {
              updateFlow.collect()
              initialSyncJob.complete(Unit)
            } catch (e: Exception) {
              initialSyncJob.completeExceptionally(e)
              throw e
            }
          } else {
            try {
              setTypeRegistryAndEnv()
            } catch (e: Exception) {
              logger.log(Level.WARNING, e) { "Error updating CEL env cache" }
            }
          }
        }
        delay(cacheRefreshInterval)
      }
    }
  }

  override fun close() {
    initialSyncJob.cancel()
    coroutineScope.cancel()
  }

  override suspend fun getTypeRegistryAndEnv(): CelEnvProvider.TypeRegistryAndEnv {
    initialSyncJob.await()
    return typeRegistryAndEnv
  }

  private suspend fun setTypeRegistryAndEnv() {
    typeRegistryAndEnv = buildTypeRegistryAndEnv()
  }

  private suspend fun buildTypeRegistryAndEnv(): CelEnvProvider.TypeRegistryAndEnv {
    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      getEventGroupMetadataDescriptors()

    val fileDescriptors: Set<Descriptors.FileDescriptor> =
      allKnownMetadataTypes.toMutableSet().apply {
        addAll(
          ProtoReflection.buildFileDescriptors(
            eventGroupMetadataDescriptors.map { it.descriptorSet },
            allKnownMetadataTypes,
          )
        )
      }

    val env = buildCelEnvironment(fileDescriptors)
    val typeRegistry: TypeRegistry = buildTypeRegistry(fileDescriptors)

    return CelEnvProvider.TypeRegistryAndEnv(typeRegistry, env)
  }

  private fun buildCelEnvironment(fileDescriptors: Iterable<Descriptors.FileDescriptor>): Env {
    // Build CEL ProtoTypeRegistry.
    val celTypeRegistry =
      ProtoTypeRegistry.newRegistry().apply {
        for (fileDescriptor in fileDescriptors) {
          registerDescriptor(fileDescriptor)
        }
        registerDescriptor(reportingEventGroupDescriptor.file)
      }

    // Build CEL Env.
    val env =
      Env.newEnv(
        EnvOption.container(reportingEventGroupDescriptor.fullName),
        EnvOption.customTypeProvider(celTypeRegistry),
        EnvOption.customTypeAdapter(celTypeRegistry),
        EnvOption.declarations(
          reportingEventGroupDescriptor.fields
            .map {
              Decls.newVar(
                it.name,
                celTypeRegistry.findFieldType(reportingEventGroupDescriptor.fullName, it.name).type,
              )
            }
            // TODO(projectnessie/cel-java#295): Remove when fixed.
            .plus(Decls.newVar(METADATA_FIELD, Checked.checkedAny))
        ),
      )
    return env
  }

  private suspend fun getEventGroupMetadataDescriptors(): List<EventGroupMetadataDescriptor> {
    try {
      val eventGroupMetadataDescriptors = mutableListOf<EventGroupMetadataDescriptor>()
      var response =
        eventGroupsMetadataDescriptorsStub.listEventGroupMetadataDescriptors(
          listEventGroupMetadataDescriptorsRequest {
            parent = "dataProviders/-"
            pageSize = MAX_PAGE_SIZE
          }
        )
      eventGroupMetadataDescriptors.addAll(response.eventGroupMetadataDescriptorsList)

      while (coroutineContext.isActive && response.nextPageToken.isNotEmpty()) {
        response =
          eventGroupsMetadataDescriptorsStub.listEventGroupMetadataDescriptors(
            listEventGroupMetadataDescriptorsRequest {
              parent = "dataProviders/-"
              pageSize = MAX_PAGE_SIZE
              pageToken = response.nextPageToken
            }
          )
        eventGroupMetadataDescriptors.addAll(response.eventGroupMetadataDescriptorsList)
      }

      return eventGroupMetadataDescriptors
    } catch (e: StatusException) {
      val message = "Error retrieving EventGroupMetadataDescriptors"
      throw when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED -> RetryableException(message, e)
        else -> NonRetryableException(message, e)
      }
    }
  }

  /** Suspends until any in-flight sync operations are complete. */
  suspend fun waitForSync() {
    initialSyncJob.join()
    syncMutex.withLock {
      // No-op.
    }
  }

  private class RetryableException(message: String? = null, cause: Throwable? = null) :
    Exception(message, cause)

  private class NonRetryableException(message: String? = null, cause: Throwable? = null) :
    Exception(message, cause)

  companion object {
    /**
     * Duration of delay between initial sync retries.
     *
     * TODO(@SanjayVas): Use exponential backoff.
     */
    @VisibleForTesting internal val RETRY_DELAY: Duration = Duration.ofMillis(100)

    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private fun buildTypeRegistry(
      fileDescriptors: Iterable<Descriptors.FileDescriptor>
    ): TypeRegistry {
      return TypeRegistry.newBuilder().add(fileDescriptors.flatMap { it.messageTypes }).build()
    }
  }
}
