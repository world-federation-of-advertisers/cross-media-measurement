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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import com.google.protobuf.TypeRegistry
import io.grpc.Status
import io.grpc.StatusException
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlin.coroutines.CoroutineContext
import kotlinx.coroutines.CompletableJob
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Job
import kotlinx.coroutines.SupervisorJob
import kotlinx.coroutines.cancel
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.retry
import kotlinx.coroutines.launch
import org.projectnessie.cel.Env
import org.projectnessie.cel.EnvOption
import org.projectnessie.cel.checker.Decls
import org.projectnessie.cel.common.types.pb.Checked
import org.projectnessie.cel.common.types.pb.ProtoTypeRegistry
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptor
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.listEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.reporting.v1alpha.EventGroup

private const val METADATA_FIELD = "metadata.metadata"
private const val MAX_PAGE_SIZE = 1000

interface CelEnvProvider {
  data class TypeRegistryAndEnv(
    val typeRegistry: TypeRegistry,
    val env: Env,
  )

  suspend fun getTypeRegistryAndEnv(): TypeRegistryAndEnv
}

class CelEnvCacheProvider(
  private val eventGroupsMetadataDescriptorsStub:
    EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub,
  private val cacheRefreshInterval: Duration,
  coroutineContext: CoroutineContext,
  private val clock: Clock,
  private val numRetriesInitialSync: Long = 3L,
) : CelEnvProvider, AutoCloseable {
  private lateinit var typeRegistryAndEnv: CelEnvProvider.TypeRegistryAndEnv
  private val coroutineScope = CoroutineScope(coroutineContext + SupervisorJob())
  private val initialSyncJob: CompletableJob = Job()

  init {
    coroutineScope.launch {
      MinimumIntervalThrottler(clock, cacheRefreshInterval).loopOnReady {
        if (initialSyncJob.isActive) {
          var updateFlow = flow<Unit> { setTypeRegistryAndEnv() }
          if (numRetriesInitialSync > 0) {
            updateFlow = updateFlow.retry(numRetriesInitialSync) { e -> e is RetriableException }
          }
          try {
            updateFlow.collect {}
            initialSyncJob.complete()
          } catch (e: Exception) {
            initialSyncJob.completeExceptionally(e)
            throw (e)
          }
        } else {
          try {
            setTypeRegistryAndEnv()
          } catch (e: Exception) {
            logger.log(Level.WARNING, e) { "Error updating CEL env cache" }
          }
        }
      }
    }
  }

  override fun close() {
    coroutineScope.cancel()
  }

  override suspend fun getTypeRegistryAndEnv(): CelEnvProvider.TypeRegistryAndEnv {
    initialSyncJob.join()
    return typeRegistryAndEnv
  }

  private suspend fun setTypeRegistryAndEnv() {
    typeRegistryAndEnv = buildTypeRegistryAndEnv()
  }

  private suspend fun buildTypeRegistryAndEnv(): CelEnvProvider.TypeRegistryAndEnv {
    val eventGroupMetadataDescriptors: List<EventGroupMetadataDescriptor> =
      getEventGroupMetadataDescriptors()

    val fileDescriptorSets: List<DescriptorProtos.FileDescriptorSet> =
      eventGroupMetadataDescriptors.map { it.descriptorSet }
    val fileDescriptors: List<Descriptors.Descriptor> =
      ProtoReflection.buildDescriptors(fileDescriptorSets)

    val env = buildCelEnvironment(fileDescriptors)
    val typeRegistry: TypeRegistry = buildTypeRegistry(fileDescriptors)

    return CelEnvProvider.TypeRegistryAndEnv(typeRegistry, env)
  }

  private fun buildCelEnvironment(
    descriptors: List<Descriptors.Descriptor>,
  ): Env {
    // Build CEL ProtoTypeRegistry.
    val celTypeRegistry = ProtoTypeRegistry.newRegistry()
    descriptors.forEach { celTypeRegistry.registerDescriptor(it.file) }

    celTypeRegistry.registerMessage(EventGroup.getDefaultInstance())

    // Build CEL Env.
    val eventGroupDescriptor = EventGroup.getDescriptor()
    val env =
      Env.newEnv(
        EnvOption.container(eventGroupDescriptor.fullName),
        EnvOption.customTypeProvider(celTypeRegistry),
        EnvOption.customTypeAdapter(celTypeRegistry),
        EnvOption.declarations(
          eventGroupDescriptor.fields
            .map {
              Decls.newVar(
                it.name,
                celTypeRegistry.findFieldType(eventGroupDescriptor.fullName, it.name).type
              )
            }
            // TODO(projectnessie/cel-java#295): Remove when fixed.
            .plus(Decls.newVar(METADATA_FIELD, Checked.checkedAny))
        )
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

      while (response.nextPageToken.isNotBlank()) {
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
      when (e.status.code) {
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED -> {
          throw RetriableException()
        }
        else -> {
          throw e
        }
      }
    }
  }

  private fun buildTypeRegistry(
    descriptors: List<Descriptors.Descriptor>,
  ): TypeRegistry {
    return TypeRegistry.newBuilder()
      .apply {
        for (descriptor in descriptors) {
          add(descriptor)
        }
      }
      .build()
  }

  private class RetriableException : Exception()

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
