/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.dataavailability

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import io.grpc.ClientInterceptors
import io.grpc.Status
import io.grpc.StatusException
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.net.URI
import java.time.Clock
import java.time.Duration
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.TimeUnit
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.delay
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.edpaggregator.ConfigLoader
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilityCleanup
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilityCleanupParams
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemPublicationCodec
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.renewWorkItemAttemptRequest

/**
 * Marks impression metadata deleted after storage deletion or archive notifications.
 *
 * Queue deliveries use the standard WorkItem attempt lifecycle. Requests with `X-DataWatcher-Path`
 * retain the legacy HTTP contract during rollout. Both paths read current metadata and check
 * storage before conditionally marking the metadata deleted.
 *
 * `THROTTLER_MILLIS` controls the minimum interval between metadata RPCs per instance.
 * `CLEANUP_RPC_TIMEOUT_SECONDS` bounds metadata API work for each delivery. WorkItem processing
 * additionally requires the Secure Computation connection in the provider configuration and
 * `SECURE_COMPUTATION_CONTROL_PLANE_TARGET`.
 */
class DataAvailabilityCleanupFunction
internal constructor(
  private val configs: List<DataAvailabilitySyncConfig>,
  private val cleanupFactory: (DataAvailabilitySyncConfig) -> DataAvailabilityCleanup,
  private val controlPlaneStubs:
    (DataAvailabilitySyncConfig) -> Pair<WorkItemsCoroutineStub, WorkItemAttemptsCoroutineStub>,
  private val renewalDelay: suspend () -> Unit,
  private val writebackRetryDelay: suspend (Int) -> Unit,
) : HttpFunction {
  /** Constructs the deployed function using committed runtime configuration. */
  constructor() :
    this(
      runtimeConfigs.configsList,
      ::createCleanup,
      ::createControlPlaneStubs,
      { delay(60_000L) },
      { attempt -> delay(writebackBackoff.durationForAttempt(attempt).toMillis()) },
    ) {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      Tracing.withW3CTraceContext(request) {
        runBlocking(Context.current().asContextElement()) {
          val path = request.getFirstHeader(DATA_WATCHER_PATH_HEADER).orElse(null)
          if (path != null) {
            val config =
              ConfigLoader.buildDataAvailabilitySyncConfig(request.reader.readText(), configs)
            cleanupFactory(config)
              .cleanup(
                path,
                request.getFirstHeader(IMPRESSION_METADATA_RESOURCE_ID_HEADER).orElse(null),
              )
          } else {
            process(WorkItemPublicationCodec.parse(request.inputStream))
          }
        }
      }
      response.setStatusCode(200)
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Metadata cleanup delivery failed", e)
      response.setStatusCode(500)
    } finally {
      EdpaTelemetry.flush()
    }
  }

  private suspend fun process(item: WorkItem) {
    require(
      item.name.startsWith("workItems/") && item.name.removePrefix("workItems/").isNotBlank()
    ) {
      "WorkItem name is required"
    }
    require(item.generation >= 0L) { "WorkItem generation must not be negative" }
    val params = item.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    val appParams = params.appParams.unpack(DataAvailabilityCleanupParams::class.java)
    val config = configs.single { it.dataProvider == appParams.dataProvider }
    val (items, attempts) = controlPlaneStubs(config)
    val generation = item.generation.takeUnless { it == 0L } ?: 1L
    val attempt =
      try {
        attempts
          .withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
          .createWorkItemAttempt(
            createWorkItemAttemptRequest {
              parent = item.name
              workItemAttemptId = "cleanup-" + UUID.randomUUID().toString()
              expectedWorkItemGeneration = generation
              supportsAttemptLease = true
            }
          )
      } catch (e: StatusException) {
        val reason = e.errorInfo?.reason
        val state = e.errorInfo?.metadataMap?.get("workItemState")
        if (
          reason == "WORK_ITEM_GENERATION_MISMATCH" ||
            reason == "WORK_ITEM_NOT_FOUND" ||
            reason == "INVALID_WORK_ITEM_STATE" && state in setOf("SUCCEEDED", "FAILED")
        ) {
          if (state == "FAILED") {
            terminalFailures.add(1)
            logger.warning("Cleanup WorkItem requires operator retry: ${item.name}")
          }
          return
        }
        throw e
      }

    withContext(
      Tracing.withW3CTraceContext(params.traceContextMap) { Context.current() }.asContextElement()
    ) {
      try {
        withAttemptLease(attempt, attempts) {
          requireNotNull(DataProviderKey.fromName(appParams.dataProvider)) {
            "data_provider must be a DataProvider resource name"
          }
          val source = params.dataPathParams
          require(
            source.hasGeneration() && source.generation > 0L ||
              !source.hasGeneration() && source.sourceEventId.isNotBlank()
          ) {
            "Source generation or legacy event identity is required"
          }
          val uri = URI.create(source.dataPath)
          require(
            uri.scheme == "gs" &&
              uri.authority == config.dataAvailabilityStorage.gcs.bucketName &&
              !uri.path.isNullOrBlank() &&
              uri.path != "/" &&
              uri.query == null &&
              uri.fragment == null
          ) {
            "Source path must belong to the configured storage bucket"
          }
          require(
            source.eventType == WorkItem.WorkItemParams.DataPathParams.StorageEventType.DELETED
          ) {
            "Cleanup requires a deletion event"
          }
          val resourceName = source.objectMetadataMap["impression-metadata-resource-id"]
          if (resourceName != null) {
            require(resourceName.startsWith(appParams.dataProvider + "/impressionMetadata/")) {
              "Metadata must belong to the configured provider"
            }
          }
          cleanupFactory(config).cleanup(source.dataPath, resourceName)
          finishAttempt(WorkItemAttempt.State.SUCCEEDED) {
            attempts
              .withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
              .completeWorkItemAttempt(completeWorkItemAttemptRequest { name = attempt.name })
          }
        }
      } catch (e: CancellationException) {
        throw e
      } catch (e: Exception) {
        try {
          finishAttempt(WorkItemAttempt.State.FAILED) {
            attempts
              .withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
              .failWorkItemAttempt(
                failWorkItemAttemptRequest {
                  name = attempt.name
                  errorMessage = e::class.java.simpleName
                }
              )
          }
          if (e is IllegalArgumentException) {
            writeback {
              items
                .withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
                .failWorkItem(
                  failWorkItemRequest {
                    name = item.name
                    expectedWorkItemGeneration = generation
                  }
                )
            }
            terminalFailures.add(1)
            logger.warning("Cleanup WorkItem requires operator retry: ${item.name}")
            return@withContext
          }
        } catch (writebackError: Exception) {
          e.addSuppressed(writebackError)
        }
        throw e
      }
    }
  }

  private suspend fun withAttemptLease(
    attempt: WorkItemAttempt,
    attempts: WorkItemAttemptsCoroutineStub,
    block: suspend () -> Unit,
  ) = coroutineScope {
    val renewal =
      if (attempt.hasLeaseExpirationTime()) {
        launch(Dispatchers.IO) {
          while (isActive) {
            renewalDelay()
            try {
              writeback {
                attempts
                  .withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS)
                  .renewWorkItemAttempt(renewWorkItemAttemptRequest { name = attempt.name })
              }
            } catch (e: StatusException) {
              if (
                e.errorInfo?.reason == "INVALID_WORK_ITEM_ATTEMPT_STATE" &&
                  e.errorInfo?.metadataMap?.get("workItemAttemptState") == "SUCCEEDED"
              )
                return@launch
              throw e
            }
          }
        }
      } else null
    try {
      block()
    } finally {
      renewal?.cancelAndJoin()
    }
  }

  private suspend fun <T> writeback(block: suspend () -> T): T {
    for (attempt in 1..3) {
      try {
        return block()
      } catch (e: StatusException) {
        if (attempt == 3 || e.status.code !in RETRYABLE_CODES) throw e
        writebackRetryDelay(attempt)
      }
    }
    error("Unreachable")
  }

  private suspend fun finishAttempt(
    desiredState: WorkItemAttempt.State,
    block: suspend () -> WorkItemAttempt,
  ) {
    try {
      writeback(block)
    } catch (e: StatusException) {
      if (
        e.errorInfo?.reason != "INVALID_WORK_ITEM_ATTEMPT_STATE" ||
          e.errorInfo?.metadataMap?.get("workItemAttemptState") != desiredState.name
      )
        throw e
    }
  }

  companion object {
    private val logger = Logger.getLogger(DataAvailabilityCleanupFunction::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER = "X-DataWatcher-Path"
    private const val IMPRESSION_METADATA_RESOURCE_ID_HEADER = "X-Impression-Metadata-Resource-Id"
    private val RETRYABLE_CODES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
        Status.Code.UNAVAILABLE,
      )
    private val writebackBackoff = ExponentialBackoff()
    private val terminalFailures by lazy {
      Instrumentation.meter
        .counterBuilder("edpa.data_availability.cleanup_work_item_failures")
        .setDescription("Cleanup WorkItems requiring operator retry")
        .build()
    }
    private val rpcTimeoutSeconds by lazy {
      val seconds = System.getenv("CLEANUP_RPC_TIMEOUT_SECONDS")?.toLong() ?: 30L
      require(seconds > 0L) { "CLEANUP_RPC_TIMEOUT_SECONDS must be positive" }
      seconds
    }
    private val runtimeConfigs by lazy {
      runBlocking {
        EdpAggregatorConfig.getConfigAsProtoMessage(
          requireNotNull(System.getenv("CONFIG_BLOB_KEY")),
          DataAvailabilitySyncConfigs.getDefaultInstance(),
        )
      }
    }
    private val metadataThrottler by lazy {
      val millis = System.getenv("THROTTLER_MILLIS")?.toLong() ?: 250L
      require(millis >= 0L) { "THROTTLER_MILLIS must not be negative" }
      MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofMillis(millis))
    }
    private val storageClients = ConcurrentHashMap<DataAvailabilitySyncConfig, GcsStorageClient>()

    private fun createCleanup(config: DataAvailabilitySyncConfig): DataAvailabilityCleanup {
      val telemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
      val channel =
        ClientInterceptors.intercept(
          DataAvailabilitySyncFunction.getOrCreateSharedChannels(config).impressionMetadataChannel,
          telemetry.newClientInterceptor(),
        )
      return DataAvailabilityCleanup(
        ImpressionMetadataServiceCoroutineStub(channel)
          .withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS),
        config.dataProvider,
        storageClients.computeIfAbsent(config) {
          val gcs = it.dataAvailabilityStorage.gcs
          GcsStorageClient(
            StorageOptions.newBuilder()
              .apply { if (gcs.projectId.isNotEmpty()) setProjectId(gcs.projectId) }
              .build()
              .service,
            gcs.bucketName,
          )
        },
        metadataThrottler,
      )
    }

    private fun createControlPlaneStubs(
      config: DataAvailabilitySyncConfig
    ): Pair<WorkItemsCoroutineStub, WorkItemAttemptsCoroutineStub> {
      val telemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
      val channel =
        ClientInterceptors.intercept(
          DataAvailabilitySyncFunction.getOrCreateSecureComputationChannel(config),
          telemetry.newClientInterceptor(),
        )
      return Pair(WorkItemsCoroutineStub(channel), WorkItemAttemptsCoroutineStub(channel))
    }
  }
}
