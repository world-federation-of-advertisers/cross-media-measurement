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

import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import io.grpc.ClientInterceptors
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
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.ExponentialBackoff
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
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
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemCloudFunction
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemInvocation
import org.wfanet.measurement.securecomputation.deploy.gcloud.workitems.WorkItemRunner

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
) : WorkItemCloudFunction() {
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

  override fun isWorkItemRequest(request: HttpRequest): Boolean =
    !request.getFirstHeader(DATA_WATCHER_PATH_HEADER).isPresent

  override fun serviceLegacyRequest(request: HttpRequest, response: HttpResponse) {
    val config = ConfigLoader.buildDataAvailabilitySyncConfig(request.reader.readText(), configs)
    Tracing.withW3CTraceContext(request) {
      runBlocking(Context.current().asContextElement()) {
        cleanupFactory(config)
          .cleanup(
            request.getFirstHeader(DATA_WATCHER_PATH_HEADER).get(),
            request.getFirstHeader(IMPRESSION_METADATA_RESOURCE_ID_HEADER).orElse(null),
          )
      }
    }
    response.setStatusCode(200)
  }

  override fun handleFailure(error: Exception, response: HttpResponse) {
    logger.log(Level.WARNING, "Metadata cleanup delivery failed", error)
    response.setStatusCode(500)
  }

  override fun flushTelemetry() {
    EdpaTelemetry.flush()
  }

  override suspend fun createWorkItemInvocation(workItem: WorkItem): WorkItemInvocation {
    require(
      workItem.name.startsWith("workItems/") &&
        workItem.name.removePrefix("workItems/").isNotEmpty()
    ) {
      "WorkItem name is required"
    }
    require(workItem.generation >= 0L) { "WorkItem generation must not be negative" }
    val params = workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    val appParams = params.appParams.unpack(DataAvailabilityCleanupParams::class.java)
    val config = configs.single { it.dataProvider == appParams.dataProvider }
    val (items, attempts) = controlPlaneStubs(config)
    val item = workItem.copy { generation = workItem.generation.takeUnless { it == 0L } ?: 1L }
    val runner =
      WorkItemRunner(
        items.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS),
        attempts.withDeadlineAfter(rpcTimeoutSeconds, TimeUnit.SECONDS),
        attemptIdGenerator = { "cleanup-" + UUID.randomUUID().toString() },
        // Redeliver while another attempt owns this WorkItem.
        activeAttemptDelay = { error("Cleanup WorkItem already running") },
        renewalDelay = renewalDelay,
        updateRetryDelay = writebackRetryDelay,
        requireAttemptLease = false,
        terminalWorkItemStates = setOf(WorkItem.State.SUCCEEDED, WorkItem.State.FAILED),
      )
    val handler =
      object : WorkItemRunner.Handler() {
        override val workItem = item

        override suspend fun execute(attempt: WorkItemRunner.Attempt): WorkItemRunner.Action {
          requireNotNull(DataProviderKey.fromName(appParams.dataProvider)) {
            "data_provider must be a DataProvider resource name"
          }
          val source = params.dataPathParams
          require(!source.hasGeneration() || source.generation > 0L) {
            "Source generation must be positive when provided"
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
          cleanupFactory(config).cleanup(source.dataPath, null)
          return WorkItemRunner.Action.Complete
        }

        override fun failureAction(error: Exception): WorkItemRunner.Action? =
          if (error is IllegalArgumentException) WorkItemRunner.Action.Fail(errorMessage(error))
          else null

        override fun errorMessage(error: Exception): String = error::class.java.simpleName

        override fun onFinished(
          attempt: WorkItemRunner.Attempt,
          action: WorkItemRunner.Action,
          error: Exception?,
        ) {
          if (action is WorkItemRunner.Action.Fail) {
            terminalFailures.add(1)
            logger.warning("Cleanup WorkItem requires operator retry: ${item.name}")
          }
        }
      }
    return WorkItemInvocation(
      runner,
      handler,
      executeInContext = { block ->
        withContext(
          Tracing.withW3CTraceContext(params.traceContextMap) { Context.current() }
            .asContextElement()
        ) {
          block()
        }
      },
      close = {},
    )
  }

  companion object {
    private val logger = Logger.getLogger(DataAvailabilityCleanupFunction::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER = "X-DataWatcher-Path"
    private const val IMPRESSION_METADATA_RESOURCE_ID_HEADER = "X-Impression-Metadata-Resource-Id"
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
