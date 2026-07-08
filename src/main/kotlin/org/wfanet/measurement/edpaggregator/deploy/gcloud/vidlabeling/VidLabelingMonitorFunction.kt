/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingMonitor
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.StorageClient

/**
 * Cloud Function that drives VID labeling dispatch sequencing and monitors pipeline health.
 *
 * Invoked by two Cloud Scheduler jobs against this one endpoint, selected by the `mode` query
 * parameter (anything else is rejected with HTTP 400):
 * - `?mode=dispatch` on a fast cadence (~6h) runs [VidLabelingMonitor.runDispatch] only, so a
 *   newly-registered upload starts promptly.
 * - `?mode=health` on a slow cadence (~daily, matching the DataAvailabilityMonitor) runs
 *   [VidLabelingMonitor.runHealth] only: staleness/failure monitoring, stuck-phase recovery, and
 *   data-quality checks. Recovery wants a slow, well-spaced clock so it never races an in-flight
 *   last-out.
 *
 * For each configured [VidLabelingConfig] it builds one [VidLabelingMonitor] keyed on the config's
 * `DataProvider` and runs the selected cadence.
 *
 * Unlike [VidLabelingDispatcherFunction], this function is not triggered by a DataWatcher "done"
 * blob. It lists uploads through the `RawImpressionUploadService` metadata API. In `dispatch` mode
 * it reads no storage; in `health` mode the data-quality checks additionally crawl the
 * raw-impression and labeled-impression storage buckets to reconcile metadata against storage.
 *
 * ## Environment Variables
 * - `EDPA_CONFIG_STORAGE_BUCKET`: Required. URI prefix for the config storage bucket.
 * - `CONFIG_BLOB_KEY`: Required. Blob key for the [VidLabelingConfigs] textproto.
 * - `CONTROL_PLANE_TARGET`: Required. Target endpoint for the Secure Computation control plane.
 * - `CONTROL_PLANE_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_LINES_TARGET`: Required. Target endpoint for the VID Repository ModelLines service (used
 *   to resolve the active window for memoized model lines).
 * - `MODEL_LINES_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_ROLLOUTS_TARGET`: Required. Target endpoint for the VID Repository ModelRollouts
 *   service.
 * - `MODEL_ROLLOUTS_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_SHARDS_TARGET`: Required. Target endpoint for the VID Repository ModelShards service.
 * - `MODEL_SHARDS_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `RAW_IMPRESSION_UPLOAD_TARGET`: Required. Target endpoint for the `RawImpressionUploadService`,
 *   `RawImpressionUploadModelLineService`, and `PoolAssignmentJobService`.
 * - `RAW_IMPRESSION_UPLOAD_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `VID_LABELER_QUEUE_NAME`: Required. Resource name of the Phase-2 VidLabeler Secure Computation
 *   queue.
 * - `POOL_ASSIGNER_QUEUE_NAME`: Required. Resource name of the Phase-0 SubpoolAssigner Secure
 *   Computation queue (memoized model lines).
 * - `CHANNEL_SHUTDOWN_DURATION_SECONDS`: Optional. gRPC channel shutdown timeout (default: 3s).
 *
 * The staleness threshold (after which a non-terminal upload is flagged as stuck) is set per
 * `DataProvider` via the `staleness_threshold` field on each [VidLabelingConfig].
 *
 * Configuration is loaded eagerly at class initialization via [runBlocking]. If the config blob is
 * unavailable at startup, the Cloud Function class will fail to load (fail-fast).
 */
/** Cadence the [VidLabelingMonitorFunction] runs, selected by the `mode` query parameter. */
enum class MonitorMode {
  DISPATCH,
  HEALTH,
}

/**
 * Maps the `mode` query parameter to a [MonitorMode], or null if it is missing or unrecognized (the
 * function rejects that with HTTP 400).
 */
fun parseMonitorMode(rawMode: String?): MonitorMode? =
  when (rawMode) {
    "dispatch" -> MonitorMode.DISPATCH
    "health" -> MonitorMode.HEALTH
    else -> null
  }

class VidLabelingMonitorFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      val mode: MonitorMode? = parseMonitorMode(request.getFirstQueryParameter("mode").orElse(null))
      if (mode == null) {
        logger.warning("Rejecting request with missing or invalid 'mode' query parameter")
        response.setStatusCode(400)
        response.writer.write("Query parameter 'mode' must be 'dispatch' or 'health'.")
        return
      }
      logger.info("Starting VidLabelingMonitorFunction in $mode mode")

      // HttpFunction.service is synchronous; runBlocking bridges to suspend functions.
      // Each config is isolated: one DataProvider's failure (e.g. a config that fails the
      // require() checks, or a backend error) is logged and treated as an issue, but does not abort
      // monitoring for the other DataProviders.
      val hasAnyIssues = runBlocking {
        monitorConfigs.configsList
          .map { config ->
            try {
              runMonitorForConfig(config, mode)
            } catch (e: Exception) {
              logger.log(Level.SEVERE, "Monitor failed for ${config.dataProvider}", e)
              true
            }
          }
          .any { it }
      }

      // Always return 200 on a completed run. Pipeline issues (stuck/failed uploads, data-quality
      // signals, exhausted recovery, dispatch errors) are surfaced via OpenTelemetry metrics and
      // SEVERE logs, not the HTTP status: returning 500 for them makes Cloud Scheduler retry the
      // whole run (re-dispatch, re-run recovery). HTTP 500 is reserved for a genuine function
      // failure (the catch below). Mirrors DataAvailabilityMonitorFunction.
      response.setStatusCode(200)
      response.writer.write(
        if (hasAnyIssues) "VID labeling pipeline issues detected. See metrics and logs."
        else "All VID labeling uploads healthy."
      )
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error in VidLabelingMonitorFunction", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  /**
   * Builds a [VidLabelingMonitor] for a single [config]'s DataProvider and runs the [mode] cadence.
   *
   * @return `true` if this config had an issue: a dispatch failure in [MonitorMode.DISPATCH], or
   *   any health issue in [MonitorMode.HEALTH].
   */
  private suspend fun runMonitorForConfig(config: VidLabelingConfig, mode: MonitorMode): Boolean {
    require(config.numberOfShards > 0) {
      "number_of_shards must be positive for data provider: ${config.dataProvider}"
    }
    requireValidStalenessThreshold(config)
    // Fail fast on per-model-line config the TEE would otherwise only reject at Phase-2.
    requireValidModelLineConfigs(config)

    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)

    val workItemsStub =
      WorkItemsGrpcKt.WorkItemsCoroutineStub(
        VidLabelingFunctionHelpers.createInstrumentedChannel(
          config.controlPlaneConnection,
          controlPlaneTarget,
          controlPlaneCertHost,
          grpcTelemetry,
        )
      )

    val modelRolloutsStub =
      ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(
        VidLabelingFunctionHelpers.createInstrumentedChannel(
          config.modelRolloutsConnection,
          modelRolloutsTarget,
          modelRolloutsCertHost,
          grpcTelemetry,
        )
      )

    val modelShardsStub =
      ModelShardsGrpcKt.ModelShardsCoroutineStub(
        VidLabelingFunctionHelpers.createInstrumentedChannel(
          config.modelShardsConnection,
          modelShardsTarget,
          modelShardsCertHost,
          grpcTelemetry,
        )
      )

    val modelLinesStub =
      ModelLinesGrpcKt.ModelLinesCoroutineStub(
        VidLabelingFunctionHelpers.createInstrumentedChannel(
          config.modelLinesConnection,
          modelLinesTarget,
          modelLinesCertHost,
          grpcTelemetry,
        )
      )

    val rawImpressionUploadChannel =
      VidLabelingFunctionHelpers.createInstrumentedChannel(
        config.rawImpressionMetadataStorageConnection,
        rawImpressionUploadTarget,
        rawImpressionUploadCertHost,
        grpcTelemetry,
      )
    val rawImpressionUploadStub =
      RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub(
        rawImpressionUploadChannel
      )
    val rawImpressionUploadModelLineStub =
      RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub(
        rawImpressionUploadChannel
      )
    // PoolAssignmentJobService is served by the same RawImpressionMetadata storage deployment.
    val poolAssignmentJobStub =
      PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub(
        rawImpressionUploadChannel
      )
    val rawImpressionUploadFileStub =
      RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
        rawImpressionUploadChannel
      )
    // VidLabelingJobService is served by the same RawImpressionMetadata storage deployment.
    val vidLabelingJobStub =
      VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(rawImpressionUploadChannel)
    // RankerJobService is served by the same RawImpressionMetadata storage deployment.
    val rankerJobStub =
      RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub(rawImpressionUploadChannel)
    val dispatchSequencer =
      VidLabelingDispatchSequencer(
        rawImpressionUploadStub = rawImpressionUploadStub,
        rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
        workItemsStub = workItemsStub,
        poolAssignmentJobStub = poolAssignmentJobStub,
        modelRolloutsStub = modelRolloutsStub,
        modelShardsStub = modelShardsStub,
        modelLinesStub = modelLinesStub,
        dataProviderName = config.dataProvider,
        vidLabelerParamsTemplate = VidLabelingFunctionHelpers.buildVidLabelerParamsTemplate(config),
        subpoolAssignerParamsTemplate =
          VidLabelingFunctionHelpers.buildSubpoolAssignerParamsTemplate(config),
        queueName = vidLabelerQueueName,
        poolAssignerQueueName = poolAssignerQueueName,
        numberOfShards = 32 /* BENCH: hardcoded Phase-0 shards */,
        modelLineConfigs =
          VidLabelingFunctionHelpers.convertModelLineConfigs(config.modelLineConfigsMap),
        rawImpressionUploadFileStub = rawImpressionUploadFileStub,
        vidLabelingJobStub = vidLabelingJobStub,
        maxFileBatchSizeBytes = config.maxFileBatchSizeBytes,
      )

    val monitor =
      VidLabelingMonitor(
        rawImpressionUploadStub = rawImpressionUploadStub,
        rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
        dispatchSequencer = dispatchSequencer,
        dataProviderName = config.dataProvider,
        stalenessThreshold = config.stalenessThreshold.toDuration(),
        rawImpressionUploadFileStub = rawImpressionUploadFileStub,
        rawImpressionsStorageClientProvider = {
          createStorageClient(
            config.rawImpressionsStorageParams.gcs.bucketName,
            config.rawImpressionsStorageParams.gcs.projectId,
          )
        },
        vidLabeledImpressionsStorageClientProvider = {
          createStorageClient(
            config.vidLabeledImpressionsStorageParams.gcs.bucketName,
            config.vidLabeledImpressionsStorageParams.gcs.projectId,
          )
        },
        rankerJobStub = rankerJobStub,
        vidLabelingJobStub = vidLabelingJobStub,
        workItemsStub = workItemsStub,
      )

    return when (mode) {
      MonitorMode.DISPATCH -> {
        val result = monitor.runDispatch()
        if (result.dispatchedUpload != null) {
          logger.info("Dispatched ${result.dispatchedUpload} for ${config.dataProvider}")
        }
        result.dispatchError
      }
      MonitorMode.HEALTH -> monitor.runHealth().hasIssues
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private val controlPlaneTarget: String = EnvVars.checkNotNullOrEmpty("CONTROL_PLANE_TARGET")
    private val controlPlaneCertHost: String? = System.getenv("CONTROL_PLANE_CERT_HOST")
    private val modelLinesTarget: String = EnvVars.checkNotNullOrEmpty("MODEL_LINES_TARGET")
    private val modelLinesCertHost: String? = System.getenv("MODEL_LINES_CERT_HOST")
    private val modelRolloutsTarget: String = EnvVars.checkNotNullOrEmpty("MODEL_ROLLOUTS_TARGET")
    private val modelRolloutsCertHost: String? = System.getenv("MODEL_ROLLOUTS_CERT_HOST")
    private val modelShardsTarget: String = EnvVars.checkNotNullOrEmpty("MODEL_SHARDS_TARGET")
    private val modelShardsCertHost: String? = System.getenv("MODEL_SHARDS_CERT_HOST")
    private val rawImpressionUploadTarget: String =
      EnvVars.checkNotNullOrEmpty("RAW_IMPRESSION_UPLOAD_TARGET")
    private val rawImpressionUploadCertHost: String? =
      System.getenv("RAW_IMPRESSION_UPLOAD_CERT_HOST")
    // TODO(world-federation-of-advertisers/cross-media-measurement#4108): Support per-EDP queue
    //   routing by sourcing the queue name from the per-DataProvider VidLabelingConfig instead of
    //   this single process-wide env var.
    private val vidLabelerQueueName: String = EnvVars.checkNotNullOrEmpty("VID_LABELER_QUEUE_NAME")
    private val poolAssignerQueueName: String =
      EnvVars.checkNotNullOrEmpty("POOL_ASSIGNER_QUEUE_NAME")

    private val configBlobKey: String = EnvVars.checkNotNullOrEmpty("CONFIG_BLOB_KEY")

    /**
     * Eagerly loaded at class init via [runBlocking]. Cloud Functions initialize the class on the
     * framework's classloading thread before serving the first request, making this effectively the
     * application startup phase. If the config blob is unavailable, the class will fail to load
     * (fail-fast), causing the Cloud Function to reject all requests with a load error.
     */
    private val monitorConfigs: VidLabelingConfigs = runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        VidLabelingConfigs.getDefaultInstance(),
      )
    }

    /** Builds a bucket-rooted [StorageClient] for the data-quality crawl. */
    private fun createStorageClient(bucketName: String, projectId: String): StorageClient {
      return GcsStorageClient(
        StorageOptions.newBuilder()
          .also { builder ->
            if (projectId.isNotEmpty()) {
              builder.setProjectId(projectId)
            }
          }
          .build()
          .service,
        bucketName,
      )
    }
  }
}
