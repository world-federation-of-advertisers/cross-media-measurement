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
import com.google.protobuf.util.JsonFormat
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatcherParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * Cloud Function that registers VID labeling uploads in the EDP Aggregator metadata store.
 *
 * Invoked when an EDP finishes uploading raw impressions and writes a "done" blob. The function
 * looks up the matching [VidLabelingConfig] for the data provider, resolves active model lines via
 * the VID Repository API, crawls the directory for raw impression files, and creates
 * `RawImpressionUpload`, `RawImpressionUploadFile`, and `RawImpressionUploadModelLine` records. It
 * then calls the shared [VidLabelingDispatchSequencer] to aggressively start pipeline work (the
 * "fast path") rather than waiting for the next `VidLabelingMonitorFunction` tick.
 *
 * ## Environment Variables
 * - `EDPA_CONFIG_STORAGE_BUCKET`: Required. URI prefix for the config storage bucket.
 * - `CONFIG_BLOB_KEY`: Required. Blob key for the [VidLabelingConfigs] textproto.
 * - `MODEL_LINES_TARGET`: Required. Target endpoint for the VID Repository ModelLines service.
 * - `MODEL_LINES_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_ROLLOUTS_TARGET`: Required. Target endpoint for the VID Repository ModelRollouts
 *   service.
 * - `MODEL_ROLLOUTS_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `MODEL_SHARDS_TARGET`: Required. Target endpoint for the VID Repository ModelShards service.
 * - `MODEL_SHARDS_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `RAW_IMPRESSION_UPLOAD_TARGET`: Required. Target endpoint for the `RawImpressionUploadService`,
 *   `RawImpressionUploadModelLineService`, and `PoolAssignmentJobService`.
 * - `RAW_IMPRESSION_UPLOAD_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `CONTROL_PLANE_TARGET`: Required. Target endpoint for the Secure Computation control plane
 *   (`WorkItemsService`).
 * - `CONTROL_PLANE_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `VID_LABELER_QUEUE_NAME`: Required. Resource name of the Phase-2 VidLabeler Secure Computation
 *   queue.
 * - `POOL_ASSIGNER_QUEUE_NAME`: Required. Resource name of the Phase-0 SubpoolAssigner Secure
 *   Computation queue (memoized model lines).
 * - `CHANNEL_SHUTDOWN_DURATION_SECONDS`: Optional. gRPC channel shutdown timeout (default: 3s).
 * - `VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH`: Optional. Enables [FileSystemStorageClient] instead
 *   of GCS. Used only in testing.
 *
 * ## Request Headers
 * - `X-DataWatcher-Path`: Required. Full storage URI of the "done" blob.
 * - `X-DataWatcher-Generation`: Required. GCS object generation number of the "done" blob.
 * - `X-Override-Model-Lines`: Optional. Comma-separated list of model line resource names to use
 *   instead of querying the VID Repository API. Supports backfilling past data where the model line
 *   may no longer be in the active window.
 */
class VidLabelingDispatcherFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting VidLabelingDispatcherFunction")

      val dispatcherParams =
        VidLabelingDispatcherParams.newBuilder()
          .apply { JsonFormat.parser().merge(request.reader, this) }
          .build()

      val doneBlobPath =
        request.getFirstHeader(DATA_WATCHER_PATH_HEADER).orElseThrow {
          IllegalArgumentException("Missing required header: $DATA_WATCHER_PATH_HEADER")
        }

      val doneBlobGeneration: Long =
        request
          .getFirstHeader(DATA_WATCHER_GENERATION_HEADER)
          .map { it.toLong() }
          .orElseThrow {
            IllegalArgumentException("Missing required header: $DATA_WATCHER_GENERATION_HEADER")
          }

      val overrideModelLines: List<String> =
        request
          .getFirstHeader(OVERRIDE_MODEL_LINES_HEADER)
          .map { header -> header.split(",").map { it.trim() }.filter { it.isNotEmpty() } }
          .orElse(emptyList())

      val config: VidLabelingConfig =
        vidLabelingConfigsByDataProvider[dispatcherParams.dataProvider]
          ?: throw IllegalArgumentException(
            "No VidLabelingConfig found for data provider: ${dispatcherParams.dataProvider}"
          )

      val storageClient: StorageClient = createStorageClient(doneBlobPath)
      val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)

      val modelLinesStub =
        ModelLinesGrpcKt.ModelLinesCoroutineStub(
          VidLabelingFunctionHelpers.createInstrumentedChannel(
            config.modelLinesConnection,
            modelLinesTarget,
            modelLinesCertHost,
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
      val rawImpressionUploadFilesStub =
        RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub(
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
      // VidLabelingJobService is served by the same RawImpressionMetadata storage deployment.
      val vidLabelingJobStub =
        VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub(rawImpressionUploadChannel)
      val workItemsStub =
        WorkItemsGrpcKt.WorkItemsCoroutineStub(
          VidLabelingFunctionHelpers.createInstrumentedChannel(
            config.controlPlaneConnection,
            controlPlaneTarget,
            controlPlaneCertHost,
            grpcTelemetry,
          )
        )

      require(config.numberOfShards > 0) {
        "number_of_shards must be positive for data provider: ${config.dataProvider}"
      }
      // Fail fast on per-model-line config the TEE would otherwise only reject at Phase-2.
      requireValidModelLineConfigs(config)
      val modelLineConfigs =
        VidLabelingFunctionHelpers.convertModelLineConfigs(config.modelLineConfigsMap)

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
          vidLabelerParamsTemplate =
            VidLabelingFunctionHelpers.buildVidLabelerParamsTemplate(config),
          subpoolAssignerParamsTemplate =
            VidLabelingFunctionHelpers.buildSubpoolAssignerParamsTemplate(config),
          queueName = vidLabelerQueueName,
          poolAssignerQueueName = poolAssignerQueueName,
          numberOfShards = config.numberOfShards,
          modelLineConfigs = modelLineConfigs,
          rawImpressionUploadFileStub = rawImpressionUploadFilesStub,
          vidLabelingJobStub = vidLabelingJobStub,
          maxFileBatchSizeBytes = config.maxFileBatchSizeBytes,
        )

      val dispatcher =
        VidLabelingDispatcher(
          storageClient = storageClient,
          rawImpressionUploadStub = rawImpressionUploadStub,
          rawImpressionUploadFilesStub = rawImpressionUploadFilesStub,
          rawImpressionUploadModelLineStub = rawImpressionUploadModelLineStub,
          modelLinesStub = modelLinesStub,
          dispatchSequencer = dispatchSequencer,
          dataProviderName = config.dataProvider,
          modelSuiteName = config.modelSuite,
          overrideModelLines = overrideModelLines,
          modelLineConfigs = modelLineConfigs,
        )

      Tracing.withW3CTraceContext(request) {
        runBlocking(Context.current().asContextElement()) {
          dispatcher.upload(doneBlobPath, doneBlobGeneration)
        }
      }
    } catch (e: IllegalArgumentException) {
      // Bad request: missing/invalid headers, malformed params, or an unknown data provider. These
      // are caller errors, so return 4xx rather than letting the framework surface them as 500.
      logger.log(Level.WARNING, "Rejecting request as bad input", e)
      response.setStatusCode(400)
      response.writer.write(e.message ?: "Bad request")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DATA_WATCHER_PATH_HEADER: String = "X-DataWatcher-Path"
    private const val DATA_WATCHER_GENERATION_HEADER: String = "X-DataWatcher-Generation"
    private const val OVERRIDE_MODEL_LINES_HEADER: String = "X-Override-Model-Lines"
    private const val GOOGLE_PROJECT_ID_ENV = "GOOGLE_PROJECT_ID"

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
    private val controlPlaneTarget: String = EnvVars.checkNotNullOrEmpty("CONTROL_PLANE_TARGET")
    private val controlPlaneCertHost: String? = System.getenv("CONTROL_PLANE_CERT_HOST")
    private val vidLabelerQueueName: String = EnvVars.checkNotNullOrEmpty("VID_LABELER_QUEUE_NAME")
    private val poolAssignerQueueName: String =
      EnvVars.checkNotNullOrEmpty("POOL_ASSIGNER_QUEUE_NAME")

    private val fileSystemPath: String? = System.getenv("VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH")

    private val configBlobKey: String = EnvVars.checkNotNullOrEmpty("CONFIG_BLOB_KEY")

    private val vidLabelingConfigs: VidLabelingConfigs by lazy {
      runBlocking {
        EdpAggregatorConfig.getConfigAsProtoMessage(
          configBlobKey,
          VidLabelingConfigs.getDefaultInstance(),
        )
      }
    }

    private val vidLabelingConfigsByDataProvider: Map<String, VidLabelingConfig> by lazy {
      vidLabelingConfigs.configsList.associateBy { it.dataProvider }
    }

    private fun createStorageClient(doneBlobPath: String): StorageClient {
      return if (!fileSystemPath.isNullOrEmpty()) {
        FileSystemStorageClient(
          File(EnvVars.checkIsPath("VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH"))
        )
      } else {
        val doneBlobUri = SelectedStorageClient.parseBlobUri(doneBlobPath)
        GcsStorageClient(
          StorageOptions.newBuilder()
            .also { builder ->
              val projectId = System.getenv(GOOGLE_PROJECT_ID_ENV)
              if (!projectId.isNullOrEmpty()) {
                builder.setProjectId(projectId)
              }
            }
            .build()
            .service,
          doneBlobUri.bucket,
        )
      }
    }
  }
}
