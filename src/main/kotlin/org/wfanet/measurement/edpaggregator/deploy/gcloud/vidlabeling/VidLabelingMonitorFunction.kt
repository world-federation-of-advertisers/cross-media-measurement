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
import io.grpc.Channel
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelShardsGrpcKt
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams as ConfigTransportLayerSecurityParams
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.SubpoolAssignerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.subpoolAssignerParams
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatchSequencer
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingMonitor
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.StorageClient

/** Channel cache key using TLS params, target, and optional hostname override. */
private data class ChannelKey(
  val tls: ConfigTransportLayerSecurityParams,
  val target: String,
  val hostName: String?,
)

/**
 * Cloud Function that drives VID labeling dispatch sequencing and monitors pipeline health.
 *
 * Designed to be invoked on a schedule (e.g., every 30 minutes via Cloud Scheduler). For each
 * configured [VidLabelingConfig] it builds one [VidLabelingMonitor] keyed on the config's
 * `DataProvider` and runs it. Per DataProvider the monitor dispatches at most one `CREATED` upload
 * when none are `ACTIVE`, and logs uploads stuck past the staleness SLA and model lines in `FAILED`
 * at `SEVERE` for Cloud Monitoring alerting.
 *
 * Unlike [VidLabelingDispatcherFunction], this function is not triggered by a DataWatcher "done"
 * blob and does not read storage; it lists uploads through the `RawImpressionUploadService`
 * metadata API.
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
class VidLabelingMonitorFunction : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.info("Starting VidLabelingMonitorFunction")

      // HttpFunction.service is synchronous; runBlocking bridges to suspend functions.
      // Each config is isolated: one DataProvider's failure (e.g. a config that fails the
      // require() checks, or a backend error) is logged and treated as an issue, but does not abort
      // monitoring for the other DataProviders.
      val hasAnyIssues = runBlocking {
        monitorConfigs.configsList
          .map { config ->
            try {
              runMonitorForConfig(config)
            } catch (e: Exception) {
              logger.log(Level.SEVERE, "Monitor failed for ${config.dataProvider}", e)
              true
            }
          }
          .any { it }
      }

      if (hasAnyIssues) {
        response.setStatusCode(500)
        response.writer.write("VID labeling pipeline issues detected. See logs for details.")
      } else {
        response.setStatusCode(200)
        response.writer.write("All VID labeling uploads healthy.")
      }
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error in VidLabelingMonitorFunction", e)
      response.setStatusCode(500)
      response.writer.write("Internal error: ${e.message}")
    } finally {
      EdpaTelemetry.flush()
    }
  }

  /**
   * Builds and runs a [VidLabelingMonitor] for a single [config]'s DataProvider.
   *
   * @return `true` if the monitor reported stuck uploads or failed model lines for this config.
   */
  private suspend fun runMonitorForConfig(config: VidLabelingConfig): Boolean {
    require(config.numberOfShards > 0) {
      "number_of_shards must be positive for data provider: ${config.dataProvider}"
    }
    require(config.hasStalenessThreshold()) {
      "staleness_threshold must be set for data provider: ${config.dataProvider}"
    }
    // Fail fast on per-model-line config the TEE would otherwise only reject at Phase-2.
    requireValidModelLineConfigs(config)

    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)

    val workItemsStub =
      WorkItemsGrpcKt.WorkItemsCoroutineStub(
        createInstrumentedChannel(
          config.controlPlaneConnection,
          controlPlaneTarget,
          controlPlaneCertHost,
          grpcTelemetry,
        )
      )

    val modelRolloutsStub =
      ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(
        createInstrumentedChannel(
          config.modelRolloutsConnection,
          modelRolloutsTarget,
          modelRolloutsCertHost,
          grpcTelemetry,
        )
      )

    val modelShardsStub =
      ModelShardsGrpcKt.ModelShardsCoroutineStub(
        createInstrumentedChannel(
          config.modelShardsConnection,
          modelShardsTarget,
          modelShardsCertHost,
          grpcTelemetry,
        )
      )

    val modelLinesStub =
      ModelLinesGrpcKt.ModelLinesCoroutineStub(
        createInstrumentedChannel(
          config.modelLinesConnection,
          modelLinesTarget,
          modelLinesCertHost,
          grpcTelemetry,
        )
      )

    val rawImpressionUploadChannel =
      createInstrumentedChannel(
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
        vidLabelerParamsTemplate = buildVidLabelerParamsTemplate(config),
        subpoolAssignerParamsTemplate = buildSubpoolAssignerParamsTemplate(config),
        queueName = vidLabelerQueueName,
        poolAssignerQueueName = poolAssignerQueueName,
        numberOfShards = config.numberOfShards,
        modelLineConfigs = convertModelLineConfigs(config.modelLineConfigsMap),
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
        rawImpressionsStorageClient =
          createStorageClient(
            config.rawImpressionsStorageParams.gcs.bucketName,
            config.rawImpressionsStorageParams.gcs.projectId,
          ),
        vidLabeledImpressionsStorageClient =
          createStorageClient(
            config.vidLabeledImpressionsStorageParams.gcs.bucketName,
            config.vidLabeledImpressionsStorageParams.gcs.projectId,
          ),
        rankerJobStub = rankerJobStub,
        vidLabelingJobStub = vidLabelingJobStub,
        workItemsStub = workItemsStub,
      )

    val result = monitor.run()
    if (result.dispatchedUpload != null) {
      logger.info("Dispatched ${result.dispatchedUpload} for ${config.dataProvider}")
    }
    return result.hasIssues
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L

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
    private val channelShutdownDuration =
      Duration.ofSeconds(
        System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
      )

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

    private val channelCache = ConcurrentHashMap<ChannelKey, ManagedChannel>()

    private fun createPublicChannel(
      connectionParams: ConfigTransportLayerSecurityParams,
      target: String,
      hostName: String?,
    ): ManagedChannel {
      val signingCerts =
        SigningCerts.fromPemFiles(
          certificateFile = checkNotNull(File(connectionParams.certFilePath)),
          privateKeyFile = checkNotNull(File(connectionParams.privateKeyFilePath)),
          trustedCertCollectionFile = checkNotNull(File(connectionParams.certCollectionFilePath)),
        )
      return buildMutualTlsChannel(target, signingCerts, hostName)
        .withShutdownTimeout(channelShutdownDuration)
    }

    private fun getOrCreateChannel(
      connectionParams: ConfigTransportLayerSecurityParams,
      target: String,
      hostName: String?,
    ): ManagedChannel {
      val channelKey = ChannelKey(connectionParams, target, hostName)
      return channelCache.computeIfAbsent(channelKey) {
        logger.info("Creating new channel for $target")
        createPublicChannel(connectionParams, target, hostName)
      }
    }

    private fun createInstrumentedChannel(
      connectionParams: ConfigTransportLayerSecurityParams,
      target: String,
      hostName: String?,
      grpcTelemetry: GrpcTelemetry,
    ): Channel {
      val channel = getOrCreateChannel(connectionParams, target, hostName)
      return ClientInterceptors.intercept(channel, grpcTelemetry.newClientInterceptor())
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

    private fun buildVidLabelerParamsTemplate(config: VidLabelingConfig): VidLabelerParams {
      require(config.rawImpressionsStorageParams.hasGcs()) {
        "VidLabelingConfig raw_impressions_storage_params must use GCS"
      }
      require(config.vidLabeledImpressionsStorageParams.hasGcs()) {
        "VidLabelingConfig vid_labeled_impressions_storage_params must use GCS"
      }

      return vidLabelerParams {
        dataProvider = config.dataProvider
        vidLabeledImpressionsStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = config.vidLabeledImpressionsStorageParams.gcs.projectId
            impressionsBlobPrefix =
              "gs://${config.vidLabeledImpressionsStorageParams.gcs.bucketName}"
          }
        rawImpressionsStorageParams =
          VidLabelerParamsKt.storageParams {
            gcsProjectId = config.rawImpressionsStorageParams.gcs.projectId
            impressionsBlobPrefix = "gs://${config.rawImpressionsStorageParams.gcs.bucketName}"
          }
        vidRepoConnection = transportLayerSecurityParams {
          clientCertResourcePath = config.vidRepoConnection.certFilePath
          clientPrivateKeyResourcePath = config.vidRepoConnection.privateKeyFilePath
        }
        // The compiled model lives in its own Cloud Storage project. Optional on VidLabelingConfig
        // (only EDPs that actually label need it); when set, thread it onto every WorkItem so the
        // TEE reads the model from its own project on both the memoized and non-memoized paths.
        if (config.modelStorageParams.hasGcs()) {
          modelStorageParams =
            VidLabelerParamsKt.storageParams {
              gcsProjectId = config.modelStorageParams.gcs.projectId
              impressionsBlobPrefix = "gs://${config.modelStorageParams.gcs.bucketName}"
            }
        }
      }
    }

    // TODO(world-federation-of-advertisers/cross-media-measurement#4020): De-duplicate this
    // template builder (with buildVidLabelerParamsTemplate and convertModelLineConfigs) into a
    // shared helper once the helper-extraction thread on #4020 (this branch's parent) is
    // addressed, rather than copying it across the dispatcher and monitor Function classes.
    /**
     * Builds the template [SubpoolAssignerParams] carrying the storage + connection fields shared
     * by every memoized Phase-0 WorkItem. The per-shard fields (model line, shard index, active
     * window, pool assignment job) are filled in by the sequencer.
     */
    private fun buildSubpoolAssignerParamsTemplate(
      config: VidLabelingConfig
    ): SubpoolAssignerParams {
      require(config.rawImpressionsStorageParams.hasGcs()) {
        "VidLabelingConfig raw_impressions_storage_params must use GCS"
      }
      require(config.vidLabeledImpressionsStorageParams.hasGcs()) {
        "VidLabelingConfig vid_labeled_impressions_storage_params must use GCS"
      }
      // vid_rank_map/subpool_map storage are consumed only by the memoized Phase-0 path and are
      // therefore optional in VidLabelingConfig; validate them only when set. An EDP whose model
      // lines are all non-memoized may omit them, and this template is then never consumed.
      if (config.hasVidRankMapStorageParams()) {
        require(config.vidRankMapStorageParams.hasGcs()) {
          "VidLabelingConfig vid_rank_map_storage_params must use GCS"
        }
      }
      if (config.hasSubpoolMapStorageParams()) {
        require(config.subpoolMapStorageParams.hasGcs()) {
          "VidLabelingConfig subpool_map_storage_params must use GCS"
        }
      }
      if (config.hasModelStorageParams()) {
        require(config.modelStorageParams.hasGcs()) {
          "VidLabelingConfig model_storage_params must use GCS"
        }
      }

      return subpoolAssignerParams {
        dataProvider = config.dataProvider
        rawImpressionStorageParams =
          SubpoolAssignerParamsKt.storageParams {
            gcsProjectId = config.rawImpressionsStorageParams.gcs.projectId
            blobPrefix = "gs://${config.rawImpressionsStorageParams.gcs.bucketName}"
          }
        vidLabeledImpressionsStorageParams =
          SubpoolAssignerParamsKt.storageParams {
            gcsProjectId = config.vidLabeledImpressionsStorageParams.gcs.projectId
            blobPrefix = "gs://${config.vidLabeledImpressionsStorageParams.gcs.bucketName}"
          }
        if (config.hasVidRankMapStorageParams()) {
          vidRankMapStorageParams =
            SubpoolAssignerParamsKt.storageParams {
              gcsProjectId = config.vidRankMapStorageParams.gcs.projectId
              blobPrefix = "gs://${config.vidRankMapStorageParams.gcs.bucketName}"
            }
        }
        if (config.hasSubpoolMapStorageParams()) {
          subpoolMapStorageParams =
            SubpoolAssignerParamsKt.storageParams {
              gcsProjectId = config.subpoolMapStorageParams.gcs.projectId
              blobPrefix = "gs://${config.subpoolMapStorageParams.gcs.bucketName}"
            }
        }
        if (config.hasModelStorageParams()) {
          modelStorageParams =
            SubpoolAssignerParamsKt.storageParams {
              gcsProjectId = config.modelStorageParams.gcs.projectId
              blobPrefix = "gs://${config.modelStorageParams.gcs.bucketName}"
            }
        }
        rawImpressionMetadataStorageConnection = transportLayerSecurityParams {
          clientCertResourcePath = config.rawImpressionMetadataStorageConnection.certFilePath
          clientPrivateKeyResourcePath =
            config.rawImpressionMetadataStorageConnection.privateKeyFilePath
        }
        // Forward the bin-packing threshold onto the memoized Phase-0 path so the Phase-1 ranker's
        // last-job-out fan-out bin-packs identically to the non-memoized dispatcher. REQUIRED on
        // SubpoolAssignerParams; SubpoolAssignerApp validates it > 0.
        maxFileBatchSizeBytes = config.maxFileBatchSizeBytes
      }
    }

    private fun convertModelLineConfigs(
      configModelLines: Map<String, VidLabelingConfig.ModelLineConfig>
    ): Map<String, VidLabelerParams.ModelLineConfig> {
      return configModelLines.mapValues { (_, configModelLine) ->
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.addAll(configModelLine.labelerInputFieldMappingList)
          eventTemplateFieldMapping.putAll(configModelLine.eventTemplateFieldMappingMap)
          eventTemplateDescriptorBlobUri = configModelLine.eventTemplateDescriptorBlobUri
          eventTemplateType = configModelLine.eventTemplateType
          requiredEntityKeyFieldMapping.putAll(configModelLine.requiredEntityKeyFieldMappingMap)
          optionalEntityKeyFieldMapping.putAll(configModelLine.optionalEntityKeyFieldMappingMap)
        }
      }
    }
  }
}
