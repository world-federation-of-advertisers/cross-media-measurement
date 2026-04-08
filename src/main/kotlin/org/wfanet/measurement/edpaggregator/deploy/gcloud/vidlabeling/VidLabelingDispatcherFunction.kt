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
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams as ConfigTransportLayerSecurityParams
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchFileServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionMetadataBatchServiceGrpcKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParamsKt
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingDispatcherParams
import org.wfanet.measurement.edpaggregator.v1alpha.transportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelerParams
import org.wfanet.measurement.edpaggregator.vidlabeling.VidLabelingDispatcher
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt
import org.wfanet.measurement.storage.SelectedStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/** Channel cache key using TLS params, target, and optional hostname override. */
private data class ChannelKey(
  val tls: ConfigTransportLayerSecurityParams,
  val target: String,
  val hostName: String?,
)

/**
 * Cloud Function that dispatches VID labeling work items to the Secure Computation control plane.
 *
 * Invoked when an EDP finishes uploading raw impressions and writes a "done" blob. The function
 * looks up the matching [VidLabelingConfig] for the data provider, crawls the directory for raw
 * impression files, partitions them into batches, and creates WorkItems via the Secure Computation
 * API.
 *
 * ## Environment Variables
 * - `EDPA_CONFIG_STORAGE_BUCKET`: Required. URI prefix for the config storage bucket.
 * - `CONFIG_BLOB_KEY`: Required. Blob key for the [VidLabelingConfigs] textproto.
 * - `CONTROL_PLANE_TARGET`: Required. Target endpoint for the Secure Computation control plane.
 * - `CONTROL_PLANE_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `RAW_IMPRESSION_METADATA_TARGET`: Required. Target endpoint for the RawImpressionMetadata
 *   service.
 * - `RAW_IMPRESSION_METADATA_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `VID_LABELER_QUEUE_NAME`: Required. Resource name of the Secure Computation queue for VID
 *   labeling work items.
 * - `CHANNEL_SHUTDOWN_DURATION_SECONDS`: Optional. gRPC channel shutdown timeout (default: 3s).
 * - `VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH`: Optional. Enables [FileSystemStorageClient] instead
 *   of GCS. Used only in testing.
 *
 * ## Configuration
 * - [VidLabelingConfigs] are loaded from the config storage bucket at startup.
 * - The request body contains a [VidLabelingDispatcherParams] JSON with the data provider name.
 * - gRPC channels are created with mutual TLS using the config's TLS connection parameters.
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

      val config: VidLabelingConfig =
        vidLabelingConfigsByDataProvider[dispatcherParams.dataProvider]
          ?: throw IllegalArgumentException(
            "No VidLabelingConfig found for data provider: ${dispatcherParams.dataProvider}"
          )

      val storageClient: StorageClient = createStorageClient(doneBlobPath)

      val controlPlaneChannel: ManagedChannel = getOrCreateControlPlaneChannel(config)
      val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
      val instrumentedChannel =
        ClientInterceptors.intercept(controlPlaneChannel, grpcTelemetry.newClientInterceptor())

      val workItemsStub = WorkItemsGrpcKt.WorkItemsCoroutineStub(instrumentedChannel)

      val rawImpressionMetadataChannel: ManagedChannel =
        getOrCreateRawImpressionMetadataChannel(config)
      val instrumentedRawImpressionMetadataChannel =
        ClientInterceptors.intercept(
          rawImpressionMetadataChannel,
          grpcTelemetry.newClientInterceptor(),
        )
      val rawImpressionMetadataBatchStub =
        RawImpressionMetadataBatchServiceGrpcKt.RawImpressionMetadataBatchServiceCoroutineStub(
          instrumentedRawImpressionMetadataChannel
        )
      val rawImpressionMetadataBatchFileStub =
        RawImpressionMetadataBatchFileServiceGrpcKt
          .RawImpressionMetadataBatchFileServiceCoroutineStub(
            instrumentedRawImpressionMetadataChannel
          )

      val vidLabelerParamsTemplate: VidLabelerParams = buildVidLabelerParamsTemplate(config)

      val batchMaxSizeBytes: Long =
        if (config.maxBatchSizeBytes > 0) {
          config.maxBatchSizeBytes
        } else {
          DEFAULT_BATCH_MAX_SIZE_BYTES
        }

      val dispatcher =
        VidLabelingDispatcher(
          storageClient = storageClient,
          workItemsStub = workItemsStub,
          rawImpressionMetadataBatchStub = rawImpressionMetadataBatchStub,
          rawImpressionMetadataBatchFileStub = rawImpressionMetadataBatchFileStub,
          dataProviderName = config.dataProvider,
          vidLabelerParamsTemplate = vidLabelerParamsTemplate,
          queueName = vidLabelerQueueName,
          batchMaxSizeBytes = batchMaxSizeBytes,
        )

      Tracing.withW3CTraceContext(request) {
        runBlocking(Context.current().asContextElement()) { dispatcher.dispatch(doneBlobPath) }
      }
    } finally {
      EdpaTelemetry.flush()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private const val DEFAULT_BATCH_MAX_SIZE_BYTES: Long = 10_000_000_000L
    private const val DATA_WATCHER_PATH_HEADER: String = "X-DataWatcher-Path"
    private const val GOOGLE_PROJECT_ID_ENV = "GOOGLE_PROJECT_ID"

    private val controlPlaneTarget: String = EnvVars.checkNotNullOrEmpty("CONTROL_PLANE_TARGET")
    private val controlPlaneCertHost: String? = System.getenv("CONTROL_PLANE_CERT_HOST")
    private val vidLabelerQueueName: String = EnvVars.checkNotNullOrEmpty("VID_LABELER_QUEUE_NAME")
    private val channelShutdownDuration =
      Duration.ofSeconds(
        System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: DEFAULT_CHANNEL_SHUTDOWN_DURATION_SECONDS
      )

    private val fileSystemPath: String? = System.getenv("VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH")

    private val rawImpressionMetadataTarget: String =
      EnvVars.checkNotNullOrEmpty("RAW_IMPRESSION_METADATA_TARGET")
    private val rawImpressionMetadataCertHost: String? =
      System.getenv("RAW_IMPRESSION_METADATA_CERT_HOST")

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

    private val channelCache = ConcurrentHashMap<ChannelKey, ManagedChannel>()

    /**
     * Creates a [StorageClient] for listing raw impression files.
     *
     * Uses [FileSystemStorageClient] when the `VID_LABELING_DISPATCHER_FILE_SYSTEM_PATH`
     * environment variable is set (testing only), otherwise creates a [GcsStorageClient] using the
     * bucket from the done blob URI.
     *
     * @param doneBlobPath the full storage URI of the "done" blob.
     * @return a [StorageClient] configured for the appropriate storage backend.
     */
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

    /**
     * Creates a gRPC [ManagedChannel] configured with mutual TLS authentication.
     *
     * @param connectionParams the TLS parameters containing file paths for the client certificate,
     *   private key, and certificate collection.
     * @param target the server target (e.g., "host:port") to connect to.
     * @param hostName an optional hostname override for TLS verification.
     * @return a [ManagedChannel] secured with mutual TLS.
     */
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

    /**
     * Gets or creates a cached gRPC channel for the Secure Computation control plane.
     *
     * Uses the [VidLabelingConfig]'s `raw_impression_metadata_storage_connection` TLS parameters
     * for channel creation, since the Cloud Function uses the same client identity for all outgoing
     * connections.
     *
     * @param config the [VidLabelingConfig] providing TLS connection parameters.
     * @return a cached [ManagedChannel] for the control plane.
     */
    private fun getOrCreateControlPlaneChannel(config: VidLabelingConfig): ManagedChannel {
      val channelKey =
        ChannelKey(
          config.rawImpressionMetadataStorageConnection,
          controlPlaneTarget,
          controlPlaneCertHost,
        )
      return channelCache.computeIfAbsent(channelKey) {
        logger.info("Creating new Control Plane channel for TLS params: $channelKey")
        createPublicChannel(
          config.rawImpressionMetadataStorageConnection,
          controlPlaneTarget,
          controlPlaneCertHost,
        )
      }
    }

    /**
     * Gets or creates a cached gRPC channel for the RawImpressionMetadata service.
     *
     * @param config the [VidLabelingConfig] providing TLS connection parameters.
     * @return a cached [ManagedChannel] for the RawImpressionMetadata service.
     */
    private fun getOrCreateRawImpressionMetadataChannel(config: VidLabelingConfig): ManagedChannel {
      val channelKey =
        ChannelKey(
          config.rawImpressionMetadataStorageConnection,
          rawImpressionMetadataTarget,
          rawImpressionMetadataCertHost,
        )
      return channelCache.computeIfAbsent(channelKey) {
        logger.info("Creating new RawImpressionMetadata channel for TLS params: $channelKey")
        createPublicChannel(
          config.rawImpressionMetadataStorageConnection,
          rawImpressionMetadataTarget,
          rawImpressionMetadataCertHost,
        )
      }
    }

    /**
     * Builds a [VidLabelerParams] template from a [VidLabelingConfig].
     *
     * Converts config-layer types to v1alpha types for the TEE application. The per-batch field
     * (`raw_impression_metadata_batch`) is set by [VidLabelingDispatcher] for each batch.
     *
     * @param config the [VidLabelingConfig] to convert.
     * @return a [VidLabelerParams] template with static fields populated.
     */
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
            impressionsBlobPrefix =
              "gs://${config.rawImpressionsStorageParams.gcs.bucketName}"
          }
        vidRepoConnection = transportLayerSecurityParams {
          clientCertResourcePath = config.vidRepoConnection.certFilePath
          clientPrivateKeyResourcePath = config.vidRepoConnection.privateKeyFilePath
        }
        modelLineConfigs.putAll(convertModelLineConfigs(config.modelLineConfigsMap))
        overrideModelLines += config.overrideModelLinesList
        vidLabeledImpressionsKekUri = config.vidLabeledImpressionsKekUri
      }
    }

    /**
     * Converts model line configs from config package types to v1alpha package types.
     *
     * Both [VidLabelingConfig.ModelLineConfig] and [VidLabelerParams.ModelLineConfig] have
     * identical fields but are different generated types.
     *
     * @param configModelLines the config-layer model line map.
     * @return the v1alpha model line map.
     */
    private fun convertModelLineConfigs(
      configModelLines: Map<String, VidLabelingConfig.ModelLineConfig>
    ): Map<String, VidLabelerParams.ModelLineConfig> {
      return configModelLines.mapValues { (_, configModelLine) ->
        VidLabelerParamsKt.modelLineConfig {
          labelerInputFieldMapping.putAll(configModelLine.labelerInputFieldMappingMap)
          eventTemplateFieldMapping.putAll(configModelLine.eventTemplateFieldMappingMap)
        }
      }
    }
  }
}
