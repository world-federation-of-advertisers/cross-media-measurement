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
import com.google.cloud.storage.BlobId
import com.google.cloud.storage.StorageOptions
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Clock
import java.time.Duration
import java.util.concurrent.ConcurrentHashMap
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.telemetry.XmmTraceAttributes
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.config.edpaggregator.copy
import org.wfanet.measurement.edpaggregator.ConfigLoader
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySyncLeaseRunner
import org.wfanet.measurement.edpaggregator.dataavailability.GrpcDataAvailabilitySyncLeaseClient
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncLeaseServiceGrpcKt.DataAvailabilitySyncLeaseServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineAvailabilitySynchronizedRequest
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.storage.BlobMetadataStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

private data class ChannelKey(
  val tls: TransportLayerSecurityParams,
  val target: String,
  val hostName: String?,
)

data class GrpcChannels(
  val cmmsChannel: ManagedChannel,
  val impressionMetadataChannel: ManagedChannel,
)

/**
 * Cloud Function that synchronizes data availability state between ImpressionMetadataStorage and
 * the Kingdom.
 *
 * Invoked by DataWatcher for externally produced data or by a durable `WorkItem` for internally
 * labeled data. The function reads the new availability, synchronizes it with
 * ImpressionMetadataStorage, and updates the impression availability interval in the Kingdom.
 *
 * The "done" blob is expected to be written to the bucket under the prefix:
 * `/edp/<edp_name>/<unique_identifier>/[optional subfolder]`.
 *
 * ## Environment Variables
 * - `KINGDOM_TARGET`: Required. Target endpoint for the Kingdom service.
 * - `KINGDOM_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `CHANNEL_SHUTDOWN_DURATION_SECONDS`: Optional. gRPC channel shutdown timeout (default: 3s).
 * - `IMPRESSION_METADATA_TARGET`: Required. Target endpoint for the Impression Metadata service.
 * - `IMPRESSION_METADATA_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `SECURE_COMPUTATION_CONTROL_PLANE_TARGET`: Required for WorkItem processing. Target endpoint
 *   for the Secure Computation public API.
 * - `SECURE_COMPUTATION_CERT_HOST`: Optional. Overrides TLS authority for testing.
 * - `SECURE_COMPUTATION_CERT_COLLECTION_FILE`: Optional trusted certificate collection for the
 *   Secure Computation public API.
 * - `DATA_AVAILABILITY_FILE_SYSTEM_PATH`: Optional. If set, enables `FileSystemStorageClient`
 *   instead of GCS. Used only in testing.
 *
 * ## Configuration
 * - DataWatcher requests provide a [DataAvailabilitySyncConfig] in the request body.
 * - WorkItems select a configured data provider from their application parameters.
 * - gRPC channels are created with mutual TLS using the provided certificate files.
 */
class DataAvailabilitySyncFunction() : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting DataAvailabilitySyncFunction")
      val doneBlobPath = request.getFirstHeader(DATA_WATHCER_PATH_HEADER).orElse(null)
      if (doneBlobPath == null) {
        serviceWorkItem(request)
        return
      }
      val requestBody = request.reader.readText()
      val dataAvailabilitySyncConfig =
        ConfigLoader.buildDataAvailabilitySyncConfig(requestBody, runtimeConfigs.configsList)

      val dataAvailabilitySync = buildDataAvailabilitySync(dataAvailabilitySyncConfig)

      Tracing.withW3CTraceContext(request) {
        val generation =
          parseDataWatcherGeneration(
            request
              .getFirstHeader(VidLabelingTraceAttributes.DATA_WATCHER_GENERATION_HEADER)
              .orElse(null)
          )
        val objectIdentity = VidLabelingTraceAttributes.gcsObjectIdentity(doneBlobPath, generation)
        val attributes =
          Attributes.builder()
            .put(
              VidLabelingTraceAttributes.DATA_PROVIDER_NAME,
              dataAvailabilitySyncConfig.dataProvider,
            )
            .put(XmmTraceAttributes.LIFECYCLE_STAGE, "data_availability_sync")
            .put(XmmTraceAttributes.OUTCOME, "started")
            .also { builder ->
              request
                .getFirstHeader(VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_HEADER)
                .ifPresent {
                  builder.put(VidLabelingTraceAttributes.RAW_IMPRESSION_UPLOAD_NAME, it)
                }
              request.getFirstHeader(VidLabelingTraceAttributes.MODEL_LINE_HEADER).ifPresent {
                builder.put(VidLabelingTraceAttributes.MODEL_LINE_NAME, it)
              }
              request.getFirstHeader(VidLabelingTraceAttributes.VID_LABELING_JOB_HEADER).ifPresent {
                builder.put(VidLabelingTraceAttributes.VID_LABELING_JOB_NAME, it)
              }
              builder
                .put(VidLabelingTraceAttributes.GCS_OBJECT_PATH_HASH, objectIdentity.pathHash)
                .put(VidLabelingTraceAttributes.GCS_OBJECT_GENERATION, objectIdentity.generation)
            }
            .build()
        Tracing.trace("edpa.data_availability.sync", attributes) {
          val outcome =
            runBlocking(Context.current().asContextElement()) {
              buildDataAvailabilitySyncLeaseRunner(dataAvailabilitySyncConfig).run(
                dataAvailabilitySyncConfig.dataProvider
              ) { lease ->
                dataAvailabilitySync.sync(
                  doneBlobPath,
                  dataAvailabilitySyncLease = lease.name,
                  ensureLeaseActive = lease::invoke,
                  doneBlobGeneration = generation,
                )
              }
            }
          Span.current().setAttribute(XmmTraceAttributes.OUTCOME, outcome.name.lowercase())
        }
      }
    } finally {
      // Critical for Cloud Functions: flush metrics before function freezes
      EdpaTelemetry.flush()
    }
  }

  private fun serviceWorkItem(request: HttpRequest) {
    val input = DataAvailabilitySyncWorkItem.parse(WorkItem.parseFrom(request.inputStream))
    val config =
      runtimeConfigs.configsList.single { it.dataProvider == input.appParams.dataProvider }
    val grpcChannels = getOrCreateSharedChannels(config)
    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
    val instrumentedMetadataChannel =
      ClientInterceptors.intercept(
        grpcChannels.impressionMetadataChannel,
        grpcTelemetry.newClientInterceptor(),
      )
    val instrumentedControlPlaneChannel =
      ClientInterceptors.intercept(
        getOrCreateSecureComputationChannel(config),
        grpcTelemetry.newClientInterceptor(),
      )
    val rawImpressionUploadFilesStub =
      RawImpressionUploadFileServiceCoroutineStub(instrumentedMetadataChannel)
    val rawImpressionUploadModelLinesStub =
      RawImpressionUploadModelLineServiceCoroutineStub(instrumentedMetadataChannel)
    val processor =
      DataAvailabilitySyncWorkItemProcessor(
        WorkItemsCoroutineStub(instrumentedControlPlaneChannel),
        WorkItemAttemptsCoroutineStub(instrumentedControlPlaneChannel),
        DataAvailabilitySyncLeaseRunner(
          GrpcDataAvailabilitySyncLeaseClient(
            DataAvailabilitySyncLeaseServiceCoroutineStub(instrumentedMetadataChannel)
          )
        ),
        synchronize = { workItem, lease, onStage ->
          val rawImpressionBlobUris =
            listRawImpressionBlobUris(rawImpressionUploadFilesStub, workItem)
          check(rawImpressionBlobUris.isNotEmpty()) {
            "No RawImpressionUploadFile rows matched the WorkItem event date"
          }
          buildDataAvailabilitySync(config)
            .sync(
              workItem.doneBlobUri,
              dataAvailabilitySyncLease = lease.name,
              doneBlobGeneration = workItem.doneBlobGeneration,
              discoveryMode =
                DataAvailabilitySync.DiscoveryMode.VidLabelerOutputs(
                  rawImpressionUpload = workItem.appParams.triggeringRawImpressionUpload,
                  rawImpressionBlobUris = rawImpressionBlobUris,
                  modelLine = workItem.appParams.modelLine,
                  eventDate = workItem.eventDate,
                ),
              onStage = onStage,
              ensureLeaseActive = lease::invoke,
            )
        },
        verifyDoneObject = { workItem -> verifyWorkItemDoneObject(workItem, config) },
        markAvailabilitySynchronized = { workItem ->
          rawImpressionUploadModelLinesStub
            .markRawImpressionUploadModelLineAvailabilitySynchronized(
              markRawImpressionUploadModelLineAvailabilitySynchronizedRequest {
                name = workItem.rawImpressionUploadModelLineName
                eventDate = workItem.appParams.eventDate
                requestId =
                  RequestIds.forMarkRawImpressionUploadModelLineAvailabilitySynchronized(
                    workItem.rawImpressionUploadModelLineName,
                    workItem.eventDate.toString(),
                  )
              }
            )
        },
      )
    runBlocking { processor.process(input) }
  }

  private suspend fun listRawImpressionBlobUris(
    stub: RawImpressionUploadFileServiceCoroutineStub,
    workItem: DataAvailabilitySyncWorkItem,
  ): List<String> {
    val blobUris = mutableListOf<String>()
    var pageToken = ""
    do {
      val response =
        stub.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            parent = workItem.appParams.triggeringRawImpressionUpload
            pageSize = RAW_IMPRESSION_UPLOAD_FILE_PAGE_SIZE
            this.pageToken = pageToken
          }
        )
      blobUris +=
        response.rawImpressionUploadFilesList
          .filter { it.eventDate.toLocalDate() == workItem.eventDate }
          .map { it.blobUri }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return blobUris.distinct()
  }

  private fun verifyWorkItemDoneObject(
    workItem: DataAvailabilitySyncWorkItem,
    config: DataAvailabilitySyncConfig,
  ) {
    if (!fileSystemPath.isNullOrEmpty()) return
    val doneBlobUri =
      org.wfanet.measurement.storage.SelectedStorageClient.parseBlobUri(workItem.doneBlobUri)
    require(doneBlobUri.scheme == "gs") { "WorkItem done object must use gs://" }
    require(doneBlobUri.bucket == config.dataAvailabilityStorage.gcs.bucketName) {
      "WorkItem done object is outside the configured bucket"
    }
    val storage =
      StorageOptions.newBuilder()
        .also { builder ->
          config.dataAvailabilityStorage.gcs.projectId.takeIf(String::isNotEmpty)?.let {
            builder.setProjectId(it)
          }
        }
        .build()
        .service
    val blob = storage.get(BlobId.of(doneBlobUri.bucket, doneBlobUri.key))
    validateWorkItemDoneObjectGeneration(blob?.generation, workItem.doneBlobGeneration)
  }

  private fun buildDataAvailabilitySync(
    dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
  ): DataAvailabilitySync {
    val storageClient = createStorageClient(dataAvailabilitySyncConfig)
    val grpcChannels = getOrCreateSharedChannels(dataAvailabilitySyncConfig)
    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
    return DataAvailabilitySync(
      dataAvailabilitySyncConfig.edpImpressionPath,
      storageClient,
      DataProvidersCoroutineStub(
        ClientInterceptors.intercept(grpcChannels.cmmsChannel, grpcTelemetry.newClientInterceptor())
      ),
      ImpressionMetadataServiceCoroutineStub(
        ClientInterceptors.intercept(
          grpcChannels.impressionMetadataChannel,
          grpcTelemetry.newClientInterceptor(),
        )
      ),
      dataAvailabilitySyncConfig.dataProvider,
      globalThrottler,
      impressionMetadataBatchSize = impressionMetadataBatchSize,
      errorIfGapsExist = dataAvailabilitySyncConfig.errorIfGapsExist,
      modelLineMap =
        dataAvailabilitySyncConfig.modelLineMapMap.mapValues { it.value.modelLinesList },
    )
  }

  private fun buildDataAvailabilitySyncLeaseRunner(
    dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
  ): DataAvailabilitySyncLeaseRunner {
    val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)
    val instrumentedMetadataChannel =
      ClientInterceptors.intercept(
        getOrCreateSharedChannels(dataAvailabilitySyncConfig).impressionMetadataChannel,
        grpcTelemetry.newClientInterceptor(),
      )
    return DataAvailabilitySyncLeaseRunner(
      GrpcDataAvailabilitySyncLeaseClient(
        DataAvailabilitySyncLeaseServiceCoroutineStub(instrumentedMetadataChannel)
      )
    )
  }

  /**
   * Creates a [BlobMetadataStorageClient] based on the current environment and the provided data
   * provider configuration.
   *
   * @param dataProviderConfig The configuration object for a `DataProvider`.
   * @return A [BlobMetadataStorageClient] instance, either for local file system access or GCS
   *   access.
   */
  // @TODO(@marcopremier): This function should be reused across Cloud Functions.
  private fun createStorageClient(
    dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
  ): BlobMetadataStorageClient {
    return if (!fileSystemPath.isNullOrEmpty()) {
      NoOpBlobMetadataStorageClient(
        FileSystemStorageClient(File(EnvVars.checkIsPath("DATA_AVAILABILITY_FILE_SYSTEM_PATH")))
      )
    } else {
      val gcsConfig = dataAvailabilitySyncConfig.dataAvailabilityStorage.gcs
      GcsStorageClient(
        StorageOptions.newBuilder()
          .also {
            if (gcsConfig.projectId.isNotEmpty()) {
              it.setProjectId(gcsConfig.projectId)
            }
          }
          .build()
          .service,
        gcsConfig.bucketName,
      )
    }
  }

  /**
   * A [BlobMetadataStorageClient] wrapper for [FileSystemStorageClient] used only in testing.
   *
   * Since [FileSystemStorageClient] doesn't support blob metadata, this provides a no-op
   * implementation of [updateBlobMetadata].
   */
  private class NoOpBlobMetadataStorageClient(private val delegate: StorageClient) :
    BlobMetadataStorageClient, StorageClient by delegate {
    override suspend fun updateBlobMetadata(
      blobKey: String,
      customCreateTime: java.time.Instant?,
      metadata: Map<String, String>,
    ) {
      // No-op for FileSystemStorageClient testing
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private const val THROTTLER_DURATION_MILLIS = 1000L
    private val throttlerDuration =
      Duration.ofMillis(System.getenv("THROTTLER_MILLIS")?.toLong() ?: THROTTLER_DURATION_MILLIS)

    private const val DATA_WATHCER_PATH_HEADER: String = "X-DataWatcher-Path"
    private const val RAW_IMPRESSION_UPLOAD_FILE_PAGE_SIZE = 1000

    private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")
    private val channelShutdownDuration =
      Duration.ofSeconds(
        System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: CHANNEL_SHUTDOWN_DURATION_SECONDS
      )

    private val impressionMetadataTarget = EnvVars.checkNotNullOrEmpty("IMPRESSION_METADATA_TARGET")
    private val impressionMetadataCertHost: String? = System.getenv("IMPRESSION_METADATA_CERT_HOST")

    private val secureComputationControlPlaneTarget: String? =
      System.getenv("SECURE_COMPUTATION_CONTROL_PLANE_TARGET")
    private val secureComputationCertHost: String? = System.getenv("SECURE_COMPUTATION_CERT_HOST")
    private val secureComputationCertCollectionFile: String? =
      System.getenv("SECURE_COMPUTATION_CERT_COLLECTION_FILE")

    private val fileSystemPath: String? = System.getenv("DATA_AVAILABILITY_FILE_SYSTEM_PATH")

    private val globalThrottler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerDuration)
    private const val DEFAULT_IMPRESSION_METADATA_BATCH_SIZE = 100
    private val impressionMetadataBatchSize =
      System.getenv("IMPRESSION_METADATA_BATCH_SIZE")?.toIntOrNull()?.takeIf { it > 0 }
        ?: DEFAULT_IMPRESSION_METADATA_BATCH_SIZE

    private val channelCache = ConcurrentHashMap<ChannelKey, ManagedChannel>()

    private val configBlobKey: String =
      requireNotNull(System.getenv("CONFIG_BLOB_KEY")) {
        "CONFIG_BLOB_KEY environment variable must be set"
      }
    private val runtimeConfigs: DataAvailabilitySyncConfigs = runBlocking {
      EdpAggregatorConfig.getConfigAsProtoMessage(
        configBlobKey,
        DataAvailabilitySyncConfigs.getDefaultInstance(),
      )
    }

    /**
     * Creates a gRPC [ManagedChannel] configured with mutual TLS authentication.
     *
     * This function loads the client certificate, private key, and trusted root certificates from
     * the file paths defined in [connecionParams]. It then uses these credentials to build a secure
     * channel to the given [target].
     *
     * Optionally, a [hostName] can be provided to override the default authority used for TLS host
     * verification.
     *
     * The returned channel is configured with a shutdown timeout defined by
     * [channelShutdownDuration].
     *
     * @param connecionParams the TLS parameters containing file paths for the client certificate,
     *   private key, and certificate collection.
     * @param target the server target (e.g., "host:port") to connect to.
     * @param hostName an optional hostname override for TLS verification.
     * @return a [ManagedChannel] secured with mutual TLS authentication.
     * @throws IllegalArgumentException if any required certificate file path is missing or invalid.
     */
    // @TODO(@marcopremier): This function should be reused across Cloud Functions.
    fun createPublicChannel(
      connecionParams: TransportLayerSecurityParams,
      target: String,
      hostName: String?,
    ): ManagedChannel {
      val signingCerts =
        SigningCerts.fromPemFiles(
          certificateFile = checkNotNull(File(connecionParams.certFilePath)),
          privateKeyFile = checkNotNull(File(connecionParams.privateKeyFilePath)),
          trustedCertCollectionFile = checkNotNull(File(connecionParams.certCollectionFilePath)),
        )
      val publicChannel =
        buildMutualTlsChannel(target, signingCerts, hostName)
          .withShutdownTimeout(channelShutdownDuration)

      return publicChannel
    }

    /**
     * Retrieves gRPC channels for CMMS and ImpressionMetadata based on the TLS configuration in
     * [dataAvailabilitySyncConfig].
     *
     * Channels are cached and keyed by their TLS parameters, target, and optional hostname
     * override. A new channel is created only when no matching entry exists in the cache.
     *
     * @return A pair of [ManagedChannel] instances for (CMMS, ImpressionMetadata).
     */
    fun getOrCreateSharedChannels(
      dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
    ): GrpcChannels {
      val cmmsChannelKey =
        ChannelKey(dataAvailabilitySyncConfig.cmmsConnection, kingdomTarget, kingdomCertHost)
      val impressionsChannelKey =
        ChannelKey(
          dataAvailabilitySyncConfig.impressionMetadataStorageConnection,
          impressionMetadataTarget,
          impressionMetadataCertHost,
        )

      val cmmsChannel =
        channelCache.computeIfAbsent(cmmsChannelKey) {
          logger.info("Creating new CMMS channel for TLS params: $cmmsChannelKey")
          createPublicChannel(
            dataAvailabilitySyncConfig.cmmsConnection,
            kingdomTarget,
            kingdomCertHost,
          )
        }

      val impressionChannel =
        channelCache.computeIfAbsent(impressionsChannelKey) {
          logger.info(
            "Creating new ImpressionMetadata channel for TLS params: $impressionsChannelKey"
          )
          createPublicChannel(
            dataAvailabilitySyncConfig.impressionMetadataStorageConnection,
            impressionMetadataTarget,
            impressionMetadataCertHost,
          )
        }

      return GrpcChannels(cmmsChannel = cmmsChannel, impressionMetadataChannel = impressionChannel)
    }

    fun getOrCreateSecureComputationChannel(
      dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
    ): ManagedChannel {
      val target =
        requireNotNull(secureComputationControlPlaneTarget) {
          "SECURE_COMPUTATION_CONTROL_PLANE_TARGET is required for WorkItem processing"
        }
      val connection =
        dataAvailabilitySyncConfig.impressionMetadataStorageConnection.copy {
          secureComputationCertCollectionFile?.let { certCollectionFilePath = it }
        }
      val channelKey = ChannelKey(connection, target, secureComputationCertHost)
      return channelCache.computeIfAbsent(channelKey) {
        createPublicChannel(connection, target, secureComputationCertHost)
      }
    }
  }
}

internal fun parseDataWatcherGeneration(value: String?): Long {
  val generation =
    requireNotNull(value?.toLongOrNull()) {
      "${VidLabelingTraceAttributes.DATA_WATCHER_GENERATION_HEADER} must contain a generation"
    }
  require(generation > 0L) {
    "${VidLabelingTraceAttributes.DATA_WATCHER_GENERATION_HEADER} must be positive"
  }
  return generation
}

internal fun validateWorkItemDoneObjectGeneration(actual: Long?, expected: Long) {
  require(actual == expected) { "WorkItem done object generation is not available" }
}
