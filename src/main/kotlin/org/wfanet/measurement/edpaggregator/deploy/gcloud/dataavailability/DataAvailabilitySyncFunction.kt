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
import com.google.protobuf.util.JsonFormat
import io.grpc.ClientInterceptors
import io.grpc.ManagedChannel
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.BufferedReader
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
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.dataavailability.DataAvailabilitySync
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing
import org.wfanet.measurement.edpaggregator.v1alpha.ImpressionMetadataServiceGrpcKt.ImpressionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
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
 * Invoked when an EDP finishes uploading impressions and writes a "done" blob to Google Cloud
 * Storage. The function reads the new availability, synchronizes it with ImpressionMetadataStorage,
 * and updates the impression availability interval in the Kingdom.
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
 * - `DATA_AVAILABILITY_FILE_SYSTEM_PATH`: Optional. If set, enables `FileSystemStorageClient`
 *   instead of GCS. Used only in testing.
 *
 * ## Configuration
 * - A [DataAvailabilitySyncConfig] is provided in the request body by the DataWatcher Cloud
 *   Function.
 * - gRPC channels are created with mutual TLS using the provided certificate files.
 */
class DataAvailabilitySyncFunction() : HttpFunction {
  init {
    EdpaTelemetry.ensureInitialized()
  }

  override fun service(request: HttpRequest, response: HttpResponse) {
    try {
      logger.fine("Starting DataAvailabilitySyncFunction")
      val requestBody: BufferedReader = request.getReader()
      val dataAvailabilitySyncConfig =
        DataAvailabilitySyncConfig.newBuilder()
          .apply { JsonFormat.parser().merge(requestBody, this) }
          .build()

      // Read the path as request header
      val doneBlobPath =
        request.getFirstHeader(DATA_WATHCER_PATH_HEADER).orElseThrow {
          IllegalArgumentException("Missing required header: $DATA_WATHCER_PATH_HEADER")
        }

      val storageClient: StorageClient = createStorageClient(dataAvailabilitySyncConfig)

      val grpcChannels = getOrCreateSharedChannels(dataAvailabilitySyncConfig)

      val cmmsPublicChannel = grpcChannels.cmmsChannel
      val impressionMetadataStoragePublicChannel = grpcChannels.impressionMetadataChannel

      val grpcTelemetry = GrpcTelemetry.create(Instrumentation.openTelemetry)

      val instrumentedCmmsChannel =
        ClientInterceptors.intercept(cmmsPublicChannel, grpcTelemetry.newClientInterceptor())

      val instrumentedImpMetadataChannel =
        ClientInterceptors.intercept(
          impressionMetadataStoragePublicChannel,
          grpcTelemetry.newClientInterceptor(),
        )

      val dataProvidersClient = DataProvidersCoroutineStub(instrumentedCmmsChannel)
      val impressionMetadataServicesClient =
        ImpressionMetadataServiceCoroutineStub(instrumentedImpMetadataChannel)

      val dataAvailabilitySync =
        DataAvailabilitySync(
          dataAvailabilitySyncConfig.edpImpressionPath,
          storageClient,
          dataProvidersClient,
          impressionMetadataServicesClient,
          dataAvailabilitySyncConfig.dataProvider,
          globalThrottler,
          impressionMetadataBatchSize = impressionMetadataBatchSize,
        )

      Tracing.withW3CTraceContext(request) {
        runBlocking(Context.current().asContextElement()) {
          dataAvailabilitySync.sync(doneBlobPath)
        }
      }
    } finally {
      // Critical for Cloud Functions: flush metrics before function freezes
      EdpaTelemetry.flush()
    }
  }

  /**
   * Creates a [StorageClient] based on the current environment and the provided data provider
   * configuration.
   *
   * @param dataProviderConfig The configuration object for a `DataProvider`.
   * @return A [StorageClient] instance, either for local file system access or GCS access.
   */
  // @TODO(@marcopremier): This function should be reused across Cloud Functions.
  private fun createStorageClient(
    dataAvailabilitySyncConfig: DataAvailabilitySyncConfig
  ): StorageClient {
    return if (!fileSystemPath.isNullOrEmpty()) {
      FileSystemStorageClient(File(EnvVars.checkIsPath("DATA_AVAILABILITY_FILE_SYSTEM_PATH")))
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

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private const val THROTTLER_DURATION_MILLIS = 1000L
    private val throttlerDuration =
      Duration.ofMillis(System.getenv("THROTTLER_MILLIS")?.toLong() ?: THROTTLER_DURATION_MILLIS)

    private const val DATA_WATHCER_PATH_HEADER: String = "X-DataWatcher-Path"

    private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")
    private val channelShutdownDuration =
      Duration.ofSeconds(
        System.getenv("CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: CHANNEL_SHUTDOWN_DURATION_SECONDS
      )

    private val impressionMetadataTarget = EnvVars.checkNotNullOrEmpty("IMPRESSION_METADATA_TARGET")
    private val impressionMetadataCertHost: String? = System.getenv("IMPRESSION_METADATA_CERT_HOST")

    private val fileSystemPath: String? = System.getenv("DATA_AVAILABILITY_FILE_SYSTEM_PATH")

    private val globalThrottler = MinimumIntervalThrottler(Clock.systemUTC(), throttlerDuration)
    private const val DEFAULT_IMPRESSION_METADATA_BATCH_SIZE = 100
    private val impressionMetadataBatchSize =
      System.getenv("IMPRESSION_METADATA_BATCH_SIZE")?.toIntOrNull()?.takeIf { it > 0 }
        ?: DEFAULT_IMPRESSION_METADATA_BATCH_SIZE

    private val channelCache = ConcurrentHashMap<ChannelKey, ManagedChannel>()

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
  }
}
