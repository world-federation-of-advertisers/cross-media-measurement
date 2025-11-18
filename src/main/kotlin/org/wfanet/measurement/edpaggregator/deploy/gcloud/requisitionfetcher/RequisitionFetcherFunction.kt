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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import io.grpc.ClientInterceptors
import io.opentelemetry.api.common.AttributeKey
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.api.trace.Span
import io.opentelemetry.context.Context
import io.opentelemetry.extension.kotlin.asContextElement
import io.opentelemetry.instrumentation.grpc.v1_6.GrpcTelemetry
import java.io.File
import java.time.Clock
import java.time.Duration
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.Instrumentation
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.edpaggregator.EdpAggregatorConfig.getConfigAsProtoMessage
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.config.edpaggregator.DataProviderRequisitionConfig
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.config.edpaggregator.TransportLayerSecurityParams
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.telemetry.EdpaTelemetry
import org.wfanet.measurement.edpaggregator.telemetry.Tracing.trace
import org.wfanet.measurement.edpaggregator.telemetry.Tracing.withW3CTraceContext
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

/**
 * A Google Cloud Function that fetches and stores requisitions for each configured data provider.
 *
 * ## Environment Variables
 * - `REQUISITION_FILE_SYSTEM_PATH`: Optional. If set, requisitions are written to this local
 *   filesystem path. If unset, the function defaults to writing to GCS using configuration in the
 *   proto.
 * - `KINGDOM_TARGET`: Required. The gRPC target address for the Kingdom services.
 * - `KINGDOM_CERT_HOST`: Optional. The server name to verify in the Kingdom service's TLS
 *   certificate. Useful when the hostname in the certificate does not match the `KINGDOM_TARGET`.
 * - `GRPC_THROTTLER`: Required. A numeric value (in milliseconds) that defines the minimum interval
 *   between gRPC calls to Kingdom to avoid overwhelming the service.
 * - `PAGE_SIZE`: Optional. Overrides the default number of requisitions fetched per page from the
 *   backend.
 */
class RequisitionFetcherFunction : HttpFunction {

  override fun service(request: HttpRequest, response: HttpResponse) =
    withW3CTraceContext(request) { handleRequest(response) }

  private fun handleRequest(response: HttpResponse) {
    try {
      val errors = mutableListOf<String>()
      for (dataProviderConfig in requisitionFetcherConfig.configsList) {
        // Keep per-provider failures as Result so we can process every config before responding.
        val result: Result<Unit> = processDataProvider(dataProviderConfig)
        val failure = result.exceptionOrNull()
        if (failure != null) {
          val errorMsg =
            if (failure is IllegalArgumentException) {
              "Invalid config for data provider: ${dataProviderConfig.dataProvider}"
            } else {
              "Failed to fetch and store requisitions for ${dataProviderConfig.dataProvider}"
            }
          errors.add(errorMsg)
          logger.log(Level.SEVERE, errorMsg, failure)
        }
      }

      if (errors.isNotEmpty()) {
        response.setStatusCode(500)
        response.writer.write("Completed with errors:\n" + errors.joinToString("\n"))
      } else {
        response.setStatusCode(200)
        response.writer.write("All requisitions fetched successfully")
      }
    } finally {
      // Critical for Cloud Functions: flush metrics before function freezes
      EdpaTelemetry.flush()
    }
  }

  /**
   * Executes the requisition fetch workflow for a single data provider and aggregates telemetry.
   *
   * @param dataProviderConfig configuration for the data provider.
   * @return a [Result] capturing success or the failure that occurred.
   */
  private fun processDataProvider(dataProviderConfig: DataProviderRequisitionConfig): Result<Unit> =
    withDataProviderTelemetry(dataProviderConfig.dataProvider) {
      validateConfig(dataProviderConfig)
      val requisitionFetcher = createRequisitionFetcher(dataProviderConfig)
      runBlocking(Context.current().asContextElement()) {
        requisitionFetcher.fetchAndStoreRequisitions()
      }
    }

  private fun withDataProviderTelemetry(dataProviderName: String, block: () -> Unit): Result<Unit> =
    trace(
      spanName = SPAN_DATA_PROVIDER_EXECUTION,
      attributes = Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName),
    ) {
      val span = Span.current()
      return@trace try {
        block()
        span.addEvent(
          EVENT_DATA_PROVIDER_COMPLETED,
          Attributes.of(ATTR_DATA_PROVIDER_KEY, dataProviderName, ATTR_STATUS_KEY, STATUS_SUCCESS),
        )
        Result.success(Unit)
      } catch (e: Exception) {
        span.addEvent(
          EVENT_DATA_PROVIDER_FAILED,
          Attributes.of(
            ATTR_DATA_PROVIDER_KEY,
            dataProviderName,
            ATTR_STATUS_KEY,
            STATUS_FAILURE,
            ATTR_ERROR_TYPE_KEY,
            e::class.simpleName ?: e::class.java.simpleName ?: e::class.java.name,
          ),
        )
        Result.failure(e)
      }
    }

  /**
   * Creates a [RequisitionFetcher] instance for the given data provider configuration.
   *
   * @param dataProviderConfig The configuration for a single data provider.
   * @return A fully initialized [RequisitionFetcher] ready to fetch and store requisitions for the
   *   data provider.
   */
  private fun createRequisitionFetcher(
    dataProviderConfig: DataProviderRequisitionConfig
  ): RequisitionFetcher {
    val storageClient = createStorageClient(dataProviderConfig)
    val requisitionBlobPrefix = createRequisitionBlobPrefix(dataProviderConfig)
    // Create instrumented gRPC channels with mutual TLS and OpenTelemetry tracing
    val instrumentedCmmsChannel =
      createInstrumentedChannel(dataProviderConfig.cmmsConnection, kingdomTarget, kingdomCertHost)
    val instrumentedMetadataChannel =
      createInstrumentedChannel(
        dataProviderConfig.requisitionMetadataStorageConnection,
        metadataStorageTarget,
        metadataStorageCertHost,
      )

    val requisitionsStub = RequisitionsCoroutineStub(instrumentedCmmsChannel)
    val requisitionMetadataStub =
      RequisitionMetadataServiceCoroutineStub(instrumentedMetadataChannel)
    val eventGroupsStub = EventGroupsCoroutineStub(instrumentedCmmsChannel)
    val edpPrivateKey = checkNotNull(File(dataProviderConfig.edpPrivateKeyPath))

    val requisitionsValidator = RequisitionsValidator(loadPrivateKey(edpPrivateKey))

    val requisitionGrouper =
      RequisitionGrouperByReportId(
        requisitionsValidator,
        dataProviderConfig.dataProvider,
        requisitionBlobPrefix,
        requisitionMetadataStub,
        storageClient,
        pageSize,
        dataProviderConfig.storagePathPrefix,
        MinimumIntervalThrottler(Clock.systemUTC(), grpcRequestInterval),
        eventGroupsStub,
        requisitionsStub,
      )

    return RequisitionFetcher(
      requisitionsStub = requisitionsStub,
      storageClient = storageClient,
      dataProviderName = dataProviderConfig.dataProvider,
      storagePathPrefix = dataProviderConfig.storagePathPrefix,
      requisitionGrouper = requisitionGrouper,
      responsePageSize = pageSize,
    )
  }

  /**
   * Creates a [StorageClient] based on the current environment and the provided data provider
   * configuration.
   *
   * @param dataProviderConfig The configuration object for a `DataProvider`.
   * @return A [StorageClient] instance, either for local file system access or GCS access.
   */
  // @TODO(@marcopremier): This function may share the logic of
  // `ResultsFulfillerAppImpl.createStorageClient` method.
  private fun createStorageClient(
    dataProviderConfig: DataProviderRequisitionConfig
  ): StorageClient {
    return if (!fileSystemPath.isNullOrEmpty()) {
      FileSystemStorageClient(File(EnvVars.checkIsPath("REQUISITION_FILE_SYSTEM_PATH")))
    } else {
      val gcsConfig = dataProviderConfig.requisitionStorage.gcs
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

  private fun createRequisitionBlobPrefix(
    dataProviderConfig: DataProviderRequisitionConfig
  ): String {
    return if (!fileSystemPath.isNullOrEmpty()) {
      "$fileSystemPath"
    } else {
      val gcsConfig = dataProviderConfig.requisitionStorage.gcs
      "gs://${gcsConfig.bucketName}"
    }
  }

  /**
   * Creates an instrumented gRPC channel with mutual TLS and OpenTelemetry tracing.
   *
   * @param tlsParams The TLS security parameters containing certificate paths.
   * @param target The gRPC target address.
   * @param certHost Optional server name to verify in the TLS certificate.
   * @return An instrumented gRPC channel ready for use with stubs.
   */
  private fun createInstrumentedChannel(
    tlsParams: TransportLayerSecurityParams,
    target: String,
    certHost: String?,
  ): io.grpc.Channel {
    val signingCerts = loadSigningCerts(tlsParams)
    val channel =
      buildMutualTlsChannel(target, signingCerts, certHost)
        .withShutdownTimeout(channelShutdownDuration)
    return ClientInterceptors.intercept(channel, grpcTelemetry.newClientInterceptor())
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val ATTR_DATA_PROVIDER_KEY = AttributeKey.stringKey("data_provider_name")
    private val ATTR_STATUS_KEY = AttributeKey.stringKey("status")
    private val ATTR_ERROR_TYPE_KEY = AttributeKey.stringKey("error_type")
    private const val STATUS_SUCCESS = "success"
    private const val STATUS_FAILURE = "failure"
    private const val SPAN_DATA_PROVIDER_EXECUTION = "requisition_fetcher.data_provider"
    private const val EVENT_DATA_PROVIDER_COMPLETED = "requisition_fetcher.data_provider_completed"
    private const val EVENT_DATA_PROVIDER_FAILED = "requisition_fetcher.data_provider_failed"
    private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
    private val metadataStorageTarget = EnvVars.checkNotNullOrEmpty("METADATA_STORAGE_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")
    private val metadataStorageCertHost: String? = System.getenv("METADATA_STORAGE_CERT_HOST")
    private val fileSystemPath: String? = System.getenv("REQUISITION_FILE_SYSTEM_PATH")
    private const val DEFAULT_GRCP_INTERVAL = "1s"
    private val grpcRequestInterval: Duration =
      (System.getenv("GRPC_REQUEST_INTERVAL") ?: DEFAULT_GRCP_INTERVAL).toDuration()
    private const val DEFAULT_PAGE_SIZE = 50

    init {
      EdpaTelemetry.ensureInitialized()
    }

    /**
     * OpenTelemetry gRPC instrumentation using the global instance initialized by [EdpaTelemetry].
     */
    private val grpcTelemetry by lazy { GrpcTelemetry.create(Instrumentation.openTelemetry) }

    val pageSize = run {
      val envPageSize = System.getenv("PAGE_SIZE")
      if (!envPageSize.isNullOrEmpty()) {
        envPageSize.toInt()
      } else {
        DEFAULT_PAGE_SIZE
      }
    }

    private const val GRPC_CHANNEL_SHUTDOWN_DURATION_SECONDS: Long = 3L
    private val channelShutdownDuration =
      Duration.ofSeconds(
        System.getenv("GRPC_CHANNEL_SHUTDOWN_DURATION_SECONDS")?.toLong()
          ?: GRPC_CHANNEL_SHUTDOWN_DURATION_SECONDS
      )

    private const val CONFIG_BLOB_KEY = "requisition-fetcher-config.textproto"
    private val requisitionFetcherConfig by lazy {
      runBlocking {
        getConfigAsProtoMessage(CONFIG_BLOB_KEY, RequisitionFetcherConfig.getDefaultInstance())
      }
    }

    /**
     * Loads [SigningCerts] from PEM-encoded certificate, private key, and trusted certificate
     * collection files specified in the given [dataProviderConfig].
     *
     * @param transportLayerSecurityParams The connection param object.
     * @return A [SigningCerts] instance loaded from the specified PEM files.
     * @throws IllegalStateException if any of the required file paths are missing in the
     *   configuration.
     */
    private fun loadSigningCerts(
      transportLayerSecurityParams: TransportLayerSecurityParams
    ): SigningCerts {
      return SigningCerts.fromPemFiles(
        certificateFile = checkNotNull(File(transportLayerSecurityParams.certFilePath)),
        privateKeyFile = checkNotNull(File(transportLayerSecurityParams.privateKeyFilePath)),
        trustedCertCollectionFile =
          checkNotNull(File(transportLayerSecurityParams.certCollectionFilePath)),
      )
    }

    fun validateConfig(dataProviderConfig: DataProviderRequisitionConfig) {
      require(dataProviderConfig.dataProvider.isNotBlank()) { "Missing 'data_provider' in config." }

      require(dataProviderConfig.hasRequisitionStorage()) {
        "Missing 'requisition_storage' in config for data provider: ${dataProviderConfig.dataProvider}."
      }

      require(
        dataProviderConfig.requisitionStorage.hasGcs() ||
          dataProviderConfig.requisitionStorage.hasFileSystem()
      ) {
        "Invalid 'requisition_storage': must specify either GCS or FileSystem storage for data provider: ${dataProviderConfig.dataProvider}."
      }

      require(dataProviderConfig.storagePathPrefix.isNotBlank()) {
        "Missing 'storage_path_prefix' for data provider: ${dataProviderConfig.dataProvider}."
      }

      require(dataProviderConfig.edpPrivateKeyPath.isNotBlank()) {
        "Missing 'edp_private_key_path' for data provider: ${dataProviderConfig.dataProvider}."
      }

      require(dataProviderConfig.hasCmmsConnection()) {
        "Missing 'cmms_connection' for data provider: ${dataProviderConfig.dataProvider}."
      }

      val tls = dataProviderConfig.cmmsConnection
      require(tls.certFilePath.isNotBlank()) {
        "Missing 'cert_file_path' in cmms_connection for data provider: ${dataProviderConfig.dataProvider}."
      }
      require(tls.privateKeyFilePath.isNotBlank()) {
        "Missing 'private_key_file_path' in cmms_connection for data provider: ${dataProviderConfig.dataProvider}."
      }
      require(tls.certCollectionFilePath.isNotBlank()) {
        "Missing 'cert_collection_file_path' in cmms_connection for data provider: ${dataProviderConfig.dataProvider}."
      }
    }
  }
}
