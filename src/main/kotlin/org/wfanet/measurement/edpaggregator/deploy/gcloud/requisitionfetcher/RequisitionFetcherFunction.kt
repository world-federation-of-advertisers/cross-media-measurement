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
import java.io.File
import java.security.MessageDigest
import java.time.Clock
import java.time.Duration
import java.util.Base64
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.edpaggregator.CloudFunctionConfig.getConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.common.toDuration
import org.wfanet.measurement.config.edpaggregator.DataProviderRequisitionConfig
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
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

  override fun service(request: HttpRequest, response: HttpResponse) {

    val errors = mutableListOf<String>()
    for (dataProviderConfig in requisitionFetcherConfig.configsList) {

      try {
        validateConfig(dataProviderConfig)
        val requisitionFetcher = createRequisitionFetcher(dataProviderConfig)
        runBlocking { requisitionFetcher.fetchAndStoreRequisitions() }
      } catch (e: IllegalArgumentException) {
        val errorMsg = "Invalid config for data provider: ${dataProviderConfig.dataProvider}"
        errors.add(errorMsg)
        logger.log(Level.SEVERE, errorMsg, e)
      } catch (e: Exception) {
        val errorMsg =
          "Failed to fetch and store requisitions for ${dataProviderConfig.dataProvider}"
        errors.add(errorMsg)
        logger.log(Level.SEVERE, errorMsg, e)
      }

      if (errors.isNotEmpty()) {
        response.setStatusCode(500)
        response.writer.write("Completed with errors:\n" + errors.joinToString("\n"))
      } else {
        response.setStatusCode(200)
        response.writer.write("All requisitions fetched successfully")
      }
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
    val signingCerts = loadSigningCerts(dataProviderConfig)
    val publicChannel by lazy {
      buildMutualTlsChannel(kingdomTarget, signingCerts, kingdomCertHost)
    }

    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    val eventGroupsStub = EventGroupsCoroutineStub(publicChannel)
    val edpPrivateKey = checkNotNull(File(dataProviderConfig.edpPrivateKeyPath))

    val requisitionsValidator =
      RequisitionsValidator(loadPrivateKey(edpPrivateKey)) { requisition, refusal ->
        runBlocking {
          logger.info("Refusing ${requisition.name}: $refusal")
          refuseRequisition(requisitionsStub, requisition, refusal)
        }
      }

    val requisitionGrouper =
      RequisitionGrouperByReportId(
        requisitionsValidator,
        eventGroupsStub,
        requisitionsStub,
        MinimumIntervalThrottler(Clock.systemUTC(), grpcRequestInterval),
      )

    return RequisitionFetcher(
      requisitionsStub = requisitionsStub,
      storageClient = storageClient,
      dataProviderName = dataProviderConfig.dataProvider,
      storagePathPrefix = dataProviderConfig.storagePathPrefix,
      requisitionGrouper = requisitionGrouper,
      groupedRequisitionsIdGenerator = ::createDeterministicId,
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

  private suspend fun refuseRequisition(
    requisitionsStub: RequisitionsCoroutineStub,
    requisition: Requisition,
    refusal: Requisition.Refusal,
  ) {
    try {
      logger.info("Requisition ${requisition.name} was refused. $refusal")
      val request = refuseRequisitionRequest {
        this.name = requisition.name
        this.refusal = refusal { justification = refusal.justification }
      }
      requisitionsStub.refuseRequisition(request)
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error while refusing requisition ${requisition.name}", e)
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")
    private val fileSystemPath: String? = System.getenv("REQUISITION_FILE_SYSTEM_PATH")
    private const val DEFAULT_GRCP_INTERVAL = "1s"
    private val grpcRequestInterval: Duration =
      (System.getenv("GRPC_REQUEST_INTERVAL") ?: DEFAULT_GRCP_INTERVAL).toDuration()

    val pageSize = run {
      val envPageSize = System.getenv("PAGE_SIZE")
      if (!envPageSize.isNullOrEmpty()) {
        envPageSize.toInt()
      } else {
        null
      }
    }

    private const val CONFIG_BLOB_KEY = "requisition-fetcher-config.textproto"
    private val requisitionFetcherConfig by lazy {
      runBlocking { getConfig(CONFIG_BLOB_KEY, RequisitionFetcherConfig.getDefaultInstance()) }
    }

    fun createDeterministicId(groupedRequisition: GroupedRequisitions): String {
      val requisitionNames =
        groupedRequisition.requisitionsList
          .mapNotNull { entry ->
            val requisition = entry.requisition.unpack(Requisition::class.java)
            requisition.name
          }
          .sorted()

      val concatenated = requisitionNames.joinToString(separator = "|")
      val digest = MessageDigest.getInstance("SHA-256").digest(concatenated.toByteArray())
      return Base64.getUrlEncoder().withoutPadding().encodeToString(digest)
    }

    /**
     * Loads [SigningCerts] from PEM-encoded certificate, private key, and trusted certificate
     * collection files specified in the given [dataProviderConfig].
     *
     * @param dataProviderConfig The configuration object.
     * @return A [SigningCerts] instance loaded from the specified PEM files.
     * @throws IllegalStateException if any of the required file paths are missing in the
     *   configuration.
     */
    private fun loadSigningCerts(dataProviderConfig: DataProviderRequisitionConfig): SigningCerts {
      val cmms = dataProviderConfig.cmmsConnection
      return SigningCerts.fromPemFiles(
        certificateFile = checkNotNull(File(cmms.certFilePath)),
        privateKeyFile = checkNotNull(File(cmms.privateKeyFilePath)),
        trustedCertCollectionFile = checkNotNull(File(cmms.certCollectionFilePath)),
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
