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
import java.time.Clock
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.EnvVars
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.edpaggregator.CloudFunctionConfig.getConfig
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionGrouperByReportId
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionsValidator

class RequisitionFetcherFunction : HttpFunction {

  override fun service(request: HttpRequest, response: HttpResponse) {

    for (dataProviderConfig in requisitionFetcherConfig.configsList) {

      val fileSystemPath = System.getenv("REQUISITION_FILE_SYSTEM_PATH")
      // 'FileSystemStorageClient' is used for testing purposes only and used by
      // [RequisitionFetcherFunctionTest]
      // in order to pull requisitions from local storage.
      val requisitionsStorageClient =
        if (!fileSystemPath.isNullOrEmpty()) {
          FileSystemStorageClient(File(EnvVars.checkIsPath("REQUISITION_FILE_SYSTEM_PATH")))
        } else {
          val requisitionsGcsBucket = dataProviderConfig.requisitionStorage.gcs.bucketName
          GcsStorageClient(
            StorageOptions.newBuilder()
              .also {
                if (dataProviderConfig.requisitionStorage.gcs.projectId.isNotEmpty()) {
                  it.setProjectId(dataProviderConfig.requisitionStorage.gcs.projectId)
                }
              }
              .build()
              .service,
            requisitionsGcsBucket,
          )
        }
      val signingCerts =
        SigningCerts.fromPemFiles(
          certificateFile = checkNotNull(File(dataProviderConfig.cmmsConnection.certFilePath)),
          privateKeyFile = checkNotNull(File(dataProviderConfig.cmmsConnection.privateKeyFilePath)),
          trustedCertCollectionFile =
            checkNotNull(File(dataProviderConfig.cmmsConnection.certCollectionFilePath)),
        )
      val publicChannel by lazy {
        buildMutualTlsChannel(kingdomTarget, signingCerts, kingdomCertHost)
      }
      val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
      val eventGroupsStub = EventGroupsCoroutineStub(publicChannel)
      val edpPrivateKey = checkNotNull(File(dataProviderConfig.edpPrivateKeyPath))
      val requisitionsValidator = RequisitionsValidator(
        loadPrivateKey(edpPrivateKey),
        ::refuseRequisition
      )
      val requisitionGrouper = RequisitionGrouperByReportId(
        requisitionsValidator,
        eventGroupsStub,
        requisitionsStub,
        MinimumIntervalThrottler(Clock.systemUTC(), Duration.ofSeconds(1L))
      )
      val requisitionFetcher =
        RequisitionFetcher(
          requisitionsStub = requisitionsStub,
          storageClient = requisitionsStorageClient,
          dataProviderName = dataProviderConfig.dataProvider,
          storagePathPrefix = dataProviderConfig.storagePathPrefix,
          requisitionGrouper = requisitionGrouper,
          responsePageSize = pageSize,
        )
      runBlocking { requisitionFetcher.fetchAndStoreRequisitions() }
    }
  }

  private fun refuseRequisition(requisition: Requisition, refusal: Requisition.Refusal) {
    logger.info("Requisition ${requisition.name} was refused. $refusal")
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val kingdomTarget = EnvVars.checkNotNullOrEmpty("KINGDOM_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")

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
  }
}
