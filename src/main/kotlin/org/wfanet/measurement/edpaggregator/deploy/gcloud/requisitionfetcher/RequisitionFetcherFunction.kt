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
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.getJarResourceFile
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.edpaggregator.requisitionfetcher.RequisitionFetcher
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

class RequisitionFetcherFunction : HttpFunction {

  override fun service(request: HttpRequest, response: HttpResponse) {
    for (dataProviderConfig in requisitionFetcherConfig.configsList) {

      val fileSystemPath = System.getenv("REQUISITION_FILE_SYSTEM_PATH")
      // 'FileSystemStorageClient' is used for testing purposes only and used by [RequisitionFetcherFunctionTest]
      // in order to pull requisitions from local storage.
      val requisitionsStorageClient =
        if (!fileSystemPath.isNullOrEmpty()) {
          FileSystemStorageClient(File(checkIsPath("REQUISITION_FILE_SYSTEM_PATH")))
        } else {
          val requisitionsGcsBucket = dataProviderConfig.gcsStorageDetails.bucketName
          GcsStorageClient(
            StorageOptions.newBuilder()
              .also {
                if (dataProviderConfig.gcsStorageDetails.projectId.isNotEmpty()) {
                  it.setProjectId(dataProviderConfig.gcsStorageDetails.projectId)
                }
              }
              .build()
              .service,
            requisitionsGcsBucket,
          )
        }
      val signingCerts =
        SigningCerts.fromPemFiles(
          certificateFile =
            checkNotNull(
              CLASS_LOADER.getJarResourceFile(
                dataProviderConfig.connectionDetails.certJarResourcePath
              )
            ),
          privateKeyFile =
            checkNotNull(
              CLASS_LOADER.getJarResourceFile(
                dataProviderConfig.connectionDetails.privateKeyJarResourcePath
              )
            ),
          trustedCertCollectionFile =
            checkNotNull(
              CLASS_LOADER.getJarResourceFile(
                dataProviderConfig.connectionDetails.certCollectionJarResourcePath
              )
            ),
        )
      val publicChannel by lazy {
        buildMutualTlsChannel(kingdomTarget, signingCerts, kingdomCertHost)
      }

      val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
      val requisitionFetcher =
        RequisitionFetcher(
          requisitionsStub,
          requisitionsStorageClient,
          dataProviderConfig.dataProvider,
          dataProviderConfig.storagePathPrefix,
          pageSize,
        )

      runBlocking { requisitionFetcher.fetchAndStoreRequisitions() }
    }
  }

  companion object {
    private val kingdomTarget = checkNotEmpty("KINGDOM_TARGET")
    private val kingdomCertHost: String? = System.getenv("KINGDOM_CERT_HOST")

    val pageSize = run {
      val envPageSize = System.getenv("PAGE_SIZE")
      if (!envPageSize.isNullOrEmpty()) {
        envPageSize.toInt()
      } else {
        null
      }
    }
    private val CLASS_LOADER: ClassLoader = Thread.currentThread().contextClassLoader
    private val requisitionFetcherConfigResourcePath =
      checkIsPath("REQUISITION_FETCHER_CONFIG_RESOURCE_PATH")
    private val config by lazy {
      checkNotNull(CLASS_LOADER.getJarResourceFile(requisitionFetcherConfigResourcePath))
    }
    private val requisitionFetcherConfig: RequisitionFetcherConfig by lazy {
      runBlocking { parseTextProto(config, RequisitionFetcherConfig.getDefaultInstance()) }
    }

    private fun checkNotEmpty(envVar: String): String {
      val value = System.getenv(envVar)
      checkNotNull(value) { "Missing env var: $envVar" }
      check(value.isNotBlank())
      return value
    }

    private fun checkIsPath(envVar: String): String {
      val value = System.getenv(envVar)
      Paths.get(value)
      return value
    }
  }
}
