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

package org.wfanet.measurement.edpaggregator.requisitionfetcher

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import java.io.File
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.storage.filesystem.FileSystemStorageClient

class RequisitionFetcherFunction : HttpFunction {
  override fun service(request: HttpRequest, response: HttpResponse) {
    runBlocking { requisitionFetcher.fetchAndStoreRequisitions() }
  }

  companion object {
    val publicChannel =
      buildMutualTlsChannel(
        System.getenv("KINGDOM_TARGET"),
        getClientCerts(),
        System.getenv("KINGDOM_CERT_HOST"),
      )

    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)

    val requisitionsStorageClient =
      if (System.getenv("REQUISITION_FILE_SYSTEM_PATH").isNotEmpty()) {
        FileSystemStorageClient(File(System.getenv("REQUISITION_FILE_SYSTEM_PATH")))
      } else {
        GcsStorageClient(
          StorageOptions.newBuilder()
            .setProjectId(System.getenv("REQUISITIONS_GCS_PROJECT_ID"))
            .build()
            .service,
          System.getenv("REQUISITIONS_GCS_BUCKET"),
        )
      }

    val pageSize =
      if (System.getenv("PAGE_SIZE").isNotEmpty()) {
        System.getenv("PAGE_SIZE").toInt()
      } else {
        null
      }

    val requisitionFetcher =
      RequisitionFetcher(
        requisitionsStub,
        requisitionsStorageClient,
        System.getenv("DATA_PROVIDER_NAME"),
        System.getenv("STORAGE_PATH_PREFIX"),
        pageSize,
      )

    private fun getClientCerts(): SigningCerts {
      return SigningCerts.fromPemFiles(
        certificateFile = Path(System.getenv("CERT_FILE_PATH")).toFile(),
        privateKeyFile = Path(System.getenv("PRIVATE_KEY_FILE_PATH")).toFile(),
        trustedCertCollectionFile = Path(System.getenv("CERT_COLLECTION_FILE_PATH")).toFile(),
      )
    }
  }
}
