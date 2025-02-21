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

package org.wfanet.measurement.eventdataprovider.edpaggregator.requisitionfetcher

import com.google.cloud.functions.HttpFunction
import com.google.cloud.functions.HttpRequest
import com.google.cloud.functions.HttpResponse
import com.google.cloud.storage.StorageOptions
import kotlin.io.path.Path
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient


class RequisitionFetcherFunction : HttpFunction {
  override fun service(request: HttpRequest, response: HttpResponse) {
    runBlocking { requisitionFetcher.executeRequisitionFetchingWorkflow() }
  }

  companion object {
    val publicChannel =
      buildMutualTlsChannel(System.getenv("TARGET"), getClientCerts(), System.getenv("CERT_HOST"))

    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    val requisitionsStorageClient =
      GcsStorageClient(
        StorageOptions.newBuilder()
          .setProjectId(System.getenv("REQUISITIONS_GCS_PROJECT_ID"))
          .build()
          .service,
        System.getenv("REQUISITIONS_GCS_BUCKET"),
      )

    val requisitionFetcher =
      RequisitionFetcher(
        requisitionsStub,
        requisitionsStorageClient,
        System.getenv("DATAPROVIDER_NAME"),
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
