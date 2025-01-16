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
package org.wfanet.measurement.securecomputation.requisitions

import com.google.cloud.storage.StorageOptions
import io.grpc.StatusException
import java.io.File
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.forEach
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
//import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
//import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.common.crypto.readCertificateCollection
//import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.grpc.buildTlsChannel
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
//import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.KingdomConfig

class RequisitionFetcher(
  val config: KingdomConfig,
  val blobUri: String // Output location to write requisitions to
) {

  suspend fun fetchRequisitions(): Flow<Requisition> {
    val publicChannel = buildTlsChannel(
      config.publicApiTarget,
      readCertificateCollection(requireNotNull(File(config.certCollectionPath))),
      config.publicApiCertHost,
    )
    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)
    return requisitionsStub
      .withAuthenticationKey(config.apiAuthenticationKey)
      .listResources { pageToken ->
        val response: ListRequisitionsResponse =
          try {
            listRequisitions(listRequisitionsRequest {
              parent = config.dataProvider
              this.pageToken = pageToken
            })
          } catch (e: StatusException) {
            throw Exception("Unable to list requisitions.", e)
          }
        ResourceList(response.requisitionsList, response.nextPageToken)
      }
      .flattenConcat()
  }

  suspend fun storeRequisitions(requisitions: Flow<Requisition>) {
    val storageClient = GcsStorageClient(
      StorageOptions.newBuilder().setProjectId(config.googleCloudStorageProject).build().service,
      config.googleCloudStorageBucket
    )
    storageClient.writeBlob(blobUri, requisitions.map { it.toByteString() })
  }

  suspend fun run() {
    val requisitions = fetchRequisitions()
    storeRequisitions(requisitions)
  }
}

// question: who are we fetching requisitions for? is it an mc? how does this fit into the datawatcher architecture?



