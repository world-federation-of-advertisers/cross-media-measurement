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
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.grpc.buildTlsChannel
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.KingdomConfig
import org.wfanet.measurement.securecomputation.requisitions.v1alpha.StorageConfig
import com.google.cloud.functions.CloudEventsFunction
import io.cloudevents.CloudEvent
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.runBlocking


// 1. Polls for new requisitions
// 2. Stores new requisitions into Google Cloud Storage
class RequisitionFetcher(
  private val kingdomConfig: KingdomConfig,
  private val storageConfig: StorageConfig,
): CloudEventsFunction {

  override fun accept(event: CloudEvent) {
    runBlocking {
      storeRequisitions(fetchRequisitions())
    }
  }

  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
  private suspend fun fetchRequisitions(): Flow<Requisition> {
    val publicChannel = buildTlsChannel(
      kingdomConfig.publicApiTarget,
      readCertificateCollection(File(kingdomConfig.certCollectionPath)),
      kingdomConfig.publicApiCertHost,
    )
    val requisitionsStub = RequisitionsCoroutineStub(publicChannel)

    return requisitionsStub
      .withAuthenticationKey(kingdomConfig.apiAuthenticationKey)
      .listResources { pageToken ->
        val response: ListRequisitionsResponse =
          try {
            listRequisitions(listRequisitionsRequest {
              parent = kingdomConfig.dataProvider
              this.pageToken = pageToken
            })
          } catch (e: StatusException) {
            throw Exception("Unable to list requisitions.", e)
          }
        ResourceList(response.requisitionsList, response.nextPageToken)
      }
      .flattenConcat()
      .filter { it.updateTime.seconds > storageConfig.lastUpdate.seconds }
  }

  private suspend fun storeRequisitions(requisitions: Flow<Requisition>) {
    val storageClient = GcsStorageClient(
      StorageOptions.newBuilder().setProjectId(storageConfig.project).build().service,
      storageConfig.bucket
    )
    storageClient.writeBlob(storageConfig.blobUri, requisitions.map { it.toByteString() })
  }
}


