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

import com.google.protobuf.Any
import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.storage.StorageClient

/**
 * Fetches requisitions from the Kingdom and persists them into GCS.
 *
 * @param requisitionsStub used to pull [Requisition]s from the kingdom
 * @param storageClient client used to store [Requisition]s
 * @param dataProviderName of the EDP for which [Requisition]s will be retrieved
 */
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val storageClient: StorageClient,
  private val dataProviderName: String,
  private val storagePathPrefix: String,
  private val responsePageSize: Int? = null,
) {

  /**
   * Fetches and stores unfulfilled requisitions from a data provider.
   *
   * This method executes a workflow to retrieve requisitions that are in the "UNFULFILLED" state
   * from a specified data provider and stores them in persistent storage. It handles pagination by
   * using a `pageToken` to fetch all available pages of requisitions.
   *
   * @throws Exception if there is an error while listing requisitions.
   */
  suspend fun fetchAndStoreRequisitions() {
    logger.info("Executing requisitionFetchingWorkflow for $dataProviderName...")

    var requisitionsCount = 0

    // TODO(world-federation-of-advertisers/cross-media-measurement#2095): Update logic once we have
    // a more efficient way to pull only the Requisitions that have not been stored in storage.
    val requisitions: Flow<Requisition> =
      requisitionsStub
        .listResources { pageToken: String ->
          val request = listRequisitionsRequest {
            parent = dataProviderName
            filter = ListRequisitionsRequestKt.filter { states += Requisition.State.UNFULFILLED }
            if (responsePageSize != null) {
              pageSize = responsePageSize
            }
            this.pageToken = pageToken
          }
          val response: ListRequisitionsResponse =
            try {
              requisitionsStub.listRequisitions(request)
            } catch (e: StatusException) {
              throw Exception("Error listing requisitions", e)
            }
          requisitionsCount += response.requisitionsList.size
          ResourceList(response.requisitionsList, response.nextPageToken)
        }
        .flattenConcat()

    val storedRequisitions: Int = storeRequisitions(requisitions)

    logger.fine {
      "$storedRequisitions unfulfilled requisitions have been persisted to storage for $dataProviderName"
    }
  }

  /**
   * Stores a flow of requisitions in persistent storage.
   *
   * This method collects requisitions from a provided flow and stores each one in storage if it
   * does not already exist. The existence check is performed by verifying if a blob with the
   * requisition's name (used as a unique key) is already present in the storage. If the blob does
   * not exist, the requisition is serialized and written to the storage.
   *
   * @param requisitions A flow of requisitions to be stored.
   * @return The number of requisitions successfully stored.
   */
  private suspend fun storeRequisitions(requisitions: Flow<Requisition>): Int {
    var storedRequisitions = 0
    requisitions.collect { requisition ->
      val blobKey = "$storagePathPrefix/${requisition.name}"

      // Only stores the requisition if it does not already exist in storage by checking if
      // the blob key(created using the requisition name, ensuring uniqueness) is populated.
      if (storageClient.getBlob(blobKey) == null) {
        storageClient.writeBlob(blobKey, Any.pack(requisition).toByteString())
        storedRequisitions += 1
      }
    }

    return storedRequisitions
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
