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
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.storage.StorageClient

/**
 * Fetches requisitions from the Kingdom and persists them into GCS.
 *
 * @param requisitionsStub used to pull [Requisition]s from the kingdom
 * @param storageClient client used to store [Requisition]s
 * @param dataProviderName of the EDP for which [Requisition]s will be retrieved
 * @param storagePathPrefix the blob key prefix to use when storing a [Requisition]
 * @param requisitionGrouper the instance of [RequisitionGrouper] to use to group requisitions
 * @param groupedRequisitionsIdGenerator deterministic ID generator
 * @param responsePageSize
 */
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val storageClient: StorageClient,
  private val dataProviderName: String,
  private val storagePathPrefix: String,
  private val requisitionGrouper: RequisitionGrouper,
  val groupedRequisitionsIdGenerator: (GroupedRequisitions) -> String,
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
  @OptIn(ExperimentalCoroutinesApi::class) // For `flattenConcat`.
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

    val groupedRequisition: List<GroupedRequisitions> =
      requisitionGrouper.groupRequisitions(requisitions.toList())
    val storedRequisitions: Int = storeRequisitions(groupedRequisition)

    logger.info {
      "$storedRequisitions unfulfilled grouped requisitions have been persisted to storage for $dataProviderName"
    }
  }

  /**
   * Stores a list of grouped requisitions in persistent storage.
   *
   * @param groupedRequisitions A list of grouped requisitions to be stored.
   * @return The number of grouped requisitions successfully stored.
   */
  private suspend fun storeRequisitions(groupedRequisitions: List<GroupedRequisitions>): Int {
    var storedGroupedRequisitions = 0
    groupedRequisitions.forEach { groupedRequisition: GroupedRequisitions ->
      val groupedRequisitionId = groupedRequisitionsIdGenerator(groupedRequisition)
      val blobKey = "$storagePathPrefix/${groupedRequisitionId}"

      // TODO(@marcopremier): Add mechanism to check whether requisitions inside grouped
      // requisitions where stored already.
      if (
        groupedRequisition.requisitionsList.isNotEmpty() && storageClient.getBlob(blobKey) == null
      ) {
        storageClient.writeBlob(blobKey, Any.pack(groupedRequisition).toByteString())
        storedGroupedRequisitions += 1
      }
    }

    return storedGroupedRequisitions
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
