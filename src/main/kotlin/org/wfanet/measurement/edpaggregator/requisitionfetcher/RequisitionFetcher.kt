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
import com.google.protobuf.Timestamp
import io.grpc.StatusException
import java.util.logging.Logger
import kotlinx.coroutines.ExperimentalCoroutinesApi
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.flattenConcat
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.v1alpha.GroupedRequisitions
import org.wfanet.measurement.edpaggregator.v1alpha.RequisitionMetadataServiceGrpcKt.RequisitionMetadataServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.fetchLatestCmmsCreateTimeRequest
import org.wfanet.measurement.edpaggregator.v1alpha.requisitionMetadata
import org.wfanet.measurement.edpaggregator.v1alpha.createRequisitionMetadataRequest
import org.wfanet.measurement.edpaggregator.v1alpha.refuseRequisitionMetadataRequest
import org.wfanet.measurement.api.v2alpha.refuseRequisitionRequest
import org.wfanet.measurement.storage.StorageClient
import java.util.logging.Level

/**
 * Fetches requisitions from the Kingdom and persists them into GCS.
 *
 * @param requisitionsStub used to pull [Requisition]s from the kingdom
 * @param requisitionMetadataStub used to sync [Requisition]s with RequisitionMetadataStorage
 * @param storageClient client used to store [Requisition]s
 * @param dataProviderName of the EDP for which [Requisition]s will be retrieved
 * @param storagePathPrefix the blob key prefix to use when storing a [Requisition]
 * @param requisitionBlobPrefix the blob key prefix including schema and bucket name
 * @param requisitionGrouper the instance of [RequisitionGrouper] to use to group requisitions
 * @param groupedRequisitionsIdGenerator deterministic ID generator
 * @param responsePageSize
 */
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val requisitionMetadataStub: RequisitionMetadataServiceCoroutineStub,
  private val storageClient: StorageClient,
  private val dataProviderName: String,
  private val storagePathPrefix: String,
  private val requisitionBlobPrefix: String,
  private val requisitionGrouper: RequisitionGrouper,
  val groupedRequisitionsIdGenerator: () -> String,
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

    // Filter requisitions excluding those that have not been written into RequisitionMetadataStorage yet.
    val fetchLatestCmmsCreateTimeRequest = fetchLatestCmmsCreateTimeRequest {
      parent = dataProviderName
    }
    val latestCmmsCreateTime = requisitionMetadataStub.fetchLatestCmmsCreateTime(fetchLatestCmmsCreateTimeRequest)

    // Filter requisitions that have been persisted already
    val groupedRequisitionsResult: List<RequisitionGrouper.GroupedRequisitionsWrapper> =
      requisitionGrouper.groupRequisitions(requisitions.filterNewerThan(latestCmmsCreateTime).toList())
    val storedRequisitions: Int = processRequisitions(groupedRequisitionsResult)

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
  private suspend fun processRequisitions(groupedRequisitionsWrappers: List<RequisitionGrouper.GroupedRequisitionsWrapper>): Int {
    var storedGroupedRequisitions = 0

    groupedRequisitionsWrappers.forEach { wrapper: RequisitionGrouper.GroupedRequisitionsWrapper ->
      val groupedRequisitionId = groupedRequisitionsIdGenerator()
      val blobKey = "$storagePathPrefix/$groupedRequisitionId"
      val requisitionBlobUri = "$requisitionBlobPrefix/$blobKey"
      wrapper.groupedRequisitions?.let {
        storageClient.writeBlob(blobKey, Any.pack(wrapper.groupedRequisitions).toByteString())
        storedGroupedRequisitions++
      }

      wrapper.requisitions.forEach { requisitionWrapper ->
        val metadata = requisitionMetadata {
          cmmsRequisition = requisitionWrapper.requisition.name
          blobUri = requisitionBlobUri
          blobTypeUrl = GROUPED_REQUISITION_BLOB_TYPE_URL
          groupId = groupedRequisitionId
          cmmsCreateTime = requisitionWrapper.requisition.updateTime
          this.report = wrapper.reportId
        }
        // TODO(@marcopremier): replace with batch create once the method is available
        val request = createRequisitionMetadataRequest {
          parent = dataProviderName
          requisitionMetadata = metadata
          requestId = groupedRequisitionId
        }
        requisitionMetadataStub.createRequisitionMetadata(request)
        if (requisitionWrapper.status == RequisitionGrouper.RequisitionValidationStatus.INVALID) {
          val request = refuseRequisitionMetadataRequest {
            name = metadata.name
            etag = metadata.etag
            refusalMessage = requisitionWrapper.refusal!!.message
          }
          requisitionMetadataStub.refuseRequisitionMetadata(request)

          refuseRequisition(requisitionWrapper.requisition, requisitionWrapper.refusal!!)
        }
      }
    }
    return storedGroupedRequisitions
  }

  protected suspend fun refuseRequisition(requisition: Requisition, refusal: Requisition.Refusal) {
    try {
      logger.info("Requisition ${requisition.name} was refused. $refusal")
      val request = refuseRequisitionRequest {
        this.name = requisition.name
        this.refusal = RequisitionKt.refusal {
          message = refusal.message
          justification = refusal.justification
        }
      }
      requisitionsStub.refuseRequisition(request)
    } catch (e: Exception) {
      logger.log(Level.SEVERE, "Error while refusing requisition ${requisition.name}", e)
    }
  }

  fun Flow<Requisition>.filterNewerThan(reference: Timestamp): Flow<Requisition> =
    this.filter { requisition ->
      requisition.updateTime.isAfter(reference)
    }

  fun Timestamp.isAfter(other: Timestamp): Boolean {
    return when {
      this.seconds > other.seconds -> true
      this.seconds < other.seconds -> false
      else -> this.nanos > other.nanos
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val GROUPED_REQUISITION_BLOB_TYPE_URL = "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.GroupedRequisitions"
  }
}
