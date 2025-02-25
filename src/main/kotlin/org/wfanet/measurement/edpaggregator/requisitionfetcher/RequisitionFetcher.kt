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

import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.edpaggregator.requisitionfetcher.requisitionsList
import org.wfanet.measurement.storage.StorageClient

/**
 * Fetches requisitions from the Kingdom and persists them into GCS.
 *
 * @param requisitionsStub used to pull [Requisition]s from the kingdom
 * @param gcsStorageClient used to store new [Requisition]s
 * @param dataProviderName of the EDP for which [Requisition]s will be retrieved
 */
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val storageClient: StorageClient,
  private val dataProviderName: String,
) {
  suspend fun executeRequisitionFetchingWorkflow() {
    logger.info("Executing requisitionFetchingWorkflow for $dataProviderName...")

    val requisitions = fetchRequisitions()

    logger.fine {"${requisitions.size} unfulfilled requisitions retrieved for $dataProviderName"}

    if (requisitions.isNotEmpty()) {
      storeRequisitions(requisitions)
    }
  }

  private suspend fun fetchRequisitions(): List<Requisition> {
    // TODO: Update logic once we have a more efficient way to pull only the Requisitions that have
    // not been stored in storage.
    val request = listRequisitionsRequest {
      parent = dataProviderName
      filter = ListRequisitionsRequestKt.filter { states += Requisition.State.UNFULFILLED }
    }

    try {
      return requisitionsStub.listRequisitions(request).requisitionsList
    } catch (e: StatusException) {
      throw Exception("Error listing requisitions", e)
    }
  }

  private suspend fun storeRequisitions(requisitions: List<Requisition>) {
    val requisitionsList = mutableListOf<Requisition>()
    for (requisition in requisitions) {
      val blobKey = requisition.name
      // Only stores the requisition if it does not already exist in the GCS bucket by checking if
      // the blob key(created using the requisition name, ensuring uniqueness) is populated.
      if (storageClient.getBlob(blobKey) == null) {
        requisitionsList.add(requisition)
      }
      if(requisitionsList.size == REQUISITION_CHUNK_SIZE) {
        storageClient.writeBlob(blobKey, requisitionsList { requisitions += requisitionsList }.toByteString())
        requisitionsList.clear()
      }
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    // TODO: Increase chunk size once design is finalized
    // The number of Requisitions that will be persisted to a blob in storage
    private const val REQUISITION_CHUNK_SIZE = 1
  }
}
