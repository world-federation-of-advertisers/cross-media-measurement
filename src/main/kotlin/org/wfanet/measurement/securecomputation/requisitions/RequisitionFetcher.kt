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

import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequestKt
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.listRequisitionsRequest
import org.wfanet.measurement.gcloud.gcs.GcsStorageClient

// 1. Polls for new requisitions
// 2. Stores new requisitions into Google Cloud Storage
class RequisitionFetcher(
  private val requisitionsStub: RequisitionsCoroutineStub,
  private val gcsStorageClient: GcsStorageClient,
  private val gcsBucket: String,
  private val dataProviderName: String,
) {
  suspend fun executeRequisitionFetchingWorkflow() {
    logger.info("Executing requisitionFetchingWorkflow for $dataProviderName...")

    val requisitions = fetchRequisitions()

    if (requisitions.isEmpty()) {
      logger.fine("No unfulfilled requisitions for $dataProviderName. Polling again later...")
      return
    }

    storeRequisitions(requisitions)
  }

  private suspend fun fetchRequisitions(): List<Requisition> {
    val request = listRequisitionsRequest {
      parent = dataProviderName
      filter =
        ListRequisitionsRequestKt.filter {
          states += Requisition.State.UNFULFILLED
          measurementStates += Measurement.State.AWAITING_REQUISITION_FULFILLMENT
        }
    }

    try {
      return requisitionsStub.listRequisitions(request).requisitionsList
    } catch (e: StatusException) {
      throw Exception("Error listing requisitions", e)
    }
  }

  private suspend fun storeRequisitions(requisitions: List<Requisition>) {
    for (requisition in requisitions) {
      val blobUri = "gs://${gcsBucket}/${requisition.name}"

      // Only stores the requisition if it does not already exist in the GCS bucket by checking if
      // the blob URI(created
      // using the requisition name, ensuring uniqueness) is populated.
      if (gcsStorageClient.getBlob(blobUri) == null) {
        gcsStorageClient.writeBlob(blobUri, requisition.toByteString())
      }
    }
  }

  companion object {
    val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
