/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import io.grpc.Status
import io.grpc.StatusException
import java.util.logging.Logger
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Dispatches a stored group of requisitions to the Secure Computation Control Plane. */
interface RequisitionWorkItemDispatcher {
  /** Returns the deterministic WorkItem resource name for [groupId]. */
  fun workItemName(groupId: String): String

  /** Creates the WorkItem that will process [blobUri]. */
  suspend fun dispatch(groupId: String, blobUri: String)
}

/** [RequisitionWorkItemDispatcher] backed by the Secure Computation WorkItems API. */
class SecureComputationRequisitionWorkItemDispatcher(
  private val workItemsStub: WorkItemsCoroutineStub,
  private val queue: String,
  private val resultsFulfillerParams: ResultsFulfillerParams,
  private val controlPlaneThrottler: Throttler,
) : RequisitionWorkItemDispatcher {

  override fun workItemName(groupId: String): String = "workItems/${workItemId(groupId)}"

  override suspend fun dispatch(groupId: String, blobUri: String) {
    val workItemId = workItemId(groupId)
    val workItemName = workItemName(groupId)
    try {
      controlPlaneThrottler.onReady {
        workItemsStub.getWorkItem(getWorkItemRequest { name = workItemName })
      }
      logger.info("WorkItem $workItemId already exists; treating dispatch as successful")
      return
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.NOT_FOUND) throw e
    }

    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = workItem {
        queue = this@SecureComputationRequisitionWorkItemDispatcher.queue
        workItemParams =
          workItemParams {
              appParams =
                this@SecureComputationRequisitionWorkItemDispatcher.resultsFulfillerParams.pack()
              dataPathParams = dataPathParams { dataPath = blobUri }
            }
            .pack()
      }
    }
    try {
      controlPlaneThrottler.onReady { workItemsStub.createWorkItem(request) }
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        logger.info(
          "WorkItem $workItemId was created concurrently; treating dispatch as successful"
        )
        return
      }
      throw e
    }
    logger.info("Created WorkItem $workItemId for requisition group $groupId")
  }

  private fun workItemId(groupId: String): String = "$WORK_ITEM_ID_PREFIX-$groupId"

  companion object {
    private val logger =
      Logger.getLogger(SecureComputationRequisitionWorkItemDispatcher::class.java.name)
    private const val WORK_ITEM_ID_PREFIX = "results-fulfiller"
  }
}
