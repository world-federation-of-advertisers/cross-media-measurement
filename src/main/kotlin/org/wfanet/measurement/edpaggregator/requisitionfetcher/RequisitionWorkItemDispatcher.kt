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
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.retryWorkItemRequest
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
    val requestedWorkItem = workItem {
      queue = this@SecureComputationRequisitionWorkItemDispatcher.queue
      workItemParams =
        workItemParams {
            appParams =
              this@SecureComputationRequisitionWorkItemDispatcher.resultsFulfillerParams.pack()
            dataPathParams = dataPathParams { dataPath = blobUri }
          }
          .pack()
    }
    try {
      val existingWorkItem =
        controlPlaneThrottler.onReady {
          workItemsStub.getWorkItem(getWorkItemRequest { name = workItemName })
        }
      validateExistingWorkItem(existingWorkItem, requestedWorkItem)
      if (existingWorkItem.state == WorkItem.State.FAILED) {
        retryFailedWorkItem(existingWorkItem)
        logger.info("Retried failed WorkItem $workItemId for requisition group $groupId")
        return
      }
      logger.info("WorkItem $workItemId already exists; treating dispatch as successful")
      return
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.NOT_FOUND) throw e
    }

    val request = createWorkItemRequest {
      this.workItemId = workItemId
      workItem = requestedWorkItem
    }
    try {
      controlPlaneThrottler.onReady { workItemsStub.createWorkItem(request) }
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        val existingWorkItem =
          controlPlaneThrottler.onReady {
            workItemsStub.getWorkItem(getWorkItemRequest { name = workItemName })
          }
        validateExistingWorkItem(existingWorkItem, requestedWorkItem)
        logger.info(
          "WorkItem $workItemId was created concurrently; treating dispatch as successful"
        )
        return
      }
      throw e
    }
    logger.info("Created WorkItem $workItemId for requisition group $groupId")
  }

  private fun validateExistingWorkItem(existing: WorkItem, requested: WorkItem) {
    check(existing.queue == requested.queue) {
      "WorkItem ${existing.name} uses queue ${existing.queue}, not ${requested.queue}"
    }
    check(existing.workItemParams == requested.workItemParams) {
      "WorkItem ${existing.name} has parameters that do not match this requisition group"
    }
    check(
      existing.state == WorkItem.State.QUEUED ||
        existing.state == WorkItem.State.RUNNING ||
        existing.state == WorkItem.State.FAILED
    ) {
      "WorkItem ${existing.name} is ${existing.state} while requisition metadata remains " +
        "unfinished"
    }
  }

  private suspend fun retryFailedWorkItem(workItem: WorkItem) {
    try {
      controlPlaneThrottler.onReady {
        workItemsStub.retryWorkItem(retryWorkItemRequest { name = workItem.name })
      }
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.FAILED_PRECONDITION) throw e

      // Another fetcher may have won the retry race. Re-read and accept only the active states.
      val current =
        controlPlaneThrottler.onReady {
          workItemsStub.getWorkItem(getWorkItemRequest { name = workItem.name })
        }
      check(current.state == WorkItem.State.QUEUED || current.state == WorkItem.State.RUNNING) {
        "WorkItem ${current.name} is ${current.state} after a concurrent retry"
      }
    }
  }

  private fun workItemId(groupId: String): String = "$WORK_ITEM_ID_PREFIX-$groupId"

  companion object {
    private val logger =
      Logger.getLogger(SecureComputationRequisitionWorkItemDispatcher::class.java.name)
    private const val WORK_ITEM_ID_PREFIX = "results-fulfiller"
  }
}
