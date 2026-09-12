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
import org.wfanet.measurement.common.telemetry.W3CTraceContext
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.WorkItemParamsKt.dataPathParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.ensureWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/** Dispatches a stored group of requisitions to the Secure Computation Control Plane. */
interface RequisitionWorkItemDispatcher {
  /** Returns the deterministic WorkItem resource name for [groupId]. */
  fun workItemName(groupId: String): String

  /** Ensures the WorkItem that will process [blobUri]. */
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
    val requestedWorkItem = workItem {
      queue = this@SecureComputationRequisitionWorkItemDispatcher.queue
      workItemParams =
        workItemParams {
            appParams =
              this@SecureComputationRequisitionWorkItemDispatcher.resultsFulfillerParams.pack()
            dataPathParams = dataPathParams { dataPath = blobUri }
            traceContext.putAll(W3CTraceContext.inject())
          }
          .pack()
    }
    val request = ensureWorkItemRequest {
      this.workItemId = workItemId
      workItem = requestedWorkItem
    }
    val ensured =
      try {
        controlPlaneThrottler.onReady { workItemsStub.ensureWorkItem(request) }
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
        val existing =
          controlPlaneThrottler.onReady {
            workItemsStub.getWorkItem(getWorkItemRequest { name = workItemName(groupId) })
          }
        val existingParams = validateExistingWorkItem(existing, requestedWorkItem)
        controlPlaneThrottler.onReady {
          workItemsStub.ensureWorkItem(
            ensureWorkItemRequest {
              this.workItemId = workItemId
              workItem = workItem {
                queue = requestedWorkItem.queue
                workItemParams =
                  workItemParams {
                      appParams = existingParams.appParams
                      dataPathParams = existingParams.dataPathParams
                      traceContext.putAll(existingParams.traceContextMap)
                    }
                    .pack()
              }
            }
          )
        }
      }
    logger.info(
      "Ensured WorkItem $workItemId for requisition group $groupId in state ${ensured.state}"
    )
  }

  private fun validateExistingWorkItem(
    existing: WorkItem,
    requested: WorkItem,
  ): WorkItem.WorkItemParams {
    check(existing.queue == requested.queue) {
      "WorkItem ${existing.name} uses queue ${existing.queue}, not ${requested.queue}"
    }
    val existingParams = existing.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    val requestedParams = requested.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
    check(
      existingParams.appParams == requestedParams.appParams &&
        existingParams.dataPathParams == requestedParams.dataPathParams
    ) {
      "WorkItem ${existing.name} has parameters that do not match this requisition group"
    }
    return existingParams
  }

  private fun workItemId(groupId: String): String = "$WORK_ITEM_ID_PREFIX-$groupId"

  companion object {
    private val logger =
      Logger.getLogger(SecureComputationRequisitionWorkItemDispatcher::class.java.name)
    private const val WORK_ITEM_ID_PREFIX = "results-fulfiller"
  }
}
