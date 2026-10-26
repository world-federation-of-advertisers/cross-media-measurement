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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.eventgroups

import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import kotlinx.coroutines.CancellationException
import kotlinx.coroutines.cancelAndJoin
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.isActive
import kotlinx.coroutines.launch
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.api.ResourceIds
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttempt
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemAttemptsGrpcKt.WorkItemAttemptsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.completeWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemAttemptRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.failWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.renewWorkItemAttemptRequest

/** Validated source identity for an EventGroupSync task. */
internal data class EventGroupSyncWorkItem(
  val workItem: WorkItem,
  val params: WorkItem.WorkItemParams,
  val appParams: EventGroupSyncParams,
) {
  val source: WorkItem.WorkItemParams.DataPathParams
    get() = params.dataPathParams

  companion object {
    fun parse(workItem: WorkItem): EventGroupSyncWorkItem {
      val id = workItem.name.removePrefix("workItems/")
      require(workItem.name == "workItems/$id" && ResourceIds.RFC_1034_REGEX.matches(id)) {
        "name must be a WorkItem resource name"
      }
      require(workItem.generation > 0L) { "WorkItem generation must be positive" }
      require(workItem.workItemParams.`is`(WorkItem.WorkItemParams::class.java)) {
        "work_item_params must contain WorkItemParams"
      }
      val params = workItem.workItemParams.unpack(WorkItem.WorkItemParams::class.java)
      require(params.appParams.`is`(EventGroupSyncParams::class.java)) {
        "app_params must contain EventGroupSyncParams"
      }
      val appParams = params.appParams.unpack(EventGroupSyncParams::class.java)
      requireNotNull(DataProviderKey.fromName(appParams.dataProvider)) {
        "data_provider must be a DataProvider resource name"
      }
      require(params.hasDataPathParams() && params.dataPathParams.dataPath.isNotEmpty()) {
        "data_path is required"
      }
      require(params.dataPathParams.hasGeneration() && params.dataPathParams.generation > 0L) {
        "source object generation must be positive"
      }
      require(
        params.dataPathParams.eventType ==
          WorkItem.WorkItemParams.DataPathParams.StorageEventType.FINALIZED
      ) {
        "source event must be FINALIZED"
      }
      return EventGroupSyncWorkItem(workItem, params, appParams)
    }
  }
}

/** Claims one attempt, renews its lease, and records completion before acknowledging delivery. */
internal class EventGroupSyncWorkItemProcessor(
  private val workItems: WorkItemsCoroutineStub,
  private val attempts: WorkItemAttemptsCoroutineStub,
  private val synchronize: suspend (EventGroupSyncWorkItem, suspend () -> Unit) -> Unit,
  private val uuidGenerator: () -> UUID,
  private val activeAttemptDelay: suspend () -> Unit,
  private val renewalDelay: suspend () -> Unit,
  private val updateRetryDelay: suspend (Int) -> Unit,
) {
  suspend fun process(input: EventGroupSyncWorkItem) {
    val attempt = claim(input) ?: return
    try {
      coroutineScope {
        check(attempt.hasLeaseExpirationTime()) { "Control plane must support attempt leases" }
        val renewal = launch {
          while (isActive) {
            renewalDelay()
            renew(attempt)
          }
        }
        try {
          synchronize(input) { renew(attempt) }
          complete(attempt)
        } finally {
          renewal.cancelAndJoin()
        }
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      try {
        update {
          attempts.failWorkItemAttempt(
            failWorkItemAttemptRequest {
              name = attempt.name
              errorMessage = e::class.java.simpleName.take(200)
            }
          )
        }
        if (e is IllegalArgumentException) {
          update {
            workItems.failWorkItem(
              failWorkItemRequest {
                name = input.workItem.name
                expectedWorkItemGeneration = input.workItem.generation
              }
            )
          }
          return
        }
      } catch (writeback: Exception) {
        e.addSuppressed(writeback)
      }
      throw e
    }
  }

  private suspend fun claim(input: EventGroupSyncWorkItem): WorkItemAttempt? {
    while (true) {
      try {
        return attempts.createWorkItemAttempt(
          createWorkItemAttemptRequest {
            parent = input.workItem.name
            workItemAttemptId = "event-group-sync-${uuidGenerator()}"
            expectedWorkItemGeneration = input.workItem.generation
            supportsAttemptLease = true
          }
        )
      } catch (e: StatusException) {
        val reason = e.errorInfo?.reason
        val state = e.errorInfo?.metadataMap?.get("workItemState")
        if (reason == "INVALID_WORK_ITEM_STATE" && state == WorkItem.State.RUNNING.name) {
          activeAttemptDelay()
          continue
        }
        if (
          reason in setOf("WORK_ITEM_GENERATION_MISMATCH", "WORK_ITEM_NOT_FOUND") ||
            reason == "INVALID_WORK_ITEM_STATE" &&
              state in setOf(WorkItem.State.SUCCEEDED.name, WorkItem.State.FAILED.name)
        ) {
          return null
        }
        throw e
      }
    }
  }

  private suspend fun renew(attempt: WorkItemAttempt) {
    update { attempts.renewWorkItemAttempt(renewWorkItemAttemptRequest { name = attempt.name }) }
  }

  private suspend fun complete(attempt: WorkItemAttempt) {
    try {
      update {
        attempts.completeWorkItemAttempt(completeWorkItemAttemptRequest { name = attempt.name })
      }
    } catch (e: StatusException) {
      if (
        e.errorInfo?.reason == "INVALID_WORK_ITEM_ATTEMPT_STATE" &&
          e.errorInfo?.metadataMap?.get("workItemAttemptState") ==
            WorkItemAttempt.State.SUCCEEDED.name
      )
        return
      throw e
    }
  }

  private suspend fun <T> update(block: suspend () -> T): T {
    for (attempt in 1..3) {
      try {
        return block()
      } catch (e: StatusException) {
        if (attempt == 3 || e.status.code !in RETRYABLE_CODES) throw e
        updateRetryDelay(attempt)
      }
    }
    error("unreachable")
  }

  companion object {
    private val RETRYABLE_CODES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.UNAVAILABLE,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.RESOURCE_EXHAUSTED,
      )
  }
}
