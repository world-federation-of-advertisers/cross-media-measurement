// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.vidlabeling

import io.grpc.Status
import io.grpc.StatusException
import java.time.Instant
import java.time.LocalDate
import kotlinx.coroutines.CancellationException
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.CreateWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.RetryWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.retryWorkItemRequest

/** Durably ensures deterministic Data Availability WorkItems for VID-labeling output. */
class DataAvailabilitySyncWorkItemPublisher(
  private val createWorkItem: suspend (String, CreateWorkItemRequest) -> WorkItem,
  private val getWorkItem: suspend (String, String) -> WorkItem,
  private val retryWorkItem: suspend (String, RetryWorkItemRequest) -> WorkItem,
  private val controlPlaneThrottler: Throttler? = null,
) {
  data class Result(val workItemName: String, val outcome: String, val state: WorkItem.State)

  suspend fun ensure(
    dataProvider: String,
    rawImpressionUpload: String,
    rawImpressionUploadModelLine: String,
    modelLine: String,
    eventDate: LocalDate,
    doneBlobUri: String,
    doneBlobGeneration: Long,
    doneBlobCreateTime: Instant,
    traceContext: Map<String, String>,
    onFailure: (String, Throwable) -> Unit = { _, _ -> },
  ): Result {
    val request =
      DataAvailabilitySyncWorkItems.createRequest(
        dataProvider,
        rawImpressionUpload,
        rawImpressionUploadModelLine,
        modelLine,
        eventDate,
        doneBlobUri,
        doneBlobGeneration,
        doneBlobCreateTime,
        traceContext,
      )
    val workItemName = "workItems/${request.workItemId}"
    try {
      val created =
        try {
          callControlPlane { createWorkItem(dataProvider, request) }
        } catch (e: StatusException) {
          if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
          null
        }
      if (created != null) {
        check(created.state != WorkItem.State.SUCCEEDED) {
          "Availability WorkItem $workItemName was suppressed without being scheduled"
        }
        return Result(workItemName, "created", created.state)
      }
      val existing = callControlPlane { getWorkItem(dataProvider, workItemName) }
      check(DataAvailabilitySyncWorkItems.hasSameIdentity(existing, request)) {
        "Existing WorkItem $workItemName does not match the current availability handoff"
      }
      return when (existing.state) {
        WorkItem.State.QUEUED,
        WorkItem.State.RUNNING,
        WorkItem.State.SUCCEEDED -> Result(workItemName, "already_exists", existing.state)
        WorkItem.State.FAILED -> retry(dataProvider, request, existing)
        WorkItem.State.STATE_UNSPECIFIED,
        WorkItem.State.UNRECOGNIZED ->
          error("Existing WorkItem $workItemName has invalid state ${existing.state}")
      }
    } catch (e: CancellationException) {
      throw e
    } catch (e: Exception) {
      onFailure(workItemName, e)
      throw e
    }
  }

  private suspend fun retry(
    dataProvider: String,
    originalRequest: CreateWorkItemRequest,
    existing: WorkItem,
  ): Result {
    check(existing.generation > 0L) { "Availability WorkItem has no execution generation" }
    val retryRequest = retryWorkItemRequest {
      name = existing.name
      expectedWorkItemGeneration = existing.generation
    }
    val retried =
      try {
        callControlPlane { retryWorkItem(dataProvider, retryRequest) }
      } catch (e: CancellationException) {
        throw e
      } catch (e: StatusException) {
        if (e.status.code !in RECONCILABLE_RETRY_STATUSES) throw e
        val observed = callControlPlane { getWorkItem(dataProvider, existing.name) }
        check(DataAvailabilitySyncWorkItems.hasSameIdentity(observed, originalRequest)) {
          "Availability WorkItem identity changed while retrying ${existing.name}"
        }
        if (
          observed.generation == existing.generation + 1L && observed.state in VALID_RETRY_STATES
        ) {
          return Result(observed.name, "already_exists", observed.state)
        }
        if (
          e.status.code == Status.Code.FAILED_PRECONDITION &&
            observed.generation == existing.generation &&
            observed.state == WorkItem.State.FAILED
        ) {
          throw Status.UNAVAILABLE.withDescription(
              "Availability retry is waiting for its attempt or publication"
            )
            .withCause(e)
            .asException()
        }
        throw e
      }
    check(DataAvailabilitySyncWorkItems.hasSameIdentity(retried, originalRequest)) {
      "Retried WorkItem ${existing.name} does not match the availability handoff"
    }
    check(retried.generation == existing.generation + 1L) {
      "Availability retry did not advance the expected execution generation"
    }
    check(retried.state in VALID_RETRY_STATES) {
      "Retried availability WorkItem has invalid state ${retried.state}"
    }
    return Result(retried.name, "retried", retried.state)
  }

  private suspend fun <T> callControlPlane(block: suspend () -> T): T {
    val throttler = controlPlaneThrottler
    return if (throttler == null) block() else throttler.onReady(block)
  }

  companion object {
    private val VALID_RETRY_STATES =
      setOf(
        WorkItem.State.QUEUED,
        WorkItem.State.RUNNING,
        WorkItem.State.SUCCEEDED,
        WorkItem.State.FAILED,
      )
    private val RECONCILABLE_RETRY_STATUSES =
      setOf(
        Status.Code.ABORTED,
        Status.Code.FAILED_PRECONDITION,
        Status.Code.DEADLINE_EXCEEDED,
        Status.Code.UNAVAILABLE,
      )
  }
}
