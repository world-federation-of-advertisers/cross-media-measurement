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
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.copy

/** Durably ensures deterministic Data Availability WorkItems for VID-labeling output. */
class DataAvailabilitySyncWorkItemPublisher(
  private val createWorkItem: suspend (String, CreateWorkItemRequest) -> WorkItem,
  private val getWorkItem: suspend (String, String) -> WorkItem,
  private val controlPlaneThrottler: Throttler? = null,
) {
  data class Result(val workItemName: String, val outcome: String)

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
    var request =
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
    while (true) {
      val workItemName = "workItems/${request.workItemId}"
      try {
        callControlPlane { createWorkItem(dataProvider, request) }
        return Result(workItemName, "created")
      } catch (e: CancellationException) {
        throw e
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.ALREADY_EXISTS) {
          onFailure(workItemName, e)
          throw e
        }
        val existing =
          try {
            callControlPlane { getWorkItem(dataProvider, workItemName) }
          } catch (validationError: CancellationException) {
            throw validationError
          } catch (validationError: Exception) {
            onFailure(workItemName, validationError)
            throw validationError
          }
        if (!DataAvailabilitySyncWorkItems.hasSameIdentity(existing, request)) {
          val validationError =
            IllegalStateException(
              "Existing WorkItem $workItemName does not match the current availability handoff"
            )
          onFailure(workItemName, validationError)
          throw validationError
        }
        when (existing.state) {
          WorkItem.State.QUEUED,
          WorkItem.State.RUNNING,
          WorkItem.State.SUCCEEDED -> return Result(workItemName, "already_exists")
          WorkItem.State.FAILED -> {
            val failureVersion = "${existing.updateTime.seconds}:${existing.updateTime.nanos}"
            request =
              request.copy {
                workItemId = RequestIds.forRetriedWorkItem(request.workItemId, failureVersion)
              }
          }
          WorkItem.State.STATE_UNSPECIFIED,
          WorkItem.State.UNRECOGNIZED -> {
            val validationError =
              IllegalStateException(
                "Existing WorkItem $workItemName has invalid state ${existing.state}"
              )
            onFailure(workItemName, validationError)
            throw validationError
          }
        }
      } catch (e: Exception) {
        onFailure(workItemName, e)
        throw e
      }
    }
  }

  private suspend fun <T> callControlPlane(block: suspend () -> T): T {
    val throttler = controlPlaneThrottler
    return if (throttler == null) block() else throttler.onReady(block)
  }
}
