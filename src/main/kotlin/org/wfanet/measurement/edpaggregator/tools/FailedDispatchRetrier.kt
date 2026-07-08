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

package org.wfanet.measurement.edpaggregator.tools

import io.grpc.Status
import io.grpc.StatusException
import java.util.UUID
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/**
 * Re-triggers a `FAILED` `(upload, model line)` after the operator has resolved the root cause of a
 * dead-lettered dispatch.
 *
 * It restarts from the beginning of the model line's path — Phase 0 for a memoized model line
 * (detected by the presence of `PoolAssignmentJob`s), Phase 2 for a non-memoized one — rather than
 * asking the operator which phase failed. It re-publishes that phase's original WorkItem(s) under a
 * fresh, deterministic id (a same-key re-publish would collide on the producers' deterministic ids
 * and never re-enqueue), then transitions the model line out of `FAILED`. The TEE apps' idempotency
 * gates (SUCCEEDED jobs, existing SNAPSHOT) skip already-completed work, so the run resumes at the
 * actual failure point.
 *
 * @param modelLinesStub stub for `RawImpressionUploadModelLineService`.
 * @param poolAssignmentJobsStub stub for `PoolAssignmentJobService` (Phase 0 / path detection).
 * @param vidLabelingJobsStub stub for `VidLabelingJobService` (Phase 2).
 * @param workItemsStub stub for the Secure Computation control-plane `WorkItems` service.
 */
class FailedDispatchRetrier(
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
) {
  /** Outcome of a [retryFailed] run. */
  data class RetryResult(
    val modelLineName: String,
    val workItemsRepublished: Int,
    val newState: RawImpressionUploadModelLine.State,
    /** True if the model line runs the memoized path (restarted from Phase 0). */
    val memoized: Boolean,
  )

  /**
   * Restarts the `FAILED` `(rawImpressionUpload, cmmsModelLine)` from the beginning of its path and
   * transitions it out of `FAILED`.
   *
   * @throws IllegalArgumentException if the model line is missing or not `FAILED`, or no jobs exist
   *   to re-publish.
   * @throws IllegalStateException if a job's original WorkItem no longer exists (its dispatch never
   *   enqueued), so it cannot be re-published standalone.
   */
  suspend fun retryFailed(rawImpressionUpload: String, cmmsModelLine: String): RetryResult {
    val modelLine =
      modelLinesStub.findModelLine(rawImpressionUpload, cmmsModelLine)
        ?: throw IllegalArgumentException(
          "No RawImpressionUploadModelLine for $cmmsModelLine under $rawImpressionUpload"
        )
    require(modelLine.state == RawImpressionUploadModelLine.State.FAILED) {
      "${modelLine.name} is ${modelLine.state}, expected FAILED"
    }

    // Memoized model lines run Phase 0 (they have PoolAssignmentJobs); non-memoized go straight to
    // Phase 2. Restart from the path's first phase; idempotency gates carry it forward.
    val shardIndices = listPoolAssignmentJobShards(rawImpressionUpload, cmmsModelLine)
    val memoized = shardIndices.isNotEmpty()

    val oldWorkItemIds: List<String>
    val targetState: RawImpressionUploadModelLine.State
    if (memoized) {
      oldWorkItemIds =
        shardIndices.map { WorkItemIds.forSubpoolAssigner(rawImpressionUpload, cmmsModelLine, it) }
      targetState = RawImpressionUploadModelLine.State.POOL_ASSIGNING
    } else {
      oldWorkItemIds =
        listVidLabelingJobNames(rawImpressionUpload, cmmsModelLine).map {
          WorkItemIds.forVidLabeler(it)
        }
      targetState = RawImpressionUploadModelLine.State.LABELING
    }
    require(oldWorkItemIds.isNotEmpty()) {
      "No ${if (memoized) "Phase-0" else "Phase-2"} jobs found for $cmmsModelLine under " +
        "$rawImpressionUpload; nothing to retry"
    }

    // Re-publish before transitioning, so the work exists before the line is claimed.
    var republished = 0
    for (oldId in oldWorkItemIds) {
      if (republishWorkItem(oldId)) republished++
    }

    val updated = transition(modelLine, targetState)
    return RetryResult(updated.name, republished, updated.state, memoized)
  }

  private suspend fun listPoolAssignmentJobShards(
    uploadName: String,
    cmmsModelLine: String,
  ): List<Int> {
    val shards = mutableListOf<Int>()
    var pageToken = ""
    do {
      val response =
        poolAssignmentJobsStub.listPoolAssignmentJobs(
          listPoolAssignmentJobsRequest {
            parent = uploadName
            filter = ListPoolAssignmentJobsRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
            this.pageToken = pageToken
          }
        )
      response.poolAssignmentJobsList.forEach { shards.add(it.shardIndex) }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return shards
  }

  private suspend fun listVidLabelingJobNames(
    uploadName: String,
    cmmsModelLine: String,
  ): List<String> {
    val names = mutableListOf<String>()
    var pageToken = ""
    do {
      val response =
        vidLabelingJobsStub.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = uploadName
            filter = ListVidLabelingJobsRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
            this.pageToken = pageToken
          }
        )
      response.vidLabelingJobsList.forEach { names.add(it.name) }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return names
  }

  /**
   * Re-publishes the WorkItem [oldWorkItemId] under a fresh deterministic id. Returns true if a new
   * WorkItem was created, false if it already existed (an idempotent repeat retry).
   */
  private suspend fun republishWorkItem(oldWorkItemId: String): Boolean {
    val existing =
      try {
        workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$oldWorkItemId" })
      } catch (e: StatusException) {
        if (e.status.code == Status.Code.NOT_FOUND) {
          throw IllegalStateException(
            "WorkItem workItems/$oldWorkItemId not found; its dispatch never enqueued, so it " +
              "cannot be re-published standalone."
          )
        }
        throw e
      }
    val newId = "rt-" + UUID.nameUUIDFromBytes("retry:$oldWorkItemId".toByteArray()).toString()
    val republished = workItem {
      queue = existing.queue
      workItemParams = existing.workItemParams
    }
    return try {
      workItemsStub.createWorkItem(
        createWorkItemRequest {
          workItemId = newId
          workItem = republished
        }
      )
      logger.info("Re-published $oldWorkItemId as $newId (queue=${existing.queue}).")
      true
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.ALREADY_EXISTS) {
        logger.info("Retry WorkItem $newId already exists; skipping (idempotent).")
        false
      } else {
        throw e
      }
    }
  }

  private suspend fun transition(
    modelLine: RawImpressionUploadModelLine,
    targetState: RawImpressionUploadModelLine.State,
  ): RawImpressionUploadModelLine =
    when (targetState) {
      RawImpressionUploadModelLine.State.POOL_ASSIGNING ->
        modelLinesStub.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            name = modelLine.name
            etag = modelLine.etag
          }
        )
      RawImpressionUploadModelLine.State.LABELING ->
        modelLinesStub.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            name = modelLine.name
            etag = modelLine.etag
          }
        )
      else -> error("unreachable: targetState is a path-start state")
    }

  companion object {
    private val logger: Logger = Logger.getLogger(FailedDispatchRetrier::class.java.name)
  }
}
