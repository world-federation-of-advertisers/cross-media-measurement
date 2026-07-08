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
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/**
 * Re-triggers a `FAILED` `(upload, model line)` after the operator has resolved the root cause of a
 * dead-lettered dispatch.
 *
 * It re-triggers the **furthest phase the model line reached** — detected from which per-phase job
 * rows exist: `VidLabelingJob`s ⇒ Phase 2 (`LABELING`), else `RankerJob`s ⇒ Phase 1 (`RANKING`),
 * else `PoolAssignmentJob`s ⇒ Phase 0 (`POOL_ASSIGNING`). It re-publishes that phase's original
 * WorkItem(s) under a fresh, deterministic id (a same-key re-publish would collide on the
 * producers' deterministic ids and never re-enqueue), then transitions the model line out of
 * `FAILED`. The TEE apps' idempotency gates (SUCCEEDED jobs, existing SNAPSHOT) skip
 * already-completed work.
 *
 * Re-triggering the *furthest* phase (rather than always Phase 0) matters for the memoized path: a
 * completed Phase-0 last-shard-out has already merged and deleted its temp per-shard blobs, so
 * restarting a Phase-1/Phase-2 failure from Phase 0 would make the SubpoolAssigner attempt a
 * re-merge of blobs that no longer exist. Phase 1 instead re-reads the still-present merged subpool
 * blob, and Phase 2 re-reads the snapshot.
 *
 * @param modelLinesStub stub for `RawImpressionUploadModelLineService`.
 * @param poolAssignmentJobsStub stub for `PoolAssignmentJobService` (Phase 0).
 * @param rankerJobsStub stub for `RankerJobService` (Phase 1).
 * @param vidLabelingJobsStub stub for `VidLabelingJobService` (Phase 2).
 * @param workItemsStub stub for the Secure Computation control-plane `WorkItems` service.
 */
class FailedDispatchRetrier(
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
) {
  /** Outcome of a [retryFailed] run. */
  data class RetryResult(
    val modelLineName: String,
    val workItemsRepublished: Int,
    /** The phase the model line was re-triggered at. */
    val newState: RawImpressionUploadModelLine.State,
  )

  /**
   * Re-triggers the `FAILED` `(rawImpressionUpload, cmmsModelLine)` at the furthest phase it
   * reached and transitions it out of `FAILED`.
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

    // Re-trigger the furthest phase reached (the deepest one that created job rows).
    val oldWorkItemIds: List<String>
    val targetState: RawImpressionUploadModelLine.State
    val vidLabelingJobNames = listVidLabelingJobNames(rawImpressionUpload, cmmsModelLine)
    if (vidLabelingJobNames.isNotEmpty()) {
      oldWorkItemIds = vidLabelingJobNames.map { WorkItemIds.forVidLabeler(it) }
      targetState = RawImpressionUploadModelLine.State.LABELING
    } else {
      val rankerJobNames = listRankerJobNames(rawImpressionUpload, cmmsModelLine)
      if (rankerJobNames.isNotEmpty()) {
        oldWorkItemIds = rankerJobNames.map { WorkItemIds.forVidRankBuilder(it) }
        targetState = RawImpressionUploadModelLine.State.RANKING
      } else {
        val shardIndices = listPoolAssignmentJobShards(rawImpressionUpload, cmmsModelLine)
        require(shardIndices.isNotEmpty()) {
          "No jobs found for $cmmsModelLine under $rawImpressionUpload; nothing to retry"
        }
        oldWorkItemIds =
          shardIndices.map {
            WorkItemIds.forSubpoolAssigner(rawImpressionUpload, cmmsModelLine, it)
          }
        targetState = RawImpressionUploadModelLine.State.POOL_ASSIGNING
      }
    }

    // Re-publish before transitioning, so the work exists before the line is claimed.
    var republished = 0
    for (oldId in oldWorkItemIds) {
      if (republishWorkItem(oldId)) republished++
    }

    val updated = transition(modelLine, targetState)
    return RetryResult(updated.name, republished, updated.state)
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

  private suspend fun listRankerJobNames(uploadName: String, cmmsModelLine: String): List<String> {
    val names = mutableListOf<String>()
    var pageToken = ""
    do {
      val response =
        rankerJobsStub.listRankerJobs(
          listRankerJobsRequest {
            parent = uploadName
            filter = ListRankerJobsRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
            this.pageToken = pageToken
          }
        )
      response.rankerJobsList.forEach { names.add(it.name) }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return names
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
      RawImpressionUploadModelLine.State.RANKING ->
        modelLinesStub.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
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
      else -> error("unreachable: targetState is a phase state")
    }

  companion object {
    private val logger: Logger = Logger.getLogger(FailedDispatchRetrier::class.java.name)
  }
}
