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
import java.util.logging.Logger
import org.wfanet.measurement.edpaggregator.VidLabelingRpcThrottlers
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
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItem
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.getWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/**
 * Re-triggers a `FAILED` `(upload, model line)` after the operator has resolved the root cause of a
 * dead-lettered dispatch. Only `PROCESSING_FAILURE` rows are retryable; `EVICTED_OUTPUT` rows
 * require a replacement upload.
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
 * @param rawImpressionModelLinesStub stub for `RawImpressionUploadModelLineService`.
 * @param poolAssignmentJobsStub stub for `PoolAssignmentJobService` (Phase 0).
 * @param rankerJobsStub stub for `RankerJobService` (Phase 1).
 * @param vidLabelingJobsStub stub for `VidLabelingJobService` (Phase 2).
 * @param workItemsStub stub for the Secure Computation control-plane `WorkItems` service.
 * @param rpcThrottlers shared throttlers for metadata and control-plane RPCs.
 */
class FailedDispatchRetrier(
  private val rawImpressionModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val rpcThrottlers: VidLabelingRpcThrottlers,
) {
  /** Outcome of a [retryFailed] run. */
  data class RetryResult(
    val modelLineName: String,
    val workItemsRepublished: Int,
    /** The phase the model line was re-triggered at. */
    val newState: RawImpressionUploadModelLine.State,
    /** Whether this invocation found a retry that a prior invocation already started. */
    val wasAlreadyStarted: Boolean,
  )

  /**
   * Re-triggers the `(rawImpressionUpload, cmmsModelLine)` after its latest failure. If that retry
   * was already started, returns its current model-line state without creating more WorkItems.
   * Otherwise, re-triggers [fromPhase] if given, or the furthest phase the model line reached.
   *
   * @param fromPhase optional override of the phase to re-trigger from (`POOL_ASSIGNING`,
   *   `RANKING`, or `LABELING`); when null the furthest reached phase is auto-detected.
   * @throws IllegalArgumentException if the model line is missing, was not a processing failure,
   *   has no failure-attempt identity, is neither `FAILED` nor the result of that failure's retry,
   *   [fromPhase] is not a phase state, or no jobs exist for the target phase to re-publish.
   * @throws IllegalStateException if the claimed model line no longer represents this retry, or a
   *   job's original WorkItem no longer exists (its dispatch never enqueued).
   */
  suspend fun retryFailed(
    rawImpressionUpload: String,
    cmmsModelLine: String,
    fromPhase: RawImpressionUploadModelLine.State? = null,
  ): RetryResult {
    val modelLine =
      rawImpressionModelLinesStub.findModelLine(
        rawImpressionUpload,
        cmmsModelLine,
        rpcThrottlers.metadataRead,
      )
        ?: throw IllegalArgumentException(
          "No RawImpressionUploadModelLine for $cmmsModelLine under $rawImpressionUpload"
        )
    require(fromPhase == null || fromPhase in RETRY_PHASES) {
      "--from-phase must be one of POOL_ASSIGNING, RANKING, LABELING; got $fromPhase"
    }
    require(modelLine.failureReason in RETRYABLE_FAILURE_REASONS) {
      "${modelLine.name} has failure_reason ${modelLine.failureReason}; only processing failures " +
        "can be retried"
    }
    require(modelLine.failureAttemptId.isNotEmpty()) {
      "${modelLine.name} has no failure_attempt_id; expected a model line that has failed"
    }

    if (modelLine.state != RawImpressionUploadModelLine.State.FAILED) {
      if (
        findExistingRetryPhase(
          rawImpressionUpload,
          cmmsModelLine,
          modelLine.state,
          modelLine.failureAttemptId,
          fromPhase,
        ) != null
      ) {
        return RetryResult(modelLine.name, 0, modelLine.state, wasAlreadyStarted = true)
      }
    }

    val phaseWorkItems =
      if (modelLine.state == RawImpressionUploadModelLine.State.FAILED) {
        // Re-trigger [fromPhase] if specified; otherwise the furthest phase that created jobs.
        if (fromPhase == null) {
          detectFurthestPhaseWorkItems(rawImpressionUpload, cmmsModelLine)
        } else {
          PhaseWorkItems(
            fromPhase,
            workItemIdsForPhase(rawImpressionUpload, cmmsModelLine, fromPhase),
          )
        }
      } else {
        // The claim may have committed before the prior CLI invocation could publish WorkItems.
        // Replay that deterministic claim, then finish any missing publications.
        require(
          modelLine.state in RETRY_PHASES && (fromPhase == null || fromPhase == modelLine.state)
        ) {
          "${modelLine.name} is ${modelLine.state}, expected FAILED or an existing retry for " +
            "failure_attempt_id ${modelLine.failureAttemptId}"
        }
        PhaseWorkItems(
          modelLine.state,
          workItemIdsForPhase(rawImpressionUpload, cmmsModelLine, modelLine.state),
        )
      }
    // Validate every source before claiming the model line so a missing WorkItem cannot strand the
    // retry in an active state with nothing runnable.
    val sourceWorkItems = mutableMapOf<String, WorkItem>()
    for (oldId in phaseWorkItems.workItemIds) {
      sourceWorkItems[oldId] = getRequiredWorkItem(oldId)
    }

    // Claim the retry before publishing. The service rejects an evicted row atomically, while the
    // deterministic request ID lets a later invocation replay a claim whose publication crashed.
    val updated = transition(modelLine, phaseWorkItems.phase, modelLine.failureAttemptId)
    check(
      updated.state == phaseWorkItems.phase && updated.failureReason in RETRYABLE_FAILURE_REASONS
    ) {
      "${updated.name} changed to ${updated.state} with failure_reason " +
        "${updated.failureReason} while claiming the retry; no WorkItems were published"
    }
    var republished = 0
    for ((oldId, sourceWorkItem) in sourceWorkItems) {
      if (republishWorkItem(oldId, sourceWorkItem, modelLine.failureAttemptId)) republished++
    }

    return RetryResult(
      updated.name,
      republished,
      updated.state,
      wasAlreadyStarted = modelLine.state != RawImpressionUploadModelLine.State.FAILED,
    )
  }

  /** Returns the phase of an existing retry for [failureAttemptId], or `null` if none exists. */
  private suspend fun findExistingRetryPhase(
    uploadName: String,
    cmmsModelLine: String,
    currentState: RawImpressionUploadModelLine.State,
    failureAttemptId: String,
    fromPhase: RawImpressionUploadModelLine.State?,
  ): RawImpressionUploadModelLine.State? {
    val possiblePhases =
      when (currentState) {
        RawImpressionUploadModelLine.State.POOL_ASSIGNING ->
          listOf(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
        RawImpressionUploadModelLine.State.RANKING ->
          listOf(
            RawImpressionUploadModelLine.State.RANKING,
            RawImpressionUploadModelLine.State.POOL_ASSIGNING,
          )
        RawImpressionUploadModelLine.State.LABELING,
        RawImpressionUploadModelLine.State.COMPLETED -> RETRY_PHASES
        RawImpressionUploadModelLine.State.CREATED,
        RawImpressionUploadModelLine.State.FAILED,
        RawImpressionUploadModelLine.State.STATE_UNSPECIFIED,
        RawImpressionUploadModelLine.State.UNRECOGNIZED -> emptyList()
      }
    val candidatePhases =
      if (fromPhase == null) possiblePhases else possiblePhases.filter { it == fromPhase }
    for (phase in candidatePhases) {
      val originalWorkItemIds = workItemIdsForPhaseOrEmpty(uploadName, cmmsModelLine, phase)
      if (
        originalWorkItemIds.isNotEmpty() &&
          originalWorkItemIds.all { retryWorkItemIsActiveOrSucceeded(it, failureAttemptId) }
      ) {
        return phase
      }
    }
    return null
  }

  private suspend fun retryWorkItemIsActiveOrSucceeded(
    originalWorkItemId: String,
    failureAttemptId: String,
  ): Boolean {
    var retryWorkItemId = RequestIds.forRetriedWorkItem(originalWorkItemId, failureAttemptId)
    while (true) {
      val retryWorkItem =
        try {
          rpcThrottlers.controlPlane.onReady {
            workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$retryWorkItemId" })
          }
        } catch (e: StatusException) {
          if (e.status.code == Status.Code.NOT_FOUND) return false
          throw e
        }
      when (retryWorkItem.state) {
        WorkItem.State.QUEUED,
        WorkItem.State.RUNNING,
        WorkItem.State.SUCCEEDED -> return true
        WorkItem.State.FAILED -> {
          val failureVersion =
            "${retryWorkItem.updateTime.seconds}:${retryWorkItem.updateTime.nanos}"
          retryWorkItemId = RequestIds.forRetriedWorkItem(retryWorkItemId, failureVersion)
        }
        WorkItem.State.STATE_UNSPECIFIED,
        WorkItem.State.UNRECOGNIZED ->
          error("Retry WorkItem $retryWorkItemId has invalid state ${retryWorkItem.state}.")
      }
    }
  }

  /**
   * The furthest phase [cmmsModelLine] under [uploadName] reached, inferred from which per-phase
   * job rows exist: `VidLabelingJob`s ⇒ `LABELING`, else `RankerJob`s ⇒ `RANKING`, else
   * `PoolAssignmentJob`s ⇒ `POOL_ASSIGNING`.
   */
  private suspend fun detectFurthestPhaseWorkItems(
    uploadName: String,
    cmmsModelLine: String,
  ): PhaseWorkItems {
    val vidLabelingJobNames = listVidLabelingJobNames(uploadName, cmmsModelLine)
    if (vidLabelingJobNames.isNotEmpty()) {
      return PhaseWorkItems(
        RawImpressionUploadModelLine.State.LABELING,
        vidLabelingJobNames.map { WorkItemIds.forVidLabeler(it) },
      )
    }
    val rankerJobNames = listRankerJobNames(uploadName, cmmsModelLine)
    if (rankerJobNames.isNotEmpty()) {
      return PhaseWorkItems(
        RawImpressionUploadModelLine.State.RANKING,
        rankerJobNames.map { WorkItemIds.forVidRankBuilder(it) },
      )
    }
    val poolAssignmentJobShards = listPoolAssignmentJobShards(uploadName, cmmsModelLine)
    require(poolAssignmentJobShards.isNotEmpty()) {
      "No jobs found for $cmmsModelLine under $uploadName; nothing to retry"
    }
    return PhaseWorkItems(
      RawImpressionUploadModelLine.State.POOL_ASSIGNING,
      poolAssignmentJobShards.map { WorkItemIds.forSubpoolAssigner(uploadName, cmmsModelLine, it) },
    )
  }

  /** The origin WorkItem ids to republish to re-trigger [phase] for (upload, model line). */
  private suspend fun workItemIdsForPhase(
    uploadName: String,
    cmmsModelLine: String,
    phase: RawImpressionUploadModelLine.State,
  ): List<String> {
    val workItemIds = workItemIdsForPhaseOrEmpty(uploadName, cmmsModelLine, phase)
    require(workItemIds.isNotEmpty()) {
      "No jobs found for $cmmsModelLine under $uploadName; cannot retry from $phase"
    }
    return workItemIds
  }

  private suspend fun workItemIdsForPhaseOrEmpty(
    uploadName: String,
    cmmsModelLine: String,
    phase: RawImpressionUploadModelLine.State,
  ): List<String> =
    when (phase) {
      RawImpressionUploadModelLine.State.LABELING ->
        listVidLabelingJobNames(uploadName, cmmsModelLine).map { WorkItemIds.forVidLabeler(it) }
      RawImpressionUploadModelLine.State.RANKING ->
        listRankerJobNames(uploadName, cmmsModelLine).map { WorkItemIds.forVidRankBuilder(it) }
      RawImpressionUploadModelLine.State.POOL_ASSIGNING ->
        listPoolAssignmentJobShards(uploadName, cmmsModelLine).map {
          WorkItemIds.forSubpoolAssigner(uploadName, cmmsModelLine, it)
        }
      else -> error("unreachable: $phase is not a retry phase")
    }

  private suspend fun listVidLabelingJobNames(
    uploadName: String,
    cmmsModelLine: String,
  ): List<String> {
    val names = mutableListOf<String>()
    var pageToken = ""
    do {
      val response =
        rpcThrottlers.metadataRead.onReady {
          vidLabelingJobsStub.listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = uploadName
              filter = ListVidLabelingJobsRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
              this.pageToken = pageToken
            }
          )
        }
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
        rpcThrottlers.metadataRead.onReady {
          rankerJobsStub.listRankerJobs(
            listRankerJobsRequest {
              parent = uploadName
              filter = ListRankerJobsRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
              this.pageToken = pageToken
            }
          )
        }
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
        rpcThrottlers.metadataRead.onReady {
          poolAssignmentJobsStub.listPoolAssignmentJobs(
            listPoolAssignmentJobsRequest {
              parent = uploadName
              filter = ListPoolAssignmentJobsRequestKt.filter { this.cmmsModelLine = cmmsModelLine }
              this.pageToken = pageToken
            }
          )
        }
      response.poolAssignmentJobsList.forEach { shards.add(it.shardIndex) }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return shards
  }

  private suspend fun getRequiredWorkItem(workItemId: String): WorkItem =
    try {
      rpcThrottlers.controlPlane.onReady {
        workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$workItemId" })
      }
    } catch (e: StatusException) {
      if (e.status.code == Status.Code.NOT_FOUND) {
        throw IllegalStateException(
          "WorkItem workItems/$workItemId not found; its dispatch never enqueued, so it " +
            "cannot be re-published standalone."
        )
      }
      throw e
    }

  /**
   * Re-publishes [sourceWorkItem] as a retry of [oldWorkItemId] for [failureAttemptId]. Returns
   * true if a new WorkItem was created, or false when the same retry attempt already created it.
   */
  private suspend fun republishWorkItem(
    oldWorkItemId: String,
    sourceWorkItem: WorkItem,
    failureAttemptId: String,
  ): Boolean {
    var newId = RequestIds.forRetriedWorkItem(oldWorkItemId, failureAttemptId)
    val republished = workItem {
      queue = sourceWorkItem.queue
      workItemParams = sourceWorkItem.workItemParams
    }
    while (true) {
      try {
        rpcThrottlers.controlPlane.onReady {
          workItemsStub.createWorkItem(
            createWorkItemRequest {
              workItemId = newId
              workItem = republished
            }
          )
        }
        logger.info("Re-published $oldWorkItemId as $newId (queue=${sourceWorkItem.queue}).")
        return true
      } catch (e: StatusException) {
        if (e.status.code != Status.Code.ALREADY_EXISTS) throw e

        val existingRetry =
          rpcThrottlers.controlPlane.onReady {
            workItemsStub.getWorkItem(getWorkItemRequest { name = "workItems/$newId" })
          }
        when (existingRetry.state) {
          WorkItem.State.QUEUED,
          WorkItem.State.RUNNING,
          WorkItem.State.SUCCEEDED -> {
            logger.info(
              "Retry WorkItem $newId already exists in ${existingRetry.state}; " +
                "continuing the idempotent retry."
            )
            return false
          }
          WorkItem.State.FAILED -> {
            val failureVersion =
              "${existingRetry.updateTime.seconds}:${existingRetry.updateTime.nanos}"
            newId = RequestIds.forRetriedWorkItem(newId, failureVersion)
          }
          WorkItem.State.STATE_UNSPECIFIED,
          WorkItem.State.UNRECOGNIZED ->
            error("Retry WorkItem $newId has invalid state ${existingRetry.state}.")
        }
      }
    }
  }

  private suspend fun transition(
    modelLine: RawImpressionUploadModelLine,
    targetState: RawImpressionUploadModelLine.State,
    failureAttemptId: String,
  ): RawImpressionUploadModelLine =
    when (targetState) {
      RawImpressionUploadModelLine.State.POOL_ASSIGNING ->
        rpcThrottlers.metadataWrite.onReady {
          rawImpressionModelLinesStub.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              name = modelLine.name
              etag = modelLine.etag
              requestId = RequestIds.forHealingRetryPoolAssigning(modelLine.name, failureAttemptId)
            }
          )
        }
      RawImpressionUploadModelLine.State.RANKING ->
        rpcThrottlers.metadataWrite.onReady {
          rawImpressionModelLinesStub.markRawImpressionUploadModelLineRanking(
            markRawImpressionUploadModelLineRankingRequest {
              name = modelLine.name
              etag = modelLine.etag
              requestId = RequestIds.forHealingRetryRanking(modelLine.name, failureAttemptId)
            }
          )
        }
      RawImpressionUploadModelLine.State.LABELING ->
        rpcThrottlers.metadataWrite.onReady {
          rawImpressionModelLinesStub.markRawImpressionUploadModelLineLabeling(
            markRawImpressionUploadModelLineLabelingRequest {
              name = modelLine.name
              etag = modelLine.etag
              requestId = RequestIds.forHealingRetryLabeling(modelLine.name, failureAttemptId)
            }
          )
        }
      else -> error("unreachable: targetState is a phase state")
    }

  companion object {
    private val RETRY_PHASES =
      listOf(
        RawImpressionUploadModelLine.State.LABELING,
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.POOL_ASSIGNING,
      )

    private val RETRYABLE_FAILURE_REASONS =
      setOf(
        RawImpressionUploadModelLine.FailureReason.FAILURE_REASON_UNSPECIFIED,
        RawImpressionUploadModelLine.FailureReason.PROCESSING_FAILURE,
      )
    private val logger: Logger = Logger.getLogger(FailedDispatchRetrier::class.java.name)
  }

  private data class PhaseWorkItems(
    val phase: RawImpressionUploadModelLine.State,
    val workItemIds: List<String>,
  )
}
