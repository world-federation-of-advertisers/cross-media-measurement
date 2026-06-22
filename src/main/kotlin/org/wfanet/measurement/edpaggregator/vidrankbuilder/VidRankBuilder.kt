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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import io.grpc.Status
import io.grpc.StatusException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.util.UUID
import java.util.logging.Level
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelerParams
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadFilesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

/**
 * Phase-1 per-WorkItem logic for the memoized VID assignment pipeline (analog of
 * `SubpoolAssigner`).
 *
 * For one `RankerJob` WorkItem (covering one or more bin-packed subpools) it:
 * 1. gates on `RankerJob` state (an already-`SUCCEEDED` job on Pub/Sub redelivery skips re-ranking
 *    and only recovers the last-job-out),
 * 2. ranks each of its subpools via [SubpoolRanker] (load prior snapshot -> retention -> allocate
 *    -> write new day-only + cumulative blobs + rows),
 * 3. marks its `RankerJob` `SUCCEEDED`; the service atomically reports whether this was the last
 *    job out for the (upload, model line),
 * 4. last-job-out only: fans out Phase 2 — bin-packs the upload's `RawImpressionUploadFile`s into
 *    `VidLabelingJob`s, publishes one memoized-`VidLabelerParams` WorkItem per job, then flips the
 *    parent `RawImpressionUploadModelLine` `RANKING` -> `LABELING` (see [fanOutLabeling]).
 *
 * ## Phase-2 fan-out (last-job-out)
 *
 * Phase 2 batches by the uploaded **file**, not by an integer shard: an output blob's `entity_keys`
 * must land in the same file groupings the EDP uploaded them in. So the job unit is a bin-packed
 * group of [RawImpressionUploadFile]s ([maxFilesPerLabelingJob] per group). For each group it
 * creates a `VidLabelingJob` row (carrying the model line + that group's files) and publishes one
 * WorkItem to [vidLabelerQueue] whose memoized [VidLabelerParams] points the Phase-2 TEE at the
 * `VidLabelingJob`; the TEE resolves the rank-index blobs from the `RankIndexBlobService` (gRPC)
 * rather than receiving their URIs in the params. The parent flip is last so `LABELING` is the
 * durable completion marker.
 *
 * Idempotent on redelivery: a `SUCCEEDED` job short-circuits ranking; `VidLabelingJob` creation is
 * keyed by a deterministic `request_id` and WorkItem creation by a deterministic `work_item_id`
 * (`ALREADY_EXISTS` tolerated); the parent flip is a no-op once advanced. On failure the job is
 * marked `FAILED` and the error rethrown so the TEE framework nacks.
 *
 * Concurrent-ranker protection: `MarkRankerJobSucceeded` carries the read `etag`; a stale write
 * (another VM won the race after Pub/Sub redelivery) surfaces as `ABORTED`/`FAILED_PRECONDITION`
 * and is treated as a benign already-done.
 *
 * @param vidLabelerParamsTemplate the memoized [VidLabelerParams] for this (upload, model line),
 *   built by [VidRankBuilderApp] from the pass-through fields on `VidRankBuilderParams`. The
 *   last-job-out copies it per `VidLabelingJob`, filling the job's name + its files.
 * @param maxFilesPerLabelingJob the bin-packing threshold: at most this many
 *   `RawImpressionUploadFile`s per `VidLabelingJob`.
 * @param vidLabelerQueue the Secure Computation queue the Phase-2 WorkItems are published to.
 */
class VidRankBuilder(
  private val subpoolRanker: SubpoolRanker,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  private val workItemsStub: WorkItemsCoroutineStub,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val rankerJob: String,
  private val subpoolMapBlobUris: Map<Long, String>,
  private val subpoolRankedSizes: Map<Long, Int>,
  private val vidLabelerParamsTemplate: VidLabelerParams,
  private val vidLabelerQueue: String,
  private val maxFilesPerLabelingJob: Int = DEFAULT_MAX_FILES_PER_LABELING_JOB,
) {
  /**
   * @property subpoolsRanked subpools this job ranked (0 on a redelivery short-circuit).
   * @property lastJobOut whether this job was the last out (and ran the Phase-2 fan-out).
   */
  data class Result(val subpoolsRanked: Int, val lastJobOut: Boolean)

  /** Runs the full Phase-1 work for one `RankerJob`. */
  suspend fun run(): Result {
    try {
      return runRankerJob()
    } catch (t: Throwable) {
      markFailedBestEffort(t)
      throw t
    }
  }

  private suspend fun runRankerJob(): Result {
    // Gate on job state: an already-SUCCEEDED job on redelivery skips the expensive re-rank and
    // only
    // recovers the last-job-out (SUCCEEDED is set only after every subpool's blobs are durable).
    val job = rankerJobsStub.getRankerJob(getRankerJobRequest { name = rankerJob })
    if (job.state == RankerJob.State.SUCCEEDED) {
      return recoverIfLastJobOut()
    }

    for ((poolOffset, blobUri) in subpoolMapBlobUris) {
      val rankedSize =
        requireNotNull(subpoolRankedSizes[poolOffset]) {
          "subpool_ranked_sizes missing offset $poolOffset for $rankerJob"
        }
      subpoolRanker.rank(poolOffset, blobUri, rankedSize)
    }

    val markResponse =
      try {
        rankerJobsStub.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            name = rankerJob
            etag = job.etag
            requestId = markSucceededRequestId()
          }
        )
      } catch (e: StatusException) {
        // Lost the etag race to a concurrent/re-delivered ranker (Concurrent Ranker Protection). If
        // the job is already SUCCEEDED, discard this attempt's work and ack (idempotent no-op);
        // otherwise the conflict is real, so rethrow to nack and retry.
        if (
          e.status.code != Status.Code.ABORTED && e.status.code != Status.Code.FAILED_PRECONDITION
        ) {
          throw e
        }
        val current = rankerJobsStub.getRankerJob(getRankerJobRequest { name = rankerJob })
        if (current.state != RankerJob.State.SUCCEEDED) throw e
        logger.info(
          "RankerJob $rankerJob already SUCCEEDED by another ranker (${e.status.code}); acking"
        )
        return Result(subpoolMapBlobUris.size, lastJobOut = false)
      }

    if (markResponse.isLastJob) {
      val parent =
        requireNotNull(getParent()) {
          "RawImpressionUploadModelLine not found for $modelLine under $rawImpressionUpload"
        }
      runLastJobOut(parent)
      return Result(subpoolMapBlobUris.size, lastJobOut = true)
    }
    return Result(subpoolMapBlobUris.size, lastJobOut = false)
  }

  /**
   * Recovers the last-job-out on redelivery of an already-`SUCCEEDED` job. With no per-job
   * persisted "last out" marker, this re-derives the condition the way the Monitor does: every
   * `RankerJob` for the (upload, model line) is `SUCCEEDED` and the parent is still `RANKING`. The
   * fan-out is idempotent, so re-running is safe.
   */
  private suspend fun recoverIfLastJobOut(): Result {
    val parent =
      requireNotNull(getParent()) {
        "RawImpressionUploadModelLine not found for $modelLine under $rawImpressionUpload"
      }
    if (parent.state != RawImpressionUploadModelLine.State.RANKING) {
      logger.info("RankerJob $rankerJob already SUCCEEDED; nothing to recover (parent advanced)")
      return Result(0, lastJobOut = false)
    }
    if (!allRankerJobsSucceeded()) {
      logger.info("RankerJob $rankerJob already SUCCEEDED; other jobs still pending")
      return Result(0, lastJobOut = false)
    }
    logger.info("RankerJob $rankerJob already SUCCEEDED; recovering last-job-out")
    runLastJobOut(parent)
    return Result(0, lastJobOut = true)
  }

  /**
   * Fans out Phase 2 for the (upload, model line): creates the file-batched `VidLabelingJob`s,
   * publishes one WorkItem per job, then flips the parent `RawImpressionUploadModelLine` `RANKING`
   * -> `LABELING`. The flip is last so `LABELING` is the durable completion marker (a redelivery
   * before the flip re-runs the idempotent fan-out).
   */
  private suspend fun runLastJobOut(parent: RawImpressionUploadModelLine) {
    if (parent.state != RawImpressionUploadModelLine.State.RANKING) {
      logger.info("Parent ${parent.name} already past RANKING; last-job-out already complete")
      return
    }
    val published = fanOutLabeling()
    markParentLabeling(parent)
    logger.info(
      "Last-job-out for $modelLine: created/published $published VidLabelingJob WorkItem(s); " +
        "parent -> LABELING"
    )
  }

  /**
   * Creates the Phase-2 `VidLabelingJob`s for the (upload, model line) — one per bin-packed group
   * of the upload's `RawImpressionUploadFile`s — and publishes a memoized-`VidLabelerParams`
   * WorkItem for each. Returns the number of WorkItems published.
   *
   * Files are sorted before chunking so the batching (and therefore each job's deterministic
   * `request_id` / `work_item_id`) is stable across redeliveries.
   */
  private suspend fun fanOutLabeling(): Int {
    require(vidLabelerQueue.isNotEmpty()) {
      "vid_labeler_queue must be configured to fan out Phase-2 for $modelLine"
    }
    val files = listUploadFiles().sorted()
    if (files.isEmpty()) {
      logger.warning(
        "Last-job-out for $modelLine: no RawImpressionUploadFiles under $rawImpressionUpload; " +
          "nothing to label"
      )
      return 0
    }

    val batches: List<List<String>> = files.chunked(maxFilesPerLabelingJob)
    val jobs = createVidLabelingJobs(batches)
    jobs.zip(batches).forEach { (job, batch) -> publishVidLabelerWorkItem(job, batch) }
    return jobs.size
  }

  /** Lists every (non-deleted) `RawImpressionUploadFile` name under this upload. */
  private suspend fun listUploadFiles(): List<String> {
    val names = mutableListOf<String>()
    rawImpressionUploadFilesStub
      .listResources { pageToken: String ->
        val response =
          listRawImpressionUploadFiles(
            listRawImpressionUploadFilesRequest {
              parent = rawImpressionUpload
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rawImpressionUploadFilesList, response.nextPageToken)
      }
      .collect { page -> page.forEach { names.add(it.name) } }
    return names
  }

  /**
   * Creates a `VidLabelingJob` per [batches] entry via `BatchCreateVidLabelingJobs` (chunked to the
   * service's per-batch limit). Each request carries a deterministic `request_id` keyed by the
   * batch's first (sorted) file, so a redelivered last-job-out reuses the existing rows. Returns
   * the created jobs in [batches] order.
   */
  private suspend fun createVidLabelingJobs(batches: List<List<String>>): List<VidLabelingJob> {
    val created = mutableListOf<VidLabelingJob>()
    for (group in batches.chunked(MAX_JOBS_PER_BATCH_CREATE)) {
      val response =
        vidLabelingJobsStub.batchCreateVidLabelingJobs(
          batchCreateVidLabelingJobsRequest {
            parent = rawImpressionUpload
            for (batch in group) {
              requests += createVidLabelingJobRequest {
                parent = rawImpressionUpload
                vidLabelingJob = vidLabelingJob {
                  cmmsModelLines += modelLine
                  rawImpressionUploadFiles += batch
                }
                requestId = labelingJobRequestId(batch)
              }
            }
          }
        )
      created += response.vidLabelingJobsList
    }
    return created
  }

  /**
   * Publishes one Phase-2 WorkItem for [job], copying [vidLabelerParamsTemplate] with the job's
   * name and its [files]. Tolerates `ALREADY_EXISTS` so a redelivered last-job-out is a no-op.
   */
  private suspend fun publishVidLabelerWorkItem(job: VidLabelingJob, files: List<String>) {
    val params =
      vidLabelerParamsTemplate.copy {
        memoizedParams =
          memoizedParams.copy {
            vidLabelingJob = job.name
            rawImpressionUploadFiles += files
          }
      }
    val workItemId = "vid-labeler-${job.name.substringAfterLast('/')}"
    try {
      workItemsStub.createWorkItem(
        createWorkItemRequest {
          this.workItemId = workItemId
          workItem = workItem {
            queue = vidLabelerQueue
            workItemParams = workItemParams { appParams = params.pack() }.pack()
          }
        }
      )
    } catch (e: StatusException) {
      if (e.status.code != Status.Code.ALREADY_EXISTS) throw e
      logger.info("WorkItem $workItemId already exists; skipping (idempotent re-run)")
    }
  }

  /**
   * Deterministic `VidLabelingJob` `request_id` for a file [batch], keyed by (upload, model line,
   * batch). The batch's first file (files are sorted, batches contiguous) uniquely identifies it
   * and is stable across redeliveries, so the create reuses the existing row.
   */
  private fun labelingJobRequestId(batch: List<String>): String =
    deterministicUuid("$rawImpressionUpload|$modelLine|labelingJob|${batch.first()}")

  /** Flips the parent `RANKING` -> `LABELING`, swallowing the benign "already advanced" races. */
  private suspend fun markParentLabeling(parent: RawImpressionUploadModelLine) {
    try {
      rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          name = parent.name
          etag = parent.etag
        }
      )
    } catch (e: StatusException) {
      if (
        e.status.code != Status.Code.FAILED_PRECONDITION && e.status.code != Status.Code.ABORTED
      ) {
        throw e
      }
      logger.info(
        "markRawImpressionUploadModelLineLabeling(${parent.name}) already advanced " +
          "(${e.status.code}); treating as done"
      )
    }
  }

  /** Best-effort transition of this job to FAILED so operators see the failure; never throws. */
  private suspend fun markFailedBestEffort(cause: Throwable) {
    try {
      val job = rankerJobsStub.getRankerJob(getRankerJobRequest { name = rankerJob })
      if (job.state == RankerJob.State.CREATED) {
        rankerJobsStub.markRankerJobFailed(
          markRankerJobFailedRequest {
            name = rankerJob
            etag = job.etag
            errorMessage = (cause.message ?: cause::class.java.simpleName).take(MAX_ERROR_MESSAGE)
          }
        )
      }
    } catch (e: Exception) {
      logger.log(Level.WARNING, "Failed to mark RankerJob $rankerJob FAILED", e)
    }
  }

  /** Whether every `RankerJob` for this (upload, model line) is `SUCCEEDED`. */
  private suspend fun allRankerJobsSucceeded(): Boolean {
    var sawAny = false
    var allSucceeded = true
    rankerJobsStub
      .listResources { pageToken: String ->
        val response =
          listRankerJobs(
            listRankerJobsRequest {
              parent = rawImpressionUpload
              filter = ListRankerJobsRequestKt.filter { cmmsModelLine = modelLine }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankerJobsList, response.nextPageToken)
      }
      .collect { page ->
        for (job in page) {
          sawAny = true
          if (job.state != RankerJob.State.SUCCEEDED) allSucceeded = false
        }
      }
    return sawAny && allSucceeded
  }

  /** The parent `RawImpressionUploadModelLine` for this (upload, model line), or `null`. */
  private suspend fun getParent(): RawImpressionUploadModelLine? {
    var parentRow: RawImpressionUploadModelLine? = null
    rawImpressionUploadModelLinesStub
      .listResources { pageToken: String ->
        val response =
          listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
              parent = rawImpressionUpload
              filter =
                ListRawImpressionUploadModelLinesRequestKt.filter { cmmsModelLine = modelLine }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
      }
      .collect { page ->
        page.forEach { line -> if (line.cmmsModelLine == modelLine) parentRow = line }
      }
    return parentRow
  }

  private fun markSucceededRequestId(): String = deterministicUuid("$rankerJob|succeeded")

  /**
   * Deterministic UUID4 from [seed], stable across redeliveries so the server reuses an existing
   * row/transition rather than duplicating. MD5 digest with RFC-4122 version (4) + variant bits
   * set.
   */
  private fun deterministicUuid(seed: String): String {
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  companion object {
    private const val MAX_ERROR_MESSAGE = 1024

    /** Max `CreateVidLabelingJobRequest`s per `BatchCreateVidLabelingJobs` call (service limit). */
    private const val MAX_JOBS_PER_BATCH_CREATE = 50

    /**
     * Default bin-packing threshold: `RawImpressionUploadFile`s per `VidLabelingJob`.
     *
     * TODO(@Marco-Premier): source from static runner config (and, once the file resource carries a
     *   size, bin-pack by total input bytes instead of file count).
     */
    private const val DEFAULT_MAX_FILES_PER_LABELING_JOB = 100

    private val logger = Logger.getLogger(VidRankBuilder::class.java.name)
  }
}
