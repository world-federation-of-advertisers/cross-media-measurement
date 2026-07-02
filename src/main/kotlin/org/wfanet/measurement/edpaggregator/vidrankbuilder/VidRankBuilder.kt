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
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionFileBinPacker
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFile
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
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
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
 * group of [RawImpressionUploadFile]s — packed First-Fit-Decreasing by `size_bytes` up to
 * [maxFileBatchSizeBytes] (a single file larger than the limit gets its own job). For each group it
 * creates a `VidLabelingJob` row (carrying the model line + that group's files) and publishes one
 * WorkItem to [vidLabelerQueue] whose memoized [VidLabelerParams] points the Phase-2 TEE at the
 * `VidLabelingJob`; the TEE resolves the rank-index blobs from the `RankIndexBlobService` (gRPC)
 * rather than receiving their URIs in the params. The parent flip is last so `LABELING` is the
 * durable completion marker.
 *
 * Idempotent on redelivery: a `SUCCEEDED` job short-circuits ranking; `VidLabelingJob` creation is
 * keyed by a deterministic `request_id` and WorkItem creation by a deterministic `work_item_id`
 * (`ALREADY_EXISTS` tolerated); the parent flip is a no-op once advanced. On failure the exception
 * propagates so the framework nacks and Pub/Sub retries; this worker never marks the job `FAILED`
 * itself -- the dead-letter (DLQ) listener owns the terminal `FAILED` transition on retry
 * exhaustion.
 *
 * Concurrent-ranker protection: `MarkRankerJobSucceeded` carries the read `etag`; a stale write
 * (another VM won the race after Pub/Sub redelivery) surfaces as `ABORTED`/`FAILED_PRECONDITION`
 * and is treated as a benign already-done.
 *
 * @param vidLabelerParamsTemplate the memoized [VidLabelerParams] for this (upload, model line),
 *   built by [VidRankBuilderApp] from the pass-through fields on `VidRankBuilderParams`. The
 *   last-job-out copies it per `VidLabelingJob`, filling the job's name + its files.
 * @param maxFileBatchSizeBytes the bin-packing threshold: the maximum total
 *   `RawImpressionUploadFile` `size_bytes` packed into one `VidLabelingJob` (best-effort; a file
 *   larger than this gets its own job). Sourced from
 *   `VidRankBuilderParams.max_file_batch_size_bytes` (REQUIRED); must be `> 0`.
 * @param maxJobsPerBatchCreate the maximum `CreateVidLabelingJobRequest`s per
 *   `BatchCreateVidLabelingJobs` call (the service's per-batch limit).
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
  private val maxFileBatchSizeBytes: Long,
  private val maxJobsPerBatchCreate: Int = DEFAULT_MAX_JOBS_PER_BATCH_CREATE,
) {
  init {
    require(maxFileBatchSizeBytes > 0) { "maxFileBatchSizeBytes must be > 0" }
    require(maxJobsPerBatchCreate > 0) { "maxJobsPerBatchCreate must be > 0" }
  }

  /**
   * @property subpoolsRanked subpools this job ranked (0 on a redelivery short-circuit).
   * @property lastJobOut whether this job was the last out (and ran the Phase-2 fan-out).
   */
  data class Result(val subpoolsRanked: Int, val lastJobOut: Boolean)

  /**
   * Runs the full Phase-1 work for one `RankerJob`.
   *
   * Any exception propagates so the framework nacks and Pub/Sub retries; this worker never marks
   * the job `FAILED` itself -- the dead-letter (DLQ) listener owns the terminal `FAILED` transition
   * on retry exhaustion.
   */
  suspend fun run(): Result = runRankerJob()

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
      runLastJobOut(parent, fromRecovery = false)
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
    runLastJobOut(parent, fromRecovery = true)
    return Result(0, lastJobOut = true)
  }

  /**
   * Fans out Phase 2 for the (upload, model line) then flips the parent
   * `RawImpressionUploadModelLine` `RANKING` -> `LABELING`. The flip is last so `LABELING` is the
   * durable completion marker (a redelivery before the flip re-runs the idempotent fan-out).
   *
   * On the fresh last-job-out ([fromRecovery] = false) it creates the file-batched
   * `VidLabelingJob`s and publishes a WorkItem for each. On the recovery path ([fromRecovery] =
   * true — a redelivered already-`SUCCEEDED` job) it re-publishes from the `VidLabelingJob`s that
   * were already created, never re-listing the upload's files (which may have changed since the
   * original last-job-out), so recovery is a pure function of what was already created.
   */
  private suspend fun runLastJobOut(parent: RawImpressionUploadModelLine, fromRecovery: Boolean) {
    if (parent.state != RawImpressionUploadModelLine.State.RANKING) {
      logger.info("Parent ${parent.name} already past RANKING; last-job-out already complete")
      return
    }
    require(vidLabelerQueue.isNotEmpty()) {
      "vid_labeler_queue must be configured to fan out Phase-2 for $modelLine"
    }
    val published = if (fromRecovery) republishExistingLabelingJobs() else fanOutLabeling()
    markParentLabeling(parent)
    if (published == 0) {
      // No `VidLabelingJob` exists for this (upload, model line) — an empty upload with no
      // `RawImpressionUploadFile`s to bin-pack — so no Phase-2 last-job-out will ever mark the
      // parent `COMPLETED`. Complete it here (there is nothing to label) instead of stranding it in
      // `LABELING`. Re-read for the post-`LABELING` etag; `markParentCompleted` swallows the benign
      // already-advanced races, so a redelivery is a no-op.
      val labeled = getParent()
      if (labeled != null) {
        markParentCompleted(labeled)
      }
      logger.info("Last-job-out for $modelLine: no files to label; parent -> COMPLETED")
      return
    }
    logger.info(
      "Last-job-out for $modelLine: published $published VidLabelingJob WorkItem(s); parent -> " +
        "LABELING"
    )
  }

  /**
   * Creates the Phase-2 `VidLabelingJob`s for the (upload, model line) — one per bin-packed group
   * of the upload's `RawImpressionUploadFile`s — and publishes a memoized-`VidLabelerParams`
   * WorkItem for each. Returns the number of WorkItems published.
   */
  private suspend fun fanOutLabeling(): Int {
    val files = listUploadFiles()
    if (files.isEmpty()) {
      logger.warning(
        "Last-job-out for $modelLine: no RawImpressionUploadFiles under $rawImpressionUpload; " +
          "nothing to label"
      )
      return 0
    }

    val batches: List<List<String>> = RawImpressionFileBinPacker.pack(files, maxFileBatchSizeBytes)
    val labelingJobs = createVidLabelingJobs(batches)
    labelingJobs.forEach { job -> publishVidLabelerWorkItem(job) }
    return labelingJobs.size
  }

  /**
   * Re-publishes a Phase-2 WorkItem for every existing `VidLabelingJob` of this (upload, model
   * line). Used on the recovery path: rather than re-listing the upload's files, recovery reads
   * back exactly the jobs that were already created and re-publishes them (publish is idempotent —
   * `ALREADY_EXISTS` tolerated). Returns the number of WorkItems published.
   */
  private suspend fun republishExistingLabelingJobs(): Int {
    var published = 0
    vidLabelingJobsStub
      .listResources { pageToken: String ->
        val response =
          listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = rawImpressionUpload
              filter = ListVidLabelingJobsRequestKt.filter { cmmsModelLine = modelLine }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.vidLabelingJobsList, response.nextPageToken)
      }
      .collect { page ->
        page.forEach { job ->
          publishVidLabelerWorkItem(job)
          published++
        }
      }
    return published
  }

  /** Lists every (non-deleted) `RawImpressionUploadFile` under this upload. */
  private suspend fun listUploadFiles(): List<RawImpressionUploadFile> {
    val files = mutableListOf<RawImpressionUploadFile>()
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
      .collect { page -> files.addAll(page) }
    return files
  }

  /**
   * Creates a `VidLabelingJob` per [batches] entry via `BatchCreateVidLabelingJobs` (chunked to
   * [maxJobsPerBatchCreate]). Each request carries a deterministic `request_id` keyed by the
   * batch's **index** (stable across redeliveries even if the upload's file list changed), so a
   * redelivered last-job-out reuses the existing rows. Each chunk is checked to return exactly as
   * many jobs as requested so a partial response can't silently drop a batch. Returns each created
   * job.
   */
  private suspend fun createVidLabelingJobs(batches: List<List<String>>): List<VidLabelingJob> {
    val created = mutableListOf<VidLabelingJob>()
    for (group in batches.withIndex().chunked(maxJobsPerBatchCreate)) {
      val response =
        vidLabelingJobsStub.batchCreateVidLabelingJobs(
          batchCreateVidLabelingJobsRequest {
            parent = rawImpressionUpload
            for ((batchIndex, batch) in group) {
              requests += createVidLabelingJobRequest {
                parent = rawImpressionUpload
                vidLabelingJob = vidLabelingJob {
                  cmmsModelLines += modelLine
                  rawImpressionUploadFiles += batch
                }
                requestId = labelingJobRequestId(batchIndex)
              }
            }
          }
        )
      check(response.vidLabelingJobsList.size == group.size) {
        "BatchCreateVidLabelingJobs returned ${response.vidLabelingJobsList.size} jobs for " +
          "${group.size} requests"
      }
      created.addAll(response.vidLabelingJobsList)
    }
    return created
  }

  /**
   * Publishes one Phase-2 WorkItem for [job], copying [vidLabelerParamsTemplate] with the job's
   * name. The TEE resolves the job's `raw_impression_upload_files` by getting the `VidLabelingJob`,
   * so they are not duplicated onto the WorkItem params. Tolerates `ALREADY_EXISTS` so a
   * redelivered last-job-out is a no-op.
   */
  private suspend fun publishVidLabelerWorkItem(job: VidLabelingJob) {
    val params = vidLabelerParamsTemplate.copy { vidLabelingJob = job.name }
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
   * Deterministic `VidLabelingJob` `request_id` for the batch at [batchIndex], keyed by (upload,
   * model line, batch index). Keying by index (rather than a filename) keeps batch slot N stable
   * across redeliveries even if a `RawImpressionUploadFile` was added/removed in between, so the
   * create reuses the existing row instead of duplicating it.
   */
  private fun labelingJobRequestId(batchIndex: Int): String =
    deterministicUuid("$rawImpressionUpload|$modelLine|labelingJob|$batchIndex")

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

  /**
   * Flips the parent `LABELING` -> `COMPLETED`, swallowing the benign "already advanced" races.
   * Used only for an empty upload (no files to label), where no Phase-2 `VidLabelingJob` exists to
   * drive the completion; the normal (non-empty) path leaves `COMPLETED` to the Phase-2
   * last-job-out.
   */
  private suspend fun markParentCompleted(parent: RawImpressionUploadModelLine) {
    try {
      rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineCompleted(
        markRawImpressionUploadModelLineCompletedRequest {
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
        "markRawImpressionUploadModelLineCompleted(${parent.name}) already advanced " +
          "(${e.status.code}); treating as done"
      )
    }
  }

  /**
   * Whether every `RankerJob` for this (upload, model line) has reached `SUCCEEDED`.
   *
   * Used on the recovery path. Computed with two O(1) COUNTs (`ListRankerJobsResponse.total_size`,
   * `page_size = 0`) rather than listing every row: the total, then the non-SUCCEEDED
   * (`CREATED`/`FAILED`) jobs. An empty set is not "all succeeded" (so a degenerate listing never
   * triggers a fan-out), and a `FAILED` job counts as not-yet-succeeded, so the fan-out waits until
   * the Monitor/DLQ re-drives it.
   */
  private suspend fun allRankerJobsSucceeded(): Boolean {
    val totalCount =
      rankerJobsStub
        .listRankerJobs(
          listRankerJobsRequest {
            parent = rawImpressionUpload
            filter = ListRankerJobsRequestKt.filter { cmmsModelLine = modelLine }
            pageSize = 0
          }
        )
        .totalSize
    if (totalCount == 0) return false
    val nonSucceededCount =
      rankerJobsStub
        .listRankerJobs(
          listRankerJobsRequest {
            parent = rawImpressionUpload
            filter =
              ListRankerJobsRequestKt.filter {
                cmmsModelLine = modelLine
                stateIn += listOf(RankerJob.State.CREATED, RankerJob.State.FAILED)
              }
            pageSize = 0
          }
        )
        .totalSize
    return nonSucceededCount == 0
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
        page.forEach { line ->
          if (line.cmmsModelLine == modelLine) {
            check(parentRow == null) {
              "Duplicate RawImpressionUploadModelLine for $modelLine under $rawImpressionUpload"
            }
            parentRow = line
          }
        }
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
    /**
     * Default max `CreateVidLabelingJobRequest`s per `BatchCreateVidLabelingJobs` call (the
     * service's per-batch limit).
     */
    private const val DEFAULT_MAX_JOBS_PER_BATCH_CREATE = 50

    private val logger = Logger.getLogger(VidRankBuilder::class.java.name)
  }
}
