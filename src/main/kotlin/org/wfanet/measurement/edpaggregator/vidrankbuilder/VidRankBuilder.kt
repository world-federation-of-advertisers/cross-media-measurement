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
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankerJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub

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
 * 4. last-job-out only: flips the parent `RawImpressionUploadModelLine` `RANKING` -> `LABELING`.
 *    The Phase-2 fan-out (`VidLabelingJob` creation + WorkItem publish) is not yet implemented —
 *    see [createVidLabelingJobs].
 *
 * Idempotent on redelivery: a `SUCCEEDED` job short-circuits ranking; the parent flip is a no-op
 * once advanced. On failure the job is marked `FAILED` and the error rethrown so the TEE framework
 * nacks.
 *
 * Concurrent-ranker protection: `MarkRankerJobSucceeded` carries the read `etag`; a stale write
 * (another VM won the race after Pub/Sub redelivery) surfaces as `ABORTED`/`FAILED_PRECONDITION`
 * and is treated as a benign already-done.
 */
class VidRankBuilder(
  private val subpoolRanker: SubpoolRanker,
  private val rankerJobsStub: RankerJobServiceCoroutineStub,
  private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  @Suppress("unused") private val vidLabelingJobsStub: VidLabelingJobServiceCoroutineStub,
  @Suppress("unused")
  private val rawImpressionUploadFilesStub: RawImpressionUploadFileServiceCoroutineStub,
  @Suppress("unused") private val workItemsStub: WorkItemsCoroutineStub,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val rankerJob: String,
  private val subpoolMapBlobUris: Map<Long, String>,
  private val subpoolRankedSizes: Map<Long, Int>,
  @Suppress("unused") private val totalShards: Int,
  @Suppress("unused") private val vidLabelerQueue: String,
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
   * Fans out Phase 2 for the (upload, model line): flips the parent `RawImpressionUploadModelLine`
   * `RANKING` -> `LABELING`. The flip is last so `LABELING` is the durable completion marker.
   * `VidLabelingJob` creation is deferred (see [createVidLabelingJobs]).
   */
  private suspend fun runLastJobOut(parent: RawImpressionUploadModelLine) {
    if (parent.state != RawImpressionUploadModelLine.State.RANKING) {
      logger.info("Parent ${parent.name} already past RANKING; last-job-out already complete")
      return
    }
    createVidLabelingJobs()
    // TODO(world-federation-of-advertisers/cross-media-measurement#4000): publish one VidLabeler
    //   WorkItem per shard to `vidLabelerQueue`. Deferred until the memoized Phase-2
    //   `VidLabelerParams` is defined (the current `VidLabelerParams` targets the legacy
    //   `RawImpressionMetadataBatch` path). The VidLabelingJob rows + the RANKING->LABELING flip
    // are
    //   written now so the Monitor can drive Phase 2 once the publisher lands.
    markParentLabeling(parent)
    logger.info(
      "Last-job-out for $modelLine: parent -> LABELING " +
        "(VidLabelingJob creation deferred — see createVidLabelingJobs)"
    )
  }

  /**
   * Creates the Phase-2 `VidLabelingJob` rows for the (upload, model line).
   *
   * TODO(@Marco-Premier): implement alongside the memoized Phase-2 `VidLabelerParams`.
   *
   * Do NOT batch by an integer shard index. Phase 2 must batch by the uploaded *file*: an output
   * blob's `entity_keys` must land in the same file groupings the EDP uploaded them in, so each
   * output blob's `BlobDetails` carries the full `entity_keys` set its source file contained. The
   * job unit is therefore the `RawImpressionUploadFile` (or a bin-packed group of files, listed via
   * [rawImpressionUploadFilesStub] and created via [vidLabelingJobsStub]) — not shard `N`.
   */
  private suspend fun createVidLabelingJobs() {
    // Intentionally empty until the file-batched Phase-2 fan-out is implemented (see KDoc).
  }

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
    private val logger = Logger.getLogger(VidRankBuilder::class.java.name)
  }
}
