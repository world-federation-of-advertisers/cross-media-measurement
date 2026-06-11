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

package org.wfanet.measurement.edpaggregator.subpoolassigner

import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow

/**
 * Phase-0 per-WorkItem logic for the memoized VID assignment pipeline (analog of
 * `ResultsFulfiller`).
 *
 * For one (`RawImpressionUpload`, `ModelLine`, shard) WorkItem it:
 * 1. streams this shard's raw impressions, projects + active-window-filters them, and pool-emit
 *    labels each surviving fingerprint into [accumulator] (subpool -> fingerprint set),
 * 2. generates a per-shard DEK and writes one RecordIO `SubpoolFingerprints` blob per subpool plus
 *    a DEK blob, via [store],
 * 3. marks this shard's `PoolAssignmentJob` `SUCCEEDED`; the service atomically reports whether
 *    this was the last shard out and, if so, returns the discovered pool offsets,
 * 4. last-shard-out only: merges every shard's per-subpool blobs into one blob per subpool
 *    (streaming concat — shards partition the fingerprint space, so the slices are disjoint and
 *    need no dedup), then deletes the temp per-shard blobs.
 *
 * Idempotent on Pub/Sub redelivery at the per-shard granularity (a job already `SUCCEEDED` short-
 * circuits the mark); on failure the job is marked `FAILED` and the error rethrown so the TEE
 * framework nacks.
 */
class SubpoolAssigner(
    private val rawImpressionSource: RawImpressionSource,
    private val mapper: LabelerInputMapper,
    private val labeler: PoolEmitLabeler,
    private val activeWindow: ActiveWindow,
    private val store: SubpoolFingerprintsStore,
    private val kekUri: String,
    private val blobPrefix: String,
    private val poolAssignmentJobsStub: PoolAssignmentJobServiceCoroutineStub,
    private val rawImpressionUploadModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
    private val rankerJobsStub: RankerJobServiceCoroutineStub,
    private val rawImpressionUploadsStub: RawImpressionUploadServiceCoroutineStub,
    private val rawImpressionUpload: String,
    private val modelLine: String,
    private val poolAssignmentJob: String,
    private val shardIndex: Int,
    private val totalShards: Int,
    private val accumulator: SubpoolFingerprintsAccumulator = SubpoolFingerprintsAccumulator(),
    private val metrics: SubpoolAssignerMetrics = SubpoolAssignerMetrics(),
) {
  /**
   * @property subpoolCount number of non-empty subpools this shard wrote.
   * @property labeled events that routed to at least one subpool.
   * @property droppedOutsideWindow events dropped by the active-window filter.
   * @property unrouted events the labeler routed to no subpool.
   * @property lastShardOut whether this shard was the last to complete (and ran the merge).
   */
  data class Result(
      val subpoolCount: Int,
      val labeled: Long,
      val droppedOutsideWindow: Long,
      val unrouted: Long,
      val lastShardOut: Boolean,
  )

  /** Runs the full Phase-0 work for one shard. */
  suspend fun assign(): Result {
    try {
      return runShard()
    } catch (t: Throwable) {
      markFailedBestEffort(t)
      throw t
    }
  }

  private suspend fun runShard(): Result {
    // 1. Read -> label -> accumulate.
    val sink = SubpoolAssignmentSink(mapper, labeler, accumulator, activeWindow, metrics)
    rawImpressionSource.streamBlobs(openSink = { sink })

    // 2. Generate this shard's DEK and stream each subpool to its own RecordIO blob, freeing the
    // subpool's in-memory map as soon as its blob is durable. Then write the per-shard DEK blob.
    val dek: EncryptedDek = store.generateDek(kekUri)
    val subpoolIds = accumulator.subpoolIds().toList()
    for (subpoolId in subpoolIds) {
      store.writeBlob(
          shardSubpoolKey(shardIndex, subpoolId),
          dek,
          subpoolId,
          accumulator.streamChunks(subpoolId),
      )
      accumulator.remove(subpoolId)
    }
    val subpoolCount = subpoolIds.size

    logger.info(
        "Shard $shardIndex/$totalShards for $modelLine: wrote $subpoolCount subpools " +
            "(labeled=${sink.labeled}, droppedOutsideWindow=${sink.droppedOutsideWindow}, " +
            "unrouted=${sink.unrouted})")

    // 3. Mark this shard SUCCEEDED (idempotent on redelivery).
    val job =
        poolAssignmentJobsStub.getPoolAssignmentJob(
            getPoolAssignmentJobRequest { name = poolAssignmentJob })
    if (job.state == PoolAssignmentJob.State.SUCCEEDED) {
      // TODO(@Marco-Premier): on redelivery, re-run only the last-shard-out check — LastShardResult
      //   is returned by MarkPoolAssignmentJobSucceeded, not Get, so it cannot be recovered here.
      logger.info("PoolAssignmentJob $poolAssignmentJob already SUCCEEDED; skipping re-mark")
      return summary(subpoolCount, sink, lastShardOut = false)
    }
    val markResponse =
        poolAssignmentJobsStub.markPoolAssignmentJobSucceeded(
            markPoolAssignmentJobSucceededRequest {
              name = poolAssignmentJob
              etag = job.etag
              encryptedDek = dek
            })

    // 4. Last-shard-out: merge per-shard blobs into one blob per subpool.
    val lastShardOut = markResponse.hasLastShardResult()
    if (lastShardOut) {
      runLastShardOut(markResponse.lastShardResult.poolOffsetsList, mergedDek = dek)
    }
    return summary(subpoolCount, sink, lastShardOut)
  }

  /**
   * Merges every shard's per-subpool blobs into one blob per subpool (encrypted with [mergedDek],
   * this last VM's own DEK), then deletes the temp per-shard blobs.
   */
  private suspend fun runLastShardOut(poolOffsets: List<Long>, mergedDek: EncryptedDek) {
    logger.info(
        "Shard $shardIndex is last-out for $modelLine; merging ${poolOffsets.size} subpools")

    // Each shard encrypted its blobs with its own DEK, persisted on its PoolAssignmentJob row by
    // MarkPoolAssignmentJobSucceeded; recover them via ListPoolAssignmentJobs for this model line.
    val shardDeks: Map<Int, EncryptedDek> = listShardDeks()

    for (poolOffset in poolOffsets) {
      val inputs =
          (0 until totalShards).map { shard ->
            SubpoolFingerprintsStore.SubpoolBlob(
                shardSubpoolKey(shard, poolOffset),
                requireNotNull(shardDeks[shard]) { "Missing DEK for shard $shard; cannot merge" },
            )
          }
      store.mergeSubpool(inputs, mergedSubpoolKey(poolOffset), mergedDek)
    }

    // Clean up the temp per-shard blobs now that the merged blobs are durable.
    for (shard in 0 until totalShards) {
      for (poolOffset in poolOffsets) {
        store.delete(shardSubpoolKey(shard, poolOffset))
      }
    }

    // TODO(@Marco-Premier): control-plane fan-out (separate change — needs bin-packing policy +
    //   resource-name derivation + the dispatcher's WorkItem-publish mechanism):
    //   - bin-pack poolOffsets into RankerJob rows by fingerprint count (small subpools share a VM)
    //     and create them via rankerJobsStub.createRankerJob (cmms_model_line=$modelLine,
    //     pool_offsets=...),
    //   - flip RawImpressionUploadModelLine POOL_ASSIGNING -> RANKING via
    //     rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineRanking,
    //   - publish one VidRankBuilder WorkItem per RankerJob with
    //     VidRankBuilderParams.encrypted_subpool_maps_dek = mergedDek and the merged blob URIs.
  }

  /** Best-effort transition of this job to FAILED so operators see the failure; never throws. */
  private suspend fun markFailedBestEffort(cause: Throwable) {
    try {
      val job =
          poolAssignmentJobsStub.getPoolAssignmentJob(
              getPoolAssignmentJobRequest { name = poolAssignmentJob })
      if (job.state == PoolAssignmentJob.State.CREATED) {
        poolAssignmentJobsStub.markPoolAssignmentJobFailed(
            markPoolAssignmentJobFailedRequest {
              name = poolAssignmentJob
              etag = job.etag
              errorMessage = (cause.message ?: cause::class.java.simpleName).take(MAX_ERROR_MESSAGE)
            })
      }
    } catch (e: Exception) {
      logger.warning("Failed to mark PoolAssignmentJob $poolAssignmentJob FAILED: $e")
    }
  }

  /**
   * Returns each shard's DEK keyed by `shard_index`, read from the `PoolAssignmentJob` rows of this
   * (`RawImpressionUpload`, `ModelLine`) via `ListPoolAssignmentJobs` (paged). Each row carries the
   * DEK its shard's `MarkPoolAssignmentJobSucceeded` persisted.
   */
  private suspend fun listShardDeks(): Map<Int, EncryptedDek> {
    val deks = HashMap<Int, EncryptedDek>(totalShards)
    poolAssignmentJobsStub
        .listResources { pageToken: String ->
          val response =
              listPoolAssignmentJobs(
                  listPoolAssignmentJobsRequest {
                    parent = rawImpressionUpload
                    filter = ListPoolAssignmentJobsRequestKt.filter { cmmsModelLine = modelLine }
                    this.pageToken = pageToken
                  })
          ResourceList(response.poolAssignmentJobsList, response.nextPageToken)
        }
        .collect { page -> page.forEach { job -> deks[job.shardIndex] = job.encryptedDek } }
    return deks
  }

  private fun summary(subpoolCount: Int, sink: SubpoolAssignmentSink, lastShardOut: Boolean) =
      Result(
          subpoolCount = subpoolCount,
          labeled = sink.labeled,
          droppedOutsideWindow = sink.droppedOutsideWindow,
          unrouted = sink.unrouted,
          lastShardOut = lastShardOut,
      )

  private val prefix: String
    get() = blobPrefix.trimEnd('/')

  private fun shardSubpoolKey(shard: Int, poolOffset: Long) =
      "$prefix/shard-$shard/subpool-$poolOffset"

  private fun mergedSubpoolKey(poolOffset: Long) = "$prefix/merged/subpool-$poolOffset"

  companion object {
    private const val MAX_ERROR_MESSAGE = 1024
    private val logger = Logger.getLogger(SubpoolAssigner::class.java.name)
  }
}
