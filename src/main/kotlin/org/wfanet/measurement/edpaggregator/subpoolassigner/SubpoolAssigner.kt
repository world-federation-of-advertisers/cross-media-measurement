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

import com.google.type.Date
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusException
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.edpaggregator.rawimpressions.LabelerInputMapper
import org.wfanet.measurement.edpaggregator.rawimpressions.RawImpressionSource
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.VidRankBuilderParams
import org.wfanet.measurement.edpaggregator.v1alpha.copy
import org.wfanet.measurement.edpaggregator.v1alpha.createRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.ActiveWindow
import org.wfanet.measurement.edpaggregator.vidlabeling.WorkItemIds
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemKt.workItemParams
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.WorkItemsGrpcKt.WorkItemsCoroutineStub
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.createWorkItemRequest
import org.wfanet.measurement.securecomputation.controlplane.v1alpha.workItem

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
 *    need no dedup), deletes the temp per-shard blobs, then fans out Phase 1 — creates a
 *    `RankerJob` per subpool, publishes one `VidRankBuilder` WorkItem per `RankerJob`, and flips
 *    the parent `RawImpressionUploadModelLine` `POOL_ASSIGNING` -> `RANKING`.
 *
 * Idempotent on Pub/Sub redelivery at the per-shard granularity (a job already `SUCCEEDED` short-
 * circuits the mark). On failure the error propagates so the TEE framework nacks (Pub/Sub retries
 * -> dead-letter); this worker never marks the job `FAILED` itself — the single authoritative
 * terminal `FAILED` transition is owned by the DLQ listener on retry exhaustion.
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
  private val workItemsStub: WorkItemsCoroutineStub,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val poolAssignmentJob: String,
  private val shardIndex: Int,
  private val totalShards: Int,
  // Phase-1 (VidRankBuilder) fan-out config. The template carries the static `VidRankBuilderParams`
  // fields (all passed through from `SubpoolAssignerParams`); the per-RankerJob fields
  // (`encrypted_subpool_maps_dek`, `ranker_job`, `subpool_map_blob_uris`, `max_event_date`) are
  // filled in at fan-out time.
  private val vidRankBuilderQueue: String,
  private val vidRankBuilderParamsTemplate: VidRankBuilderParams,
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

  /**
   * Runs the full Phase-0 work for one shard. Any exception propagates so the TEE framework nacks
   * the message; this worker never marks the job `FAILED` (the DLQ listener owns the terminal
   * `FAILED` transition on retry exhaustion).
   */
  suspend fun assign(): Result = runShard()

  private suspend fun runShard(): Result {
    // Gate on job state first: on Pub/Sub redelivery of an already-completed shard, skip the
    // expensive re-read/re-label/re-write (SUCCEEDED is set only after the per-shard write is
    // durable, so it is a reliable completion marker) and only recover the last-shard-out merge.
    val job =
      poolAssignmentJobsStub.getPoolAssignmentJob(
        getPoolAssignmentJobRequest { name = poolAssignmentJob }
      )
    if (job.state == PoolAssignmentJob.State.SUCCEEDED) {
      return recoverIfLastShardOut()
    }

    // 1. Read -> label -> accumulate.
    val sink = SubpoolAssignmentSink(mapper, labeler, accumulator, activeWindow, metrics)
    // Phase 0 has no entity keys; ignore the per-file footer metadata.
    rawImpressionSource.streamBlobs(openSink = { _, _ -> sink })

    // 2. Generate this shard's DEK and stream each subpool to its own RecordIO blob, freeing the
    // subpool's in-memory map as soon as its blob is durable.
    // NOTE(world-federation-of-advertisers/cross-media-measurement#3999): the per-shard write below
    //   is intentionally unconditional (not write-if-absent) — see
    // SubpoolFingerprintsStore.writeBlob.
    //   For these envelope-encrypted blobs (per-attempt random DEK + IV) a write-if-absent
    // precondition
    //   would turn a crash between the write and MarkPoolAssignmentJobSucceeded — today
    // auto-recovered
    //   by re-writing — into a permanent FAILED, since the orphan ciphertext fails every retry's
    // 412
    //   and the job never reaches SUCCEEDED. A correct fix (recover the winner's DEK on retry:
    //   row-first DEK, or key-on-row) is an API change; kept unconditional for now.
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
        "unrouted=${sink.unrouted})"
    )

    // 3. Mark this shard SUCCEEDED, reporting its discovered subpool offsets and latest event date
    // so the service can union the offsets and reduce the dates to a max across shards, persisting
    // both on the parent RawImpressionUploadModelLine.
    val markResponse =
      poolAssignmentJobsStub.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          name = poolAssignmentJob
          etag = job.etag
          // AIP-155 retry-idempotency key: a Pub/Sub redelivery reuses the same request_id so the
          // server returns the cached LastShardResult instead of hitting the etag-mismatch path.
          requestId = deterministicUuid("$poolAssignmentJob|succeeded")
          encryptedDek = dek
          poolOffsets += subpoolIds
          sink.maxTimestampUsec?.let { maxEventDate = usecToUtcDate(it) }
        }
      )

    // 4. Last-shard-out: merge per-shard blobs into one blob per subpool, then fan out Phase 1.
    val lastShardOut = markResponse.hasLastShardResult()
    if (lastShardOut) {
      val parent =
        requireNotNull(getParent()) {
          "RawImpressionUploadModelLine not found for $modelLine under $rawImpressionUpload"
        }
      runLastShardOut(
        parent,
        markResponse.lastShardResult.poolOffsetsList,
        markResponse.lastShardResult.maxEventDate,
        mergedDek = dek,
      )
    }
    return summary(subpoolCount, sink, lastShardOut)
  }

  /**
   * Recovers the last-shard-out on Pub/Sub redelivery of an already-SUCCEEDED shard. The union of
   * discovered subpool offsets, the max event date, and the merged-blob DEK were persisted on the
   * parent `RawImpressionUploadModelLine` by the last-shard-out's `MarkPoolAssignmentJobSucceeded`,
   * so we read them back:
   * - empty `pool_offsets` -> this shard was not the last out; nothing to do,
   * - parent state already `RANKING`/`LABELING`/`COMPLETED` -> the last-shard-out fully finished
   *   (the state flip is its last step), so there is nothing to recover,
   * - otherwise -> a crash interrupted the last-shard-out; re-run it. The merge, RankerJob
   *   creation, and WorkItem publish are all idempotent, so re-running is safe.
   *
   * The merge reuses the persisted `encrypted_merged_dek` (not a freshly generated DEK): the merged
   * blobs and every published `VidRankBuilderParams.encrypted_subpool_maps_dek` must agree on the
   * DEK across re-runs, so the re-merge must re-encrypt with the exact DEK the first run used.
   */
  private suspend fun recoverIfLastShardOut(): Result {
    val parent = getParent()
    if (parent == null || parent.poolOffsetsList.isEmpty()) {
      logger.info("PoolAssignmentJob $poolAssignmentJob already SUCCEEDED; not last-shard-out")
      return Result(0, 0, 0, 0, lastShardOut = false)
    }
    if (parent.state in COMPLETED_FANOUT_STATES) {
      logger.info(
        "PoolAssignmentJob $poolAssignmentJob already SUCCEEDED; last-shard-out already complete " +
          "(parent state=${parent.state})"
      )
      return Result(0, 0, 0, 0, lastShardOut = true)
    }
    require(parent.hasEncryptedMergedDek()) {
      "RawImpressionUploadModelLine ${parent.name} has pool_offsets but no encrypted_merged_dek; " +
        "cannot recover the merge with a consistent DEK"
    }
    logger.info("PoolAssignmentJob $poolAssignmentJob already SUCCEEDED; recovering last-shard-out")
    runLastShardOut(
      parent,
      parent.poolOffsetsList,
      parent.maxEventDate,
      mergedDek = parent.encryptedMergedDek,
    )
    return Result(0, 0, 0, 0, lastShardOut = true)
  }

  /**
   * Merges every shard's per-subpool blobs into one blob per subpool (encrypted with [mergedDek],
   * this last VM's own DEK), deletes the temp per-shard blobs, then fans out Phase 1.
   */
  private suspend fun runLastShardOut(
    parent: RawImpressionUploadModelLine,
    poolOffsets: List<Long>,
    maxEventDate: Date,
    mergedDek: EncryptedDek,
  ) {
    logger.info(
      "Shard $shardIndex is last-out for $modelLine; merging ${poolOffsets.size} subpools"
    )

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
      // NOTE(world-federation-of-advertisers/cross-media-measurement#3999): mergeSubpool writes
      //   unconditionally (see SubpoolFingerprintsStore.mergeSubpool). This recovery path
      //   re-runs the merge idempotently by re-writing the merged blob, which a write-if-absent
      //   precondition would break — so the merge is deliberately left unconditional.
      store.mergeSubpool(inputs, mergedSubpoolKey(poolOffset), mergedDek)
    }

    fanOutRanking(parent, poolOffsets, maxEventDate, mergedDek)

    // Clean up the temp per-shard blobs only AFTER fanOutRanking flips the parent to RANKING.
    // recovery re-runs the last-shard-out only while the parent is still POOL_ASSIGNING (pre-flip),
    // so deferring the delete to here guarantees a recovery re-merge still finds its inputs; once
    // the flip lands, recovery short-circuits on the completed state and never re-merges. A crash
    // in
    // the narrow post-flip/pre-delete window leaks the temp blobs (no corruption) — they are
    // reclaimed by the monitor (#3958) or the bucket lifecycle policy.
    for (shard in 0 until totalShards) {
      for (poolOffset in poolOffsets) {
        store.delete(shardSubpoolKey(shard, poolOffset))
      }
    }
  }

  /**
   * Fans out Phase 1 for the merged subpools: creates a `RankerJob`, publishes a `VidRankBuilder`
   * WorkItem for it, and finally flips the parent `RawImpressionUploadModelLine` `POOL_ASSIGNING`
   * -> `RANKING`. The state flip is last so that `RANKING` is the durable marker that the whole
   * last-shard-out completed (see [recoverIfLastShardOut]).
   *
   * Every step is idempotent on re-run: `RankerJob` creation is keyed by a deterministic
   * `request_id`, WorkItem creation by a deterministic `work_item_id` (a duplicate is tolerated),
   * and the flip is a no-op once the parent has advanced past `POOL_ASSIGNING`.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#3998): bin-pack subpools into
   *   shared `RankerJob`s by fingerprint count so small subpools share a VM. For now this creates
   *   one `RankerJob` (and one `VidRankBuilder` VM) per subpool.
   */
  private suspend fun fanOutRanking(
    parent: RawImpressionUploadModelLine,
    poolOffsets: List<Long>,
    maxEventDate: Date,
    mergedDek: EncryptedDek,
  ) {
    for (poolOffset in poolOffsets) {
      val offsets = listOf(poolOffset)
      val rankerJob = createRankerJob(offsets)
      publishVidRankBuilderWorkItem(rankerJob, offsets, maxEventDate, mergedDek)
    }

    markParentRanking(parent)
    logger.info(
      "Fanned out ${poolOffsets.size} RankerJob(s) for $modelLine; parent advanced to RANKING"
    )
  }

  /**
   * Creates (idempotently) a `RankerJob` for [poolOffsets] under this upload. The `request_id` is
   * derived deterministically from (model line, offsets) so a redelivered last-shard-out reuses the
   * existing row instead of creating a duplicate.
   */
  private suspend fun createRankerJob(poolOffsets: List<Long>): RankerJob =
    rankerJobsStub.createRankerJob(
      createRankerJobRequest {
        parent = rawImpressionUpload
        rankerJob = rankerJob {
          cmmsModelLine = modelLine
          this.poolOffsets += poolOffsets
        }
        requestId = rankerJobRequestId(poolOffsets)
      }
    )

  /**
   * Publishes one `VidRankBuilder` WorkItem for [rankerJob], pointing it at the merged blobs for
   * [offsets] (resolved against `subpool_map_storage_params`) and the [mergedDek] that encrypts
   * them. Tolerates `ALREADY_EXISTS` so a redelivered last-shard-out is a no-op.
   */
  private suspend fun publishVidRankBuilderWorkItem(
    rankerJob: RankerJob,
    offsets: List<Long>,
    maxEventDate: Date,
    mergedDek: EncryptedDek,
  ) {
    val params =
      vidRankBuilderParamsTemplate.copy {
        encryptedSubpoolMapsDek = mergedDek
        this.rankerJob = rankerJob.name
        this.maxEventDate = maxEventDate
        offsets.forEach { subpoolMapBlobUris.put(it, mergedSubpoolKey(it)) }
        offsets.forEach { subpoolRankedSizes.put(it, labeler.rankedSize(it)) }
      }
    val workItemId = WorkItemIds.forVidRankBuilder(rankerJob.name)
    try {
      workItemsStub.createWorkItem(
        createWorkItemRequest {
          this.workItemId = workItemId
          workItem = workItem {
            queue = vidRankBuilderQueue
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
   * Flips the parent `RawImpressionUploadModelLine` `POOL_ASSIGNING` -> `RANKING`. Only attempted
   * when the parent is still `POOL_ASSIGNING`; any failure is swallowed so a redelivered
   * last-shard-out (where another runner may have already advanced the state) is a no-op.
   */
  private suspend fun markParentRanking(parent: RawImpressionUploadModelLine) {
    if (parent.state != RawImpressionUploadModelLine.State.POOL_ASSIGNING) return
    try {
      // Pass the parent's etag for AIP-154 optimistic locking: the POOL_ASSIGNING -> RANKING flip
      // only lands if the row hasn't changed since getParent() read it, so a concurrent runner that
      // already advanced (or otherwise mutated) the parent loses the CAS with ABORTED rather than
      // racing on the state guard alone.
      rawImpressionUploadModelLinesStub.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          name = parent.name
          etag = parent.etag
        }
      )
    } catch (e: StatusException) {
      // Swallow only the benign "already advanced" races: the parent read at getParent() time is
      // stale, so a concurrent runner that already flipped this row (or bumped its etag) surfaces
      // as
      // FAILED_PRECONDITION/ABORTED (the latter is the etag-mismatch code) and re-doing the flip is
      // unnecessary. Any other error (e.g. UNAVAILABLE) is transient and must propagate so the
      // message nacks and the idempotent last-shard-out is retried — otherwise the POOL_ASSIGNING
      // ->
      // RANKING flip (the completion marker that recovery gates on) is silently lost on ack.
      if (
        e.status.code != Status.Code.FAILED_PRECONDITION && e.status.code != Status.Code.ABORTED
      ) {
        throw e
      }
      logger.info(
        "markRawImpressionUploadModelLineRanking(${parent.name}) already advanced " +
          "(${e.status.code}); treating as done"
      )
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
            }
          )
        ResourceList(response.poolAssignmentJobsList, response.nextPageToken)
      }
      .collect { page -> page.forEach { job -> deks[job.shardIndex] = job.encryptedDek } }
    return deks
  }

  /**
   * Returns the parent `RawImpressionUploadModelLine` for this (`RawImpressionUpload`,
   * `ModelLine`), located via `ListRawImpressionUploadModelLines` filtered by model line, or `null`
   * if absent. Carries the resource name (for the state flip), the `pool_offsets` union, the
   * reduced `max_event_date`, and the current pipeline state — all populated by the
   * last-shard-out's `MarkPoolAssignmentJobSucceeded`.
   */
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

  /** Converts an epoch-microsecond timestamp to a UTC `google.type.Date`. */
  private fun usecToUtcDate(timestampUsec: Long): Date {
    val instant =
      Instant.ofEpochSecond(
        Math.floorDiv(timestampUsec, 1_000_000L),
        Math.floorMod(timestampUsec, 1_000_000L) * 1_000L,
      )
    val localDate = instant.atZone(ZoneOffset.UTC).toLocalDate()
    return date {
      year = localDate.year
      month = localDate.monthValue
      day = localDate.dayOfMonth
    }
  }

  /**
   * Deterministic UUID4 idempotency key for the `RankerJob` covering [poolOffsets], stable across
   * redeliveries of the same (upload, model line, offsets) so the server reuses an existing row
   * rather than duplicating.
   */
  private fun rankerJobRequestId(poolOffsets: List<Long>): String =
    deterministicUuid("$rawImpressionUpload|$modelLine|${poolOffsets.sorted().joinToString(",")}")

  /**
   * Derives a deterministic UUID4 from [seed], stable across redeliveries, for use as an AIP-155
   * `request_id`. Computed from an MD5 digest of the seed with the RFC-4122 version (4) and variant
   * bits forced, so it satisfies a field's `format = UUID4`.
   */
  private fun deterministicUuid(seed: String): String {
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  private fun summary(subpoolCount: Int, sink: SubpoolAssignmentSink, lastShardOut: Boolean) =
    Result(
      subpoolCount = subpoolCount,
      labeled = sink.labeled,
      droppedOutsideWindow = sink.droppedOutsideWindow,
      unrouted = sink.unrouted,
      lastShardOut = lastShardOut,
    )

  // Blob layout lives in SubpoolFingerprintsStore so Phase-1 reads the same keys; these bind this
  // run's (blobPrefix, upload, model line) context.
  private fun shardSubpoolKey(shard: Int, poolOffset: Long) =
    SubpoolFingerprintsStore.shardSubpoolKey(
      blobPrefix,
      rawImpressionUpload,
      modelLine,
      shard,
      poolOffset,
    )

  private fun mergedSubpoolKey(poolOffset: Long) =
    SubpoolFingerprintsStore.mergedSubpoolKey(
      blobPrefix,
      rawImpressionUpload,
      modelLine,
      poolOffset,
    )

  companion object {
    private val logger = Logger.getLogger(SubpoolAssigner::class.java.name)

    /**
     * Parent states that mean the last-shard-out fan-out already completed (state flip is last).
     */
    private val COMPLETED_FANOUT_STATES =
      setOf(
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.LABELING,
        RawImpressionUploadModelLine.State.COMPLETED,
      )
  }
}
