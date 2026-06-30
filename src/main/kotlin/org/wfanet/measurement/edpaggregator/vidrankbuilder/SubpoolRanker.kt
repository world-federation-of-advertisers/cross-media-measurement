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

import com.google.type.Date
import com.google.type.date
import java.nio.ByteBuffer
import java.security.MessageDigest
import java.time.LocalDate
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.rawimpressions.SubpoolFingerprintsStore
import org.wfanet.measurement.edpaggregator.v1alpha.EncryptedDek
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexBlob
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * Ranks a **single** subpool: the per-subpool engine driven by [VidRankBuilder] for each subpool a
 * `RankerJob` covers.
 *
 * For one subpool it:
 * 1. **idempotency gate** — if a `SNAPSHOT` `RankIndexBlob` row already exists for this (upload,
 *    subpool), a prior attempt already committed its blobs and rows atomically, so the subpool is
 *    done and is skipped (cheap re-delivery no-op),
 * 2. loads the prior cumulative `SNAPSHOT` (the most recent by `create_time` across all uploads —
 *    the current cumulative state) into a [RankAllocator] (rebuilding its rank `BitSet` and
 *    per-rank `last_seen`), validating the blob checksum,
 * 3. decides whether this dispatch is a **backfill** (see below),
 * 4. materializes this dispatch's fingerprint set from the Phase-0 merged `SubpoolFingerprints`
 *    blob,
 * 5. garbage-collects aged-out `DAY_ONLY` blobs ([SubpoolRetention]) and frees aged-out ranks in
 *    heap via the `last_seen` index, excluding fingerprints seen this dispatch,
 * 6. allocates ranks to today's fingerprints (renewing already-ranked ones, capping at
 *    `ranked_size`); on a backfill it instead replays each fingerprint's historical rank (below),
 * 7. writes the new `DAY_ONLY` + `SNAPSHOT` blobs (DEK-encrypted) under **per-attempt-unique keys**
 *    and records both as `RankIndexBlob` rows.
 *
 * ## Backfill (Problem 1 — give a person back their original rank)
 *
 * The pipeline keeps a single live cumulative snapshot per (EDP, model line, subpool). That is
 * correct for forward data appended in chronological order, but a **backfill** — an upload whose
 * newest event date ([maxEventDate]) is older than the most recent data already ranked — can land a
 * fingerprint whose original rank was freed and re-handed to someone else by the time the latest
 * snapshot was written. Giving it a brand-new rank then splits one person into two VIDs
 * (overcount).
 *
 * A dispatch is treated as a backfill when **all** hold (see [resolveBackfillOldSnapshot]):
 * * the latest cumulative `SNAPSHOT` carries a `max_event_date` strictly newer than this dispatch's
 *   [maxEventDate] (fresher data already exists — not forward append), and
 * * the gap is within [retentionDays] (an older dispatch has no live ranks left to reclaim — that
 *   residual corruption is Problem 3, explicitly out of scope), and
 * * an **old snapshot** exists: the `SNAPSHOT` whose `max_event_date` is the greatest value
 *   strictly less than this dispatch's [maxEventDate] (the cumulative just before the backfilled
 *   day). If none exists (data older than anything on record), it is not a backfill.
 *
 * On a backfill, both the latest cumulative (loaded into the allocator) and the old snapshot's `(fp
 * -> rank)` map are held in heap, and [RankAllocator.assignBackfill] writes **two** outputs per
 * fingerprint, which may diverge:
 * * the **`DAY_ONLY`** blob — what Phase-2 uses to label the backfilled day — gets the rank from
 *   the old snapshot if present, else the latest snapshot's rank, else a fresh rank, and
 * * the **`SNAPSHOT`** blob — the cumulative carried forward — is the latest snapshot **plus** the
 *   backfill's fingerprints: an already-ranked fingerprint keeps its latest rank, a fingerprint
 *   only in the old snapshot takes its old rank back if still free, and a genuinely new fingerprint
 *   gets a fresh rank.
 *
 * ### Why a backfill must still write a `SNAPSHOT`
 *
 * If a backfill wrote only a `DAY_ONLY`, a fingerprint that is new on the backfilled day would
 * never enter the cumulative, so a later forward dispatch would re-rank it — reintroducing the very
 * split we are preventing. Writing the cumulative as "latest + backfill additions" persists those
 * new fingerprints. Its `max_event_date` is inherited from the latest snapshot (the backfill only
 * adds *older* fingerprints, so the newest data represented is unchanged); this keeps the
 * `create_time`-newest snapshot the most complete cumulative and never regresses the forward chain.
 *
 * ### Monitoring (the accepted undercount)
 *
 * When a fingerprint's old rank was already re-handed to a *different* fingerprint in the latest
 * snapshot, labeling the backfilled day with the old rank makes the two share a VID for that day
 * (reach undercount). We still give back the old rank (one-person-one-VID is the goal) but
 * **count** these: [Result.backfillRankCollisions] is the number of backfilled fingerprints whose
 * reused old rank differs from their rank in the latest snapshot, so the undercount is observable.
 *
 * ## Memory
 *
 * A backfill holds **two** in-heap maps for the subpool — the latest cumulative and the old
 * snapshot. At the India worst case (~1.5 B fingerprints) a single cumulative already dominates
 * heap, so loading a second can OOM an `n2d-highmem-16`; backfill-capable rankers should run on a
 * larger machine type (e.g. `n2d-highmem-32`+). Normal (non-backfill) dispatches hold only one map,
 * as before.
 *
 * ## Idempotency & concurrency
 *
 * Blob keys carry a per-attempt UUID, so a re-delivered or concurrent ranker never overwrites
 * another attempt's bytes. The `RankIndexBlob` rows are created with deterministic `request_id`s,
 * so exactly one row wins per (upload, subpool, blob type); that winning row's `blob_uri` + `DEK`
 * are always self-consistent (the winner durably wrote those bytes before inserting). Losing
 * attempts' blobs are orphaned and reclaimed by the bucket lifecycle / monitor. This removes the
 * write-before-CAS clobber hazard without needing a write-if-absent storage primitive.
 *
 * @param subpoolFingerprintsStore reads the Phase-0 merged `SubpoolFingerprints` blob.
 * @param rankIndexStore reads the prior/old snapshots and writes the new rank-index blobs.
 * @param rankIndexBlobsStub metadata-storage service for the idempotency gate, locating the prior
 *   and old snapshots, and inserting the new blob rows.
 * @param retention garbage-collects aged-out `DAY_ONLY` blobs.
 * @param dataProvider EDP resource name (`dataProviders/{dp}`), parent of the upload wildcard for
 *   the prior-snapshot lookup.
 * @param rawImpressionUpload the upload resource name (parent of the new blob rows + key scoping).
 * @param modelLine the model line being ranked.
 * @param vidRankMapBlobPrefix static blob prefix for the vid-rank-map bucket.
 * @param kekUri KEK URI used to wrap each new blob's DEK.
 * @param encryptedSubpoolMapsDek DEK that decrypts the Phase-0 merged blobs.
 * @param maxEventDate newest event date this dispatch covers; stamped as `last_seen`, on the
 *   `DAY_ONLY` row, and (for a forward dispatch) on the `SNAPSHOT` row, and used for backfill
 *   detection.
 * @param retentionDays retention window in days (must exceed the max measurement-report window).
 * @param today the UTC date treated as "now" for the rank-age cutoff.
 * @param metrics OpenTelemetry instruments.
 */
class SubpoolRanker(
  private val subpoolFingerprintsStore: SubpoolFingerprintsStore,
  private val rankIndexStore: RankIndexStore,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val retention: SubpoolRetention,
  private val dataProvider: String,
  private val rawImpressionUpload: String,
  private val modelLine: String,
  private val vidRankMapBlobPrefix: String,
  private val kekUri: String,
  private val encryptedSubpoolMapsDek: EncryptedDek,
  private val maxEventDate: Date,
  private val retentionDays: Int,
  private val today: LocalDate,
  private val metrics: VidRankBuilderMetrics = VidRankBuilderMetrics(),
) {
  /**
   * @property poolOffset the subpool ranked.
   * @property allocated ranks newly assigned to never-before-seen fingerprints.
   * @property renewed already-ranked fingerprints re-observed (rank preserved).
   * @property overflow fingerprints left unranked because the subpool was full.
   * @property freed ranks released by the retention pass.
   * @property cumulativeSize total ranked fingerprints after this dispatch.
   * @property skipped whether the subpool was already ranked for this upload (idempotency gate).
   * @property backfill whether this dispatch was handled as a backfill.
   * @property backfillReusedOldRank backfilled fingerprints given back their old-snapshot rank.
   * @property backfillRankCollisions subset of [backfillReusedOldRank] whose old rank differs from
   *   the rank they hold in the latest snapshot (the accepted reach-undercount sizing).
   */
  data class Result(
    val poolOffset: Long,
    val allocated: Long,
    val renewed: Long,
    val overflow: Long,
    val freed: Long,
    val cumulativeSize: Long,
    val skipped: Boolean = false,
    val backfill: Boolean = false,
    val backfillReusedOldRank: Long = 0L,
    val backfillRankCollisions: Long = 0L,
  )

  /** Ranks [poolOffset]; [subpoolBlobUri] is its Phase-0 merged blob; caps at [rankedSize]. */
  suspend fun rank(poolOffset: Long, subpoolBlobUri: String, rankedSize: Int): Result {
    // Idempotency gate: an existing SNAPSHOT row for this (upload, subpool) means a prior attempt
    // committed both blobs + rows atomically (a backfill writes a SNAPSHOT too), so skip the
    // re-rank.
    if (findUploadBlob(poolOffset, RankIndexBlob.BlobType.SNAPSHOT) != null) {
      logger.info(
        "Subpool $poolOffset for $modelLine already ranked for $rawImpressionUpload; skipping"
      )
      return Result(poolOffset, 0, 0, 0, 0, 0, skipped = true)
    }

    val eventDay = epochDayOf(maxEventDate)
    val cutoffEpochDay = (today.toEpochDay() - retentionDays).toInt()

    // 1. Load the latest cumulative snapshot, then decide whether this dispatch is a backfill and,
    // if so, which older snapshot to replay ranks from.
    val priorSnapshot = findPriorSnapshot(poolOffset)
    val oldSnapshot = resolveBackfillOldSnapshot(poolOffset, eventDay, priorSnapshot)
    val isBackfill = oldSnapshot != null

    val allocator = RankAllocator(poolOffset, rankedSize, eventDay)
    if (priorSnapshot != null) {
      allocator.loadFrom(
        rankIndexStore.readBlob(
          priorSnapshot.blobUri,
          priorSnapshot.encryptedDek,
          priorSnapshot.blobChecksum,
        )
      )
    }

    // 2. Materialize this dispatch's fingerprint set for the subpool from the Phase-0 merged blob.
    val todayFps = Bytes12IntMap()
    subpoolFingerprintsStore.readBlob(subpoolBlobUri, encryptedSubpoolMapsDek).collect { record ->
      val fps = record.fingerprints
      val count = fps.size() / EventIdDigestBytes.WIDTH
      var off = 0
      repeat(count) {
        todayFps.put(
          EventIdDigestBytes.readHi(fps, off),
          EventIdDigestBytes.readLo(fps, off + 8),
          1,
        )
        off += EventIdDigestBytes.WIDTH
      }
    }

    // 3a. Storage GC of aged-out DAY_ONLY blobs (independent of rank freeing).
    retention.deleteAgedBlobs(poolOffset)
    // 3b. Free aged-out ranks in heap from the `last_seen` index, before allocating so freed slots
    // are reusable today. Fingerprints seen this dispatch are excluded so their ranks (VIDs)
    // survive.
    allocator.freeAgedRanks(cutoffEpochDay, todayFps)

    // 4. Assign ranks to today's fingerprints.
    if (oldSnapshot != null) {
      // Backfill: replay each fingerprint's historical rank into the day-only delta while extending
      // the cumulative forward (see class KDoc and RankAllocator.assignBackfill).
      val oldRanks = loadFingerprintRanks(oldSnapshot)
      metrics.backfillOldSnapshotEntriesHistogram.record(oldRanks.size)
      todayFps.forEach { keyHi, keyLo, _ ->
        allocator.assignBackfill(keyHi, keyLo, oldRanks.get(keyHi, keyLo))
      }
    } else {
      todayFps.forEach { keyHi, keyLo, _ -> allocator.assign(keyHi, keyLo) }
    }

    // 5. Write the new DAY_ONLY + SNAPSHOT blobs and record both as RankIndexBlob rows. Keys carry
    // a
    // per-attempt UUID so a concurrent/re-delivered attempt never overwrites another's bytes; the
    // winning (deterministic-request_id) row's blob_uri + DEK are always self-consistent.
    val attemptId = UUID.randomUUID().toString()
    val dek = rankIndexStore.generateDek(kekUri)
    val snapshotKey =
      RankIndexStore.snapshotKey(
        vidRankMapBlobPrefix,
        rawImpressionUpload,
        modelLine,
        poolOffset,
        attemptId,
      )
    val dayOnlyKey =
      RankIndexStore.dayOnlyKey(
        vidRankMapBlobPrefix,
        rawImpressionUpload,
        modelLine,
        poolOffset,
        attemptId,
      )
    val snapshotChecksum =
      rankIndexStore.writeBlob(snapshotKey, dek, allocator.streamCumulativeChunks())
    val dayOnlyChecksum = rankIndexStore.writeBlob(dayOnlyKey, dek, allocator.streamDayOnlyChunks())
    // The SNAPSHOT's max_event_date is the newest event date in the cumulative: this dispatch's
    // date
    // for a forward upload, but the prior snapshot's (unchanged) date for a backfill, which only
    // adds older fingerprints.
    val snapshotMaxEventDate = if (isBackfill) priorSnapshot!!.maxEventDate else maxEventDate
    insertBlobRows(
      poolOffset,
      snapshotKey,
      snapshotChecksum,
      dayOnlyKey,
      dayOnlyChecksum,
      dek,
      snapshotMaxEventDate,
    )

    recordMetrics(allocator)
    logger.info(
      "Subpool $poolOffset for $modelLine: allocated=${allocator.allocated}, " +
        "renewed=${allocator.renewed}, overflow=${allocator.overflow}, freed=${allocator.freed}, " +
        "cumulative=${allocator.cumulativeSize}" +
        if (isBackfill) {
          " [backfill: reusedOldRank=${allocator.backfillReusedOldRank}, " +
            "rankCollisions=${allocator.backfillRankCollisions}]"
        } else {
          ""
        }
    )
    if (allocator.backfillRankCollisions > 0) {
      logger.warning(
        "Subpool $poolOffset for $modelLine backfill reach-undercount: " +
          "${allocator.backfillRankCollisions} fingerprint(s) reused an old rank already re-handed " +
          "to a different fingerprint in the latest snapshot"
      )
    }
    return Result(
      poolOffset = poolOffset,
      allocated = allocator.allocated,
      renewed = allocator.renewed,
      overflow = allocator.overflow,
      freed = allocator.freed,
      cumulativeSize = allocator.cumulativeSize,
      backfill = isBackfill,
      backfillReusedOldRank = allocator.backfillReusedOldRank,
      backfillRankCollisions = allocator.backfillRankCollisions,
    )
  }

  /**
   * The [blobType] `RankIndexBlob` row for [poolOffset] under **this** upload, or `null`. A
   * non-null result is the idempotency-gate signal that the subpool was already ranked for this
   * dispatch.
   */
  private suspend fun findUploadBlob(
    poolOffset: Long,
    blobType: RankIndexBlob.BlobType,
  ): RankIndexBlob? =
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = rawImpressionUpload
              filter =
                ListRankIndexBlobsRequestKt.filter {
                  this.blobType = blobType
                  cmmsModelLine = modelLine
                  this.poolOffset = poolOffset
                }
              pageSize = 1
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      // Short-circuit at the first page that has a match: cancels pagination instead of scanning
      // the whole filter result for a question whose answer is binary (exists / not).
      .firstOrNull { page -> page.firstOrNull() != null }
      ?.firstOrNull()

  /**
   * The newest non-deleted `SNAPSHOT` blob for [poolOffset] of this (data provider, model line)
   * across all uploads, or `null` on a cold (Day-1) subpool. The most recent `create_time` is the
   * current cumulative state (the design's N−1 recovery baseline). A backfill writes a `SNAPSHOT`
   * too (the latest cumulative plus the backfill's fingerprints), so the most recent `create_time`
   * always reflects the most complete cumulative.
   *
   * TODO(world-federation-of-advertisers/cross-media-measurement#4008): this lists every
   *   non-deleted SNAPSHOT across all uploads and scans for the newest — O(all snapshots), which
   *   grows unbounded because SNAPSHOTs are never retention-pruned. Replace with an O(1)
   *   latest-snapshot lookup and bound SNAPSHOT retention.
   */
  private suspend fun findPriorSnapshot(poolOffset: Long): RankIndexBlob? {
    var latest: RankIndexBlob? = null
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = "$dataProvider/rawImpressionUploads/${ResourceKey.WILDCARD_ID}"
              filter =
                ListRankIndexBlobsRequestKt.filter {
                  blobType = RankIndexBlob.BlobType.SNAPSHOT
                  cmmsModelLine = modelLine
                  this.poolOffset = poolOffset
                }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      .collect { page ->
        for (blob in page) {
          val current = latest
          if (
            current == null || blob.createTime.toComparable() > current.createTime.toComparable()
          ) {
            latest = blob
          }
        }
      }
    return latest
  }

  /**
   * The `SNAPSHOT` to replay historical ranks from when [eventDay] is a backfill, or `null` when
   * this dispatch is not a backfill.
   *
   * Returns the old snapshot — the `SNAPSHOT` whose `max_event_date` is the greatest value strictly
   * less than [eventDay] (the cumulative state just before the backfilled day) — only when:
   * * [priorSnapshot] (the latest cumulative) exists and carries a `max_event_date` strictly newer
   *   than [eventDay] (fresher data already exists), and
   * * such an older snapshot actually exists (otherwise the data provider uploaded data older than
   *   anything on record — not a backfill).
   *
   * Throws [OutOfRetentionBackfillException] (after incrementing the out-of-retention metric) when
   * the gap exceeds [retentionDays]: the rank state in force at the backfilled date has already
   * aged out, so the dispatch fails loudly instead of silently forward-appending (Problem 3, out of
   * scope).
   *
   * The `max_event_date_on_or_before = eventDay - 1` filter selects snapshots strictly older than
   * the backfilled day; the greatest among them is the closest prior cumulative.
   */
  private suspend fun resolveBackfillOldSnapshot(
    poolOffset: Long,
    eventDay: Int,
    priorSnapshot: RankIndexBlob?,
  ): RankIndexBlob? {
    if (priorSnapshot == null || !priorSnapshot.hasMaxEventDate()) return null
    val latestEventDay = priorSnapshot.maxEventDate.epochDay()
    // Forward append (newest or same day) — not a backfill.
    if (eventDay >= latestEventDay) return null
    // Older than the retention window — the rank state in force at the backfilled date has already
    // aged out, so the original rank cannot be reclaimed (Problem 3, out of scope). Fail loudly
    // rather than silently forward-appending, which would re-rank still-tracked fingerprints and
    // corrupt produced labels; the orchestrator marks the RankerJob FAILED so an operator can act.
    if (latestEventDay - eventDay > retentionDays) {
      metrics.outOfRetentionBackfillCounter.add(1)
      throw OutOfRetentionBackfillException(
        poolOffset,
        modelLine,
        latestEventDay - eventDay,
        retentionDays,
      )
    }

    val cutoff = epochDayToDate(eventDay - 1) // strictly older than the backfilled day
    var best: RankIndexBlob? = null
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = "$dataProvider/rawImpressionUploads/${ResourceKey.WILDCARD_ID}"
              filter =
                ListRankIndexBlobsRequestKt.filter {
                  blobType = RankIndexBlob.BlobType.SNAPSHOT
                  cmmsModelLine = modelLine
                  this.poolOffset = poolOffset
                  maxEventDateOnOrBefore = cutoff
                }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      .collect { page ->
        for (blob in page) {
          if (!blob.hasMaxEventDate()) continue
          val current = best
          if (current == null || blob.maxEventDate.epochDay() > current.maxEventDate.epochDay()) {
            best = blob
          }
        }
      }
    return best
  }

  /** Loads a `SNAPSHOT` blob's `(fingerprint -> rank)` pairs into an in-heap map. */
  private suspend fun loadFingerprintRanks(snapshot: RankIndexBlob): Bytes12IntMap {
    val map = Bytes12IntMap()
    rankIndexStore
      .readBlob(snapshot.blobUri, snapshot.encryptedDek, snapshot.blobChecksum)
      .collect { record ->
        val fps = record.fingerprints
        val count = record.ranksCount
        var off = 0
        for (i in 0 until count) {
          map.put(
            EventIdDigestBytes.readHi(fps, off),
            EventIdDigestBytes.readLo(fps, off + 8),
            record.getRanks(i),
          )
          off += EventIdDigestBytes.WIDTH
        }
      }
    return map
  }

  /**
   * Inserts the SNAPSHOT + DAY_ONLY rows in one idempotent batch. [snapshotMaxEventDate] is the
   * newest event date represented in the cumulative (this dispatch's date for a forward upload, the
   * prior snapshot's date for a backfill); the DAY_ONLY row always carries this dispatch's date so
   * the retention sweep ages it by the day it actually covers.
   */
  private suspend fun insertBlobRows(
    poolOffset: Long,
    snapshotKey: String,
    snapshotChecksum: com.google.protobuf.ByteString,
    dayOnlyKey: String,
    dayOnlyChecksum: com.google.protobuf.ByteString,
    dek: EncryptedDek,
    snapshotMaxEventDate: Date,
  ) {
    rankIndexBlobsStub.batchCreateRankIndexBlobs(
      batchCreateRankIndexBlobsRequest {
        parent = rawImpressionUpload
        requests += createRankIndexBlobRequest {
          parent = rawImpressionUpload
          rankIndexBlob = rankIndexBlob {
            blobType = RankIndexBlob.BlobType.SNAPSHOT
            cmmsModelLine = modelLine
            this.poolOffset = poolOffset
            blobUri = snapshotKey
            blobChecksum = snapshotChecksum
            encryptedDek = dek
            maxEventDate = snapshotMaxEventDate
          }
          requestId = blobRequestId(poolOffset, RankIndexBlob.BlobType.SNAPSHOT)
        }
        requests += createRankIndexBlobRequest {
          parent = rawImpressionUpload
          rankIndexBlob = rankIndexBlob {
            blobType = RankIndexBlob.BlobType.DAY_ONLY
            cmmsModelLine = modelLine
            this.poolOffset = poolOffset
            blobUri = dayOnlyKey
            blobChecksum = dayOnlyChecksum
            encryptedDek = dek
            maxEventDate = this@SubpoolRanker.maxEventDate
          }
          requestId = blobRequestId(poolOffset, RankIndexBlob.BlobType.DAY_ONLY)
        }
      }
    )
  }

  private fun recordMetrics(allocator: RankAllocator) {
    metrics.ranksAllocatedCounter.add(allocator.allocated)
    metrics.ranksRenewedCounter.add(allocator.renewed)
    metrics.ranksFreedCounter.add(allocator.freed)
    metrics.overflowFingerprintsCounter.add(allocator.overflow)
    metrics.subpoolsRankedCounter.add(1)
    if (allocator.backfillReusedOldRank > 0) {
      metrics.backfillReusedOldRankCounter.add(allocator.backfillReusedOldRank)
    }
    if (allocator.backfillRankCollisions > 0) {
      metrics.backfillRankCollisionsCounter.add(allocator.backfillRankCollisions)
    }
  }

  /**
   * Epoch-day of a UTC [Date]. [maxEventDate] is REQUIRED: it drives both `last_seen` stamping and
   * the `DAY_ONLY` row's `MaxEventDate` (which retention ages blobs by), so an unset value would
   * silently corrupt rank state. Fail loudly instead of defaulting.
   */
  private fun epochDayOf(date: Date): Int {
    require(date.year != 0) { "maxEventDate must be set on VidRankBuilderParams" }
    return LocalDate.of(date.year, date.month, date.day).toEpochDay().toInt()
  }

  /**
   * Deterministic UUID4 idempotency key for the (upload, model line, subpool, blob type) blob row,
   * stable across redeliveries so the batch insert reuses existing rows rather than duplicating.
   * Derived from an MD5 digest with the RFC-4122 version (4) and variant bits forced.
   */
  private fun blobRequestId(poolOffset: Long, blobType: RankIndexBlob.BlobType): String {
    val seed = "$rawImpressionUpload|$modelLine|$poolOffset|${blobType.name}"
    val bytes = MessageDigest.getInstance("MD5").digest(seed.toByteArray(Charsets.UTF_8))
    bytes[6] = ((bytes[6].toInt() and 0x0f) or 0x40).toByte() // version 4
    bytes[8] = ((bytes[8].toInt() and 0x3f) or 0x80).toByte() // variant 10xx
    val buffer = ByteBuffer.wrap(bytes)
    return UUID(buffer.long, buffer.long).toString()
  }

  companion object {
    private val logger = Logger.getLogger(SubpoolRanker::class.java.name)

    /** Orders timestamps for "newest snapshot" without depending on a proto comparator. */
    private fun com.google.protobuf.Timestamp.toComparable(): Long =
      seconds * 1_000_000_000L + nanos

    /** Epoch-day of a set UTC [Date]. Caller must ensure the date is set (`hasMaxEventDate`). */
    private fun Date.epochDay(): Int = LocalDate.of(year, month, day).toEpochDay().toInt()

    private fun epochDayToDate(epochDay: Int): Date {
      val localDate = LocalDate.ofEpochDay(epochDay.toLong())
      return date {
        year = localDate.year
        month = localDate.monthValue
        day = localDate.dayOfMonth
      }
    }
  }
}
