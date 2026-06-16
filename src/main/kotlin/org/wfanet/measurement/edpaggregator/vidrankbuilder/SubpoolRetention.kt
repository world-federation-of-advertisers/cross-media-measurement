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
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * Two-phase data-deletion pass for a **single** subpool (the Data Deletion section of the design).
 *
 * A `DAY_ONLY` rank-index blob is eligible for deletion when `today - MaxEventDate(blob) >
 * RETENTION_DAYS`. A rank is freed only when its fingerprint is **not renewed** — i.e. it does not
 * appear in any surviving (non-deleted) `DAY_ONLY` blob of the subpool, nor in today's input. This
 * keeps a fingerprint's VID stable as long as it is still being observed.
 *
 * Runs at the start of the ranker pass, before any allocation, so freed slots become available to
 * today's allocation. Each candidate's `DeleteTime` is stamped immediately so a concurrent retry
 * skips it.
 *
 * @param rankIndexBlobsStub metadata-storage service for listing/soft-deleting `RankIndexBlob`s.
 * @param rankIndexStore reads the candidate blobs' `(fp, rank)` pairs and hard-deletes their bytes.
 * @param dataProvider the EDP resource name (`dataProviders/{dp}`), parent of the upload wildcard.
 * @param modelLine the model line scoping the retention query.
 * @param retentionDays retention window in days (must exceed the max measurement-report window).
 * @param today the UTC date treated as "now" for the age computation.
 */
class SubpoolRetention(
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rankIndexStore: RankIndexStore,
  private val dataProvider: String,
  private val modelLine: String,
  private val retentionDays: Int,
  private val today: LocalDate,
) {
  /**
   * Frees the ranks of fingerprints that have aged out of [poolOffset], mutating [allocator] in
   * place. [todayFps] is this dispatch's fingerprint set for the subpool (membership-only), used as
   * part of the renewal set so today's fingerprints are never freed.
   */
  suspend fun prune(poolOffset: Long, allocator: RankAllocator, todayFps: Bytes12IntMap) {
    val cutoff = today.minusDays(retentionDays.toLong() + 1L) // strictly older than RETENTION_DAYS
    val candidates = listDeletionCandidates(poolOffset, cutoff)
    if (candidates.isEmpty()) return

    logger.info(
      "Subpool $poolOffset for $modelLine: ${candidates.size} DAY_ONLY blob(s) eligible for deletion"
    )

    // Stamp DeleteTime on every candidate up front so a concurrent retry skips them and the
    // surviving-blob scan below excludes them.
    for (candidate in candidates) {
      rankIndexBlobsStub.deleteRankIndexBlob(deleteRankIndexBlobRequest { name = candidate.name })
    }

    // renewal_set = (today's fingerprints) ∪ (fingerprints in all surviving DAY_ONLY blobs).
    val renewal = Bytes12IntMap()
    todayFps.forEach { keyHi, keyLo, _ -> renewal.put(keyHi, keyLo, PRESENT) }
    addSurvivingDayOnlyFingerprints(poolOffset, renewal)

    // Free every candidate fingerprint that is not renewed; then hard-delete the blob bytes.
    for (candidate in candidates) {
      rankIndexStore.readBlob(candidate.blobUri, candidate.encryptedDek).collect { record ->
        val fps = record.fingerprints
        var off = 0
        repeat(record.ranksCount) {
          val keyHi = FingerprintCodec.readHi(fps, off)
          val keyLo = FingerprintCodec.readLo(fps, off + 8)
          if (!renewal.containsKey(keyHi, keyLo)) allocator.free(keyHi, keyLo)
          off += FingerprintCodec.WIDTH
        }
      }
      rankIndexStore.delete(candidate.blobUri)
    }
  }

  /**
   * `DAY_ONLY` blobs for [poolOffset] whose `max_event_date` is on or before [cutoff], not deleted.
   */
  private suspend fun listDeletionCandidates(
    poolOffset: Long,
    cutoff: LocalDate
  ): List<RankIndexBlob> {
    val candidates = mutableListOf<RankIndexBlob>()
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = blobsParent()
              filter =
                ListRankIndexBlobsRequestKt.filter {
                  blobType = RankIndexBlob.BlobType.DAY_ONLY
                  cmmsModelLine = modelLine
                  this.poolOffset = poolOffset
                  maxEventDateOnOrBefore = cutoff.toDate()
                }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      .collect { page -> candidates.addAll(page) }
    return candidates
  }

  /** Adds every fingerprint of every surviving (non-deleted) `DAY_ONLY` blob of [poolOffset]. */
  private suspend fun addSurvivingDayOnlyFingerprints(poolOffset: Long, renewal: Bytes12IntMap) {
    // TODO(world-federation-of-advertisers/cross-media-measurement#3958): this scans every
    //   surviving DAY_ONLY blob in the retention window for the subpool, which is O(retention) I/O
    //   per ranker run. Consider a reference-count or last-seen index so renewal can be computed
    //   without re-reading the full history.
    val survivingBlobs = mutableListOf<RankIndexBlob>()
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = blobsParent()
              filter =
                ListRankIndexBlobsRequestKt.filter {
                  blobType = RankIndexBlob.BlobType.DAY_ONLY
                  cmmsModelLine = modelLine
                  this.poolOffset = poolOffset
                }
              this.pageToken = pageToken
            }
          )
        ResourceList(response.rankIndexBlobsList, response.nextPageToken)
      }
      .collect { page -> survivingBlobs.addAll(page) }

    for (blob in survivingBlobs) {
      rankIndexStore.readBlob(blob.blobUri, blob.encryptedDek).collect { record ->
        val fps = record.fingerprints
        var off = 0
        repeat(record.ranksCount) {
          renewal.put(
            FingerprintCodec.readHi(fps, off),
            FingerprintCodec.readLo(fps, off + 8),
            PRESENT,
          )
          off += FingerprintCodec.WIDTH
        }
      }
    }
  }

  private fun blobsParent(): String = "$dataProvider/rawImpressionUploads/-"

  private fun LocalDate.toDate(): Date = date {
    year = this@toDate.year
    month = this@toDate.monthValue
    day = this@toDate.dayOfMonth
  }

  companion object {
    private const val PRESENT = 1
    private val logger = Logger.getLogger(SubpoolRetention::class.java.name)
  }
}
