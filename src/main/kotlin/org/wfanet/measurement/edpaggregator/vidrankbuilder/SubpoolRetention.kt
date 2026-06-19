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
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.api.grpc.listResources
import org.wfanet.measurement.edpaggregator.rawimpressions.RankIndexStore
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest

/**
 * Storage garbage-collection of aged-out `DAY_ONLY` rank-index blobs for a single subpool.
 *
 * Since the cumulative `SNAPSHOT` now carries each fingerprint's `last_seen` recency (see
 * [RankAllocator]), the **rank-freeing** decision is made entirely in heap by
 * [RankAllocator.freeAgedRanks] and no longer depends on these blobs. This class is therefore pure
 * storage cleanup: it soft-deletes the `RankIndexBlob` rows whose `max_event_date` is older than
 * the retention window and deletes their bytes from Cloud Storage. It reads no blob contents and
 * scans no history.
 *
 * A `DAY_ONLY` blob is eligible when `today - MaxEventDate(blob) > RETENTION_DAYS`. The retention
 * window MUST exceed the deployment's maximum measurement-report window (see Data Deletion).
 *
 * @param rankIndexBlobsStub metadata-storage service for listing/soft-deleting `RankIndexBlob`s.
 * @param rankIndexStore deletes the aged blobs' bytes from Cloud Storage.
 * @param dataProvider the EDP resource name (`dataProviders/{dp}`), parent of the upload wildcard.
 * @param modelLine the model line scoping the retention query.
 * @param retentionDays retention window in days.
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
  /** Soft-deletes and hard-deletes the aged-out `DAY_ONLY` blobs of [poolOffset]. */
  suspend fun deleteAgedBlobs(poolOffset: Long) {
    val cutoff = today.minusDays(retentionDays.toLong() + 1L) // strictly older than RETENTION_DAYS
    val candidates = listDeletionCandidates(poolOffset, cutoff)
    if (candidates.isEmpty()) return

    logger.info(
      "Subpool $poolOffset for $modelLine: garbage-collecting ${candidates.size} aged DAY_ONLY blob(s)"
    )
    for (candidate in candidates) {
      // Soft-delete the row first so a concurrent retry skips it, then delete the bytes. A failure
      // between the two leaks the bytes (no corruption — the row is gone, so nothing references
      // them) and they're reclaimed by the monitor
      // (world-federation-of-advertisers/cross-media-measurement#3958) or the bucket lifecycle.
      rankIndexBlobsStub.deleteRankIndexBlob(deleteRankIndexBlobRequest { name = candidate.name })
      rankIndexStore.delete(candidate.blobUri)
    }
  }

  /**
   * `DAY_ONLY` blobs for [poolOffset] whose `max_event_date` is on or before [cutoff], not deleted.
   */
  private suspend fun listDeletionCandidates(
    poolOffset: Long,
    cutoff: LocalDate,
  ): List<RankIndexBlob> {
    val candidates = mutableListOf<RankIndexBlob>()
    rankIndexBlobsStub
      .listResources { pageToken: String ->
        val response =
          listRankIndexBlobs(
            listRankIndexBlobsRequest {
              parent = "$dataProvider/rawImpressionUploads/${ResourceKey.WILDCARD_ID}"
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

  private fun LocalDate.toDate(): Date = date {
    year = this@toDate.year
    month = this@toDate.monthValue
    day = this@toDate.dayOfMonth
  }

  companion object {
    private val logger = Logger.getLogger(SubpoolRetention::class.java.name)
  }
}
