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

import java.time.Instant
import java.util.logging.Logger
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineFailedRequest

/**
 * Evicts uploads that carry bad data for a model line, using the last-good-snapshot rebuild
 * strategy (no eviction TEE): it marks the bad upload and every later upload for the model line
 * `FAILED` and soft-deletes their cumulative `SNAPSHOT` rank-index blobs. Phase-1's prior-snapshot
 * selection (newest non-deleted `SNAPSHOT`) then falls back to the last good snapshot, so when the
 * data provider re-triggers the affected uploads the pipeline rebuilds forward from clean state.
 *
 * Because each subsequent cumulative snapshot was built on the corrupted one, eviction must cascade
 * forward from the earliest bad upload to the most recent (`Up_k … Up_n`). Eviction is confined to
 * the retention window; uploads older than the window are rejected (out-of-window recovery is out
 * of scope).
 *
 * @param uploadsStub stub for `RawImpressionUploadService` (create-time ordering + retention
 *   check).
 * @param modelLinesStub stub for `RawImpressionUploadModelLineService` (mark FAILED).
 * @param rankIndexBlobsStub stub for `RankIndexBlobService` (soft-delete SNAPSHOT rows).
 */
class EvictUploader(
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
) {
  /** A single `(upload, model line)` in the eviction cascade. */
  data class CascadeEntry(
    val uploadId: String,
    val modelLineName: String,
    val etag: String,
    val alreadyFailed: Boolean,
  )

  /** The forward cascade to evict, ordered by upload create time. */
  data class EvictionPlan(
    val cascade: List<CascadeEntry>,
    /** Uploads pulled into the cascade beyond the requested bad ids (they came after them). */
    val extraUploadIds: List<String>,
  )

  /** Outcome of an [evict] run. */
  data class EvictionResult(val failedModelLines: List<String>, val deletedSnapshots: Int)

  /**
   * Builds the forward eviction cascade for [cmmsModelLine] starting at the earliest of
   * [badUploadIds]. Validates that every requested bad upload exists and was created on or after
   * [cutoffTime] (within the retention window). Does not mutate anything.
   *
   * @throws IllegalArgumentException if [badUploadIds] is empty, names an unknown upload, or names
   *   an upload older than [cutoffTime].
   */
  suspend fun plan(
    dataProvider: String,
    cmmsModelLine: String,
    badUploadIds: List<String>,
    cutoffTime: Instant,
  ): EvictionPlan {
    require(badUploadIds.isNotEmpty()) { "at least one bad upload id is required" }

    val createTimeByUploadId: Map<String, Instant> = listUploadCreateTimes(dataProvider)

    val unknown = badUploadIds.filter { it !in createTimeByUploadId }
    require(unknown.isEmpty()) { "unknown upload id(s) for $dataProvider: $unknown" }
    val outOfWindow = badUploadIds.filter { createTimeByUploadId.getValue(it).isBefore(cutoffTime) }
    require(outOfWindow.isEmpty()) {
      "upload id(s) older than the retention window (create_time before $cutoffTime): $outOfWindow"
    }

    val earliestBadTime: Instant = badUploadIds.minOf { createTimeByUploadId.getValue(it) }

    val entries = mutableListOf<Pair<Instant, CascadeEntry>>()
    var pageToken = ""
    do {
      val response =
        modelLinesStub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = "$dataProvider/rawImpressionUploads/-"
            filter =
              ListRawImpressionUploadModelLinesRequestKt.filter {
                this.cmmsModelLine = cmmsModelLine
              }
            this.pageToken = pageToken
          }
        )
      for (row in response.rawImpressionUploadModelLinesList) {
        if (row.cmmsModelLine != cmmsModelLine) continue
        val uploadId = uploadIdOf(row.name)
        val uploadTime = createTimeByUploadId[uploadId] ?: continue
        if (uploadTime.isBefore(earliestBadTime)) continue
        entries.add(
          uploadTime to
            CascadeEntry(
              uploadId = uploadId,
              modelLineName = row.name,
              etag = row.etag,
              alreadyFailed = row.state == RawImpressionUploadModelLine.State.FAILED,
            )
        )
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    val cascade = entries.sortedBy { it.first }.map { it.second }
    val requested = badUploadIds.toSet()
    val extraUploadIds = cascade.map { it.uploadId }.filter { it !in requested }.distinct()
    return EvictionPlan(cascade, extraUploadIds)
  }

  /**
   * Executes [plan]: marks each cascade `(upload, model line)` `FAILED` (recording [reason]) and
   * soft-deletes its cumulative `SNAPSHOT` rank-index blobs. Model lines already `FAILED` are left
   * as-is; snapshot soft-deletes are still applied (idempotent).
   */
  suspend fun evict(
    dataProvider: String,
    cmmsModelLine: String,
    plan: EvictionPlan,
    reason: String,
  ): EvictionResult {
    val failed = mutableListOf<String>()
    var deleted = 0
    for (entry in plan.cascade) {
      if (!entry.alreadyFailed) {
        modelLinesStub.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            name = entry.modelLineName
            errorMessage = reason
            etag = entry.etag
          }
        )
        failed.add(entry.modelLineName)
        logger.info("Marked ${entry.modelLineName} FAILED.")
      }
      deleted += softDeleteSnapshots(dataProvider, entry.uploadId, cmmsModelLine)
    }
    return EvictionResult(failed, deleted)
  }

  private suspend fun listUploadCreateTimes(dataProvider: String): Map<String, Instant> {
    val createTimes = mutableMapOf<String, Instant>()
    var pageToken = ""
    do {
      val response =
        uploadsStub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            parent = dataProvider
            this.pageToken = pageToken
          }
        )
      for (upload in response.rawImpressionUploadsList) {
        createTimes[uploadIdOf(upload.name)] = upload.createTime.toInstant()
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return createTimes
  }

  private suspend fun softDeleteSnapshots(
    dataProvider: String,
    uploadId: String,
    cmmsModelLine: String,
  ): Int {
    var count = 0
    var pageToken = ""
    do {
      val response =
        rankIndexBlobsStub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = "$dataProvider/rawImpressionUploads/$uploadId"
            filter =
              ListRankIndexBlobsRequestKt.filter {
                blobType = RankIndexBlob.BlobType.SNAPSHOT
                this.cmmsModelLine = cmmsModelLine
              }
            this.pageToken = pageToken
          }
        )
      for (blob in response.rankIndexBlobsList) {
        rankIndexBlobsStub.deleteRankIndexBlob(deleteRankIndexBlobRequest { name = blob.name })
        count++
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return count
  }

  companion object {
    private val logger: Logger = Logger.getLogger(EvictUploader::class.java.name)

    /** Extracts the `rawImpressionUploads/{id}` segment from an upload or child resource name. */
    private fun uploadIdOf(resourceName: String): String =
      resourceName.substringAfter("/rawImpressionUploads/").substringBefore("/")
  }
}
