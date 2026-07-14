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
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.deleteRankIndexBlobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadModelLineRequest
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
 * @param rawImpressionModelLinesStub stub for `RawImpressionUploadModelLineService` (mark FAILED).
 * @param rankIndexBlobsStub stub for `RankIndexBlobService` (soft-delete SNAPSHOT rows).
 */
class EvictUploader(
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val rawImpressionModelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
) {
  /** A single `(upload, model line)` in the eviction cascade. */
  data class CascadeEntry(val uploadName: String, val modelLineName: String)

  /** The forward cascade to evict, ordered by upload create time. */
  data class EvictionPlan(
    val cascade: List<CascadeEntry>,
    /** Uploads pulled into the cascade beyond the requested bad ones (they came after them). */
    val extraUploads: List<String>,
  )

  /** Outcome of an [evict] run. */
  data class EvictionResult(val failedModelLines: List<String>, val deletedSnapshots: Int)

  /**
   * Builds the forward eviction cascade for [cmmsModelLine] starting at the earliest of
   * [badUploads]. Validates that every requested bad upload exists and was created on or after
   * [cutoffTime] (within the retention window). Does not mutate anything.
   *
   * @param badUploads `RawImpressionUpload` resource names of the bad uploads (all under the same
   *   DataProvider).
   * @throws IllegalArgumentException if [badUploads] is empty, spans multiple DataProviders, names
   *   an unknown upload, or names an upload older than [cutoffTime].
   */
  suspend fun plan(
    cmmsModelLine: String,
    badUploads: List<String>,
    cutoffTime: Instant,
  ): EvictionPlan {
    require(badUploads.isNotEmpty()) { "at least one bad upload is required" }
    val dataProvider = dataProviderOf(badUploads.first())
    require(badUploads.all { dataProviderOf(it) == dataProvider }) {
      "all bad uploads must be under the same DataProvider"
    }

    val createTimeByUpload: Map<String, Instant> = listUploadCreateTimes(dataProvider)

    val unknown = badUploads.filter { it !in createTimeByUpload }
    require(unknown.isEmpty()) { "unknown upload(s) for $dataProvider: $unknown" }
    val outOfWindow = badUploads.filter { createTimeByUpload.getValue(it).isBefore(cutoffTime) }
    require(outOfWindow.isEmpty()) {
      "upload(s) older than the retention window (create_time before $cutoffTime): $outOfWindow"
    }

    val earliestBadTime: Instant = badUploads.minOf { createTimeByUpload.getValue(it) }

    val entries = mutableListOf<Pair<Instant, CascadeEntry>>()
    var pageToken = ""
    do {
      val response =
        rawImpressionModelLinesStub.listRawImpressionUploadModelLines(
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
        val uploadName = uploadNameOf(row.name)
        val uploadTime = createTimeByUpload[uploadName] ?: continue
        if (uploadTime.isBefore(earliestBadTime)) continue
        entries.add(uploadTime to CascadeEntry(uploadName = uploadName, modelLineName = row.name))
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())

    val cascade = entries.sortedBy { it.first }.map { it.second }
    val requested = badUploads.toSet()
    val extraUploads = cascade.map { it.uploadName }.filter { it !in requested }.distinct()
    return EvictionPlan(cascade, extraUploads)
  }

  /**
   * Executes [plan]: marks each cascade `(upload, model line)` `FAILED` (recording [reason]) and
   * soft-deletes its cumulative `SNAPSHOT` rank-index blobs. Model lines already `FAILED` are left
   * as-is; snapshot soft-deletes are still applied (idempotent).
   */
  suspend fun evict(cmmsModelLine: String, plan: EvictionPlan, reason: String): EvictionResult {
    val failed = mutableListOf<String>()
    var deleted = 0
    for (entry in plan.cascade) {
      // Re-fetch the model line so the Mark uses a current etag and state: the plan may be minutes
      // old, and the Monitor or another operator could have advanced the row since. Reusing the
      // plan-time etag would throw ABORTED partway through the cascade; re-reading also lets us
      // skip
      // rows that are already FAILED.
      val current =
        rawImpressionModelLinesStub.getRawImpressionUploadModelLine(
          getRawImpressionUploadModelLineRequest { name = entry.modelLineName }
        )
      if (current.state != RawImpressionUploadModelLine.State.FAILED) {
        // TODO(world-federation-of-advertisers/cross-media-measurement#4211): once #4211 makes
        // request_id REQUIRED on the Mark* RPCs, set
        // requestId = RequestIds.forMarkRawImpressionUploadModelLineFailed(entry.modelLineName) so
        // a
        // re-run hits the AIP-155 replay short-circuit instead of failing INVALID_ARGUMENT.
        rawImpressionModelLinesStub.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            name = entry.modelLineName
            errorMessage = reason
            etag = current.etag
          }
        )
        failed.add(entry.modelLineName)
        logger.info("Marked ${entry.modelLineName} FAILED.")
      }
      deleted += softDeleteSnapshots(entry.uploadName, cmmsModelLine)
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
        createTimes[upload.name] = upload.createTime.toInstant()
      }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return createTimes
  }

  private suspend fun softDeleteSnapshots(uploadName: String, cmmsModelLine: String): Int {
    var count = 0
    var pageToken = ""
    do {
      val response =
        rankIndexBlobsStub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = uploadName
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

    /** The `dataProviders/{data_provider}` parent of an upload resource name. */
    private fun dataProviderOf(uploadName: String): String =
      requireNotNull(RawImpressionUploadKey.fromName(uploadName)) {
          "Malformed RawImpressionUpload resource name: $uploadName"
        }
        .parentKey
        .toName()

    /** The parent `RawImpressionUpload` resource name of a model-line resource name. */
    private fun uploadNameOf(modelLineName: String): String =
      requireNotNull(RawImpressionUploadModelLineKey.fromName(modelLineName)) {
          "Malformed RawImpressionUploadModelLine resource name: $modelLineName"
        }
        .parentKey
        .toName()
  }
}
