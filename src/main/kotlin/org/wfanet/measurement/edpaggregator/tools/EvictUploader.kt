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

import com.google.type.interval
import java.time.Instant
import java.util.logging.Logger
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.sync.Semaphore
import kotlinx.coroutines.sync.withPermit
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
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
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds

/**
 * Evicts uploads that carry bad data across both VID-labeling paths. Non-memoized model lines are
 * isolated to the requested uploads. Memoized model lines are cascaded forward and have their
 * cumulative `SNAPSHOT` rank-index blobs soft-deleted, so Phase-1 falls back to the last good
 * snapshot when the data provider re-triggers corrected uploads.
 *
 * For a memoized line, each subsequent cumulative snapshot was built on the corrupted one, so
 * eviction cascades from the earliest bad upload to the most recent (`Up_k … Up_n`). A non-memoized
 * line has no cumulative state and is evicted in isolation. Eviction is confined to the retention
 * window; uploads older than the window are rejected.
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
  /** A single `(upload, model line)` in the eviction plan. */
  data class CascadeEntry(
    val uploadName: String,
    val modelLineName: String,
    val cmmsModelLine: String,
    val memoized: Boolean,
  )

  /** The forward cascade to evict, ordered by upload create time. */
  data class EvictionPlan(
    val cascade: List<CascadeEntry>,
    /** Uploads pulled into the cascade beyond the requested bad ones (they came after them). */
    val extraUploads: List<String>,
    val memoizedModelLines: Set<String>,
    val nonMemoizedModelLines: Set<String>,
    val badUploads: List<String>,
    val cutoffTime: Instant,
  )

  /** Outcome of an [evict] run. */
  data class EvictionResult(val failedModelLines: List<String>, val deletedSnapshots: Int)

  /**
   * Builds one eviction plan for every model line attached to [badUploads]. Memoized lines cascade
   * from their earliest bad upload; non-memoized lines contain only explicitly bad uploads.
   * Snapshot presence identifies the memoized path because completed memoized processing always
   * writes a cumulative `SNAPSHOT`, while non-memoized processing never does.
   *
   * @param badUploads `RawImpressionUpload` resource names of the bad uploads (all under the same
   *   DataProvider).
   * @throws IllegalArgumentException if [badUploads] is empty, spans multiple DataProviders, names
   *   an unknown upload, names an upload older than [cutoffTime], or the DataProvider still has
   *   queued or running model-line work.
   */
  suspend fun plan(badUploads: List<String>, cutoffTime: Instant): EvictionPlan {
    require(badUploads.isNotEmpty()) { "at least one bad upload is required" }
    val dataProvider = dataProviderOf(badUploads.first())
    require(badUploads.all { dataProviderOf(it) == dataProvider }) {
      "all bad uploads must be under the same DataProvider"
    }

    val createTimeByUpload: Map<String, Instant> = listUploadCreateTimes(dataProvider, cutoffTime)

    val unknown = badUploads.filter { it !in createTimeByUpload }
    require(unknown.isEmpty()) {
      "unknown upload(s), or upload(s) older than the retention window " +
        "(create_time before $cutoffTime), for $dataProvider: $unknown"
    }
    val outOfWindow = badUploads.filter { createTimeByUpload.getValue(it) < cutoffTime }
    require(outOfWindow.isEmpty()) {
      "upload(s) older than the retention window (create_time before $cutoffTime): $outOfWindow"
    }

    val queuedOrRunningRows = listQueuedOrRunningModelLines(dataProvider)
    require(queuedOrRunningRows.isEmpty()) {
      "$dataProvider has queued or running upload/model-line rows; pause new dispatches and wait " +
        "for processing to finish before eviction: ${queuedOrRunningRows.map { it.name }}"
    }

    val rowsByUpload = badUploads.associateWith { listModelLines(it) }
    val requestedMissing = badUploads.filter { rowsByUpload.getValue(it).isEmpty() }
    require(requestedMissing.isEmpty()) {
      "requested upload(s) have no model-line rows (nothing to evict): $requestedMissing"
    }

    val requestedRows = rowsByUpload.values.flatten()
    val rowsByCmmsModelLine =
      requestedRows
        .map { it.cmmsModelLine }
        .toSet()
        .associateWith { listModelLines("$dataProvider/rawImpressionUploads/-", it, cutoffTime) }
    val snapshotRows = coroutineScope {
      val semaphore = Semaphore(SNAPSHOT_LOOKUP_PARALLELISM)
      rowsByCmmsModelLine.values
        .flatten()
        .map { row ->
          async {
            val uploadName = uploadNameOf(row.name)
            if (semaphore.withPermit { hasSnapshot(uploadName, row.cmmsModelLine) }) {
              uploadName to row.cmmsModelLine
            } else {
              null
            }
          }
        }
        .awaitAll()
        .filterNotNull()
        .toSet()
    }
    val memoizedRequestedRows = requestedRows.filter { isMemoized(it, snapshotRows) }
    val nonMemoizedRequestedRows = requestedRows - memoizedRequestedRows.toSet()
    val memoizedModelLines = memoizedRequestedRows.mapTo(mutableSetOf()) { it.cmmsModelLine }
    val nonMemoizedModelLines = nonMemoizedRequestedRows.mapTo(mutableSetOf()) { it.cmmsModelLine }

    val entries = mutableListOf<Pair<Instant, CascadeEntry>>()
    for (cmmsModelLine in memoizedModelLines) {
      val earliestBadTime =
        memoizedRequestedRows
          .filter { it.cmmsModelLine == cmmsModelLine }
          .minOf { createTimeByUpload.getValue(uploadNameOf(it.name)) }
      for (row in rowsByCmmsModelLine.getValue(cmmsModelLine)) {
        val uploadName = uploadNameOf(row.name)
        val uploadTime = createTimeByUpload[uploadName] ?: continue
        if (uploadTime < earliestBadTime) continue
        if (!isMemoized(row, snapshotRows)) continue
        entries +=
          uploadTime to
            CascadeEntry(uploadName, row.name, cmmsModelLine = cmmsModelLine, memoized = true)
      }
    }
    for (row in nonMemoizedRequestedRows) {
      val uploadName = uploadNameOf(row.name)
      entries +=
        createTimeByUpload.getValue(uploadName) to
          CascadeEntry(uploadName, row.name, row.cmmsModelLine, memoized = false)
    }

    val cascade =
      entries
        .sortedWith(
          compareBy<Pair<Instant, CascadeEntry>> { it.first }.thenBy { it.second.cmmsModelLine }
        )
        .map { it.second }
    val requestedNames = badUploads.toSet()
    val extraUploads = cascade.map { it.uploadName }.filter { it !in requestedNames }.distinct()
    return EvictionPlan(
      cascade,
      extraUploads,
      memoizedModelLines,
      nonMemoizedModelLines,
      badUploads,
      cutoffTime,
    )
  }

  /**
   * Executes [plan]: marks each cascade `(upload, model line)` `FAILED` (recording [reason]) and
   * soft-deletes its cumulative `SNAPSHOT` rank-index blobs. The caller must first quiesce dispatch
   * and workers: marking a row FAILED does not cancel a worker that has already loaded a corrupt
   * predecessor. The plan is refreshed immediately before mutation and execution aborts if it has
   * changed since operator confirmation.
   */
  suspend fun evict(plan: EvictionPlan, reason: String): EvictionResult {
    val refreshed = plan(plan.badUploads, plan.cutoffTime)
    require(refreshed.cascade == plan.cascade) {
      "eviction plan changed after confirmation; review the new plan and retry"
    }
    val failed = mutableListOf<String>()
    var deleted = 0
    for (entry in plan.cascade) {
      // Re-fetch the model line so the Mark uses a current etag and state: the plan may be
      // minutes
      // old, and the Monitor or another operator could have advanced the row since. Reusing the
      // plan-time etag would throw ABORTED partway through the cascade; re-reading also lets us
      // skip
      // rows that are already FAILED.
      val current =
        rawImpressionModelLinesStub.getRawImpressionUploadModelLine(
          getRawImpressionUploadModelLineRequest { name = entry.modelLineName }
        )
      if (current.state != RawImpressionUploadModelLine.State.FAILED) {
        rawImpressionModelLinesStub.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            name = entry.modelLineName
            errorMessage = reason
            etag = current.etag
            requestId =
              RequestIds.forMarkRawImpressionUploadModelLineFailed(
                entry.modelLineName,
                current.etag,
              )
          }
        )
        failed.add(entry.modelLineName)
        logger.info("Marked ${entry.modelLineName} FAILED.")
      }
      if (entry.memoized) {
        deleted += softDeleteSnapshots(entry.uploadName, entry.cmmsModelLine)
      }
    }
    return EvictionResult(failed, deleted)
  }

  private suspend fun listUploadCreateTimes(
    dataProvider: String,
    cutoffTime: Instant,
  ): Map<String, Instant> {
    val createTimes = mutableMapOf<String, Instant>()
    var pageToken = ""
    do {
      val response =
        uploadsStub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            parent = dataProvider
            filter =
              ListRawImpressionUploadsRequestKt.filter {
                createTimeIn = interval { startTime = cutoffTime.toProtoTime() }
              }
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

  private fun isMemoized(
    row: RawImpressionUploadModelLine,
    snapshotRows: Set<Pair<String, String>>,
  ): Boolean {
    return uploadNameOf(row.name) to row.cmmsModelLine in snapshotRows
  }

  private suspend fun listModelLines(
    parent: String,
    cmmsModelLine: String = "",
    cutoffTime: Instant? = null,
  ): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        rawImpressionModelLinesStub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            this.parent = parent
            if (cmmsModelLine.isNotEmpty() || cutoffTime != null) {
              filter =
                ListRawImpressionUploadModelLinesRequestKt.filter {
                  if (cmmsModelLine.isNotEmpty()) this.cmmsModelLine = cmmsModelLine
                  if (cutoffTime != null) {
                    createTimeIn = interval { startTime = cutoffTime.toProtoTime() }
                  }
                }
            }
            this.pageToken = pageToken
          }
        )
      rows += response.rawImpressionUploadModelLinesList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return rows
  }

  private suspend fun listQueuedOrRunningModelLines(
    dataProvider: String
  ): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        rawImpressionModelLinesStub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = "$dataProvider/rawImpressionUploads/-"
            filter =
              ListRawImpressionUploadModelLinesRequestKt.filter {
                stateIn += QUEUED_OR_RUNNING_STATES
              }
            this.pageToken = pageToken
          }
        )
      rows +=
        response.rawImpressionUploadModelLinesList.filter { it.state in QUEUED_OR_RUNNING_STATES }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return rows
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

  private suspend fun hasSnapshot(uploadName: String, cmmsModelLine: String): Boolean {
    val response =
      rankIndexBlobsStub.listRankIndexBlobs(
        listRankIndexBlobsRequest {
          parent = uploadName
          pageSize = 1
          showDeleted = true
          filter =
            ListRankIndexBlobsRequestKt.filter {
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              this.cmmsModelLine = cmmsModelLine
            }
        }
      )
    return response.rankIndexBlobsCount > 0
  }

  companion object {
    private val logger: Logger = Logger.getLogger(EvictUploader::class.java.name)
    private const val SNAPSHOT_LOOKUP_PARALLELISM = 16
    private val QUEUED_OR_RUNNING_STATES =
      setOf(
        RawImpressionUploadModelLine.State.CREATED,
        RawImpressionUploadModelLine.State.POOL_ASSIGNING,
        RawImpressionUploadModelLine.State.RANKING,
        RawImpressionUploadModelLine.State.LABELING,
      )

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
