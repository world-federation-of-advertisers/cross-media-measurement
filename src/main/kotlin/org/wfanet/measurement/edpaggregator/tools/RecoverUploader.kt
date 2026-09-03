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

import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest
import org.wfanet.measurement.securecomputation.datawatcher.WatchedBlobs

/** Registers a fresh done-object generation for a memoized upload invalidated by an eviction. */
class RecoverUploader(
  private val uploadsStub: RawImpressionUploadServiceCoroutineStub,
  private val modelLinesStub: RawImpressionUploadModelLineServiceCoroutineStub,
  private val rankIndexBlobsStub: RankIndexBlobServiceCoroutineStub,
  private val rewriteDoneBlob:
    suspend (doneBlobUri: String, expectedGeneration: Long, metadata: Map<String, String>) -> Long,
) {
  data class Result(
    val sourceUpload: String,
    val doneBlobUri: String,
    val doneBlobGeneration: Long,
    val modelLines: List<String>,
  )

  /**
   * Rewrites the source upload's done object so DataWatcher dispatches a new upload restricted to
   * [cmmsModelLines].
   */
  suspend fun recover(sourceUploadName: String, cmmsModelLines: List<String>): Result {
    val sourceKey =
      requireNotNull(RawImpressionUploadKey.fromName(sourceUploadName)) {
        "Malformed RawImpressionUpload resource name: $sourceUploadName"
      }
    require(cmmsModelLines.isNotEmpty()) { "at least one model line is required" }
    require(cmmsModelLines.all { ModelLineKey.fromName(it) != null }) {
      "all model lines must be valid CMMS ModelLine resource names: $cmmsModelLines"
    }
    require(cmmsModelLines.distinct().size == cmmsModelLines.size) {
      "model lines must not contain duplicates: $cmmsModelLines"
    }

    val source =
      uploadsStub.getRawImpressionUpload(getRawImpressionUploadRequest { name = sourceUploadName })
    require(source.doneBlobGeneration > 0L) {
      "$sourceUploadName does not have a valid done-object generation"
    }

    val latest = findLatestUpload(sourceKey.parentKey.toName(), source.doneBlobUri)
    require(latest?.name == source.name) {
      "$sourceUploadName has been superseded by ${latest?.name}; recover the latest revision"
    }

    val rowsByCmmsModelLine = listModelLines(sourceUploadName).associateBy { it.cmmsModelLine }
    val missing = cmmsModelLines.filter { it !in rowsByCmmsModelLine }
    require(missing.isEmpty()) { "$sourceUploadName has no model-line rows for: $missing" }

    val notFailed =
      cmmsModelLines
        .map { rowsByCmmsModelLine.getValue(it) }
        .filter { it.state != RawImpressionUploadModelLine.State.FAILED }
    require(notFailed.isEmpty()) {
      "recover-upload only accepts FAILED model-line rows: ${notFailed.map { it.name to it.state }}"
    }

    val recoverableModelLines =
      rowsByCmmsModelLine.values
        .filter { it.state == RawImpressionUploadModelLine.State.FAILED }
        .filter { hasDeletedSnapshotHistory(sourceUploadName, it.cmmsModelLine) }
        .mapTo(mutableSetOf()) { it.cmmsModelLine }
    require(cmmsModelLines.toSet() == recoverableModelLines) {
      "recover-upload requires the complete set of FAILED memoized model lines whose snapshots " +
        "were deleted; requested=$cmmsModelLines, recoverable=$recoverableModelLines"
    }

    val metadata =
      mapOf(
        WatchedBlobs.OVERRIDE_MODEL_LINES_KEY to cmmsModelLines.joinToString(separator = ","),
        WatchedBlobs.RECOVERY_SOURCE_UPLOAD_KEY to source.name,
      )
    val generation = rewriteDoneBlob(source.doneBlobUri, source.doneBlobGeneration, metadata)
    require(generation > source.doneBlobGeneration) {
      "rewriting ${source.doneBlobUri} did not create a newer generation: $generation"
    }
    return Result(source.name, source.doneBlobUri, generation, cmmsModelLines)
  }

  private suspend fun findLatestUpload(parent: String, doneBlobUri: String): RawImpressionUpload? {
    var pageToken = ""
    var latest: RawImpressionUpload? = null
    do {
      val response =
        uploadsStub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            this.parent = parent
            filter = ListRawImpressionUploadsRequestKt.filter { this.doneBlobUri = doneBlobUri }
            this.pageToken = pageToken
          }
        )
      latest =
        (response.rawImpressionUploadsList + listOfNotNull(latest)).maxByOrNull {
          it.doneBlobGeneration
        }
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return latest
  }

  private suspend fun listModelLines(uploadName: String): List<RawImpressionUploadModelLine> {
    val rows = mutableListOf<RawImpressionUploadModelLine>()
    var pageToken = ""
    do {
      val response =
        modelLinesStub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = uploadName
            this.pageToken = pageToken
          }
        )
      rows += response.rawImpressionUploadModelLinesList
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return rows
  }

  private suspend fun hasDeletedSnapshotHistory(
    uploadName: String,
    cmmsModelLine: String,
  ): Boolean {
    var pageToken = ""
    var found = false
    do {
      val response =
        rankIndexBlobsStub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = uploadName
            showDeleted = true
            filter =
              ListRankIndexBlobsRequestKt.filter {
                blobType = RankIndexBlob.BlobType.SNAPSHOT
                this.cmmsModelLine = cmmsModelLine
              }
            this.pageToken = pageToken
          }
        )
      if (response.rankIndexBlobsList.any { !it.hasDeleteTime() }) return false
      found = found || response.rankIndexBlobsCount > 0
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return found
  }
}
