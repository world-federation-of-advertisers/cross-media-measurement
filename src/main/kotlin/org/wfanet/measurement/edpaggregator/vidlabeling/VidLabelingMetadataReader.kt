// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.vidlabeling

import com.google.protobuf.Message
import org.wfanet.measurement.common.api.grpc.ResourceList
import org.wfanet.measurement.common.throttler.Throttler
import org.wfanet.measurement.edpaggregator.v1alpha.ListRankIndexBlobsRequestKt.filter as rankIndexFilter
import org.wfanet.measurement.edpaggregator.v1alpha.ListRawImpressionUploadsRequestKt.filter as uploadFilter
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlob
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUpload
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.listRankIndexBlobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadsRequest

/** Throttled reads of upload revisions, model lines, and rank snapshots. */
object VidLabelingMetadataReader {
  /** One page of registered revisions of [doneBlobUri]. */
  suspend fun uploadRevisionsPage(
    stub: RawImpressionUploadServiceCoroutineStub,
    throttler: Throttler,
    parent: String,
    doneBlobUri: String,
    pageToken: String,
  ): ResourceList<RawImpressionUpload, String> {
    val response =
      throttler.onReady {
        stub.listRawImpressionUploads(
          listRawImpressionUploadsRequest {
            this.parent = parent
            filter = uploadFilter { this.doneBlobUri = doneBlobUri }
            this.pageToken = pageToken
          }
        )
      }
    return ResourceList(response.rawImpressionUploadsList, response.nextPageToken)
  }

  /** All registered revisions of [doneBlobUri], in service order. */
  suspend fun uploadRevisions(
    stub: RawImpressionUploadServiceCoroutineStub,
    throttler: Throttler,
    parent: String,
    doneBlobUri: String,
  ): List<RawImpressionUpload> = readAllPages { pageToken ->
    uploadRevisionsPage(stub, throttler, parent, doneBlobUri, pageToken)
  }

  /** One page of model lines registered under [uploadName]. */
  suspend fun uploadModelLinesPage(
    stub: RawImpressionUploadModelLineServiceCoroutineStub,
    throttler: Throttler,
    uploadName: String,
    pageToken: String,
  ): ResourceList<RawImpressionUploadModelLine, String> {
    val response =
      throttler.onReady {
        stub.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = uploadName
            this.pageToken = pageToken
          }
        )
      }
    return ResourceList(response.rawImpressionUploadModelLinesList, response.nextPageToken)
  }

  /** All model lines registered under [uploadName], in service order. */
  suspend fun uploadModelLines(
    stub: RawImpressionUploadModelLineServiceCoroutineStub,
    throttler: Throttler,
    uploadName: String,
  ): List<RawImpressionUploadModelLine> = readAllPages { pageToken ->
    uploadModelLinesPage(stub, throttler, uploadName, pageToken)
  }

  /**
   * All rank snapshots for [cmmsModelLine] under [uploadName], optionally including deleted rows.
   */
  suspend fun snapshots(
    stub: RankIndexBlobServiceCoroutineStub,
    throttler: Throttler,
    uploadName: String,
    cmmsModelLine: String,
    showDeleted: Boolean,
  ): List<RankIndexBlob> = readAllPages { pageToken ->
    snapshotsPage(stub, throttler, uploadName, cmmsModelLine, showDeleted, pageToken, pageSize = 0)
  }

  /** Whether [uploadName] has a non-deleted snapshot for [cmmsModelLine]. */
  suspend fun hasActiveSnapshot(
    stub: RankIndexBlobServiceCoroutineStub,
    throttler: Throttler,
    uploadName: String,
    cmmsModelLine: String,
  ): Boolean =
    snapshotsPage(
        stub,
        throttler,
        uploadName,
        cmmsModelLine,
        showDeleted = false,
        pageToken = "",
        pageSize = 1,
      )
      .isNotEmpty()

  /** Whether at least one snapshot exists and every snapshot for this identity is deleted. */
  suspend fun hasDeletedSnapshotHistory(
    stub: RankIndexBlobServiceCoroutineStub,
    throttler: Throttler,
    uploadName: String,
    cmmsModelLine: String,
  ): Boolean {
    var pageToken = ""
    var found = false
    do {
      val response =
        snapshotsPage(
          stub,
          throttler,
          uploadName,
          cmmsModelLine,
          showDeleted = true,
          pageToken = pageToken,
          pageSize = 0,
        )
      if (response.any { !it.hasDeleteTime() }) return false
      found = found || response.isNotEmpty()
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return found
  }

  private suspend fun snapshotsPage(
    stub: RankIndexBlobServiceCoroutineStub,
    throttler: Throttler,
    uploadName: String,
    cmmsModelLine: String,
    showDeleted: Boolean,
    pageToken: String,
    pageSize: Int,
  ): ResourceList<RankIndexBlob, String> {
    val response =
      throttler.onReady {
        stub.listRankIndexBlobs(
          listRankIndexBlobsRequest {
            parent = uploadName
            this.showDeleted = showDeleted
            this.pageToken = pageToken
            this.pageSize = pageSize
            filter = rankIndexFilter {
              blobType = RankIndexBlob.BlobType.SNAPSHOT
              this.cmmsModelLine = cmmsModelLine
            }
          }
        )
      }
    return ResourceList(response.rankIndexBlobsList, response.nextPageToken)
  }

  private suspend fun <R : Message> readAllPages(
    readPage: suspend (String) -> ResourceList<R, String>
  ): List<R> {
    val resources = mutableListOf<R>()
    var pageToken = ""
    do {
      val response = readPage(pageToken)
      resources += response.resources
      pageToken = response.nextPageToken
    } while (pageToken.isNotEmpty())
    return resources
  }
}
