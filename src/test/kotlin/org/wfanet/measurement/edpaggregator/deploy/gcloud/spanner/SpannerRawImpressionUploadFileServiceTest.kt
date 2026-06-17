// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import java.util.UUID
import java.util.concurrent.atomic.AtomicLong
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadFileServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadFilesPageToken
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadFilesRequest

class SpannerRawImpressionUploadFileServiceTest : RawImpressionUploadFileServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  override fun newFileService(): RawImpressionUploadFileServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRawImpressionUploadFileService(databaseClient)
  }

  override suspend fun createUpload(dataProviderResourceId: String): String {
    val rawImpressionUploadId: Long = idCounter.incrementAndGet()
    val rawImpressionUploadResourceId: String = UUID.randomUUID().toString()
    insertUpload(dataProviderResourceId, rawImpressionUploadId, rawImpressionUploadResourceId)
    return rawImpressionUploadResourceId
  }

  private suspend fun insertUpload(
    dataProviderResourceId: String,
    rawImpressionUploadId: Long,
    rawImpressionUploadResourceId: String,
  ) {
    spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
      txn.bufferInsertMutation("RawImpressionUpload") {
        set("DataProviderResourceId").to(dataProviderResourceId)
        set("RawImpressionUploadId").to(rawImpressionUploadId)
        set("RawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
        set("DoneBlobUri").to("gs://bucket/done")
        set("State").to(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED)
        set("CreateTime").to(Value.COMMIT_TIMESTAMP)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }
  }

  /**
   * Inserts a file row with an explicit [createTime] so tests can force CreateTime ties (which
   * exercise the keyset-pagination tie-break branches).
   */
  private suspend fun insertFile(
    dataProviderResourceId: String,
    rawImpressionUploadId: Long,
    fileId: Long,
    fileResourceId: String,
    blobUri: String,
    createTime: Timestamp,
    deleted: Boolean = false,
  ) {
    spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
      txn.bufferInsertMutation("RawImpressionUploadFile") {
        set("DataProviderResourceId").to(dataProviderResourceId)
        set("RawImpressionUploadId").to(rawImpressionUploadId)
        set("FileId").to(fileId)
        set("FileResourceId").to(fileResourceId)
        set("BlobUri").to(blobUri)
        set("CreateTime").to(createTime)
        set("UpdateTime").to(createTime)
        if (deleted) {
          set("DeleteTime").to(createTime)
        }
      }
    }
  }

  /** Lists all files for [dataProviderResourceId]/[uploadResourceId] one page at a time. */
  private suspend fun listAllFileResourceIds(
    service: RawImpressionUploadFileServiceCoroutineImplBase,
    dataProviderResourceId: String,
    uploadResourceId: String,
  ): List<String> {
    val collected = mutableListOf<String>()
    var token: ListRawImpressionUploadFilesPageToken? = null
    var pages = 0
    do {
      val pageToken = token
      val response =
        service.listRawImpressionUploadFiles(
          listRawImpressionUploadFilesRequest {
            this.dataProviderResourceId = dataProviderResourceId
            rawImpressionUploadResourceId = uploadResourceId
            pageSize = 1
            if (pageToken != null) {
              this.pageToken = pageToken
            }
          }
        )
      collected += response.rawImpressionUploadFilesList.map { it.fileResourceId }
      token = if (response.hasNextPageToken()) response.nextPageToken else null
      pages++
    } while (token != null && pages < MAX_PAGES)
    return collected
  }

  @Test
  fun `listRawImpressionUploadFiles does not leak across data providers on create time ties`() =
    runBlocking {
      val service = SpannerRawImpressionUploadFileService(spannerDatabase.databaseClient)
      val tie: Timestamp = Timestamp.ofTimeSecondsAndNanos(1_600_000_000L, 0)
      insertUpload(DATA_PROVIDER_A, 1L, "upload-a")
      insertUpload(DATA_PROVIDER_B, 2L, "upload-b")
      insertFile(DATA_PROVIDER_A, 1L, 11L, "file-a1", "gs://a/1", tie)
      insertFile(DATA_PROVIDER_A, 1L, 12L, "file-a2", "gs://a/2", tie)
      // Same CreateTime tie, different DataProvider, and an upload resource id sorting after
      // "upload-a" so the buggy keyset OR-branch would surface it.
      insertFile(DATA_PROVIDER_B, 2L, 21L, "file-b1", "gs://b/1", tie)

      val collected = listAllFileResourceIds(service, DATA_PROVIDER_A, "upload-a")

      assertThat(collected).containsExactly("file-a1", "file-a2").inOrder()
      assertThat(collected).doesNotContain("file-b1")
    }

  @Test
  fun `listRawImpressionUploadFiles excludes soft-deleted rows during keyset pagination on ties`() =
    runBlocking {
      val service = SpannerRawImpressionUploadFileService(spannerDatabase.databaseClient)
      val tie: Timestamp = Timestamp.ofTimeSecondsAndNanos(1_600_000_100L, 0)
      insertUpload(DATA_PROVIDER_A, 1L, "upload-a")
      insertFile(DATA_PROVIDER_A, 1L, 11L, "file-a1", "gs://a/1", tie)
      insertFile(DATA_PROVIDER_A, 1L, 12L, "file-a2", "gs://a/2", tie, deleted = true)
      insertFile(DATA_PROVIDER_A, 1L, 13L, "file-a3", "gs://a/3", tie)

      val collected = listAllFileResourceIds(service, DATA_PROVIDER_A, "upload-a")

      assertThat(collected).containsExactly("file-a1", "file-a3").inOrder()
      assertThat(collected).doesNotContain("file-a2")
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_A = "data-provider-a"
    private const val DATA_PROVIDER_B = "data-provider-b"
    private const val MAX_PAGES = 10
    private val idCounter = AtomicLong(1000L)
  }
}
