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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsPageToken
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadsRequest
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

/**
 * Direct database-layer tests for [readRawImpressionUploads] pagination.
 *
 * These exercise the keyset-pagination predicate at the layer where the inputs that trigger the
 * cross-partition leak can actually be constructed: rows that share an identical CreateTime and
 * belong to different data providers. The public service commits one row per transaction, so it can
 * never produce two rows with the same commit timestamp; only a direct multi-row commit can.
 */
@RunWith(JUnit4::class)
class RawImpressionUploadPaginationTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @Test
  fun `pagination does not leak rows across data providers sharing CreateTime`(): Unit =
    runBlocking {
      val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient

      // Insert one row for each of two data providers in a SINGLE transaction so both rows receive
      // the same commit timestamp (CreateTime).
      databaseClient.readWriteTransaction().run { txn ->
        txn.insertRawImpressionUpload(
          rawImpressionUploadId = 1L,
          dataProviderResourceId = DATA_PROVIDER_A,
          rawImpressionUploadResourceId = RESOURCE_A,
          doneBlobUri = DONE_BLOB_URI,
          createRequestId = "",
          state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
        )
        txn.insertRawImpressionUpload(
          rawImpressionUploadId = 1L,
          dataProviderResourceId = DATA_PROVIDER_B,
          rawImpressionUploadResourceId = RESOURCE_B,
          doneBlobUri = DONE_BLOB_URI,
          createRequestId = "",
          state = RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED,
        )
      }

      // Read provider A's row back to obtain the shared CreateTime for the page boundary.
      val rowA =
        databaseClient.singleUse().use { txn ->
          txn.getRawImpressionUploadByResourceId(DATA_PROVIDER_A, RESOURCE_A)
        }

      // The page boundary sits exactly on provider A's row. RESOURCE_B sorts after RESOURCE_A and
      // shares its CreateTime, so a predicate that fails to scope the OR by DataProviderResourceId
      // would surface provider B's row on the next page.
      val after: ListRawImpressionUploadsPageToken.After =
        ListRawImpressionUploadsPageToken.After.newBuilder()
          .setCreateTime(rowA.rawImpressionUpload.createTime)
          .setRawImpressionUploadResourceId(RESOURCE_A)
          .build()

      val nextPage: List<RawImpressionUploadResult> =
        databaseClient.singleUse().use { txn ->
          txn
            .readRawImpressionUploads(
              dataProviderResourceId = DATA_PROVIDER_A,
              filter = ListRawImpressionUploadsRequest.Filter.getDefaultInstance(),
              limit = 10,
              after = after,
            )
            .toList()
        }

      // Provider A has no rows after the boundary; provider B's row must NOT leak in.
      assertThat(nextPage.map { it.rawImpressionUpload.rawImpressionUploadResourceId }).isEmpty()
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_A = "data-provider-a"
    private const val DATA_PROVIDER_B = "data-provider-b"
    private const val RESOURCE_A = "resource-a"
    private const val RESOURCE_B = "resource-b"
    private const val DONE_BLOB_URI = "gs://test-bucket/2026-06-16/done"
  }
}
