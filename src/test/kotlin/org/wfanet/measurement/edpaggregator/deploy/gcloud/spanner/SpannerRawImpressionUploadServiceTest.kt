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

import com.google.cloud.spanner.Options
import com.google.common.truth.Truth.assertThat
import java.util.UUID
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.db.insertRawImpressionUpload
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadActiveRequest

class SpannerRawImpressionUploadServiceTest : RawImpressionUploadServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  override fun newService(
    idGenerator: IdGenerator
  ): RawImpressionUploadServiceGrpcKt.RawImpressionUploadServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRawImpressionUploadService(databaseClient, idGenerator = idGenerator)
  }

  /** Seeds a `RawImpressionUpload` row directly in the given [state]. */
  private suspend fun seedUpload(
    rawImpressionUploadId: Long,
    rawImpressionUploadResourceId: String,
    state: RawImpressionUploadState,
  ) {
    spannerDatabase.databaseClient.readWriteTransaction(Options.tag("action=seedUpload")).run { txn
      ->
      txn.insertRawImpressionUpload(
        rawImpressionUploadId,
        DATA_PROVIDER_RESOURCE_ID,
        rawImpressionUploadResourceId,
        DONE_BLOB_URI,
        UUID.randomUUID().toString(),
        state,
      )
    }
  }

  @Test
  fun `markRawImpressionUploadActive reactivates a COMPLETED upload`() = runBlocking {
    val resourceId = "rawImpressionUpload-completed"
    seedUpload(1L, resourceId, RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)

    val result =
      newService()
        .markRawImpressionUploadActive(
          markRawImpressionUploadActiveRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = resourceId
          }
        )

    assertThat(result.state).isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)
  }

  @Test
  fun `markRawImpressionUploadActive is a no-op for an ACTIVE upload`() = runBlocking {
    val resourceId = "rawImpressionUpload-active"
    seedUpload(2L, resourceId, RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)

    val result =
      newService()
        .markRawImpressionUploadActive(
          markRawImpressionUploadActiveRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = resourceId
          }
        )

    assertThat(result.state).isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private const val DATA_PROVIDER_RESOURCE_ID = "data-provider-1"
    private const val DONE_BLOB_URI = "gs://test-bucket/2026-06-16/done"
  }
}
