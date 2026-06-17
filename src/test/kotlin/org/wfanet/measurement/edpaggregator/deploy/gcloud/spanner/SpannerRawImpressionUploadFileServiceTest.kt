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

import com.google.cloud.spanner.Value
import java.util.UUID
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadFileServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadFileServiceGrpcKt.RawImpressionUploadFileServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

class SpannerRawImpressionUploadFileServiceTest : RawImpressionUploadFileServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  override fun newFileService(): RawImpressionUploadFileServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRawImpressionUploadFileService(databaseClient)
  }

  override suspend fun createUpload(dataProviderResourceId: String): String {
    val uploadResourceId: String = UUID.randomUUID().toString()
    spannerDatabase.databaseClient.readWriteTransaction().run { txn ->
      txn.bufferInsertMutation("RawImpressionUpload") {
        set("DataProviderResourceId").to(dataProviderResourceId)
        set("RawImpressionUploadId").to(uploadResourceId)
        set("DoneBlobUri").to("gs://bucket/done")
        set("State").to(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED)
        set("CreateTime").to(Value.COMMIT_TIMESTAMP)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      }
    }
    return uploadResourceId
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
