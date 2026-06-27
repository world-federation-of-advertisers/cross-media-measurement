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

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.single
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadModelLineServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

class SpannerRawImpressionUploadModelLineServiceTest : RawImpressionUploadModelLineServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private var nextUploadId: Long = 1L

  override fun newService(
    idGenerator: IdGenerator
  ): RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRawImpressionUploadModelLineService(databaseClient, idGenerator = idGenerator)
  }

  override suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ) {
    val uploadId = nextUploadId++
    val mutation =
      Mutation.newInsertBuilder("RawImpressionUpload")
        .set("DataProviderResourceId")
        .to(dataProviderResourceId)
        .set("RawImpressionUploadId")
        .to(uploadId)
        .set("RawImpressionUploadResourceId")
        .to(rawImpressionUploadResourceId)
        .set("DoneBlobUri")
        .to("gs://bucket/done-$uploadId")
        .set("State")
        .to(
          Value.protoEnum(
            org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
              .RAW_IMPRESSION_UPLOAD_STATE_CREATED
          )
        )
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    spannerDatabase.databaseClient.write(listOf(mutation))
  }

  override suspend fun getParentUploadState(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ): RawImpressionUploadState {
    return spannerDatabase.databaseClient.singleUse().use { txn ->
      val row =
        txn
          .executeQuery(
            statement(
              """
              SELECT CAST(State AS INT64) AS StateValue
              FROM RawImpressionUpload
              WHERE DataProviderResourceId = @dataProviderResourceId
                AND RawImpressionUploadResourceId = @rawImpressionUploadResourceId
              """
                .trimIndent()
            ) {
              bind("dataProviderResourceId").to(dataProviderResourceId)
              bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
            }
          )
          .single()
      RawImpressionUploadState.forNumber(row.getLong("StateValue").toInt())
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
