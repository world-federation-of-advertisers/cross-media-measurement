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
import java.util.UUID
import kotlinx.coroutines.flow.single
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.PoolAssignmentJobServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.EncryptedDek
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

class SpannerPoolAssignmentJobServiceTest : PoolAssignmentJobServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private var nextUploadId: Long = 1L
  private var nextModelLineId: Long = 1L

  override fun newService(
    idGenerator: IdGenerator
  ): PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerPoolAssignmentJobService(databaseClient, idGenerator = idGenerator)
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
        .to("gs://bucket/done")
        .set("State")
        .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED))
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    spannerDatabase.databaseClient.write(listOf(mutation))
  }

  override suspend fun createRawImpressionUploadModelLine(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
  ) {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    val uploadId: Long =
      databaseClient.singleUse().use { txn ->
        txn
          .executeQuery(
            statement(
              """
              SELECT RawImpressionUploadId
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
          .getLong("RawImpressionUploadId")
      }

    val mutation =
      Mutation.newInsertBuilder("RawImpressionUploadModelLine")
        .set("DataProviderResourceId")
        .to(dataProviderResourceId)
        .set("RawImpressionUploadId")
        .to(uploadId)
        .set("RawImpressionUploadModelLineId")
        .to(nextModelLineId++)
        .set("RawImpressionUploadModelLineResourceId")
        .to("ml-${UUID.randomUUID()}")
        .set("CmmsModelLine")
        .to(cmmsModelLine)
        .set("State")
        .to(
          Value.protoEnum(
            RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
          )
        )
        .set("PoolOffsets")
        .toInt64Array(emptyList())
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    databaseClient.write(listOf(mutation))
  }

  override suspend fun getRawImpressionUploadModelLineMergedDek(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
  ): EncryptedDek? {
    return spannerDatabase.databaseClient.singleUse().use { txn ->
      val row =
        txn
          .executeQuery(
            statement(
              """
              SELECT ModelLine.EncryptedMergedDek AS EncryptedMergedDek
              FROM RawImpressionUploadModelLine AS ModelLine
              JOIN RawImpressionUpload AS Upload
                USING (DataProviderResourceId, RawImpressionUploadId)
              WHERE ModelLine.DataProviderResourceId = @dataProviderResourceId
                AND Upload.RawImpressionUploadResourceId = @rawImpressionUploadResourceId
                AND ModelLine.CmmsModelLine = @cmmsModelLine
              """
                .trimIndent()
            ) {
              bind("dataProviderResourceId").to(dataProviderResourceId)
              bind("rawImpressionUploadResourceId").to(rawImpressionUploadResourceId)
              bind("cmmsModelLine").to(cmmsModelLine)
            }
          )
          .single()
      if (row.isNull("EncryptedMergedDek")) {
        null
      } else {
        row.getProtoMessage("EncryptedMergedDek", EncryptedDek.getDefaultInstance())
      }
    }
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
