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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import java.util.UUID
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RankIndexBlobServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RankIndexBlobServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

class SpannerRankIndexBlobServiceTest : RankIndexBlobServiceTest() {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  private var nextUploadId: Long = 1L
  private var nextModelLineId: Long = 1L
  private val uploadIdsByResourceId = mutableMapOf<String, Long>()
  private val modelLineIdsByUploadAndCmms = mutableMapOf<Pair<String, String>, Long>()

  override fun newService(
    idGenerator: IdGenerator
  ): RankIndexBlobServiceGrpcKt.RankIndexBlobServiceCoroutineImplBase {
    val databaseClient: AsyncDatabaseClient = spannerDatabase.databaseClient
    return SpannerRankIndexBlobService(databaseClient, idGenerator = idGenerator)
  }

  override suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ) {
    val uploadId = nextUploadId++
    val uploadMutation =
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
    uploadIdsByResourceId[rawImpressionUploadResourceId] = uploadId
    val modelLineMutations =
      MODEL_LINES.map { cmmsModelLine ->
        val modelLineId = nextModelLineId++
        modelLineIdsByUploadAndCmms[rawImpressionUploadResourceId to cmmsModelLine] = modelLineId
        Mutation.newInsertBuilder("RawImpressionUploadModelLine")
          .set("DataProviderResourceId")
          .to(dataProviderResourceId)
          .set("RawImpressionUploadId")
          .to(uploadId)
          .set("RawImpressionUploadModelLineId")
          .to(modelLineId)
          .set("RawImpressionUploadModelLineResourceId")
          .to("ml-${UUID.randomUUID()}")
          .set("CmmsModelLine")
          .to(cmmsModelLine)
          .set("State")
          .to(
            Value.protoEnum(
              RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING
            )
          )
          .set("PoolOffsets")
          .toInt64Array(emptyList())
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      }
    spannerDatabase.databaseClient.write(listOf(uploadMutation) + modelLineMutations)
  }

  override suspend fun setParentModelLineState(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
    state: RawImpressionUploadModelLineState,
  ) {
    val uploadId = checkNotNull(uploadIdsByResourceId[rawImpressionUploadResourceId])
    val modelLineId =
      checkNotNull(modelLineIdsByUploadAndCmms[rawImpressionUploadResourceId to cmmsModelLine])
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newUpdateBuilder("RawImpressionUploadModelLine")
          .set("DataProviderResourceId")
          .to(dataProviderResourceId)
          .set("RawImpressionUploadId")
          .to(uploadId)
          .set("RawImpressionUploadModelLineId")
          .to(modelLineId)
          .set("State")
          .to(Value.protoEnum(state))
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build()
      )
    )
  }

  companion object {
    private val MODEL_LINES =
      listOf(
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml1",
        "modelProviders/mp1/modelSuites/ms1/modelLines/ml2",
      )
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
