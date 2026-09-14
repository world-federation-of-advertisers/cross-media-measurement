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
import org.junit.ClassRule
import org.junit.Rule
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.internal.testing.RawImpressionUploadServiceTest
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.insertMutation
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadServiceGrpcKt
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

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

  override suspend fun createActiveModelLine(dataProviderResourceId: String) {
    val databaseClient = spannerDatabase.databaseClient
    databaseClient.write(
      listOf(
        insertMutation("RawImpressionUpload") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(1L)
          set("RawImpressionUploadResourceId").to("upload-active")
          set("DoneBlobUri").to("gs://bucket/active/done")
          set("RegistrationComplete").to(true)
          set("State")
            .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE))
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
    databaseClient.write(
      listOf(
        insertMutation("RawImpressionUploadModelLine") {
          set("DataProviderResourceId").to(dataProviderResourceId)
          set("RawImpressionUploadId").to(1L)
          set("RawImpressionUploadModelLineId").to(1L)
          set("RawImpressionUploadModelLineResourceId").to("model-line-active")
          set("CmmsModelLine").to("modelProviders/mp/modelSuites/ms/modelLines/ml")
          set("State")
            .to(
              Value.protoEnum(
                RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
              )
            )
          set("CreateTime").to(Value.COMMIT_TIMESTAMP)
          set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        }
      )
    )
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()
  }
}
