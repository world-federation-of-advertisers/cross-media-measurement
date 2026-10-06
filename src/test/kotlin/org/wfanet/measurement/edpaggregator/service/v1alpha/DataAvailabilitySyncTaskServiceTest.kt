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

package org.wfanet.measurement.edpaggregator.service.v1alpha

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerDataAvailabilitySyncTaskService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.DataAvailabilitySyncTaskKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.v1alpha.DataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.v1alpha.createDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.edpaggregator.v1alpha.dataAvailabilitySyncTask
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskServiceGrpcKt.DataAvailabilitySyncTaskServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

@RunWith(JUnit4::class)
class DataAvailabilitySyncTaskServiceTest {
  private val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)
  private val grpcServer = GrpcTestServerRule {
    addService(
      SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient, EmptyCoroutineContext)
    )
  }

  @get:Rule val ruleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcServer)

  private lateinit var service: DataAvailabilitySyncTaskService

  @Before
  fun setUp() {
    service =
      DataAvailabilitySyncTaskService(
        DataAvailabilitySyncTaskServiceCoroutineStub(grpcServer.channel)
      )
  }

  @Test
  fun `create returns a pending task with a stable name`() =
    runBlocking<Unit> {
      insertUpload()

      val task = service.createDataAvailabilitySyncTask(createRequest())

      assertThat(task.name)
        .isEqualTo(DataAvailabilitySyncTaskKey(DATA_PROVIDER_ID, UPLOAD_ID, TASK_ID).toName())
      assertThat(task.state).isEqualTo(DataAvailabilitySyncTask.State.PENDING)
      assertThat(task.doneBlobPathHash)
        .isEqualTo(VidLabelingTraceAttributes.gcsObjectPathHash(DONE_URI))
      assertThat(task.createTime).isEqualTo(task.updateTime)
      assertThat(task.etag).isNotEmpty()
    }

  @Test
  fun `create rejects missing request ID`() =
    runBlocking<Unit> {
      insertUpload()

      val error =
        assertFailsWith<StatusRuntimeException> {
          service.createDataAvailabilitySyncTask(
            createRequest().toBuilder().clearRequestId().build()
          )
        }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `create rejects malformed trace context`() =
    runBlocking<Unit> {
      insertUpload()
      val request =
        createRequest()
          .toBuilder()
          .setDataAvailabilitySyncTask(
            createRequest().dataAvailabilitySyncTask.toBuilder().setTraceparent("not-a-traceparent")
          )
          .build()

      val error =
        assertFailsWith<StatusRuntimeException> { service.createDataAvailabilitySyncTask(request) }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `create rejects malformed done blob URI`() =
    runBlocking<Unit> {
      insertUpload()
      val request =
        createRequest()
          .toBuilder()
          .setDataAvailabilitySyncTask(
            createRequest()
              .dataAvailabilitySyncTask
              .toBuilder()
              .setDoneBlobUri("https://bucket/path/done")
          )
          .build()

      val error =
        assertFailsWith<StatusRuntimeException> { service.createDataAvailabilitySyncTask(request) }

      assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `create canonicalizes the GCS scheme before deriving the task ID`() =
    runBlocking<Unit> {
      insertUpload()
      val uppercaseUri = "GS://bucket/labeled/2026-09-30/done"
      val request =
        createRequest()
          .toBuilder()
          .setDataAvailabilitySyncTask(
            createRequest().dataAvailabilitySyncTask.toBuilder().setDoneBlobUri(uppercaseUri)
          )
          .build()

      val task = service.createDataAvailabilitySyncTask(request)

      assertThat(task.name)
        .isEqualTo(DataAvailabilitySyncTaskKey(DATA_PROVIDER_ID, UPLOAD_ID, TASK_ID).toName())
      assertThat(task.doneBlobUri).isEqualTo(DONE_URI)
    }

  private fun createRequest() = createDataAvailabilitySyncTaskRequest {
    parent = RawImpressionUploadKey(DATA_PROVIDER_ID, UPLOAD_ID).toName()
    dataAvailabilitySyncTaskId = TASK_ID
    requestId = REQUEST_ID
    dataAvailabilitySyncTask = dataAvailabilitySyncTask {
      doneBlobUri = DONE_URI
      doneBlobGeneration = GENERATION
      cmmsModelLine = MODEL_LINE
      eventDate = date {
        year = 2026
        month = 9
        day = 30
      }
      traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
    }
  }

  private suspend fun insertUpload() {
    spannerDatabase.databaseClient.write(
      listOf(
        Mutation.newInsertBuilder("RawImpressionUpload")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadId")
          .to(1L)
          .set("RawImpressionUploadResourceId")
          .to(UPLOAD_ID)
          .set("DoneBlobUri")
          .to("gs://bucket/raw/done")
          .set("DoneBlobGeneration")
          .to(1L)
          .set("DoneBlobCreateTime")
          .to(com.google.cloud.Timestamp.ofTimeSecondsAndNanos(1L, 0))
          .set("RegistrationComplete")
          .to(true)
          .set("State")
          .to(Value.protoEnum(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED))
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
        Mutation.newInsertBuilder("RawImpressionUploadModelLine")
          .set("DataProviderResourceId")
          .to(DATA_PROVIDER_ID)
          .set("RawImpressionUploadId")
          .to(1L)
          .set("RawImpressionUploadModelLineId")
          .to(1L)
          .set("RawImpressionUploadModelLineResourceId")
          .to("upload-model-line")
          .set("CmmsModelLine")
          .to(MODEL_LINE)
          .set("State")
          .to(
            Value.protoEnum(
              RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
            )
          )
          .set("CreateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .set("UpdateTime")
          .to(Value.COMMIT_TIMESTAMP)
          .build(),
      )
    )
  }

  companion object {
    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()
    private const val DATA_PROVIDER_ID = "data-provider"
    private const val UPLOAD_ID = "upload"
    private const val DONE_URI = "gs://bucket/labeled/2026-09-30/done"
    private const val GENERATION = 123L
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private val TASK_ID =
      RequestIds.forDataAvailabilitySyncTask(
        VidLabelingTraceAttributes.gcsObjectPathHash(DONE_URI),
        GENERATION,
      )
    private val REQUEST_ID = TASK_ID
  }
}
