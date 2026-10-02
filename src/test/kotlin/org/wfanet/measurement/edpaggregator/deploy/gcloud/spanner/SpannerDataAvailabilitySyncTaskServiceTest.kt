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
import com.google.common.truth.Truth.assertThat
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.telemetry.VidLabelingTraceAttributes
import org.wfanet.measurement.edpaggregator.vidlabeling.RequestIds
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.DataAvailabilitySyncTaskState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.createDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.dataAvailabilitySyncTask
import org.wfanet.measurement.internal.edpaggregator.getDataAvailabilitySyncTaskRequest
import org.wfanet.measurement.internal.edpaggregator.listDataAvailabilitySyncTasksRequest

@RunWith(JUnit4::class)
class SpannerDataAvailabilitySyncTaskServiceTest {
  @get:Rule
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  @Test
  fun `create is idempotent and task is readable`() = runBlocking {
    insertUpload()
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    val request = createRequest()

    val created = service.createDataAvailabilitySyncTask(request)
    val replayed = service.createDataAvailabilitySyncTask(request)
    val read =
      service.getDataAvailabilitySyncTask(
        getDataAvailabilitySyncTaskRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          rawImpressionUploadResourceId = UPLOAD_ID
          dataAvailabilitySyncTaskResourceId = TASK_ID
        }
      )

    assertThat(created.state)
      .isEqualTo(DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING)
    assertThat(created.doneBlobPathHash)
      .isEqualTo(VidLabelingTraceAttributes.gcsObjectPathHash(DONE_URI))
    assertThat(created.attemptCount).isEqualTo(0)
    assertThat(replayed).isEqualTo(created)
    assertThat(read).isEqualTo(created)
    Unit
  }

  @Test
  fun `list filters tasks by state`() = runBlocking {
    insertUpload()
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    val created = service.createDataAvailabilitySyncTask(createRequest())

    val response =
      service.listDataAvailabilitySyncTasks(
        listDataAvailabilitySyncTasksRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          rawImpressionUploadResourceId = UPLOAD_ID
          filter =
            org.wfanet.measurement.internal.edpaggregator.ListDataAvailabilitySyncTasksRequestKt
              .filter {
                state = DataAvailabilitySyncTaskState.DATA_AVAILABILITY_SYNC_TASK_STATE_PENDING
              }
        }
      )

    assertThat(response.dataAvailabilitySyncTasksList).containsExactly(created)
    Unit
  }

  @Test
  fun `same done object with fresh trace context returns existing task`() = runBlocking {
    insertUpload()
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    val created = service.createDataAvailabilitySyncTask(createRequest())

    val replayed =
      service.createDataAvailabilitySyncTask(
        createRequest()
          .toBuilder()
          .clearRequestId()
          .setDataAvailabilitySyncTask(
            createRequest()
              .dataAvailabilitySyncTask
              .toBuilder()
              .setTraceparent("00-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa-bbbbbbbbbbbbbbbb-01")
          )
          .build()
      )

    assertThat(replayed).isEqualTo(created)
    Unit
  }

  @Test
  fun `request ID for a different object is rejected`() = runBlocking {
    insertUpload()
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    service.createDataAvailabilitySyncTask(createRequest())
    val otherUri = "gs://bucket/labeled/2026-10-01/done"

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.createDataAvailabilitySyncTask(
          createRequest()
            .toBuilder()
            .setDataAvailabilitySyncTaskResourceId(taskId(otherUri, GENERATION))
            .setDataAvailabilitySyncTask(
              createRequest().dataAvailabilitySyncTask.toBuilder().setDoneBlobUri(otherUri)
            )
            .build()
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Unit
  }

  @Test
  fun `create rejects a non-deterministic resource ID`() = runBlocking {
    insertUpload()
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.createDataAvailabilitySyncTask(
          createRequest().toBuilder().setDataAvailabilitySyncTaskResourceId("different").build()
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Unit
  }

  @Test
  fun `create rejects a model line outside the parent upload`() = runBlocking {
    insertUpload(includeModelLine = false)
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.createDataAvailabilitySyncTask(createRequest())
      }

    assertThat(error.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    Unit
  }

  @Test
  fun `page token cannot be reused for a different parent`() = runBlocking {
    insertUpload()
    val service = SpannerDataAvailabilitySyncTaskService(spannerDatabase.databaseClient)
    service.createDataAvailabilitySyncTask(createRequest())
    service.createDataAvailabilitySyncTask(
      createRequest("gs://bucket/labeled/2026-10-01/done", 124L)
    )
    val firstPage =
      service.listDataAvailabilitySyncTasks(
        listDataAvailabilitySyncTasksRequest {
          dataProviderResourceId = DATA_PROVIDER_ID
          rawImpressionUploadResourceId = UPLOAD_ID
          pageSize = 1
        }
      )

    val error =
      assertFailsWith<StatusRuntimeException> {
        service.listDataAvailabilitySyncTasks(
          listDataAvailabilitySyncTasksRequest {
            dataProviderResourceId = DATA_PROVIDER_ID
            pageSize = 1
            pageToken = firstPage.nextPageToken
          }
        )
      }

    assertThat(error.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    Unit
  }

  private fun createRequest(doneUri: String = DONE_URI, generation: Long = GENERATION) =
    createDataAvailabilitySyncTaskRequest {
      dataProviderResourceId = DATA_PROVIDER_ID
      rawImpressionUploadResourceId = UPLOAD_ID
      dataAvailabilitySyncTaskResourceId = taskId(doneUri, generation)
      requestId = taskId(doneUri, generation)
      dataAvailabilitySyncTask = dataAvailabilitySyncTask {
        doneBlobUri = doneUri
        doneBlobGeneration = generation
        cmmsModelLine = MODEL_LINE
        eventDate = date {
          year = 2026
          month = 9
          day = 30
        }
        traceparent = "00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01"
      }
    }

  private suspend fun insertUpload(includeModelLine: Boolean = true) {
    spannerDatabase.databaseClient.write(
      buildList {
        add(
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
            .build()
        )
        if (includeModelLine) {
          add(
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
              .build()
          )
        }
      }
    )
  }

  companion object {
    @ClassRule @JvmField val spannerEmulator = SpannerEmulatorRule()
    private const val DATA_PROVIDER_ID = "data-provider"
    private const val UPLOAD_ID = "upload"
    private const val DONE_URI = "gs://bucket/labeled/2026-09-30/done"
    private const val GENERATION = 123L
    private const val MODEL_LINE = "modelProviders/mp/modelSuites/ms/modelLines/ml"
    private val TASK_ID = taskId(DONE_URI, GENERATION)

    private fun taskId(doneUri: String, generation: Long): String =
      RequestIds.forDataAvailabilitySyncTask(
        VidLabelingTraceAttributes.gcsObjectPathHash(doneUri),
        generation,
      )
  }
}
