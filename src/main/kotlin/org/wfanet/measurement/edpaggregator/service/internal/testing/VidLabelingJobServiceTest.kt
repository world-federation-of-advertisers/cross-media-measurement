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

package org.wfanet.measurement.edpaggregator.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.service.internal.Errors
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListVidLabelingJobsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkVidLabelingJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJob
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.VidLabelingState
import org.wfanet.measurement.internal.edpaggregator.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.internal.edpaggregator.createVidLabelingJobRequest
import org.wfanet.measurement.internal.edpaggregator.getVidLabelingJobRequest
import org.wfanet.measurement.internal.edpaggregator.listVidLabelingJobsRequest
import org.wfanet.measurement.internal.edpaggregator.markVidLabelingJobFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markVidLabelingJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.vidLabelingJob

@RunWith(JUnit4::class)
abstract class VidLabelingJobServiceTest {
  private lateinit var service: VidLabelingJobServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): VidLabelingJobServiceCoroutineImplBase

  /**
   * Creates a parent [RawImpressionUpload] row so that child VidLabelingJob rows can be inserted
   * (interleaved table).
   */
  protected abstract suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  )

  @Before
  fun initService() {
    service = newService()
    runBlocking { createParentUpload(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID) }
  }

  private suspend fun createJob(
    cmmsModelLines: List<String> = listOf(CMMS_MODEL_LINE),
    rawImpressionUploadFiles: List<String> = listOf(FILE_1),
    requestId: String = "",
  ): VidLabelingJob {
    return service.createVidLabelingJob(
      createVidLabelingJobRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        vidLabelingJob = vidLabelingJob {
          this.cmmsModelLines += cmmsModelLines
          this.rawImpressionUploadFiles += rawImpressionUploadFiles
        }
        this.requestId = requestId
      }
    )
  }

  @Test
  fun `createVidLabelingJob creates successfully`() =
    runBlocking<Unit> {
      val startTime: Instant = Instant.now()

      val job: VidLabelingJob = createJob()

      assertThat(job.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
      assertThat(job.rawImpressionUploadResourceId).isEqualTo(RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
      assertThat(job.cmmsModelLinesList).containsExactly(CMMS_MODEL_LINE)
      assertThat(job.rawImpressionUploadFilesList).containsExactly(FILE_1)
      assertThat(job.state).isEqualTo(VidLabelingState.VID_LABELING_STATE_CREATED)
      assertThat(job.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(job.updateTime).isEqualTo(job.createTime)
      assertThat(job.etag).isNotEmpty()
    }

  @Test
  fun `createVidLabelingJob is idempotent with same request_id`() =
    runBlocking<Unit> {
      val requestId: String = UUID.randomUUID().toString()

      val job1: VidLabelingJob = createJob(requestId = requestId)
      val job2: VidLabelingJob = createJob(requestId = requestId)

      assertThat(job2).isEqualTo(job1)
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJob = vidLabelingJob {
                cmmsModelLines += CMMS_MODEL_LINE
                rawImpressionUploadFiles += FILE_1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "data_provider_resource_id"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT if raw_impression_upload_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              vidLabelingJob = vidLabelingJob {
                cmmsModelLines += CMMS_MODEL_LINE
                rawImpressionUploadFiles += FILE_1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_upload_resource_id"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT if vid_labeling_job not set`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "vid_labeling_job"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT if cmms_model_lines empty`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJob = vidLabelingJob { rawImpressionUploadFiles += FILE_1 }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "vid_labeling_job.cmms_model_lines"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT if raw_impression_upload_files empty`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJob = vidLabelingJob { cmmsModelLines += CMMS_MODEL_LINE }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "vid_labeling_job.raw_impression_upload_files"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requestId = "not-a-valid-uuid"
              vidLabelingJob = vidLabelingJob {
                cmmsModelLines += CMMS_MODEL_LINE
                rawImpressionUploadFiles += FILE_1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
          }
        )
    }

  @Test
  fun `batchCreateVidLabelingJobs creates multiple`() =
    runBlocking<Unit> {
      val response =
        service.batchCreateVidLabelingJobs(
          batchCreateVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createVidLabelingJobRequest {
              vidLabelingJob = vidLabelingJob {
                cmmsModelLines += CMMS_MODEL_LINE
                rawImpressionUploadFiles += FILE_1
              }
            }
            requests += createVidLabelingJobRequest {
              vidLabelingJob = vidLabelingJob {
                cmmsModelLines += CMMS_MODEL_LINE_2
                rawImpressionUploadFiles += FILE_2
              }
            }
          }
        )

      assertThat(response.vidLabelingJobsList).hasSize(2)
      assertThat(response.vidLabelingJobsList.flatMap { it.cmmsModelLinesList })
        .containsExactly(CMMS_MODEL_LINE, CMMS_MODEL_LINE_2)
    }

  @Test
  fun `getVidLabelingJob returns existing resource`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val job: VidLabelingJob =
        service.getVidLabelingJob(
          getVidLabelingJobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
          }
        )

      assertThat(job).isEqualTo(created)
    }

  @Test
  fun `getVidLabelingJob throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getVidLabelingJob(
            getVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = "nonexistent-job"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `getVidLabelingJob throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getVidLabelingJob(
            getVidLabelingJobRequest {
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = "some-job"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getVidLabelingJob throws INVALID_ARGUMENT if vid_labeling_job_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getVidLabelingJob(
            getVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listVidLabelingJobs returns resources`() =
    runBlocking<Unit> {
      createJob()
      createJob(cmmsModelLines = listOf(CMMS_MODEL_LINE_2))

      val response: ListVidLabelingJobsResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          }
        )

      assertThat(response.vidLabelingJobsList).hasSize(2)
    }

  @Test
  fun `listVidLabelingJobs respects page_size`() =
    runBlocking<Unit> {
      repeat(3) { createJob() }

      val response: ListVidLabelingJobsResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            pageSize = 2
          }
        )

      assertThat(response.vidLabelingJobsList).hasSize(2)
      assertThat(response.hasNextPageToken()).isTrue()
    }

  @Test
  fun `listVidLabelingJobs filters by state`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()
      createJob()

      service.markVidLabelingJobSucceeded(
        markVidLabelingJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          vidLabelingJobResourceId = created.vidLabelingJobResourceId
          etag = created.etag
        }
      )

      val response: ListVidLabelingJobsResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            filter =
              ListVidLabelingJobsRequestKt.filter {
                state = VidLabelingState.VID_LABELING_STATE_SUCCEEDED
              }
          }
        )

      assertThat(response.vidLabelingJobsList).hasSize(1)
      assertThat(response.vidLabelingJobsList.first().vidLabelingJobResourceId)
        .isEqualTo(created.vidLabelingJobResourceId)
    }

  @Test
  fun `listVidLabelingJobs filters by cmms_model_line`() =
    runBlocking<Unit> {
      createJob(cmmsModelLines = listOf(CMMS_MODEL_LINE))
      createJob(cmmsModelLines = listOf(CMMS_MODEL_LINE_2))

      val response: ListVidLabelingJobsResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            filter = ListVidLabelingJobsRequestKt.filter { cmmsModelLine = CMMS_MODEL_LINE }
          }
        )

      assertThat(response.vidLabelingJobsList).hasSize(1)
      assertThat(response.vidLabelingJobsList.first().cmmsModelLinesList)
        .containsExactly(CMMS_MODEL_LINE)
    }

  @Test
  fun `listVidLabelingJobs pagination with page_token`() =
    runBlocking<Unit> {
      repeat(3) { createJob() }

      val firstPage: ListVidLabelingJobsResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            pageSize = 2
          }
        )

      assertThat(firstPage.vidLabelingJobsList).hasSize(2)
      assertThat(firstPage.hasNextPageToken()).isTrue()

      val secondPage: ListVidLabelingJobsResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            pageSize = 2
            pageToken = firstPage.nextPageToken
          }
        )

      assertThat(secondPage.vidLabelingJobsList).hasSize(1)
      assertThat(secondPage.hasNextPageToken()).isFalse()
    }

  @Test
  fun `markVidLabelingJobSucceeded transitions CREATED to SUCCEEDED`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val response: MarkVidLabelingJobSucceededResponse =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
          }
        )

      assertThat(response.vidLabelingJob.state)
        .isEqualTo(VidLabelingState.VID_LABELING_STATE_SUCCEEDED)
    }

  @Test
  fun `markVidLabelingJobSucceeded reports completed model lines for the last job`() =
    runBlocking<Unit> {
      // job1 covers ML1 and ML2; job2 covers ML1 only.
      val job1: VidLabelingJob =
        createJob(cmmsModelLines = listOf(CMMS_MODEL_LINE, CMMS_MODEL_LINE_2))
      val job2: VidLabelingJob = createJob(cmmsModelLines = listOf(CMMS_MODEL_LINE))

      // Completing job2 first does not complete ML1, since job1 still covers it.
      val response2: MarkVidLabelingJobSucceededResponse =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = job2.vidLabelingJobResourceId
            etag = job2.etag
          }
        )
      assertThat(response2.hasLastVidLabelingJobResult()).isFalse()

      // Completing job1 completes both ML1 (job1 + job2 done) and ML2 (only job1 covers it).
      val response1: MarkVidLabelingJobSucceededResponse =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = job1.vidLabelingJobResourceId
            etag = job1.etag
          }
        )

      assertThat(response1.hasLastVidLabelingJobResult()).isTrue()
      assertThat(response1.lastVidLabelingJobResult.completedModelLinesList)
        .containsExactly(CMMS_MODEL_LINE, CMMS_MODEL_LINE_2)
    }

  @Test
  fun `markVidLabelingJobSucceeded is idempotent with same request_id`() =
    runBlocking<Unit> {
      val requestId = UUID.randomUUID().toString()
      val created: VidLabelingJob = createJob()

      val response1: MarkVidLabelingJobSucceededResponse =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
            this.requestId = requestId
          }
        )

      val response2: MarkVidLabelingJobSucceededResponse =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
            this.requestId = requestId
          }
        )

      assertThat(response2).isEqualTo(response1)
    }

  @Test
  fun `markVidLabelingJobSucceeded throws ABORTED for etag mismatch`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobSucceeded(
            markVidLabelingJobSucceededRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = created.vidLabelingJobResourceId
              etag = "wrong-etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    }

  @Test
  fun `markVidLabelingJobSucceeded throws FAILED_PRECONDITION from SUCCEEDED state`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val response =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobSucceeded(
            markVidLabelingJobSucceededRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = created.vidLabelingJobResourceId
              etag = response.vidLabelingJob.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markVidLabelingJobSucceeded throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobSucceeded(
            markVidLabelingJobSucceededRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = "nonexistent-job"
              etag = "some-etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `markVidLabelingJobFailed transitions from CREATED to FAILED`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val job: VidLabelingJob =
        service.markVidLabelingJobFailed(
          markVidLabelingJobFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
            errorMessage = "something went wrong"
          }
        )

      assertThat(job.state).isEqualTo(VidLabelingState.VID_LABELING_STATE_FAILED)
      assertThat(job.errorMessage).isEqualTo("something went wrong")
    }

  @Test
  fun `markVidLabelingJobFailed is idempotent with same request_id`() =
    runBlocking<Unit> {
      val requestId = UUID.randomUUID().toString()
      val created: VidLabelingJob = createJob()

      val failed1: VidLabelingJob =
        service.markVidLabelingJobFailed(
          markVidLabelingJobFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
            errorMessage = "boom"
            this.requestId = requestId
          }
        )

      val failed2: VidLabelingJob =
        service.markVidLabelingJobFailed(
          markVidLabelingJobFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
            errorMessage = "boom"
            this.requestId = requestId
          }
        )

      assertThat(failed2).isEqualTo(failed1)
    }

  @Test
  fun `markVidLabelingJobFailed throws ABORTED for etag mismatch`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobFailed(
            markVidLabelingJobFailedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = created.vidLabelingJobResourceId
              etag = "wrong-etag"
              errorMessage = "something went wrong"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    }

  @Test
  fun `markVidLabelingJobFailed throws FAILED_PRECONDITION from SUCCEEDED state`() =
    runBlocking<Unit> {
      val created: VidLabelingJob = createJob()

      val response =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            vidLabelingJobResourceId = created.vidLabelingJobResourceId
            etag = created.etag
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobFailed(
            markVidLabelingJobFailedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = created.vidLabelingJobResourceId
              etag = response.vidLabelingJob.etag
              errorMessage = "should not work"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `createVidLabelingJob throws NOT_FOUND when parent upload does not exist`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createVidLabelingJob(
            createVidLabelingJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = "nonexistent-upload"
              vidLabelingJob = vidLabelingJob {
                cmmsModelLines += CMMS_MODEL_LINE
                rawImpressionUploadFiles += FILE_1
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchCreateVidLabelingJobs throws NOT_FOUND when parent upload does not exist`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateVidLabelingJobs(
            batchCreateVidLabelingJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = "nonexistent-upload"
              requests += createVidLabelingJobRequest {
                vidLabelingJob = vidLabelingJob {
                  cmmsModelLines += CMMS_MODEL_LINE
                  rawImpressionUploadFiles += FILE_1
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `batchCreateVidLabelingJobs is idempotent with same request_ids`() =
    runBlocking<Unit> {
      val requestId1 = UUID.randomUUID().toString()
      val requestId2 = UUID.randomUUID().toString()
      val request = batchCreateVidLabelingJobsRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        requests += createVidLabelingJobRequest {
          vidLabelingJob = vidLabelingJob {
            cmmsModelLines += CMMS_MODEL_LINE
            rawImpressionUploadFiles += FILE_1
          }
          requestId = requestId1
        }
        requests += createVidLabelingJobRequest {
          vidLabelingJob = vidLabelingJob {
            cmmsModelLines += CMMS_MODEL_LINE_2
            rawImpressionUploadFiles += FILE_2
          }
          requestId = requestId2
        }
      }

      val first = service.batchCreateVidLabelingJobs(request)
      val second = service.batchCreateVidLabelingJobs(request)

      assertThat(second).isEqualTo(first)
    }

  @Test
  fun `batchCreateVidLabelingJobs throws INVALID_ARGUMENT when too many requests`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateVidLabelingJobs(
            batchCreateVidLabelingJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              repeat(MAX_BATCH_SIZE + 1) {
                requests += createVidLabelingJobRequest {
                  vidLabelingJob = vidLabelingJob {
                    cmmsModelLines += CMMS_MODEL_LINE
                    rawImpressionUploadFiles += FILE_1
                  }
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreateVidLabelingJobs throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateVidLabelingJobs(
            batchCreateVidLabelingJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += createVidLabelingJobRequest {
                vidLabelingJob = vidLabelingJob {
                  cmmsModelLines += CMMS_MODEL_LINE
                  rawImpressionUploadFiles += FILE_1
                }
                requestId = "not-a-valid-uuid"
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markVidLabelingJobFailed throws NOT_FOUND when not found`() =
    runBlocking<Unit> {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobFailed(
            markVidLabelingJobFailedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              vidLabelingJobResourceId = "nonexistent-job"
              etag = "some-etag"
              errorMessage = "boom"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  companion object {
    private const val MAX_BATCH_SIZE = 50
    private const val DATA_PROVIDER_RESOURCE_ID = "dp-1"
    private const val RAW_IMPRESSION_UPLOAD_RESOURCE_ID = "upload-1"
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private const val FILE_1 = "file-1"
    private const val FILE_2 = "file-2"
  }
}
