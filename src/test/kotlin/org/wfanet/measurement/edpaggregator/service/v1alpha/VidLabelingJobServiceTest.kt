// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import java.util.UUID
import kotlin.coroutines.EmptyCoroutineContext
import kotlin.test.assertFailsWith
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerVidLabelingJobService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.VidLabelingJobKey
import org.wfanet.measurement.edpaggregator.v1alpha.VidLabelingJob
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getVidLabelingJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listVidLabelingJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markVidLabelingJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.vidLabelingJob
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineImplBase as InternalVidLabelingJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.VidLabelingJobServiceGrpcKt.VidLabelingJobServiceCoroutineStub as InternalVidLabelingJobServiceCoroutineStub

@RunWith(JUnit4::class)
class VidLabelingJobServiceTest {
  private lateinit var internalService: InternalVidLabelingJobServiceCoroutineImplBase
  private lateinit var service: VidLabelingJobService

  private var nextUploadId: Long = 1L

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService = SpannerVidLabelingJobService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      VidLabelingJobService(InternalVidLabelingJobServiceCoroutineStub(grpcTestServerRule.channel))
  }

  private suspend fun createParentUpload(
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

  @Test
  fun `createVidLabelingJob returns a job successfully`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val startTime = Instant.now()
      val request = createVidLabelingJobRequest {
        parent = UPLOAD_KEY.toName()
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += CMMS_MODEL_LINE
          rawImpressionUploadFiles += FILE_1
        }
        requestId = REQUEST_ID
      }

      val job = service.createVidLabelingJob(request)

      val jobKey = assertNotNull(VidLabelingJobKey.fromName(job.name))
      assertThat(jobKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
      assertThat(jobKey.rawImpressionUploadId).isEqualTo(RAW_IMPRESSION_UPLOAD_ID)
      assertThat(job.state).isEqualTo(VidLabelingJob.State.CREATED)
      assertThat(job.cmmsModelLinesList).containsExactly(CMMS_MODEL_LINE)
      assertThat(job.rawImpressionUploadFilesList).containsExactly(FILE_1)
      assertThat(job.createTime.toInstant()).isGreaterThan(startTime)
      assertThat(job.updateTime).isEqualTo(job.createTime)
      assertThat(job.etag).isNotEmpty()
    }

  @Test
  fun `createVidLabelingJob with requestId is idempotent`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val request = createVidLabelingJobRequest {
        parent = UPLOAD_KEY.toName()
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += CMMS_MODEL_LINE
          rawImpressionUploadFiles += FILE_1
        }
        requestId = REQUEST_ID
      }
      val existing = service.createVidLabelingJob(request)

      val duplicate = service.createVidLabelingJob(request)

      assertThat(duplicate).isEqualTo(existing)
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT for empty parent`() =
    runBlocking<Unit> {
      val request = createVidLabelingJobRequest {
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += CMMS_MODEL_LINE
          rawImpressionUploadFiles += FILE_1
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking<Unit> {
      val request = createVidLabelingJobRequest {
        parent = "invalid-parent"
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += CMMS_MODEL_LINE
          rawImpressionUploadFiles += FILE_1
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws INVALID_ARGUMENT for missing vid_labeling_job`() =
    runBlocking<Unit> {
      val request = createVidLabelingJobRequest { parent = UPLOAD_KEY.toName() }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
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
  fun `createVidLabelingJob throws INVALID_ARGUMENT for empty cmms_model_lines`() =
    runBlocking<Unit> {
      val request = createVidLabelingJobRequest {
        parent = UPLOAD_KEY.toName()
        vidLabelingJob = vidLabelingJob { rawImpressionUploadFiles += FILE_1 }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
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
  fun `createVidLabelingJob throws INVALID_ARGUMENT for empty raw_impression_upload_files`() =
    runBlocking {
      val request = createVidLabelingJobRequest {
        parent = UPLOAD_KEY.toName()
        vidLabelingJob = vidLabelingJob { cmmsModelLines += CMMS_MODEL_LINE }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
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
  fun `createVidLabelingJob throws INVALID_ARGUMENT for malformed requestId`() =
    runBlocking<Unit> {
      val request = createVidLabelingJobRequest {
        parent = UPLOAD_KEY.toName()
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += CMMS_MODEL_LINE
          rawImpressionUploadFiles += FILE_1
        }
        requestId = "invalid-request-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
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
  fun `batchCreateVidLabelingJobs returns jobs successfully`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val request = batchCreateVidLabelingJobsRequest {
        parent = UPLOAD_KEY.toName()
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

      val response = service.batchCreateVidLabelingJobs(request)

      assertThat(response.vidLabelingJobsList).hasSize(2)
      assertThat(response.vidLabelingJobsList.flatMap { it.cmmsModelLinesList })
        .containsExactly(CMMS_MODEL_LINE, CMMS_MODEL_LINE_2)
    }

  @Test
  fun `batchCreateVidLabelingJobs throws INVALID_ARGUMENT for empty parent`() =
    runBlocking<Unit> {
      val request = batchCreateVidLabelingJobsRequest {
        requests += createVidLabelingJobRequest {
          vidLabelingJob = vidLabelingJob {
            cmmsModelLines += CMMS_MODEL_LINE
            rawImpressionUploadFiles += FILE_1
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.batchCreateVidLabelingJobs(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
          }
        )
    }

  @Test
  fun `batchCreateVidLabelingJobs throws INVALID_ARGUMENT for empty requests`() =
    runBlocking<Unit> {
      val request = batchCreateVidLabelingJobsRequest { parent = UPLOAD_KEY.toName() }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.batchCreateVidLabelingJobs(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests"
          }
        )
    }

  @Test
  fun `getVidLabelingJob returns a job`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
            requestId = REQUEST_ID
          }
        )

      val job = service.getVidLabelingJob(getVidLabelingJobRequest { name = created.name })

      assertThat(job).isEqualTo(created)
    }

  @Test
  fun `getVidLabelingJob throws INVALID_ARGUMENT for empty name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getVidLabelingJob(getVidLabelingJobRequest {})
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `getVidLabelingJob throws INVALID_ARGUMENT for malformed name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getVidLabelingJob(getVidLabelingJobRequest { name = "invalid-name" })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `getVidLabelingJob throws NOT_FOUND for missing job`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val name = VidLabelingJobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "missing").toName()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getVidLabelingJob(getVidLabelingJobRequest { this.name = name })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `listVidLabelingJobs returns jobs`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
          }
        )

      val response =
        service.listVidLabelingJobs(listVidLabelingJobsRequest { parent = UPLOAD_KEY.toName() })

      assertThat(response.vidLabelingJobsList).containsExactly(created)
    }

  @Test
  fun `listVidLabelingJobs supports wildcard upload`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
          }
        )

      val wildcardParent = RawImpressionUploadKey(DATA_PROVIDER_ID, "-").toName()
      val response =
        service.listVidLabelingJobs(listVidLabelingJobsRequest { parent = wildcardParent })

      assertThat(response.vidLabelingJobsList).containsExactly(created)
    }

  @Test
  fun `listVidLabelingJobs respects page size and pagination`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created1 =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
          }
        )
      val created2 =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE_2
              rawImpressionUploadFiles += FILE_2
            }
          }
        )
      val sortedCreated = listOf(created1, created2).sortedBy { it.name }

      val firstResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = UPLOAD_KEY.toName()
            pageSize = 1
          }
        )

      assertThat(firstResponse.vidLabelingJobsList).hasSize(1)
      assertThat(firstResponse.nextPageToken).isNotEmpty()

      val secondResponse =
        service.listVidLabelingJobs(
          listVidLabelingJobsRequest {
            parent = UPLOAD_KEY.toName()
            pageSize = 1
            pageToken = firstResponse.nextPageToken
          }
        )

      assertThat(secondResponse.vidLabelingJobsList).hasSize(1)
      assertThat(
          (firstResponse.vidLabelingJobsList + secondResponse.vidLabelingJobsList).sortedBy {
            it.name
          }
        )
        .isEqualTo(sortedCreated)
    }

  @Test
  fun `listVidLabelingJobs throws INVALID_ARGUMENT for empty parent`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listVidLabelingJobs(listVidLabelingJobsRequest {})
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
          }
        )
    }

  @Test
  fun `listVidLabelingJobs throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listVidLabelingJobs(listVidLabelingJobsRequest { parent = "invalid-parent" })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
          }
        )
    }

  @Test
  fun `listVidLabelingJobs throws INVALID_ARGUMENT for negative page size`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = UPLOAD_KEY.toName()
              pageSize = -1
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
          }
        )
    }

  @Test
  fun `listVidLabelingJobs throws INVALID_ARGUMENT for malformed page token`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listVidLabelingJobs(
            listVidLabelingJobsRequest {
              parent = UPLOAD_KEY.toName()
              pageToken = "invalid-token"
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
          }
        )
    }

  @Test
  fun `markVidLabelingJobSucceeded transitions state`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
          }
        )

      val response =
        service.markVidLabelingJobSucceeded(
          markVidLabelingJobSucceededRequest {
            name = created.name
            etag = created.etag
          }
        )

      assertThat(response.vidLabelingJob.state).isEqualTo(VidLabelingJob.State.SUCCEEDED)
      assertThat(response.vidLabelingJob.name).isEqualTo(created.name)
      // The only job covering its model line, so it is the last job out.
      assertThat(response.hasLastVidLabelingJobResult()).isTrue()
      assertThat(response.lastVidLabelingJobResult.completedModelLinesList)
        .containsExactly(CMMS_MODEL_LINE)
    }

  @Test
  fun `markVidLabelingJobSucceeded throws INVALID_ARGUMENT for empty name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobSucceeded(
            markVidLabelingJobSucceededRequest { etag = "some-etag" }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `markVidLabelingJobSucceeded throws INVALID_ARGUMENT for empty etag`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobSucceeded(
            markVidLabelingJobSucceededRequest { name = created.name }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "etag"
          }
        )
    }

  @Test
  fun `markVidLabelingJobFailed transitions state`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createVidLabelingJob(
          createVidLabelingJobRequest {
            parent = UPLOAD_KEY.toName()
            vidLabelingJob = vidLabelingJob {
              cmmsModelLines += CMMS_MODEL_LINE
              rawImpressionUploadFiles += FILE_1
            }
          }
        )

      val failed =
        service.markVidLabelingJobFailed(
          markVidLabelingJobFailedRequest {
            name = created.name
            etag = created.etag
            errorMessage = "Something went wrong"
          }
        )

      assertThat(failed.state).isEqualTo(VidLabelingJob.State.FAILED)
      assertThat(failed.name).isEqualTo(created.name)
      assertThat(failed.errorMessage).isEqualTo("Something went wrong")
    }

  @Test
  fun `markVidLabelingJobFailed throws INVALID_ARGUMENT for empty name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobFailed(markVidLabelingJobFailedRequest { etag = "some-etag" })
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markVidLabelingJobFailed throws INVALID_ARGUMENT for malformed name`() =
    runBlocking<Unit> {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markVidLabelingJobFailed(
            markVidLabelingJobFailedRequest {
              name = "invalid-name"
              etag = "some-etag"
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `createVidLabelingJob throws NOT_FOUND when parent upload does not exist`() =
    runBlocking<Unit> {
      val request = createVidLabelingJobRequest {
        parent = UPLOAD_KEY.toName()
        vidLabelingJob = vidLabelingJob {
          cmmsModelLines += CMMS_MODEL_LINE
          rawImpressionUploadFiles += FILE_1
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.createVidLabelingJob(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private const val RAW_IMPRESSION_UPLOAD_ID = "upload-1"
    private val UPLOAD_KEY = RawImpressionUploadKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private const val FILE_1 = "file-1"
    private const val FILE_2 = "file-2"
    private val REQUEST_ID = UUID.randomUUID().toString()
  }
}
