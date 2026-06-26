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
import com.google.type.date
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
import org.wfanet.measurement.edpaggregator.v1alpha.encryptedDek
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListPoolAssignmentJobsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkPoolAssignmentJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJob
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentState
import org.wfanet.measurement.internal.edpaggregator.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.createPoolAssignmentJobRequest
import org.wfanet.measurement.internal.edpaggregator.getPoolAssignmentJobRequest
import org.wfanet.measurement.internal.edpaggregator.listPoolAssignmentJobsRequest
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.poolAssignmentJob

@RunWith(JUnit4::class)
abstract class PoolAssignmentJobServiceTest {
  private lateinit var service: PoolAssignmentJobServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): PoolAssignmentJobServiceCoroutineImplBase

  /**
   * Creates a parent [RawImpressionUpload] row so that child pool assignment job rows can be
   * inserted (interleaved table).
   */
  protected abstract suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  )

  /**
   * Creates a parent [RawImpressionUploadModelLine] row (empty pool offsets, no max event date)
   * for the given (upload, model line). MarkSucceeded requires this parent row so it can merge
   * each shard's pool offsets / max event date into it.
   */
  protected abstract suspend fun createRawImpressionUploadModelLine(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
    cmmsModelLine: String,
  )

  @Before
  fun initService() {
    service = newService()
    runBlocking {
      createParentUpload(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
      createRawImpressionUploadModelLine(
        DATA_PROVIDER_RESOURCE_ID,
        RAW_IMPRESSION_UPLOAD_RESOURCE_ID,
        CMMS_MODEL_LINE,
      )
    }
  }

  @Test
  fun `createPoolAssignmentJob creates successfully`() = runBlocking {
    val startTime: Instant = Instant.now()

    val job: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    assertThat(job.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(job.rawImpressionUploadResourceId).isEqualTo(RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
    assertThat(job.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(job.shardIndex).isEqualTo(0)
    assertThat(job.state).isEqualTo(PoolAssignmentState.POOL_ASSIGNMENT_STATE_CREATED)
    assertThat(job.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(job.updateTime).isEqualTo(job.createTime)
    assertThat(job.etag).isNotEmpty()
  }

  @Test
  fun `createPoolAssignmentJob is idempotent with same request_id`() = runBlocking {
    val requestId: String = UUID.randomUUID().toString()

    val job1: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          this.requestId = requestId
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val job2: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          this.requestId = requestId
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    assertThat(job2).isEqualTo(job1)
  }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(
            createPoolAssignmentJobRequest {
              poolAssignmentJob = poolAssignmentJob {
                rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                cmmsModelLine = CMMS_MODEL_LINE
                shardIndex = 0
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
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "pool_assignment_job.data_provider_resource_id"
          }
        )
    }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT if raw_impression_upload_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(
            createPoolAssignmentJobRequest {
              poolAssignmentJob = poolAssignmentJob {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                cmmsModelLine = CMMS_MODEL_LINE
                shardIndex = 0
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
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "pool_assignment_job.raw_impression_upload_resource_id"
          }
        )
    }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT if pool_assignment_job not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(createPoolAssignmentJobRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "pool_assignment_job"
          }
        )
    }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT if cmms_model_line not set`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              shardIndex = 0
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "pool_assignment_job.cmms_model_line"
        }
      )
  }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT for malformed request_id`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            requestId = "not-a-valid-uuid"
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
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
  fun `batchCreatePoolAssignmentJobs creates multiple`() =
    runBlocking<Unit> {
      val response =
        service.batchCreatePoolAssignmentJobs(
          batchCreatePoolAssignmentJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createPoolAssignmentJobRequest {
              poolAssignmentJob = poolAssignmentJob {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                cmmsModelLine = CMMS_MODEL_LINE
                shardIndex = 0
              }
            }
            requests += createPoolAssignmentJobRequest {
              poolAssignmentJob = poolAssignmentJob {
                dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                cmmsModelLine = CMMS_MODEL_LINE
                shardIndex = 1
              }
            }
          }
        )

      assertThat(response.poolAssignmentJobsList).hasSize(2)
      assertThat(response.poolAssignmentJobsList.map { it.shardIndex }).containsExactly(0, 1)
    }

  @Test
  fun `getPoolAssignmentJob returns existing resource`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val job: PoolAssignmentJob =
      service.getPoolAssignmentJob(
        getPoolAssignmentJobRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
        }
      )

    assertThat(job).isEqualTo(created)
  }

  @Test
  fun `getPoolAssignmentJob throws NOT_FOUND when not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getPoolAssignmentJob(
          getPoolAssignmentJobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = "nonexistent-job"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getPoolAssignmentJob throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getPoolAssignmentJob(
            getPoolAssignmentJobRequest {
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              poolAssignmentJobResourceId = "some-job"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getPoolAssignmentJob throws INVALID_ARGUMENT if pool_assignment_job_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getPoolAssignmentJob(
            getPoolAssignmentJobRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listPoolAssignmentJobs returns resources`() = runBlocking {
    service.createPoolAssignmentJob(
      createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 0
        }
      }
    )
    service.createPoolAssignmentJob(
      createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 1
        }
      }
    )

    val response: ListPoolAssignmentJobsResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        }
      )

    assertThat(response.poolAssignmentJobsList).hasSize(2)
  }

  @Test
  fun `listPoolAssignmentJobs respects page_size`() = runBlocking {
    for (i in 0..2) {
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = i
          }
        }
      )
    }

    val response: ListPoolAssignmentJobsResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response.poolAssignmentJobsList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listPoolAssignmentJobs filters by state_in`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )
    service.createPoolAssignmentJob(
      createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 1
        }
      }
    )

    service.markPoolAssignmentJobSucceeded(
      markPoolAssignmentJobSucceededRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
        etag = created.etag
      }
    )

    val response: ListPoolAssignmentJobsResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter =
            ListPoolAssignmentJobsRequestKt.filter {
              stateIn += PoolAssignmentState.POOL_ASSIGNMENT_STATE_SUCCEEDED
            }
        }
      )

    assertThat(response.poolAssignmentJobsList).hasSize(1)
    assertThat(response.poolAssignmentJobsList.first().poolAssignmentJobResourceId)
      .isEqualTo(created.poolAssignmentJobResourceId)
  }

  @Test
  fun `listPoolAssignmentJobs filters by cmms_model_line`() = runBlocking {
    service.createPoolAssignmentJob(
      createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 0
        }
      }
    )
    service.createPoolAssignmentJob(
      createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE_2
          shardIndex = 0
        }
      }
    )

    val response: ListPoolAssignmentJobsResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter = ListPoolAssignmentJobsRequestKt.filter { cmmsModelLine = CMMS_MODEL_LINE }
        }
      )

    assertThat(response.poolAssignmentJobsList).hasSize(1)
    assertThat(response.poolAssignmentJobsList.first().cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
  }

  @Test
  fun `listPoolAssignmentJobs pagination with page_token`() = runBlocking {
    for (i in 0..2) {
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = i
          }
        }
      )
    }

    val firstPage: ListPoolAssignmentJobsResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(firstPage.poolAssignmentJobsList).hasSize(2)
    assertThat(firstPage.hasNextPageToken()).isTrue()

    val secondPage: ListPoolAssignmentJobsResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
          pageToken = firstPage.nextPageToken
        }
      )

    assertThat(secondPage.poolAssignmentJobsList).hasSize(1)
    assertThat(secondPage.hasNextPageToken()).isFalse()
  }

  @Test
  fun `markPoolAssignmentJobSucceeded transitions CREATED to SUCCEEDED`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val response: MarkPoolAssignmentJobSucceededResponse =
      service.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = created.etag
        }
      )

    assertThat(response.poolAssignmentJob.state)
      .isEqualTo(PoolAssignmentState.POOL_ASSIGNMENT_STATE_SUCCEEDED)
  }

  @Test
  fun `markPoolAssignmentJobSucceeded is idempotent with same request_id`() = runBlocking {
    val requestId = UUID.randomUUID().toString()
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val response1: MarkPoolAssignmentJobSucceededResponse =
      service.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = created.etag
          this.requestId = requestId
        }
      )

    val response2: MarkPoolAssignmentJobSucceededResponse =
      service.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = created.etag
          this.requestId = requestId
        }
      )

    assertThat(response2.poolAssignmentJob).isEqualTo(response1.poolAssignmentJob)
  }

  @Test
  fun `markPoolAssignmentJobSucceeded throws ABORTED for etag mismatch`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
            etag = "wrong-etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markPoolAssignmentJobSucceeded throws FAILED_PRECONDITION from SUCCEEDED state`() =
    runBlocking {
      val created: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )

      val response =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
            etag = created.etag
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markPoolAssignmentJobSucceeded(
            markPoolAssignmentJobSucceededRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
              etag = response.poolAssignmentJob.etag
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markPoolAssignmentJobFailed transitions from CREATED to FAILED`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val job: PoolAssignmentJob =
      service.markPoolAssignmentJobFailed(
        markPoolAssignmentJobFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = created.etag
          errorMessage = "something went wrong"
        }
      )

    assertThat(job.state).isEqualTo(PoolAssignmentState.POOL_ASSIGNMENT_STATE_FAILED)
    assertThat(job.errorMessage).isEqualTo("something went wrong")
  }

  @Test
  fun `markPoolAssignmentJobFailed throws ABORTED for etag mismatch`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markPoolAssignmentJobFailed(
          markPoolAssignmentJobFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
            etag = "wrong-etag"
            errorMessage = "something went wrong"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markPoolAssignmentJobFailed throws FAILED_PRECONDITION from SUCCEEDED state`() =
    runBlocking {
      val created: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )

      val response =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
            etag = created.etag
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markPoolAssignmentJobFailed(
            markPoolAssignmentJobFailedRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
              etag = response.poolAssignmentJob.etag
              errorMessage = "should not work"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markPoolAssignmentJobSucceeded throws NOT_FOUND when not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = "nonexistent-job"
            etag = "some-etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `markPoolAssignmentJobSucceeded persists encrypted_dek`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val response: MarkPoolAssignmentJobSucceededResponse =
      service.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = created.etag
          encryptedDek = ENCRYPTED_DEK
        }
      )

    assertThat(response.poolAssignmentJob.encryptedDek).isEqualTo(ENCRYPTED_DEK)

    val fetched: PoolAssignmentJob =
      service.getPoolAssignmentJob(
        getPoolAssignmentJobRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
        }
      )
    assertThat(fetched.encryptedDek).isEqualTo(ENCRYPTED_DEK)
  }

  @Test
  fun `markPoolAssignmentJobSucceeded returns last_shard_result only on the last shard`() =
    runBlocking {
      val shard0: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )
      val shard1: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 1
            }
          }
        )

      val firstResponse: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = shard0.poolAssignmentJobResourceId
            etag = shard0.etag
          }
        )
      assertThat(firstResponse.hasLastShardResult()).isFalse()

      val lastResponse: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = shard1.poolAssignmentJobResourceId
            etag = shard1.etag
          }
        )
      assertThat(lastResponse.hasLastShardResult()).isTrue()
    }

  @Test
  fun `markPoolAssignmentJobSucceeded last_shard_result carries merged pool offsets and max date`() =
    runBlocking {
      val shard: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )

      val response: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = shard.poolAssignmentJobResourceId
            etag = shard.etag
            poolOffsets += POOL_OFFSETS
            maxEventDate = MAX_EVENT_DATE
          }
        )

      assertThat(response.hasLastShardResult()).isTrue()
      assertThat(response.lastShardResult.poolOffsetsList)
        .containsExactlyElementsIn(POOL_OFFSETS)
        .inOrder()
      assertThat(response.lastShardResult.maxEventDate).isEqualTo(MAX_EVENT_DATE)
    }

  @Test
  fun `markPoolAssignmentJobSucceeded replay of last shard returns last_shard_result`() =
    runBlocking {
      val requestId: String = UUID.randomUUID().toString()
      val shard: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )

      val first: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = shard.poolAssignmentJobResourceId
            etag = shard.etag
            this.requestId = requestId
            poolOffsets += POOL_OFFSETS
            maxEventDate = MAX_EVENT_DATE
          }
        )
      assertThat(first.hasLastShardResult()).isTrue()

      val replay: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = shard.poolAssignmentJobResourceId
            etag = shard.etag
            this.requestId = requestId
          }
        )

      assertThat(replay.poolAssignmentJob).isEqualTo(first.poolAssignmentJob)
      assertThat(replay.hasLastShardResult()).isTrue()
      assertThat(replay.lastShardResult.poolOffsetsList)
        .containsExactlyElementsIn(POOL_OFFSETS)
        .inOrder()
      assertThat(replay.lastShardResult.maxEventDate).isEqualTo(MAX_EVENT_DATE)
    }

  @Test
  fun `markPoolAssignmentJobSucceeded scopes last shard detection per model line`() =
    runBlocking {
      createRawImpressionUploadModelLine(
        DATA_PROVIDER_RESOURCE_ID,
        RAW_IMPRESSION_UPLOAD_RESOURCE_ID,
        CMMS_MODEL_LINE_2,
      )
      // Model line A has two shards under the upload.
      val modelLineAShard0: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )
      val modelLineAShard1: PoolAssignmentJob =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            poolAssignmentJob = poolAssignmentJob {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 1
            }
          }
        )
      // Model line B has a single shard under the same upload that stays pending for the
      // whole test, so it can neither trigger nor suppress model line A's last-shard result.
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE_2
            shardIndex = 0
          }
        }
      )

      // Marking A's first shard is NOT the last shard for A (A still has shard 1 pending);
      // model line B's pending shard must not spuriously trigger a last-shard result.
      val firstResponse: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = modelLineAShard0.poolAssignmentJobResourceId
            etag = modelLineAShard0.etag
          }
        )
      assertThat(firstResponse.hasLastShardResult()).isFalse()

      // Marking A's second shard IS A's last shard; model line B's still-pending shard under
      // the same upload must not suppress A's last-shard result.
      val lastResponse: MarkPoolAssignmentJobSucceededResponse =
        service.markPoolAssignmentJobSucceeded(
          markPoolAssignmentJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            poolAssignmentJobResourceId = modelLineAShard1.poolAssignmentJobResourceId
            etag = modelLineAShard1.etag
            poolOffsets += POOL_OFFSETS
          }
        )

      assertThat(lastResponse.hasLastShardResult()).isTrue()
      assertThat(lastResponse.lastShardResult.poolOffsetsList)
        .containsExactlyElementsIn(POOL_OFFSETS)
        .inOrder()
    }

  @Test
  fun `markPoolAssignmentJobSucceeded clears error_message on FAILED to SUCCEEDED`() = runBlocking {
    val created: PoolAssignmentJob =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val failed: PoolAssignmentJob =
      service.markPoolAssignmentJobFailed(
        markPoolAssignmentJobFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = created.etag
          errorMessage = "boom"
        }
      )
    assertThat(failed.errorMessage).isEqualTo("boom")

    val succeeded: MarkPoolAssignmentJobSucceededResponse =
      service.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          poolAssignmentJobResourceId = created.poolAssignmentJobResourceId
          etag = failed.etag
        }
      )

    assertThat(succeeded.poolAssignmentJob.state)
      .isEqualTo(PoolAssignmentState.POOL_ASSIGNMENT_STATE_SUCCEEDED)
    assertThat(succeeded.poolAssignmentJob.errorMessage).isEmpty()
  }

  @Test
  fun `batchCreatePoolAssignmentJobs is idempotent with same request_id`() =
    runBlocking<Unit> {
      val request = batchCreatePoolAssignmentJobsRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        requests += createPoolAssignmentJobRequest {
          requestId = UUID.randomUUID().toString()
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
        requests += createPoolAssignmentJobRequest {
          requestId = UUID.randomUUID().toString()
          poolAssignmentJob = poolAssignmentJob {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 1
          }
        }
      }

      val first = service.batchCreatePoolAssignmentJobs(request)
      val second = service.batchCreatePoolAssignmentJobs(request)

      assertThat(second.poolAssignmentJobsList).isEqualTo(first.poolAssignmentJobsList)
    }

  @Test
  fun `batchCreatePoolAssignmentJobs throws INVALID_ARGUMENT for duplicate model_line and shard`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreatePoolAssignmentJobs(
            batchCreatePoolAssignmentJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += createPoolAssignmentJobRequest {
                poolAssignmentJob = poolAssignmentJob {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                  cmmsModelLine = CMMS_MODEL_LINE
                  shardIndex = 0
                }
              }
              requests += createPoolAssignmentJobRequest {
                poolAssignmentJob = poolAssignmentJob {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                  cmmsModelLine = CMMS_MODEL_LINE
                  shardIndex = 0
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreatePoolAssignmentJobs throws INVALID_ARGUMENT when over max batch size`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreatePoolAssignmentJobs(
            batchCreatePoolAssignmentJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              for (i in 0..MAX_BATCH_SIZE) {
                requests += createPoolAssignmentJobRequest {
                  poolAssignmentJob = poolAssignmentJob {
                    dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                    rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                    cmmsModelLine = CMMS_MODEL_LINE
                    shardIndex = i
                  }
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreatePoolAssignmentJobs throws INVALID_ARGUMENT for malformed element request_id`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreatePoolAssignmentJobs(
            batchCreatePoolAssignmentJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += createPoolAssignmentJobRequest {
                requestId = "not-a-valid-uuid"
                poolAssignmentJob = poolAssignmentJob {
                  dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
                  rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
                  cmmsModelLine = CMMS_MODEL_LINE
                  shardIndex = 0
                }
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  companion object {
    private const val MAX_BATCH_SIZE = 50
    private const val DATA_PROVIDER_RESOURCE_ID = "dataProviders/dp1"
    private const val RAW_IMPRESSION_UPLOAD_RESOURCE_ID = "uploads/upload1"
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private val POOL_OFFSETS = listOf(0L, 4L, 8L)
    private val MAX_EVENT_DATE = date {
      year = 2026
      month = 6
      day = 25
    }
    private val ENCRYPTED_DEK = encryptedDek {
      kekUri = "gcp-kms://projects/test/locations/us/keyRings/r/cryptoKeys/k"
    }
  }
}
