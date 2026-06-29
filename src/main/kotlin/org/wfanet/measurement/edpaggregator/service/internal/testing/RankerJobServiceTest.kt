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

package org.wfanet.measurement.edpaggregator.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import com.google.type.interval
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
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRankerJobsResponse
import org.wfanet.measurement.internal.edpaggregator.MarkRankerJobSucceededResponse
import org.wfanet.measurement.internal.edpaggregator.RankerJob
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RankerState
import org.wfanet.measurement.internal.edpaggregator.batchCreateRankerJobsRequest
import org.wfanet.measurement.internal.edpaggregator.createRankerJobRequest
import org.wfanet.measurement.internal.edpaggregator.getRankerJobRequest
import org.wfanet.measurement.internal.edpaggregator.listRankerJobsRequest
import org.wfanet.measurement.internal.edpaggregator.markRankerJobFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRankerJobSucceededRequest
import org.wfanet.measurement.internal.edpaggregator.rankerJob

@RunWith(JUnit4::class)
abstract class RankerJobServiceTest {
  private lateinit var service: RankerJobServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RankerJobServiceCoroutineImplBase

  /**
   * Creates a parent [RawImpressionUpload] row so that child ranker job rows can be inserted
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

  @Test
  fun `createRankerJob creates successfully`() = runBlocking {
    val startTime: Instant = Instant.now()

    val job: RankerJob =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    assertThat(job.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(job.rawImpressionUploadResourceId).isEqualTo(RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
    assertThat(job.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(job.poolOffsetsList).containsExactlyElementsIn(POOL_OFFSETS).inOrder()
    assertThat(job.state).isEqualTo(RankerState.RANKER_STATE_CREATED)
    assertThat(job.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(job.updateTime).isEqualTo(job.createTime)
    assertThat(job.etag).isNotEmpty()
  }

  @Test
  fun `createRankerJob is idempotent with same request_id`() = runBlocking {
    val requestId: String = UUID.randomUUID().toString()

    val job1: RankerJob =
      service.createRankerJob(
        createRankerJobRequest {
          this.requestId = requestId
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    val job2: RankerJob =
      service.createRankerJob(
        createRankerJobRequest {
          this.requestId = requestId
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    assertThat(job2).isEqualTo(job1)
  }

  @Test
  fun `createRankerJob throws NOT_FOUND when parent upload missing`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRankerJob(
          createRankerJobRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = "uploads/missing"
            rankerJob = rankerJob {
              cmmsModelLine = CMMS_MODEL_LINE
              poolOffsets += POOL_OFFSETS
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `createRankerJob throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRankerJob(
            createRankerJobRequest {
              requestId = UUID.randomUUID().toString()
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rankerJob = rankerJob {
                cmmsModelLine = CMMS_MODEL_LINE
                poolOffsets += POOL_OFFSETS
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
  fun `createRankerJob throws INVALID_ARGUMENT if ranker_job not set`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> { service.createRankerJob(createRankerJobRequest {}) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "ranker_job"
        }
      )
  }

  @Test
  fun `createRankerJob throws INVALID_ARGUMENT if cmms_model_line not set`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRankerJob(
          createRankerJobRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJob = rankerJob { poolOffsets += POOL_OFFSETS }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "ranker_job.cmms_model_line"
        }
      )
  }

  @Test
  fun `createRankerJob throws INVALID_ARGUMENT if pool_offsets not set`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRankerJob(
          createRankerJobRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJob = rankerJob { cmmsModelLine = CMMS_MODEL_LINE }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "ranker_job.pool_offsets"
        }
      )
  }

  @Test
  fun `createRankerJob throws INVALID_ARGUMENT for malformed request_id`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRankerJob(
          createRankerJobRequest {
            requestId = "not-a-valid-uuid"
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJob = rankerJob {
              cmmsModelLine = CMMS_MODEL_LINE
              poolOffsets += POOL_OFFSETS
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
  fun `createRankerJob throws INVALID_ARGUMENT for empty request_id`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.createRankerJob(
          createRankerJobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJob = rankerJob {
              cmmsModelLine = CMMS_MODEL_LINE
              poolOffsets += POOL_OFFSETS
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
        }
      )
  }

  @Test
  fun `batchCreateRankerJobs creates multiple`() =
    runBlocking<Unit> {
      val response =
        service.batchCreateRankerJobs(
          batchCreateRankerJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createRankerJobRequest {
              requestId = UUID.randomUUID().toString()
              rankerJob = rankerJob {
                cmmsModelLine = CMMS_MODEL_LINE
                poolOffsets += listOf(0L, 1L)
              }
            }
            requests += createRankerJobRequest {
              requestId = UUID.randomUUID().toString()
              rankerJob = rankerJob {
                cmmsModelLine = CMMS_MODEL_LINE
                poolOffsets += listOf(2L, 3L)
              }
            }
          }
        )

      assertThat(response.rankerJobsList).hasSize(2)
      assertThat(response.rankerJobsList.flatMap { it.poolOffsetsList })
        .containsExactly(0L, 1L, 2L, 3L)
    }

  @Test
  fun `batchCreateRankerJobs is idempotent on repeated request_ids`() =
    runBlocking<Unit> {
      val requestId = UUID.randomUUID().toString()
      val request = batchCreateRankerJobsRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        requests += createRankerJobRequest {
          this.requestId = requestId
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      }

      val first = service.batchCreateRankerJobs(request)
      val second = service.batchCreateRankerJobs(request)

      assertThat(second.rankerJobsList).isEqualTo(first.rankerJobsList)
    }

  @Test
  fun `getRankerJob returns existing resource`() = runBlocking {
    val created: RankerJob =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    val job: RankerJob =
      service.getRankerJob(
        getRankerJobRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
        }
      )

    assertThat(job).isEqualTo(created)
  }

  @Test
  fun `getRankerJob throws NOT_FOUND when not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRankerJob(
          getRankerJobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = "nonexistent-job"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getRankerJob throws INVALID_ARGUMENT if ranker_job_resource_id not set`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRankerJob(
          getRankerJobRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listRankerJobs returns resources`() = runBlocking {
    createRanker(0L)
    createRanker(1L)

    val response: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        }
      )

    assertThat(response.rankerJobsList).hasSize(2)
  }

  @Test
  fun `listRankerJobs respects page_size`() = runBlocking {
    for (i in 0..2) {
      createRanker(i.toLong())
    }

    val response: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response.rankerJobsList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listRankerJobs filters by state_in`() = runBlocking {
    val created: RankerJob = createRanker(0L)
    createRanker(1L)

    service.markRankerJobSucceeded(
      markRankerJobSucceededRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rankerJobResourceId = created.rankerJobResourceId
        etag = created.etag
        requestId = UUID.randomUUID().toString()
      }
    )

    val response: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter = ListRankerJobsRequestKt.filter { stateIn += RankerState.RANKER_STATE_SUCCEEDED }
        }
      )

    assertThat(response.rankerJobsList).hasSize(1)
    assertThat(response.rankerJobsList.first().rankerJobResourceId)
      .isEqualTo(created.rankerJobResourceId)
  }

  @Test
  fun `listRankerJobs filters by cmms_model_line`() = runBlocking {
    createRanker(0L, cmmsModelLine = CMMS_MODEL_LINE)
    createRanker(1L, cmmsModelLine = CMMS_MODEL_LINE_2)

    val response: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter = ListRankerJobsRequestKt.filter { cmmsModelLine = CMMS_MODEL_LINE }
        }
      )

    assertThat(response.rankerJobsList).hasSize(1)
    assertThat(response.rankerJobsList.first().cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
  }

  @Test
  fun `listRankerJobs paginates with page_token`() = runBlocking {
    for (i in 0..2) {
      createRanker(i.toLong())
    }

    val firstPage: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(firstPage.rankerJobsList).hasSize(2)
    assertThat(firstPage.hasNextPageToken()).isTrue()

    val secondPage: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
          pageToken = firstPage.nextPageToken
        }
      )

    assertThat(secondPage.rankerJobsList).hasSize(1)
    assertThat(secondPage.hasNextPageToken()).isFalse()
  }

  @Test
  fun `markRankerJobSucceeded transitions CREATED to SUCCEEDED`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.rankerJob.state).isEqualTo(RankerState.RANKER_STATE_SUCCEEDED)
  }

  @Test
  fun `markRankerJobSucceeded sets is_last_job true when it is the only job`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.isLastJob).isTrue()
  }

  @Test
  fun `markRankerJobSucceeded sets is_last_job false when sibling still pending`() = runBlocking {
    val created1: RankerJob = createRanker(0L)
    createRanker(1L)

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created1.rankerJobResourceId
          etag = created1.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.isLastJob).isFalse()
  }

  @Test
  fun `markRankerJobSucceeded sets is_last_job true on the last sibling`() = runBlocking {
    val created1: RankerJob = createRanker(0L)
    val created2: RankerJob = createRanker(1L)

    service.markRankerJobSucceeded(
      markRankerJobSucceededRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rankerJobResourceId = created1.rankerJobResourceId
        etag = created1.etag
        requestId = UUID.randomUUID().toString()
      }
    )

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created2.rankerJobResourceId
          etag = created2.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.isLastJob).isTrue()
  }

  @Test
  fun `markRankerJobSucceeded does not count a sibling for a different model line`() = runBlocking {
    val created: RankerJob = createRanker(0L, cmmsModelLine = CMMS_MODEL_LINE)
    createRanker(1L, cmmsModelLine = CMMS_MODEL_LINE_2)

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.isLastJob).isTrue()
  }

  @Test
  fun `markRankerJobSucceeded is idempotent with same request_id`() = runBlocking {
    val requestId = UUID.randomUUID().toString()
    val created: RankerJob = createRanker(0L)

    val response1: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          this.requestId = requestId
        }
      )

    val response2: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          this.requestId = requestId
        }
      )

    assertThat(response2.rankerJob).isEqualTo(response1.rankerJob)
    assertThat(response2.isLastJob).isEqualTo(response1.isLastJob)
  }

  @Test
  fun `markRankerJobSucceeded throws ABORTED for etag mismatch`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = created.rankerJobResourceId
            etag = "wrong-etag"
            requestId = UUID.randomUUID().toString()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRankerJobSucceeded throws FAILED_PRECONDITION from SUCCEEDED state`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val response =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = created.rankerJobResourceId
            etag = response.rankerJob.etag
            requestId = UUID.randomUUID().toString()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `markRankerJobSucceeded throws NOT_FOUND when not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = "nonexistent-job"
            etag = "some-etag"
            requestId = UUID.randomUUID().toString()
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `markRankerJobSucceeded throws INVALID_ARGUMENT for empty request_id`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = "some-job"
            etag = "some-etag"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
        }
      )
  }

  @Test
  fun `markRankerJobFailed transitions from CREATED to FAILED`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val job: RankerJob =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          errorMessage = "something went wrong"
        }
      )

    assertThat(job.state).isEqualTo(RankerState.RANKER_STATE_FAILED)
    assertThat(job.errorMessage).isEqualTo("something went wrong")
  }

  @Test
  fun `markRankerJobFailed then markRankerJobSucceeded allows retry`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val failed: RankerJob =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          errorMessage = "transient"
        }
      )

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = failed.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.rankerJob.state).isEqualTo(RankerState.RANKER_STATE_SUCCEEDED)
  }

  @Test
  fun `markRankerJobSucceeded clears error_message when retrying from FAILED`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val failed: RankerJob =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          errorMessage = "transient"
        }
      )
    assertThat(failed.errorMessage).isEqualTo("transient")

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = failed.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.rankerJob.state).isEqualTo(RankerState.RANKER_STATE_SUCCEEDED)
    assertThat(response.rankerJob.errorMessage).isEmpty()

    // The cleared error_message must also be persisted, not just reflected in the response copy.
    val fetched: RankerJob =
      service.getRankerJob(
        getRankerJobRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
        }
      )
    assertThat(fetched.errorMessage).isEmpty()
  }

  @Test
  fun `markRankerJobFailed throws ABORTED for etag mismatch`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobFailed(
          markRankerJobFailedRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = created.rankerJobResourceId
            etag = "wrong-etag"
            errorMessage = "something went wrong"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRankerJobFailed throws INVALID_ARGUMENT for empty request_id`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobFailed(
          markRankerJobFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rankerJobResourceId = "some-job"
            etag = "some-etag"
            errorMessage = "boom"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
        }
      )
  }

  @Test
  fun `markRankerJobFailed is idempotent with same request_id`() = runBlocking {
    val requestId = UUID.randomUUID().toString()
    val created: RankerJob = createRanker(0L)

    val failed1: RankerJob =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          errorMessage = "boom"
          this.requestId = requestId
        }
      )

    val failed2: RankerJob =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          errorMessage = "boom"
          this.requestId = requestId
        }
      )

    assertThat(failed2).isEqualTo(failed1)
  }

  @Test
  fun `mark RPCs track succeeded and failed request_ids independently`() = runBlocking {
    val requestId = UUID.randomUUID().toString()
    val created: RankerJob = createRanker(0L)

    // Mark FAILED then SUCCEEDED reusing the SAME request_id. Each Mark RPC keys idempotency on
    // its own column, so the succeeded call must genuinely transition the job rather than replay
    // the earlier failed mark.
    val failed: RankerJob =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = created.etag
          errorMessage = "transient"
          this.requestId = requestId
        }
      )
    assertThat(failed.state).isEqualTo(RankerState.RANKER_STATE_FAILED)

    val response: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = failed.etag
          this.requestId = requestId
        }
      )
    assertThat(response.rankerJob.state).isEqualTo(RankerState.RANKER_STATE_SUCCEEDED)

    // The succeeded mark stays independently idempotent on its own request_id.
    val replay: MarkRankerJobSucceededResponse =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rankerJobResourceId = created.rankerJobResourceId
          etag = response.rankerJob.etag
          this.requestId = requestId
        }
      )
    assertThat(replay).isEqualTo(response)
  }

  @Test
  fun `listRankerJobs filters by create_time_in`() = runBlocking {
    val created: RankerJob = createRanker(0L)

    val withinResponse: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter =
            ListRankerJobsRequestKt.filter {
              createTimeIn = interval { startTime = created.createTime }
            }
        }
      )
    assertThat(withinResponse.rankerJobsList).hasSize(1)

    val excludedResponse: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter =
            ListRankerJobsRequestKt.filter {
              createTimeIn = interval { endTime = created.createTime }
            }
        }
      )
    assertThat(excludedResponse.rankerJobsList).isEmpty()
  }

  @Test
  fun `listRankerJobs lists across uploads when upload id omitted`() = runBlocking {
    createRanker(0L)
    createParentUpload(DATA_PROVIDER_RESOURCE_ID, SECOND_UPLOAD_RESOURCE_ID)
    service.createRankerJob(
      createRankerJobRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = SECOND_UPLOAD_RESOURCE_ID
        rankerJob = rankerJob {
          cmmsModelLine = CMMS_MODEL_LINE
          poolOffsets += POOL_OFFSETS
        }
      }
    )

    val response: ListRankerJobsResponse =
      service.listRankerJobs(
        listRankerJobsRequest { dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID }
      )

    assertThat(response.rankerJobsList).hasSize(2)
  }

  @Test
  fun `batchCreateRankerJobs throws INVALID_ARGUMENT when exceeding max batch size`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRankerJobs(
            batchCreateRankerJobsRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              for (i in 0 until 51) {
                requests += createRankerJobRequest {
                  requestId = UUID.randomUUID().toString()
                  rankerJob = rankerJob {
                    cmmsModelLine = CMMS_MODEL_LINE
                    poolOffsets += i.toLong()
                  }
                }
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests"
          }
        )
    }

  @Test
  fun `batchCreateRankerJobs throws INVALID_ARGUMENT for duplicate request_id`() = runBlocking {
    val requestId = UUID.randomUUID().toString()

    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateRankerJobs(
          batchCreateRankerJobsRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createRankerJobRequest {
              this.requestId = requestId
              rankerJob = rankerJob {
                cmmsModelLine = CMMS_MODEL_LINE
                poolOffsets += POOL_OFFSETS
              }
            }
            requests += createRankerJobRequest {
              this.requestId = requestId
              rankerJob = rankerJob {
                cmmsModelLine = CMMS_MODEL_LINE
                poolOffsets += POOL_OFFSETS
              }
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
          metadata[Errors.Metadata.FIELD_NAME.key] = "requests[1].request_id"
        }
      )
  }

  private suspend fun createRanker(
    firstPoolOffset: Long,
    cmmsModelLine: String = CMMS_MODEL_LINE,
  ): RankerJob {
    return service.createRankerJob(
      createRankerJobRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rankerJob = rankerJob {
          this.cmmsModelLine = cmmsModelLine
          poolOffsets += firstPoolOffset
        }
      }
    )
  }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "dataProviders/dp1"
    private const val RAW_IMPRESSION_UPLOAD_RESOURCE_ID = "uploads/upload1"
    private const val SECOND_UPLOAD_RESOURCE_ID = "uploads/upload2"
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private val POOL_OFFSETS = listOf(0L, 1L, 2L)
  }
}
