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
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRankerJobService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RankerJobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.RankerJob
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRankerJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRankerJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRankerJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rankerJob
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt.RankerJobServiceCoroutineImplBase as InternalRankerJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RankerJobServiceGrpcKt.RankerJobServiceCoroutineStub as InternalRankerJobServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

@RunWith(JUnit4::class)
class RankerJobServiceTest {
  private lateinit var internalService: InternalRankerJobServiceCoroutineImplBase
  private lateinit var service: RankerJobService

  private var nextUploadId: Long = 1L

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService = SpannerRankerJobService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service = RankerJobService(InternalRankerJobServiceCoroutineStub(grpcTestServerRule.channel))
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
  fun `createRankerJob returns a job successfully`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val startTime = Instant.now()
    val request = createRankerJobRequest {
      parent = UPLOAD_KEY.toName()
      rankerJob = rankerJob {
        cmmsModelLine = CMMS_MODEL_LINE
        poolOffsets += POOL_OFFSETS
      }
      requestId = REQUEST_ID
    }

    val job = service.createRankerJob(request)

    val jobKey = assertNotNull(RankerJobKey.fromName(job.name))
    assertThat(jobKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(jobKey.rawImpressionUploadId).isEqualTo(RAW_IMPRESSION_UPLOAD_ID)
    assertThat(job.state).isEqualTo(RankerJob.State.CREATED)
    assertThat(job.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(job.poolOffsetsList).containsExactlyElementsIn(POOL_OFFSETS).inOrder()
    assertThat(job.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(job.updateTime).isEqualTo(job.createTime)
    assertThat(job.etag).isNotEmpty()
  }

  @Test
  fun `createRankerJob with requestId is idempotent`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val request = createRankerJobRequest {
      parent = UPLOAD_KEY.toName()
      rankerJob = rankerJob {
        cmmsModelLine = CMMS_MODEL_LINE
        poolOffsets += POOL_OFFSETS
      }
      requestId = REQUEST_ID
    }
    val existing = service.createRankerJob(request)

    val duplicate = service.createRankerJob(request)

    assertThat(duplicate).isEqualTo(existing)
  }

  @Test
  fun `createRankerJob throws INVALID_ARGUMENT for empty parent`() = runBlocking {
    val request = createRankerJobRequest {
      requestId = UUID.randomUUID().toString()
      rankerJob = rankerJob {
        cmmsModelLine = CMMS_MODEL_LINE
        poolOffsets += POOL_OFFSETS
      }
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRankerJob(request) }
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
  fun `createRankerJob throws INVALID_ARGUMENT for malformed parent`() = runBlocking {
    val request = createRankerJobRequest {
      requestId = UUID.randomUUID().toString()
      parent = "invalid-parent"
      rankerJob = rankerJob {
        cmmsModelLine = CMMS_MODEL_LINE
        poolOffsets += POOL_OFFSETS
      }
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRankerJob(request) }
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
  fun `createRankerJob throws INVALID_ARGUMENT for missing ranker_job`() = runBlocking {
    val request = createRankerJobRequest { parent = UPLOAD_KEY.toName() }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRankerJob(request) }
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
  fun `createRankerJob throws INVALID_ARGUMENT for empty cmmsModelLine`() = runBlocking {
    val request = createRankerJobRequest {
      requestId = UUID.randomUUID().toString()
      parent = UPLOAD_KEY.toName()
      rankerJob = rankerJob { poolOffsets += POOL_OFFSETS }
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRankerJob(request) }
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
  fun `createRankerJob throws INVALID_ARGUMENT for empty pool_offsets`() = runBlocking {
    val request = createRankerJobRequest {
      requestId = UUID.randomUUID().toString()
      parent = UPLOAD_KEY.toName()
      rankerJob = rankerJob { cmmsModelLine = CMMS_MODEL_LINE }
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRankerJob(request) }
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
  fun `createRankerJob throws INVALID_ARGUMENT for malformed requestId`() = runBlocking {
    val request = createRankerJobRequest {
      parent = UPLOAD_KEY.toName()
      rankerJob = rankerJob {
        cmmsModelLine = CMMS_MODEL_LINE
        poolOffsets += POOL_OFFSETS
      }
      requestId = "invalid-request-id"
    }

    val exception = assertFailsWith<StatusRuntimeException> { service.createRankerJob(request) }
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
  fun `batchCreateRankerJobs returns jobs successfully`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val request = batchCreateRankerJobsRequest {
        parent = UPLOAD_KEY.toName()
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

      val response = service.batchCreateRankerJobs(request)

      assertThat(response.rankerJobsList).hasSize(2)
      assertThat(response.rankerJobsList.flatMap { it.poolOffsetsList }.sorted())
        .containsExactly(0L, 1L, 2L, 3L)
    }

  @Test
  fun `batchCreateRankerJobs throws INVALID_ARGUMENT for empty parent`() = runBlocking {
    val request = batchCreateRankerJobsRequest {
      requests += createRankerJobRequest {
        requestId = UUID.randomUUID().toString()
        rankerJob = rankerJob {
          cmmsModelLine = CMMS_MODEL_LINE
          poolOffsets += POOL_OFFSETS
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.batchCreateRankerJobs(request) }
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
  fun `batchCreateRankerJobs throws INVALID_ARGUMENT for empty requests`() = runBlocking {
    val request = batchCreateRankerJobsRequest { parent = UPLOAD_KEY.toName() }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.batchCreateRankerJobs(request) }
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
  fun `getRankerJob returns a job`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRankerJob(
        createRankerJobRequest {
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
          requestId = REQUEST_ID
        }
      )

    val job = service.getRankerJob(getRankerJobRequest { name = created.name })

    assertThat(job).isEqualTo(created)
  }

  @Test
  fun `getRankerJob throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> { service.getRankerJob(getRankerJobRequest {}) }
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
  fun `getRankerJob throws INVALID_ARGUMENT for malformed name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRankerJob(getRankerJobRequest { name = "invalid-name" })
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
  fun `listRankerJobs returns jobs`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createRankerJob(
          createRankerJobRequest {
            requestId = UUID.randomUUID().toString()
            parent = UPLOAD_KEY.toName()
            rankerJob = rankerJob {
              cmmsModelLine = CMMS_MODEL_LINE
              poolOffsets += POOL_OFFSETS
            }
          }
        )

      val response = service.listRankerJobs(listRankerJobsRequest { parent = UPLOAD_KEY.toName() })

      assertThat(response.rankerJobsList).containsExactly(created)
    }

  @Test
  fun `listRankerJobs respects page size and pagination`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created1 =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += listOf(0L)
          }
        }
      )
    val created2 =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += listOf(1L)
          }
        }
      )
    val sortedCreated = listOf(created1, created2).sortedBy { it.name }

    val firstResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          parent = UPLOAD_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(firstResponse.rankerJobsList).hasSize(1)
    assertThat(firstResponse.nextPageToken).isNotEmpty()

    val secondResponse =
      service.listRankerJobs(
        listRankerJobsRequest {
          parent = UPLOAD_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.rankerJobsList).hasSize(1)
    assertThat((firstResponse.rankerJobsList + secondResponse.rankerJobsList).sortedBy { it.name })
      .isEqualTo(sortedCreated)
  }

  @Test
  fun `listRankerJobs throws INVALID_ARGUMENT for empty parent`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> { service.listRankerJobs(listRankerJobsRequest {}) }
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
  fun `listRankerJobs throws INVALID_ARGUMENT for negative page size`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRankerJobs(
          listRankerJobsRequest {
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
  fun `listRankerJobs throws INVALID_ARGUMENT for malformed page token`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listRankerJobs(
          listRankerJobsRequest {
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
  fun `markRankerJobSucceeded transitions state and reports is_last_job`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    val response =
      service.markRankerJobSucceeded(
        markRankerJobSucceededRequest {
          name = created.name
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )

    assertThat(response.rankerJob.state).isEqualTo(RankerJob.State.SUCCEEDED)
    assertThat(response.rankerJob.name).isEqualTo(created.name)
    assertThat(response.isLastJob).isTrue()
  }

  @Test
  fun `markRankerJobSucceeded throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(markRankerJobSucceededRequest { etag = "some-etag" })
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
  fun `markRankerJobSucceeded throws INVALID_ARGUMENT for empty etag`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(markRankerJobSucceededRequest { name = created.name })
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
  fun `markRankerJobSucceeded throws INVALID_ARGUMENT for empty requestId`() = runBlocking {
    val name = RankerJobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "rankerJob-some").toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            this.name = name
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
  fun `markRankerJobSucceeded throws ABORTED for etag mismatch`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            name = created.name
            etag = "wrong-etag"
            requestId = UUID.randomUUID().toString()
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRankerJobFailed transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRankerJob(
        createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = UPLOAD_KEY.toName()
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      )

    val failed =
      service.markRankerJobFailed(
        markRankerJobFailedRequest {
          requestId = UUID.randomUUID().toString()
          name = created.name
          etag = created.etag
          errorMessage = "Something went wrong"
        }
      )

    assertThat(failed.state).isEqualTo(RankerJob.State.FAILED)
    assertThat(failed.name).isEqualTo(created.name)
    assertThat(failed.errorMessage).isEqualTo("Something went wrong")
  }

  @Test
  fun `markRankerJobFailed throws INVALID_ARGUMENT for malformed name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobFailed(
          markRankerJobFailedRequest {
            requestId = UUID.randomUUID().toString()
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
  fun `markRankerJobFailed throws INVALID_ARGUMENT for empty requestId`() = runBlocking {
    val name = RankerJobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "rankerJob-some").toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobFailed(
          markRankerJobFailedRequest {
            this.name = name
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
  fun `batchCreateRankerJobs throws INVALID_ARGUMENT when exceeding max batch size`() =
    runBlocking {
      val request = batchCreateRankerJobsRequest {
        parent = UPLOAD_KEY.toName()
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

      val exception =
        assertFailsWith<StatusRuntimeException> { service.batchCreateRankerJobs(request) }
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
    val duplicateId = UUID.randomUUID().toString()
    val request = batchCreateRankerJobsRequest {
      parent = UPLOAD_KEY.toName()
      requests += createRankerJobRequest {
        requestId = duplicateId
        rankerJob = rankerJob {
          cmmsModelLine = CMMS_MODEL_LINE
          poolOffsets += POOL_OFFSETS
        }
      }
      requests += createRankerJobRequest {
        requestId = duplicateId
        rankerJob = rankerJob {
          cmmsModelLine = CMMS_MODEL_LINE
          poolOffsets += POOL_OFFSETS
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { service.batchCreateRankerJobs(request) }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "requests.1.request_id"
        }
      )
  }

  @Test
  fun `batchCreateRankerJobs throws INVALID_ARGUMENT when sub-request parent mismatches`() =
    runBlocking {
      val request = batchCreateRankerJobsRequest {
        parent = UPLOAD_KEY.toName()
        requests += createRankerJobRequest {
          requestId = UUID.randomUUID().toString()
          parent = "dataProviders/other/rawImpressionUploads/other"
          rankerJob = rankerJob {
            cmmsModelLine = CMMS_MODEL_LINE
            poolOffsets += POOL_OFFSETS
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> { service.batchCreateRankerJobs(request) }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "requests.0.parent"
          }
        )
    }

  @Test
  fun `listRankerJobs lists across uploads with wildcard parent`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      createParentUpload(DATA_PROVIDER_ID, SECOND_UPLOAD_ID)
      val firstJob =
        service.createRankerJob(
          createRankerJobRequest {
            requestId = UUID.randomUUID().toString()
            parent = UPLOAD_KEY.toName()
            rankerJob = rankerJob {
              cmmsModelLine = CMMS_MODEL_LINE
              poolOffsets += POOL_OFFSETS
            }
          }
        )
      val secondJob =
        service.createRankerJob(
          createRankerJobRequest {
            requestId = UUID.randomUUID().toString()
            parent = RawImpressionUploadKey(DATA_PROVIDER_ID, SECOND_UPLOAD_ID).toName()
            rankerJob = rankerJob {
              cmmsModelLine = CMMS_MODEL_LINE
              poolOffsets += POOL_OFFSETS
            }
          }
        )

      val response =
        service.listRankerJobs(
          listRankerJobsRequest {
            parent = RawImpressionUploadKey(DATA_PROVIDER_ID, ResourceKey.WILDCARD_ID).toName()
          }
        )

      assertThat(response.rankerJobsList.map { it.name })
        .containsExactly(firstJob.name, secondJob.name)
    }

  @Test
  fun `getRankerJob throws NOT_FOUND for missing job`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val missingName =
      RankerJobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "rj-missing").toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRankerJob(getRankerJobRequest { name = missingName })
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `markRankerJobSucceeded throws NOT_FOUND for missing job`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val missingName =
      RankerJobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "rj-missing").toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobSucceeded(
          markRankerJobSucceededRequest {
            name = missingName
            etag = "some-etag"
            requestId = UUID.randomUUID().toString()
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `markRankerJobFailed throws NOT_FOUND for missing job`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val missingName =
      RankerJobKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID, "rj-missing").toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.markRankerJobFailed(
          markRankerJobFailedRequest {
            requestId = UUID.randomUUID().toString()
            name = missingName
            etag = "some-etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private const val RAW_IMPRESSION_UPLOAD_ID = "upload-1"
    private const val SECOND_UPLOAD_ID = "upload-2"
    private val UPLOAD_KEY = RawImpressionUploadKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private val POOL_OFFSETS = listOf(0L, 1L, 2L)
    private val REQUEST_ID = UUID.randomUUID().toString()
  }
}
