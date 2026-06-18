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
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerPoolAssignmentJobService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.PoolAssignmentJobKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.v1alpha.PoolAssignmentJob
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreatePoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getPoolAssignmentJobRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listPoolAssignmentJobsRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markPoolAssignmentJobSucceededRequest
import org.wfanet.measurement.edpaggregator.v1alpha.poolAssignmentJob
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineImplBase as InternalPoolAssignmentJobServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.PoolAssignmentJobServiceGrpcKt.PoolAssignmentJobServiceCoroutineStub as InternalPoolAssignmentJobServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

@RunWith(JUnit4::class)
class PoolAssignmentJobServiceTest {
  private lateinit var internalService: InternalPoolAssignmentJobServiceCoroutineImplBase
  private lateinit var service: PoolAssignmentJobService

  private var nextUploadId: Long = 1L

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService =
      SpannerPoolAssignmentJobService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      PoolAssignmentJobService(
        InternalPoolAssignmentJobServiceCoroutineStub(grpcTestServerRule.channel)
      )
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
        .to(
          Value.protoEnum(
            RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED
          )
        )
        .set("CreateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .set("UpdateTime")
        .to(Value.COMMIT_TIMESTAMP)
        .build()
    spannerDatabase.databaseClient.write(listOf(mutation))
  }

  @Test
  fun `createPoolAssignmentJob returns a job successfully`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val startTime = Instant.now()
    val request = createPoolAssignmentJobRequest {
      parent = UPLOAD_KEY.toName()
      poolAssignmentJob = poolAssignmentJob {
        cmmsModelLine = CMMS_MODEL_LINE
        shardIndex = 0
      }
      requestId = REQUEST_ID
    }

    val job = service.createPoolAssignmentJob(request)

    val jobKey = assertNotNull(PoolAssignmentJobKey.fromName(job.name))
    assertThat(jobKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(jobKey.rawImpressionUploadId).isEqualTo(RAW_IMPRESSION_UPLOAD_ID)
    assertThat(job.state).isEqualTo(PoolAssignmentJob.State.CREATED)
    assertThat(job.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(job.shardIndex).isEqualTo(0)
    assertThat(job.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(job.updateTime).isEqualTo(job.createTime)
    assertThat(job.etag).isNotEmpty()
  }

  @Test
  fun `createPoolAssignmentJob with requestId is idempotent`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val request = createPoolAssignmentJobRequest {
      parent = UPLOAD_KEY.toName()
      poolAssignmentJob = poolAssignmentJob {
        cmmsModelLine = CMMS_MODEL_LINE
        shardIndex = 0
      }
      requestId = REQUEST_ID
    }
    val existing = service.createPoolAssignmentJob(request)

    val duplicate = service.createPoolAssignmentJob(request)

    assertThat(duplicate).isEqualTo(existing)
  }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val request = createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 0
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(request)
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
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking {
      val request = createPoolAssignmentJobRequest {
        parent = "invalid-parent"
        poolAssignmentJob = poolAssignmentJob {
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 0
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(request)
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
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT for missing pool_assignment_job`() =
    runBlocking {
      val request = createPoolAssignmentJobRequest {
        parent = UPLOAD_KEY.toName()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(request)
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
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT for empty cmmsModelLine`() =
    runBlocking {
      val request = createPoolAssignmentJobRequest {
        parent = UPLOAD_KEY.toName()
        poolAssignmentJob = poolAssignmentJob {
          shardIndex = 0
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "pool_assignment_job.cmms_model_line"
          }
        )
    }

  @Test
  fun `createPoolAssignmentJob throws INVALID_ARGUMENT for malformed requestId`() =
    runBlocking {
      val request = createPoolAssignmentJobRequest {
        parent = UPLOAD_KEY.toName()
        poolAssignmentJob = poolAssignmentJob {
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 0
        }
        requestId = "invalid-request-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createPoolAssignmentJob(request)
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
  fun `batchCreatePoolAssignmentJobs returns jobs successfully`() = runBlocking<Unit> {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val request = batchCreatePoolAssignmentJobsRequest {
      parent = UPLOAD_KEY.toName()
      requests += createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 0
        }
      }
      requests += createPoolAssignmentJobRequest {
        poolAssignmentJob = poolAssignmentJob {
          cmmsModelLine = CMMS_MODEL_LINE
          shardIndex = 1
        }
      }
    }

    val response = service.batchCreatePoolAssignmentJobs(request)

    assertThat(response.poolAssignmentJobsList).hasSize(2)
    val shardIndices =
      response.poolAssignmentJobsList.map { it.shardIndex }.sorted()
    assertThat(shardIndices).containsExactly(0, 1)
  }

  @Test
  fun `batchCreatePoolAssignmentJobs throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val request = batchCreatePoolAssignmentJobsRequest {
        requests += createPoolAssignmentJobRequest {
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreatePoolAssignmentJobs(request)
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
  fun `batchCreatePoolAssignmentJobs throws INVALID_ARGUMENT for empty requests`() =
    runBlocking {
      val request = batchCreatePoolAssignmentJobsRequest {
        parent = UPLOAD_KEY.toName()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreatePoolAssignmentJobs(request)
        }
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
  fun `getPoolAssignmentJob returns a job`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          parent = UPLOAD_KEY.toName()
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
          requestId = REQUEST_ID
        }
      )

    val job =
      service.getPoolAssignmentJob(
        getPoolAssignmentJobRequest { name = created.name }
      )

    assertThat(job).isEqualTo(created)
  }

  @Test
  fun `getPoolAssignmentJob throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getPoolAssignmentJob(getPoolAssignmentJobRequest {})
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
  fun `getPoolAssignmentJob throws INVALID_ARGUMENT for malformed name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getPoolAssignmentJob(
            getPoolAssignmentJobRequest { name = "invalid-name" }
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
  fun `listPoolAssignmentJobs returns jobs`() = runBlocking<Unit> {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          parent = UPLOAD_KEY.toName()
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val response =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest { parent = UPLOAD_KEY.toName() }
      )

    assertThat(response.poolAssignmentJobsList).containsExactly(created)
  }

  @Test
  fun `listPoolAssignmentJobs respects page size and pagination`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created1 =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          parent = UPLOAD_KEY.toName()
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )
    val created2 =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          parent = UPLOAD_KEY.toName()
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 1
          }
        }
      )
    val sortedCreated = listOf(created1, created2).sortedBy { it.name }

    val firstResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          parent = UPLOAD_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(firstResponse.poolAssignmentJobsList).hasSize(1)
    assertThat(firstResponse.nextPageToken).isNotEmpty()

    val secondResponse =
      service.listPoolAssignmentJobs(
        listPoolAssignmentJobsRequest {
          parent = UPLOAD_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.poolAssignmentJobsList).hasSize(1)
    assertThat(
        (firstResponse.poolAssignmentJobsList +
            secondResponse.poolAssignmentJobsList)
          .sortedBy { it.name }
      )
      .isEqualTo(sortedCreated)
  }

  @Test
  fun `listPoolAssignmentJobs throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listPoolAssignmentJobs(
            listPoolAssignmentJobsRequest {}
          )
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
  fun `listPoolAssignmentJobs throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listPoolAssignmentJobs(
            listPoolAssignmentJobsRequest { parent = "invalid-parent" }
          )
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
  fun `listPoolAssignmentJobs throws INVALID_ARGUMENT for negative page size`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listPoolAssignmentJobs(
            listPoolAssignmentJobsRequest {
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
  fun `listPoolAssignmentJobs throws INVALID_ARGUMENT for malformed page token`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listPoolAssignmentJobs(
            listPoolAssignmentJobsRequest {
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
  fun `markPoolAssignmentJobSucceeded transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          parent = UPLOAD_KEY.toName()
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val response =
      service.markPoolAssignmentJobSucceeded(
        markPoolAssignmentJobSucceededRequest {
          name = created.name
          etag = created.etag
        }
      )

    assertThat(response.poolAssignmentJob.state)
      .isEqualTo(PoolAssignmentJob.State.SUCCEEDED)
    assertThat(response.poolAssignmentJob.name).isEqualTo(created.name)
  }

  @Test
  fun `markPoolAssignmentJobSucceeded throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markPoolAssignmentJobSucceeded(
            markPoolAssignmentJobSucceededRequest {
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "name"
          }
        )
    }

  @Test
  fun `markPoolAssignmentJobSucceeded throws INVALID_ARGUMENT for empty etag`() =
    runBlocking {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      val created =
        service.createPoolAssignmentJob(
          createPoolAssignmentJobRequest {
            parent = UPLOAD_KEY.toName()
            poolAssignmentJob = poolAssignmentJob {
              cmmsModelLine = CMMS_MODEL_LINE
              shardIndex = 0
            }
          }
        )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markPoolAssignmentJobSucceeded(
            markPoolAssignmentJobSucceededRequest {
              name = created.name
            }
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
  fun `markPoolAssignmentJobFailed transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createPoolAssignmentJob(
        createPoolAssignmentJobRequest {
          parent = UPLOAD_KEY.toName()
          poolAssignmentJob = poolAssignmentJob {
            cmmsModelLine = CMMS_MODEL_LINE
            shardIndex = 0
          }
        }
      )

    val failed =
      service.markPoolAssignmentJobFailed(
        markPoolAssignmentJobFailedRequest {
          name = created.name
          etag = created.etag
          errorMessage = "Something went wrong"
        }
      )

    assertThat(failed.state).isEqualTo(PoolAssignmentJob.State.FAILED)
    assertThat(failed.name).isEqualTo(created.name)
    assertThat(failed.errorMessage).isEqualTo("Something went wrong")
  }

  @Test
  fun `markPoolAssignmentJobFailed throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markPoolAssignmentJobFailed(
            markPoolAssignmentJobFailedRequest {
              etag = "some-etag"
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markPoolAssignmentJobFailed throws INVALID_ARGUMENT for malformed name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markPoolAssignmentJobFailed(
            markPoolAssignmentJobFailedRequest {
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

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    private val DATA_PROVIDER_ID = externalIdToApiId(111L)
    private val DATA_PROVIDER_KEY = DataProviderKey(DATA_PROVIDER_ID)
    private const val RAW_IMPRESSION_UPLOAD_ID = "upload-1"
    private val UPLOAD_KEY = RawImpressionUploadKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private val REQUEST_ID = UUID.randomUUID().toString()
  }
}
