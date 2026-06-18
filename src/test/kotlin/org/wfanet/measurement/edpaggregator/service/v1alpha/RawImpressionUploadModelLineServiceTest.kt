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
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.SpannerRawImpressionUploadModelLineService
import org.wfanet.measurement.edpaggregator.deploy.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.edpaggregator.service.Errors
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadKey
import org.wfanet.measurement.edpaggregator.service.RawImpressionUploadModelLineKey
import org.wfanet.measurement.edpaggregator.v1alpha.RawImpressionUploadModelLine
import org.wfanet.measurement.edpaggregator.v1alpha.batchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.edpaggregator.v1alpha.listRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.edpaggregator.v1alpha.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.edpaggregator.v1alpha.rawImpressionUploadModelLine
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase as InternalModelLineServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineStub as InternalModelLineServiceCoroutineStub
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState

@RunWith(JUnit4::class)
class RawImpressionUploadModelLineServiceTest {
  private lateinit var internalService: InternalModelLineServiceCoroutineImplBase
  private lateinit var service: RawImpressionUploadModelLineService

  private var nextUploadId: Long = 1L

  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.EDP_AGGREGATOR_CHANGELOG_PATH)

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    internalService =
      SpannerRawImpressionUploadModelLineService(spannerDatabaseClient, EmptyCoroutineContext)
    addService(internalService)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  @Before
  fun initService() {
    service =
      RawImpressionUploadModelLineService(
        InternalModelLineServiceCoroutineStub(grpcTestServerRule.channel)
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
  fun `createRawImpressionUploadModelLine returns a model line successfully`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val startTime = Instant.now()
    val request = createRawImpressionUploadModelLineRequest {
      parent = UPLOAD_KEY.toName()
      rawImpressionUploadModelLine = rawImpressionUploadModelLine {
        cmmsModelLine = CMMS_MODEL_LINE
      }
      requestId = REQUEST_ID
    }

    val modelLine = service.createRawImpressionUploadModelLine(request)

    val modelLineKey = assertNotNull(RawImpressionUploadModelLineKey.fromName(modelLine.name))
    assertThat(modelLineKey.dataProviderId).isEqualTo(DATA_PROVIDER_ID)
    assertThat(modelLineKey.rawImpressionUploadId).isEqualTo(RAW_IMPRESSION_UPLOAD_ID)
    assertThat(modelLine.state).isEqualTo(RawImpressionUploadModelLine.State.CREATED)
    assertThat(modelLine.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(modelLine.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(modelLine.updateTime).isEqualTo(modelLine.createTime)
  }

  @Test
  fun `createRawImpressionUploadModelLine with requestId is idempotent`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val request = createRawImpressionUploadModelLineRequest {
      parent = UPLOAD_KEY.toName()
      rawImpressionUploadModelLine = rawImpressionUploadModelLine {
        cmmsModelLine = CMMS_MODEL_LINE
      }
      requestId = REQUEST_ID
    }
    val existing = service.createRawImpressionUploadModelLine(request)

    val duplicate = service.createRawImpressionUploadModelLine(request)

    assertThat(duplicate).isEqualTo(existing)
  }

  @Test
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val request = createRawImpressionUploadModelLineRequest {
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(request)
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
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking {
      val request = createRawImpressionUploadModelLineRequest {
        parent = "invalid-parent"
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(request)
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
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT for missing model line`() =
    runBlocking {
      val request = createRawImpressionUploadModelLineRequest {
        parent = UPLOAD_KEY.toName()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_upload_model_line"
          }
        )
    }

  @Test
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT for empty cmmsModelLine`() =
    runBlocking {
      val request = createRawImpressionUploadModelLineRequest {
        parent = UPLOAD_KEY.toName()
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {}
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(request)
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "raw_impression_upload_model_line.cmms_model_line"
          }
        )
    }

  @Test
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT for malformed requestId`() =
    runBlocking {
      val request = createRawImpressionUploadModelLineRequest {
        parent = UPLOAD_KEY.toName()
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
        requestId = "invalid-request-id"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(request)
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
  fun `batchCreateRawImpressionUploadModelLines returns model lines successfully`() = runBlocking<Unit> {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val request = batchCreateRawImpressionUploadModelLinesRequest {
      parent = UPLOAD_KEY.toName()
      requests += createRawImpressionUploadModelLineRequest {
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
      requests += createRawImpressionUploadModelLineRequest {
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE_2
        }
      }
    }

    val response = service.batchCreateRawImpressionUploadModelLines(request)

    assertThat(response.rawImpressionUploadModelLinesList).hasSize(2)
    val cmmsModelLines =
      response.rawImpressionUploadModelLinesList.map { it.cmmsModelLine }.sorted()
    assertThat(cmmsModelLines).containsExactly(CMMS_MODEL_LINE, CMMS_MODEL_LINE_2)
  }

  @Test
  fun `batchCreateRawImpressionUploadModelLines throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val request = batchCreateRawImpressionUploadModelLinesRequest {
        requests += createRawImpressionUploadModelLineRequest {
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRawImpressionUploadModelLines(request)
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
  fun `batchCreateRawImpressionUploadModelLines throws INVALID_ARGUMENT for empty requests`() =
    runBlocking {
      val request = batchCreateRawImpressionUploadModelLinesRequest {
        parent = UPLOAD_KEY.toName()
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRawImpressionUploadModelLines(request)
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
  fun `getRawImpressionUploadModelLine returns a model line`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
          requestId = REQUEST_ID
        }
      )

    val modelLine =
      service.getRawImpressionUploadModelLine(
        getRawImpressionUploadModelLineRequest { name = created.name }
      )

    assertThat(modelLine).isEqualTo(created)
  }

  @Test
  fun `getRawImpressionUploadModelLine throws INVALID_ARGUMENT for empty name`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionUploadModelLine(getRawImpressionUploadModelLineRequest {})
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
  fun `getRawImpressionUploadModelLine throws INVALID_ARGUMENT for malformed name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getRawImpressionUploadModelLine(
            getRawImpressionUploadModelLineRequest { name = "invalid-name" }
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
  fun `listRawImpressionUploadModelLines returns model lines`() = runBlocking<Unit> {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    val response =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest { parent = UPLOAD_KEY.toName() }
      )

    assertThat(response.rawImpressionUploadModelLinesList).containsExactly(created)
  }

  @Test
  fun `listRawImpressionUploadModelLines supports wildcard parent across uploads`() =
    runBlocking<Unit> {
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
      createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID_2)
      val created1 =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            parent = UPLOAD_KEY.toName()
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      val created2 =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            parent = UPLOAD_KEY_2.toName()
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      val response =
        service.listRawImpressionUploadModelLines(
          listRawImpressionUploadModelLinesRequest {
            parent = RawImpressionUploadKey(DATA_PROVIDER_ID, "-").toName()
          }
        )

      assertThat(response.rawImpressionUploadModelLinesList)
        .containsExactly(created1, created2)
    }

  @Test
  fun `listRawImpressionUploadModelLines respects page size and pagination`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created1 =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val created2 =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE_2
          }
        }
      )
    val sortedCreated = listOf(created1, created2).sortedBy { it.name }

    val firstResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          parent = UPLOAD_KEY.toName()
          pageSize = 1
        }
      )

    assertThat(firstResponse.rawImpressionUploadModelLinesList).hasSize(1)
    assertThat(firstResponse.nextPageToken).isNotEmpty()

    val secondResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          parent = UPLOAD_KEY.toName()
          pageSize = 1
          pageToken = firstResponse.nextPageToken
        }
      )

    assertThat(secondResponse.rawImpressionUploadModelLinesList).hasSize(1)
    assertThat(
        (firstResponse.rawImpressionUploadModelLinesList +
            secondResponse.rawImpressionUploadModelLinesList)
          .sortedBy { it.name }
      )
      .isEqualTo(sortedCreated)
  }

  @Test
  fun `listRawImpressionUploadModelLines throws INVALID_ARGUMENT for empty parent`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {}
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
  fun `listRawImpressionUploadModelLines throws INVALID_ARGUMENT for malformed parent`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest { parent = "invalid-parent" }
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
  fun `listRawImpressionUploadModelLines throws INVALID_ARGUMENT for negative page size`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
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
  fun `listRawImpressionUploadModelLines throws INVALID_ARGUMENT for malformed page token`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listRawImpressionUploadModelLines(
            listRawImpressionUploadModelLinesRequest {
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
  fun `markRawImpressionUploadModelLinePoolAssigning transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    val poolAssigning =
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          name = created.name
          etag = created.etag
        }
      )

    assertThat(poolAssigning.state)
      .isEqualTo(RawImpressionUploadModelLine.State.POOL_ASSIGNING)
    assertThat(poolAssigning.name).isEqualTo(created.name)
    assertThat(poolAssigning.updateTime.toInstant())
      .isGreaterThan(created.updateTime.toInstant())
  }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {}
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
  fun `markRawImpressionUploadModelLinePoolAssigning throws INVALID_ARGUMENT for malformed name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest { name = "invalid-name" }
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
  fun `markRawImpressionUploadModelLineRanking transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val poolAssigning =
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          name = created.name
          etag = created.etag
        }
      )

    val ranking =
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          name = created.name
          etag = poolAssigning.etag
        }
      )

    assertThat(ranking.state).isEqualTo(RawImpressionUploadModelLine.State.RANKING)
    assertThat(ranking.name).isEqualTo(created.name)
  }

  @Test
  fun `markRawImpressionUploadModelLineRanking throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineRanking(
            markRawImpressionUploadModelLineRankingRequest {}
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markRawImpressionUploadModelLineLabeling transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val poolAssigning =
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          name = created.name
          etag = created.etag
        }
      )
    val ranking =
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          name = created.name
          etag = poolAssigning.etag
        }
      )

    val labeling =
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          name = created.name
          etag = ranking.etag
        }
      )

    assertThat(labeling.state).isEqualTo(RawImpressionUploadModelLine.State.LABELING)
    assertThat(labeling.name).isEqualTo(created.name)
  }

  @Test
  fun `markRawImpressionUploadModelLineLabeling throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineLabeling(
            markRawImpressionUploadModelLineLabelingRequest {}
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markRawImpressionUploadModelLineCompleted transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val poolAssigning =
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          name = created.name
          etag = created.etag
        }
      )
    val ranking =
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          name = created.name
          etag = poolAssigning.etag
        }
      )
    val labeling =
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          name = created.name
          etag = ranking.etag
        }
      )

    val completed =
      service.markRawImpressionUploadModelLineCompleted(
        markRawImpressionUploadModelLineCompletedRequest {
          name = created.name
          etag = labeling.etag
        }
      )

    assertThat(completed.state).isEqualTo(RawImpressionUploadModelLine.State.COMPLETED)
    assertThat(completed.name).isEqualTo(created.name)
  }

  @Test
  fun `markRawImpressionUploadModelLineCompleted throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineCompleted(
            markRawImpressionUploadModelLineCompletedRequest {}
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed transitions state`() = runBlocking {
    createParentUpload(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID)
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          parent = UPLOAD_KEY.toName()
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    val failed =
      service.markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          name = created.name
          etag = created.etag
          errorMessage = "Something went wrong"
        }
      )

    assertThat(failed.state).isEqualTo(RawImpressionUploadModelLine.State.FAILED)
    assertThat(failed.name).isEqualTo(created.name)
    assertThat(failed.errorMessage).isEqualTo("Something went wrong")
  }

  @Test
  fun `markRawImpressionUploadModelLineFailed throws INVALID_ARGUMENT for empty name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineFailed(
            markRawImpressionUploadModelLineFailedRequest {}
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed throws INVALID_ARGUMENT for malformed name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineFailed(
            markRawImpressionUploadModelLineFailedRequest { name = "invalid-name" }
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
    private const val RAW_IMPRESSION_UPLOAD_ID_2 = "upload-2"
    private val UPLOAD_KEY_2 = RawImpressionUploadKey(DATA_PROVIDER_ID, RAW_IMPRESSION_UPLOAD_ID_2)
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
    private val REQUEST_ID = UUID.randomUUID().toString()
  }
}
