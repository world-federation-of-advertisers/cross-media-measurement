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
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadState
import org.wfanet.measurement.internal.edpaggregator.batchCreateRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.createRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.getRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.listRawImpressionUploadModelLinesRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineCompletedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineFailedRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineLabelingRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLinePoolAssigningRequest
import org.wfanet.measurement.internal.edpaggregator.markRawImpressionUploadModelLineRankingRequest
import org.wfanet.measurement.internal.edpaggregator.rawImpressionUploadModelLine

@RunWith(JUnit4::class)
abstract class RawImpressionUploadModelLineServiceTest {
  private lateinit var service: RawImpressionUploadModelLineServiceCoroutineImplBase

  protected abstract fun newService(
    idGenerator: IdGenerator = IdGenerator.Default
  ): RawImpressionUploadModelLineServiceCoroutineImplBase

  /**
   * Creates a parent [RawImpressionUpload] row so that child model line rows can be inserted
   * (interleaved table).
   */
  protected abstract suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  )

  /** Returns the parent [RawImpressionUpload]'s current state, for cascade assertions. */
  protected abstract suspend fun getParentUploadState(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  ): RawImpressionUploadState

  @Before
  fun initService() {
    service = newService()
    runBlocking { createParentUpload(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID) }
  }

  /** Returns the current etag of a model line, for supplying to Mark (CAS) transitions. */
  private suspend fun currentEtag(rawImpressionUploadModelLineResourceId: String): String =
    service
      .getRawImpressionUploadModelLine(
        getRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          this.rawImpressionUploadModelLineResourceId = rawImpressionUploadModelLineResourceId
        }
      )
      .etag

  /**
   * Creates one model line and drives it — and therefore the sole-child parent upload — to
   * COMPLETED (non-memoized path: CREATED -> LABELING -> COMPLETED).
   */
  private suspend fun completeSoleModelLine(cmmsModelLine: String = CMMS_MODEL_LINE) {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            this.cmmsModelLine = cmmsModelLine
          }
        }
      )
    service.markRawImpressionUploadModelLineLabeling(
      markRawImpressionUploadModelLineLabelingRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
      }
    )
    service.markRawImpressionUploadModelLineCompleted(
      markRawImpressionUploadModelLineCompletedRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
      }
    )
  }

  @Test
  fun `createRawImpressionUploadModelLine creates successfully`() = runBlocking {
    val startTime: Instant = Instant.now()

    val modelLine: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    assertThat(modelLine.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(modelLine.rawImpressionUploadResourceId).isEqualTo(RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
    assertThat(modelLine.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(modelLine.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED)
    assertThat(modelLine.createTime.toInstant()).isGreaterThan(startTime)
    assertThat(modelLine.updateTime).isEqualTo(modelLine.createTime)
  }

  @Test
  fun `createRawImpressionUploadModelLine is idempotent with same request_id`() = runBlocking {
    val requestId: String = UUID.randomUUID().toString()

    val modelLine1: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          this.requestId = requestId
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    val modelLine2: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          this.requestId = requestId
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    assertThat(modelLine2).isEqualTo(modelLine1)
  }

  @Test
  fun `createRawImpressionUploadModelLine throws ALREADY_EXISTS when request_id reused with different cmms_model_line`() =
    runBlocking {
      val requestId: String = UUID.randomUUID().toString()

      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          this.requestId = requestId
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              this.requestId = requestId
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE_2
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
              requestId = UUID.randomUUID().toString()
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
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
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT if raw_impression_upload_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
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
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT if raw_impression_upload_model_line not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
              requestId = UUID.randomUUID().toString()
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
            metadata[Errors.Metadata.FIELD_NAME.key] = "raw_impression_upload_model_line"
          }
        )
    }

  @Test
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT if cmms_model_line not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {}
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
              "raw_impression_upload_model_line.cmms_model_line"
          }
        )
    }

  @Test
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT for malformed request_id`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requestId = "not-a-valid-uuid"
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
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
  fun `batchCreateRawImpressionUploadModelLines creates multiple`() =
    runBlocking<Unit> {
      val response =
        service.batchCreateRawImpressionUploadModelLines(
          batchCreateRawImpressionUploadModelLinesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createRawImpressionUploadModelLineRequest {
              requestId = UUID.randomUUID().toString()
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
              }
            }
            requests += createRawImpressionUploadModelLineRequest {
              requestId = UUID.randomUUID().toString()
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE_2
              }
            }
          }
        )

      assertThat(response.rawImpressionUploadModelLinesList).hasSize(2)
      assertThat(response.rawImpressionUploadModelLinesList.map { it.cmmsModelLine })
        .containsExactly(CMMS_MODEL_LINE, CMMS_MODEL_LINE_2)
    }

  @Test
  fun `getRawImpressionUploadModelLine returns existing resource`() = runBlocking {
    val created: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    val modelLine: RawImpressionUploadModelLine =
      service.getRawImpressionUploadModelLine(
        getRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        }
      )

    assertThat(modelLine.dataProviderResourceId).isEqualTo(DATA_PROVIDER_RESOURCE_ID)
    assertThat(modelLine.rawImpressionUploadResourceId).isEqualTo(RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
    assertThat(modelLine.cmmsModelLine).isEqualTo(CMMS_MODEL_LINE)
    assertThat(modelLine.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_CREATED)
  }

  @Test
  fun `getRawImpressionUploadModelLine throws NOT_FOUND when not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.getRawImpressionUploadModelLine(
          getRawImpressionUploadModelLineRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = "nonexistent-resource-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getRawImpressionUploadModelLine throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getRawImpressionUploadModelLine(
            getRawImpressionUploadModelLineRequest {
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId = "some-resource-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getRawImpressionUploadModelLine throws INVALID_ARGUMENT if raw_impression_upload_model_line_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.getRawImpressionUploadModelLine(
            getRawImpressionUploadModelLineRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `listRawImpressionUploadModelLines returns resources`() = runBlocking {
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
    )
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE_2
        }
      }
    )

    val response: ListRawImpressionUploadModelLinesResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        }
      )

    assertThat(response.rawImpressionUploadModelLinesList).hasSize(2)
  }

  @Test
  fun `listRawImpressionUploadModelLines respects page_size`() = runBlocking {
    for (i in 1..3) {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = "modelProviders/mp1/modelSuites/ms1/modelLines/ml$i"
          }
        }
      )
    }

    val response: ListRawImpressionUploadModelLinesResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(response.rawImpressionUploadModelLinesList).hasSize(2)
    assertThat(response.hasNextPageToken()).isTrue()
  }

  @Test
  fun `listRawImpressionUploadModelLines filters by state_in`() = runBlocking {
    val created: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE_2
        }
      }
    )

    service.markRawImpressionUploadModelLinePoolAssigning(
      markRawImpressionUploadModelLinePoolAssigningRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
      }
    )

    val response: ListRawImpressionUploadModelLinesResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter =
            ListRawImpressionUploadModelLinesRequestKt.filter {
              stateIn +=
                RawImpressionUploadModelLineState
                  .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
            }
        }
      )

    assertThat(response.rawImpressionUploadModelLinesList).hasSize(1)
    assertThat(response.rawImpressionUploadModelLinesList.first().cmmsModelLine)
      .isEqualTo(CMMS_MODEL_LINE)
  }

  @Test
  fun `listRawImpressionUploadModelLines filters by cmms_model_line`() = runBlocking {
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
    )
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE_2
        }
      }
    )

    val response: ListRawImpressionUploadModelLinesResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          filter =
            ListRawImpressionUploadModelLinesRequestKt.filter { cmmsModelLine = CMMS_MODEL_LINE }
        }
      )

    assertThat(response.rawImpressionUploadModelLinesList).hasSize(1)
    assertThat(response.rawImpressionUploadModelLinesList.first().cmmsModelLine)
      .isEqualTo(CMMS_MODEL_LINE)
  }

  @Test
  fun `listRawImpressionUploadModelLines pagination with page_token`() = runBlocking {
    for (i in 1..3) {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = "modelProviders/mp1/modelSuites/ms1/modelLines/ml$i"
          }
        }
      )
    }

    val firstPage: ListRawImpressionUploadModelLinesResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
        }
      )

    assertThat(firstPage.rawImpressionUploadModelLinesList).hasSize(2)
    assertThat(firstPage.hasNextPageToken()).isTrue()

    val secondPage: ListRawImpressionUploadModelLinesResponse =
      service.listRawImpressionUploadModelLines(
        listRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          pageSize = 2
          pageToken = firstPage.nextPageToken
        }
      )

    assertThat(secondPage.rawImpressionUploadModelLinesList).hasSize(1)
    assertThat(secondPage.hasNextPageToken()).isFalse()
  }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning transitions CREATED to POOL_ASSIGNING`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
        )
    }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning throws ABORTED on etag mismatch`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId =
                created.rawImpressionUploadModelLineResourceId
              etag = "wrong-etag"
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
    }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning throws INVALID_ARGUMENT for empty etag`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId =
                created.rawImpressionUploadModelLineResourceId
              // Intentionally no etag — internal layer must reject it.
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning succeeds from FAILED and clears error_message`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      val poolAssigning =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
          }
        )
      val failed =
        service.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = poolAssigning.etag
            errorMessage = "something went wrong"
          }
        )
      val resumed =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = failed.etag
          }
        )
      assertThat(resumed.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
        )
      assertThat(resumed.errorMessage).isEmpty()
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking transitions POOL_ASSIGNING to RANKING`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        }
      )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING)
    }

  @Test
  fun `markRawImpressionUploadModelLineLabeling transitions RANKING to LABELING`() = runBlocking {
    val created: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    service.markRawImpressionUploadModelLinePoolAssigning(
      markRawImpressionUploadModelLinePoolAssigningRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
      }
    )
    service.markRawImpressionUploadModelLineRanking(
      markRawImpressionUploadModelLineRankingRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
      }
    )

    val modelLine: RawImpressionUploadModelLine =
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        }
      )

    assertThat(modelLine.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING)
  }

  @Test
  fun `markRawImpressionUploadModelLineCompleted transitions LABELING to COMPLETED`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        }
      )
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        }
      )
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        }
      )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLineCompleted(
          markRawImpressionUploadModelLineCompletedRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
        )
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed transitions from CREATED to FAILED`() = runBlocking {
    val created: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    val modelLine: RawImpressionUploadModelLine =
      service.markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
          errorMessage = "something went wrong"
        }
      )

    assertThat(modelLine.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
    assertThat(modelLine.errorMessage).isEqualTo("something went wrong")
  }

  @Test
  fun `markRawImpressionUploadModelLineFailed transitions from POOL_ASSIGNING to FAILED`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        }
      )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
            errorMessage = "pool assignment failed"
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking throws FAILED_PRECONDITION from CREATED state`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineRanking(
            markRawImpressionUploadModelLineRankingRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId =
                created.rawImpressionUploadModelLineResourceId
              etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markRawImpressionUploadModelLineCompleted throws FAILED_PRECONDITION from CREATED state`() =
    runBlocking {
      val created: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLineCompleted(
            markRawImpressionUploadModelLineCompletedRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId =
                created.rawImpressionUploadModelLineResourceId
              etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning throws NOT_FOUND when not found`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId = "nonexistent-resource-id"
              etag = "some-etag"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking throws ABORTED on etag mismatch`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = "wrong-etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRawImpressionUploadModelLineLabeling throws ABORTED on etag mismatch`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = "wrong-etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRawImpressionUploadModelLineCompleted throws ABORTED on etag mismatch`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRawImpressionUploadModelLineCompleted(
          markRawImpressionUploadModelLineCompletedRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = "wrong-etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRawImpressionUploadModelLineFailed throws ABORTED on etag mismatch`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = "wrong-etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.ABORTED)
  }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning throws FAILED_PRECONDITION from RANKING`() =
    runBlocking {
      val created =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      val poolAssigning =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
          }
        )
      val ranking =
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = poolAssigning.etag
          }
        )
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId =
                created.rawImpressionUploadModelLineResourceId
              etag = ranking.etag
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking throws NOT_FOUND when not found`() = runBlocking {
    val exception: StatusRuntimeException =
      assertFailsWith<StatusRuntimeException> {
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = "riuml-does-not-exist"
            etag = "some-etag"
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `batchCreateRawImpressionUploadModelLines throws INVALID_ARGUMENT over max batch size`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRawImpressionUploadModelLines(
            batchCreateRawImpressionUploadModelLinesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              for (i in 0 until 51) {
                requests += createRawImpressionUploadModelLineRequest {
                  requestId = UUID.randomUUID().toString()
                  rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                    cmmsModelLine = "modelProviders/mp1/modelSuites/ms1/modelLines/ml$i"
                  }
                }
              }
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `batchCreateRawImpressionUploadModelLines is idempotent across replays`() = runBlocking {
    val request = batchCreateRawImpressionUploadModelLinesRequest {
      dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
      rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
      requests += createRawImpressionUploadModelLineRequest {
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
        requestId = UUID.randomUUID().toString()
      }
      requests += createRawImpressionUploadModelLineRequest {
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE_2
        }
        requestId = UUID.randomUUID().toString()
      }
    }
    val first = service.batchCreateRawImpressionUploadModelLines(request)
    val second = service.batchCreateRawImpressionUploadModelLines(request)
    assertThat(second.rawImpressionUploadModelLinesList)
      .isEqualTo(first.rawImpressionUploadModelLinesList)
  }

  @Test
  fun `batchCreateRawImpressionUploadModelLines throws ALREADY_EXISTS when request_id reused with different cmms_model_line`() =
    runBlocking {
      val requestId: String = UUID.randomUUID().toString()

      service.batchCreateRawImpressionUploadModelLines(
        batchCreateRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          requests += createRawImpressionUploadModelLineRequest {
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
            this.requestId = requestId
          }
        }
      )

      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateRawImpressionUploadModelLines(
            batchCreateRawImpressionUploadModelLinesRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              requests += createRawImpressionUploadModelLineRequest {
                rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                  cmmsModelLine = CMMS_MODEL_LINE_2
                }
                this.requestId = requestId
              }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    }

  @Test
  fun `batchCreateRawImpressionUploadModelLines request_id is scoped to parent upload`() =
    runBlocking {
      val secondUpload = "uploads/upload2"
      createParentUpload(DATA_PROVIDER_RESOURCE_ID, secondUpload)
      val requestId = UUID.randomUUID().toString()
      val first =
        service.batchCreateRawImpressionUploadModelLines(
          batchCreateRawImpressionUploadModelLinesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            requests += createRawImpressionUploadModelLineRequest {
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
              }
              this.requestId = requestId
            }
          }
        )
      val second =
        service.batchCreateRawImpressionUploadModelLines(
          batchCreateRawImpressionUploadModelLinesRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = secondUpload
            requests += createRawImpressionUploadModelLineRequest {
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
              }
              this.requestId = requestId
            }
          }
        )
      assertThat(
          second.rawImpressionUploadModelLinesList.single().rawImpressionUploadModelLineResourceId
        )
        .isNotEqualTo(
          first.rawImpressionUploadModelLinesList.single().rawImpressionUploadModelLineResourceId
        )
    }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning activates the parent upload`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    service.markRawImpressionUploadModelLinePoolAssigning(
      markRawImpressionUploadModelLinePoolAssigningRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
      }
    )

    assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)
  }

  @Test
  fun `markRawImpressionUploadModelLineLabeling activates the parent on the non-memoized path`() =
    runBlocking {
      val created =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      // Non-memoized uploads transition straight from CREATED to LABELING.
      val modelLine =
        service.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
        .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed fails the parent upload`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )

    service.markRawImpressionUploadModelLineFailed(
      markRawImpressionUploadModelLineFailedRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        errorMessage = "boom"
      }
    )

    assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED)
  }

  @Test
  fun `parent upload completes only after all model lines complete`() = runBlocking {
    val modelLine1 =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    val modelLine2 =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE_2
          }
        }
      )

    // Complete model line 1 (non-memoized: CREATED -> LABELING -> COMPLETED).
    service.markRawImpressionUploadModelLineLabeling(
      markRawImpressionUploadModelLineLabelingRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = modelLine1.rawImpressionUploadModelLineResourceId
        etag = currentEtag(modelLine1.rawImpressionUploadModelLineResourceId)
      }
    )
    service.markRawImpressionUploadModelLineCompleted(
      markRawImpressionUploadModelLineCompletedRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = modelLine1.rawImpressionUploadModelLineResourceId
        etag = currentEtag(modelLine1.rawImpressionUploadModelLineResourceId)
      }
    )

    // Parent is not COMPLETED while model line 2 is still pending.
    assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
      .isNotEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)

    // Complete model line 2; now every model line is COMPLETED.
    service.markRawImpressionUploadModelLineLabeling(
      markRawImpressionUploadModelLineLabelingRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = modelLine2.rawImpressionUploadModelLineResourceId
        etag = currentEtag(modelLine2.rawImpressionUploadModelLineResourceId)
      }
    )
    service.markRawImpressionUploadModelLineCompleted(
      markRawImpressionUploadModelLineCompletedRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = modelLine2.rawImpressionUploadModelLineResourceId
        etag = currentEtag(modelLine2.rawImpressionUploadModelLineResourceId)
      }
    )

    assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)
  }

  @Test
  fun `createRawImpressionUploadModelLine reactivates a COMPLETED parent (backfill)`() =
    runBlocking {
      completeSoleModelLine()
      assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
        .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)

      // Backfilling a new model line onto the COMPLETED upload reactivates the parent.
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE_2
          }
        }
      )

      assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
        .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)
    }

  @Test
  fun `batchCreateRawImpressionUploadModelLines reactivates a COMPLETED parent (backfill)`() =
    runBlocking<Unit> {
      completeSoleModelLine()
      assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
        .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_COMPLETED)

      service.batchCreateRawImpressionUploadModelLines(
        batchCreateRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          requests += createRawImpressionUploadModelLineRequest {
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE_2
            }
          }
        }
      )

      assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
        .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_ACTIVE)
    }

  @Test
  fun `createRawImpressionUploadModelLine rejects backfill onto a FAILED parent`() = runBlocking {
    val created =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    service.markRawImpressionUploadModelLineFailed(
      markRawImpressionUploadModelLineFailedRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
        etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
        errorMessage = "boom"
      }
    )
    assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_FAILED)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE_2
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `createRawImpressionUploadModelLine leaves a CREATED parent unchanged`() = runBlocking {
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
    )

    assertThat(getParentUploadState(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID))
      .isEqualTo(RawImpressionUploadState.RAW_IMPRESSION_UPLOAD_STATE_CREATED)
  }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning throws FAILED_PRECONDITION when another upload's same model line is in flight`() =
    runBlocking {
      val secondUpload = "uploads/upload2"
      createParentUpload(DATA_PROVIDER_RESOURCE_ID, secondUpload)

      // Two uploads, same cmms_model_line.
      val first: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )
      val second: RawImpressionUploadModelLine =
        service.createRawImpressionUploadModelLine(
          createRawImpressionUploadModelLineRequest {
            requestId = UUID.randomUUID().toString()
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = secondUpload
            rawImpressionUploadModelLine = rawImpressionUploadModelLine {
              cmmsModelLine = CMMS_MODEL_LINE
            }
          }
        )

      // Put the first upload's model line in flight.
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          requestId = UUID.randomUUID().toString()
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = first.rawImpressionUploadModelLineResourceId
          etag = first.etag
        }
      )

      // The second upload's same-model-line transition into a processing state must be rejected.
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              requestId = UUID.randomUUID().toString()
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = secondUpload
              rawImpressionUploadModelLineResourceId = second.rawImpressionUploadModelLineResourceId
              etag = second.etag
            }
          )
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      // The rejection is a concurrency conflict (not a state violation) and names the blocking
      // upload so an operator does not have to hand-query for it.
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.RAW_IMPRESSION_UPLOAD_MODEL_LINE_CONCURRENT.name
            metadata[Errors.Metadata.DATA_PROVIDER_RESOURCE_ID.key] = DATA_PROVIDER_RESOURCE_ID
            metadata[Errors.Metadata.CMMS_MODEL_LINE.key] = CMMS_MODEL_LINE
            metadata[Errors.Metadata.CONFLICTING_RAW_IMPRESSION_UPLOAD_RESOURCE_IDS.key] =
              RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          }
        )
    }

  /** Creates a model line in the CREATED state with a fresh (valid) request_id. */
  private suspend fun createModelLine(
    cmmsModelLine: String = CMMS_MODEL_LINE
  ): RawImpressionUploadModelLine =
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        requestId = UUID.randomUUID().toString()
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          this.cmmsModelLine = cmmsModelLine
        }
      }
    )

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning is idempotent with same request_id`() =
    runBlocking {
      val created = createModelLine()
      val requestId = UUID.randomUUID().toString()

      val first =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            this.requestId = requestId
          }
        )
      // A Pub/Sub redelivery replays the identical request (same etag, same request_id).
      val second =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            this.requestId = requestId
          }
        )

      assertThat(first.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
        )
      assertThat(second).isEqualTo(first)
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking is idempotent with same request_id`() = runBlocking {
    val created = createModelLine()
    val poolAssigning =
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )
    val requestId = UUID.randomUUID().toString()

    val first =
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = poolAssigning.etag
          this.requestId = requestId
        }
      )
    val second =
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = poolAssigning.etag
          this.requestId = requestId
        }
      )

    assertThat(first.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING)
    assertThat(second).isEqualTo(first)
  }

  @Test
  fun `markRawImpressionUploadModelLineRanking replays after the line advanced past RANKING`() =
    runBlocking {
      // AIP-155 stale success: a redelivered mark whose request_id already ran must return success
      // reflecting the current committed state, even after the line advanced (RANKING -> LABELING),
      // rather than FAILED_PRECONDITION.
      val created = createModelLine()
      val poolAssigning =
        service.markRawImpressionUploadModelLinePoolAssigning(
          markRawImpressionUploadModelLinePoolAssigningRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            requestId = UUID.randomUUID().toString()
          }
        )
      val rankingRequestId = UUID.randomUUID().toString()
      val ranking =
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = poolAssigning.etag
            requestId = rankingRequestId
          }
        )
      // The line moves on to LABELING before the Ranking mark is redelivered.
      val labeling =
        service.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = ranking.etag
            requestId = UUID.randomUUID().toString()
          }
        )

      // Pub/Sub redelivers the original Ranking mark (same request_id) after the advance.
      val replay =
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = poolAssigning.etag
            requestId = rankingRequestId
          }
        )

      assertThat(labeling.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      assertThat(replay.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      assertThat(replay.etag).isEqualTo(labeling.etag)
    }

  @Test
  fun `markRawImpressionUploadModelLineLabeling is idempotent with same request_id`() =
    runBlocking {
      // Non-memoized path: CREATED -> LABELING directly.
      val created = createModelLine()
      val requestId = UUID.randomUUID().toString()

      val first =
        service.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            this.requestId = requestId
          }
        )
      val second =
        service.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            this.requestId = requestId
          }
        )

      assertThat(first.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
        )
      assertThat(second).isEqualTo(first)
    }

  @Test
  fun `markRawImpressionUploadModelLineCompleted is idempotent with same request_id`() =
    runBlocking {
      val created = createModelLine()
      val labeling =
        service.markRawImpressionUploadModelLineLabeling(
          markRawImpressionUploadModelLineLabelingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            requestId = UUID.randomUUID().toString()
          }
        )
      val requestId = UUID.randomUUID().toString()

      val first =
        service.markRawImpressionUploadModelLineCompleted(
          markRawImpressionUploadModelLineCompletedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = labeling.etag
            this.requestId = requestId
          }
        )
      val second =
        service.markRawImpressionUploadModelLineCompleted(
          markRawImpressionUploadModelLineCompletedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = labeling.etag
            this.requestId = requestId
          }
        )

      assertThat(first.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
        )
      assertThat(second).isEqualTo(first)
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed is idempotent with same request_id`() = runBlocking {
    val created = createModelLine()
    val requestId = UUID.randomUUID().toString()

    val first =
      service.markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = created.etag
          this.requestId = requestId
          errorMessage = "boom"
        }
      )
    val second =
      service.markRawImpressionUploadModelLineFailed(
        markRawImpressionUploadModelLineFailedRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = created.etag
          this.requestId = requestId
          errorMessage = "boom"
        }
      )

    assertThat(first.state)
      .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
    assertThat(second).isEqualTo(first)
  }

  @Test
  fun `markRawImpressionUploadModelLineFailed preserves original error_message on same-request_id replay`() =
    runBlocking {
      val created = createModelLine()
      val requestId = UUID.randomUUID().toString()

      val first =
        service.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            this.requestId = requestId
            errorMessage = "first failure"
          }
        )
      // Same request_id, different error_message: the replay returns the first response as-is.
      val second =
        service.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
            etag = created.etag
            this.requestId = requestId
            errorMessage = "second failure"
          }
        )

      assertThat(first.errorMessage).isEqualTo("first failure")
      assertThat(second.errorMessage).isEqualTo("first failure")
    }

  @Test
  fun `markRawImpressionUploadModelLinePoolAssigning with different request_id after transition throws FAILED_PRECONDITION`() =
    runBlocking {
      val created = createModelLine()
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLineResourceId = created.rawImpressionUploadModelLineResourceId
          etag = created.etag
          requestId = UUID.randomUUID().toString()
        }
      )

      // A different request_id does NOT replay: the row is already POOL_ASSIGNING, which is not a
      // valid previous state for PoolAssigning, so this is a state violation (etag is current, so
      // it is not an etag mismatch).
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.markRawImpressionUploadModelLinePoolAssigning(
            markRawImpressionUploadModelLinePoolAssigningRequest {
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              rawImpressionUploadModelLineResourceId =
                created.rawImpressionUploadModelLineResourceId
              etag = currentEtag(created.rawImpressionUploadModelLineResourceId)
              requestId = UUID.randomUUID().toString()
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "dataProviders/dp1"
    private const val RAW_IMPRESSION_UPLOAD_RESOURCE_ID = "uploads/upload1"
    private const val CMMS_MODEL_LINE = "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 = "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
  }
}
