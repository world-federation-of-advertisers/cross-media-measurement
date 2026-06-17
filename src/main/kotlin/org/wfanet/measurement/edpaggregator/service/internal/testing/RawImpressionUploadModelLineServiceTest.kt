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
import org.wfanet.measurement.internal.edpaggregator.CreateRawImpressionUploadModelLineRequest
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesRequestKt
import org.wfanet.measurement.internal.edpaggregator.ListRawImpressionUploadModelLinesResponse
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLine
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineServiceGrpcKt.RawImpressionUploadModelLineServiceCoroutineImplBase
import org.wfanet.measurement.internal.edpaggregator.RawImpressionUploadModelLineState
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
   * Creates a parent [RawImpressionUpload] row so that child model line rows
   * can be inserted (interleaved table).
   */
  protected abstract suspend fun createParentUpload(
    dataProviderResourceId: String,
    rawImpressionUploadResourceId: String,
  )

  @Before
  fun initService() {
    service = newService()
    runBlocking {
      createParentUpload(DATA_PROVIDER_RESOURCE_ID, RAW_IMPRESSION_UPLOAD_RESOURCE_ID)
    }
  }

  @Test
  fun `createRawImpressionUploadModelLine creates successfully`() = runBlocking {
    val startTime: Instant = Instant.now()

    val modelLine: RawImpressionUploadModelLine =
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
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
  fun `createRawImpressionUploadModelLine throws INVALID_ARGUMENT if data_provider_resource_id not set`() =
    runBlocking {
      val exception: StatusRuntimeException =
        assertFailsWith<StatusRuntimeException> {
          service.createRawImpressionUploadModelLine(
            createRawImpressionUploadModelLineRequest {
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
  fun `batchCreateRawImpressionUploadModelLines creates multiple`() = runBlocking {
    val response =
      service.batchCreateRawImpressionUploadModelLines(
        batchCreateRawImpressionUploadModelLinesRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          requests +=
            createRawImpressionUploadModelLineRequest {
              rawImpressionUploadModelLine = rawImpressionUploadModelLine {
                cmmsModelLine = CMMS_MODEL_LINE
              }
            }
          requests +=
            createRawImpressionUploadModelLineRequest {
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
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
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
          cmmsModelLine = CMMS_MODEL_LINE
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
            cmmsModelLine = "nonexistent-model-line"
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
              cmmsModelLine = CMMS_MODEL_LINE
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `getRawImpressionUploadModelLine throws INVALID_ARGUMENT if cmms_model_line not set`() =
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
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
    )
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
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
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE_2
        }
      }
    )

    service.markRawImpressionUploadModelLinePoolAssigning(
      markRawImpressionUploadModelLinePoolAssigningRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        cmmsModelLine = CMMS_MODEL_LINE
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
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
    )
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
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
            ListRawImpressionUploadModelLinesRequestKt.filter {
              cmmsModelLine = CMMS_MODEL_LINE
            }
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
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
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
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(
          RawImpressionUploadModelLineState
            .RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_POOL_ASSIGNING
        )
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking transitions POOL_ASSIGNING to RANKING`() =
    runBlocking {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
        }
      )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLineRanking(
          markRawImpressionUploadModelLineRankingRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_RANKING
        )
    }

  @Test
  fun `markRawImpressionUploadModelLineLabeling transitions RANKING to LABELING`() = runBlocking {
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        rawImpressionUploadModelLine = rawImpressionUploadModelLine {
          cmmsModelLine = CMMS_MODEL_LINE
        }
      }
    )
    service.markRawImpressionUploadModelLinePoolAssigning(
      markRawImpressionUploadModelLinePoolAssigningRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        cmmsModelLine = CMMS_MODEL_LINE
      }
    )
    service.markRawImpressionUploadModelLineRanking(
      markRawImpressionUploadModelLineRankingRequest {
        dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
        rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
        cmmsModelLine = CMMS_MODEL_LINE
      }
    )

    val modelLine: RawImpressionUploadModelLine =
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
        }
      )

    assertThat(modelLine.state)
      .isEqualTo(
        RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_LABELING
      )
  }

  @Test
  fun `markRawImpressionUploadModelLineCompleted transitions LABELING to COMPLETED`() =
    runBlocking {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
        }
      )
      service.markRawImpressionUploadModelLineRanking(
        markRawImpressionUploadModelLineRankingRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
        }
      )
      service.markRawImpressionUploadModelLineLabeling(
        markRawImpressionUploadModelLineLabelingRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
        }
      )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLineCompleted(
          markRawImpressionUploadModelLineCompletedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(
          RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_COMPLETED
        )
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed transitions from CREATED to FAILED`() = runBlocking {
    service.createRawImpressionUploadModelLine(
      createRawImpressionUploadModelLineRequest {
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
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
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
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          rawImpressionUploadModelLine = rawImpressionUploadModelLine {
            cmmsModelLine = CMMS_MODEL_LINE
          }
        }
      )
      service.markRawImpressionUploadModelLinePoolAssigning(
        markRawImpressionUploadModelLinePoolAssigningRequest {
          dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
          rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
          cmmsModelLine = CMMS_MODEL_LINE
        }
      )

      val modelLine: RawImpressionUploadModelLine =
        service.markRawImpressionUploadModelLineFailed(
          markRawImpressionUploadModelLineFailedRequest {
            dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
            rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
            cmmsModelLine = CMMS_MODEL_LINE
            errorMessage = "pool assignment failed"
          }
        )

      assertThat(modelLine.state)
        .isEqualTo(RawImpressionUploadModelLineState.RAW_IMPRESSION_UPLOAD_MODEL_LINE_STATE_FAILED)
    }

  @Test
  fun `markRawImpressionUploadModelLineFailed throws INVALID_ARGUMENT if error_message not set`() =
    runBlocking {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
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
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "error_message"
          }
        )
    }

  @Test
  fun `markRawImpressionUploadModelLineRanking throws FAILED_PRECONDITION from CREATED state`() =
    runBlocking {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
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
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `markRawImpressionUploadModelLineCompleted throws FAILED_PRECONDITION from CREATED state`() =
    runBlocking {
      service.createRawImpressionUploadModelLine(
        createRawImpressionUploadModelLineRequest {
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
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = CMMS_MODEL_LINE
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
              dataProviderResourceId = DATA_PROVIDER_RESOURCE_ID
              rawImpressionUploadResourceId = RAW_IMPRESSION_UPLOAD_RESOURCE_ID
              cmmsModelLine = "nonexistent-model-line"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }

  companion object {
    private const val DATA_PROVIDER_RESOURCE_ID = "dataProviders/dp1"
    private const val RAW_IMPRESSION_UPLOAD_RESOURCE_ID = "uploads/upload1"
    private const val CMMS_MODEL_LINE =
      "modelProviders/mp1/modelSuites/ms1/modelLines/ml1"
    private const val CMMS_MODEL_LINE_2 =
      "modelProviders/mp1/modelSuites/ms1/modelLines/ml2"
  }
}
