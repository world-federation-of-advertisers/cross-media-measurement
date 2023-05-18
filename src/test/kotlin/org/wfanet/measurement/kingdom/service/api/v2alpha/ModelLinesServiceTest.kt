/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.ListModelLinesPageTokenKt.previousPageEnd
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequest
import org.wfanet.measurement.api.v2alpha.ListModelLinesRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLine.Type
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.setActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLine.Type as InternalType
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter as internalFilter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.modelLine as internalModelLine
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest as internalsetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest as internalSetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest as internalStreamModelLinesRequest

private const val DEFAULT_LIMIT = 50
private val TYPES: Set<InternalType> =
  setOf(
    InternalType.PROD,
    InternalType.DEV,
    InternalType.HOLDBACK,
  )

private const val MEASUREMENT_CONSUMER_NAME = "measurementConsumers/AAAAAAAAAHs"
private const val DUCHY_NAME = "duchies/AAAAAAAAAHs"
private const val DATA_PROVIDER_NAME = "dataProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_PROVIDER_NAME_2 = "modelProviders/BBBBBBBBBHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME_2/modelSuites/AAAAAAAAAJs"
private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAHs"
private const val MODEL_LINE_NAME_2 = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAJs"
private const val MODEL_LINE_NAME_3 = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAKs"
private const val MODEL_LINE_NAME_4 = "$MODEL_SUITE_NAME_2/modelLines/AAAAAAAAAHs"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_LINE_ID =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME)!!.modelLineId)
private val EXTERNAL_MODEL_LINE_ID_2 =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME_2)!!.modelLineId)
private val EXTERNAL_MODEL_LINE_ID_3 =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME_3)!!.modelLineId)
private val EXTERNAL_HOLDBACK_MODEL_LINE_ID =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME_2)!!.modelLineId)

private const val DISPLAY_NAME = "Display name"
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val ACTIVE_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val ACTIVE_END_TIME: Timestamp = Instant.ofEpochSecond(789).toProtoTime()

private val INTERNAL_MODEL_LINE: InternalModelLine = internalModelLine {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelLineId = EXTERNAL_MODEL_LINE_ID
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  activeStartTime = ACTIVE_START_TIME
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

private val MODEL_LINE: ModelLine = modelLine {
  name = MODEL_LINE_NAME
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  activeStartTime = ACTIVE_START_TIME
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
}

@RunWith(JUnit4::class)
class ModelLinesServiceTest {

  private val internalModelLinesMock: ModelLinesCoroutineImplBase =
    mockService() {
      onBlocking { createModelLine(any()) }
        .thenAnswer {
          val request = it.getArgument<InternalModelLine>(0)
          if (request.externalModelSuiteId != 123L) {
            failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
          } else {
            when (request.type) {
              InternalType.DEV -> INTERNAL_MODEL_LINE.copy { type = InternalType.DEV }
              InternalType.PROD -> INTERNAL_MODEL_LINE.copy { type = InternalType.PROD }
              InternalType.HOLDBACK -> INTERNAL_MODEL_LINE.copy { type = InternalType.HOLDBACK }
              InternalType.TYPE_UNSPECIFIED,
              InternalType.UNRECOGNIZED ->
                INTERNAL_MODEL_LINE.copy { type = InternalType.TYPE_UNSPECIFIED }
              else -> INTERNAL_MODEL_LINE.copy { type = InternalType.TYPE_UNSPECIFIED }
            }
          }
        }
      onBlocking { setActiveEndTime(any()) }
        .thenReturn(INTERNAL_MODEL_LINE.copy { activeEndTime = ACTIVE_END_TIME })
      onBlocking { setModelLineHoldbackModelLine(any()) }
        .thenReturn(
          INTERNAL_MODEL_LINE.copy { externalHoldbackModelLineId = EXTERNAL_HOLDBACK_MODEL_LINE_ID }
        )
      onBlocking { streamModelLines(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_LINE.copy { type = InternalType.PROD },
            INTERNAL_MODEL_LINE.copy {
              externalModelLineId = EXTERNAL_MODEL_LINE_ID_2
              type = InternalType.DEV
            },
            INTERNAL_MODEL_LINE.copy {
              externalModelLineId = EXTERNAL_MODEL_LINE_ID_3
              type = InternalType.HOLDBACK
            }
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelLinesMock) }

  private lateinit var service: ModelLinesService

  @Before
  fun initService() {
    service =
      ModelLinesService(
        ModelLinesCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `createModelLine returns model line`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME
      modelLine = MODEL_LINE.copy { type = Type.PROD }
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelLine(request) }
      }

    val expected = MODEL_LINE.copy { type = Type.PROD }

    verifyProtoArgument(internalModelLinesMock, ModelLinesCoroutineImplBase::createModelLine)
      .isEqualTo(
        INTERNAL_MODEL_LINE.copy {
          clearCreateTime()
          clearUpdateTime()
          clearExternalModelLineId()
          type = InternalType.PROD
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createModelLine throws UNAUTHENTICATED when no principal is found`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME
      modelLine = MODEL_LINE.copy { type = Type.PROD }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createModelLine(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createModelLine throws PERMISSION_DENIED when principal is data provider`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME
      modelLine = MODEL_LINE.copy { type = Type.PROD }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.createModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelLine throws PERMISSION_DENIED when principal is duchy`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME
      modelLine = MODEL_LINE.copy { type = Type.PROD }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.createModelLine(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelLine throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME
      modelLine = MODEL_LINE.copy { type = Type.PROD }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.createModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelLine throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME
      modelLine = MODEL_LINE.copy { type = Type.PROD }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createModelLine throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.createModelLine(createModelLineRequest { modelLine = MODEL_LINE }) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createModelLine throws NOT_FOUND if model suite is not found`() {
    val request = createModelLineRequest {
      parent = MODEL_SUITE_NAME_2
      modelLine = modelLine {
        name = MODEL_LINE_NAME_4
        displayName = DISPLAY_NAME
        description = DESCRIPTION
        activeStartTime = ACTIVE_START_TIME
        type = Type.PROD
        createTime = CREATE_TIME
        updateTime = UPDATE_TIME
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.createModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `setActiveEndTime returns model line with active end time`() {
    val request = setActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.setActiveEndTime(request) }
      }

    val expected = MODEL_LINE.copy { activeEndTime = ACTIVE_END_TIME }

    verifyProtoArgument(internalModelLinesMock, ModelLinesCoroutineImplBase::setActiveEndTime)
      .isEqualTo(
        internalsetActiveEndTimeRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          activeEndTime = ACTIVE_END_TIME
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `setActiveEndTime throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setActiveEndTime(setActiveEndTimeRequest { activeEndTime = ACTIVE_END_TIME })
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `setActiveEndTime throws UNAUTHENTICATED when no principal is found`() {
    val request = setActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.setActiveEndTime(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `setActiveEndTime throws PERMISSION_DENIED when principal is data provider`() {
    val request = setActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.setActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setActiveEndTime throws PERMISSION_DENIED when principal is duchy`() {
    val request = setActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.setActiveEndTime(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setActiveEndTime throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = setActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.setActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setActiveEndTime throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = setActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.setActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setHoldbackModelLine returns model line with holdback model line`() {
    val request = setModelLineHoldbackModelLineRequest {
      name = MODEL_LINE_NAME
      holdbackModelLine = MODEL_LINE_NAME_2
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.setModelLineHoldbackModelLine(request) }
      }

    val expected = MODEL_LINE.copy { holdbackModelLine = MODEL_LINE_NAME_2 }

    verifyProtoArgument(
      internalModelLinesMock,
      ModelLinesCoroutineImplBase::setModelLineHoldbackModelLine
    )
      .isEqualTo(
        internalSetModelLineHoldbackModelLineRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          externalHoldbackModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalHoldbackModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalHoldbackModelLineId = EXTERNAL_HOLDBACK_MODEL_LINE_ID
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `setHoldbackModelLine throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineHoldbackModelLine(
              setModelLineHoldbackModelLineRequest { holdbackModelLine = MODEL_LINE_NAME_2 }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `setHoldbackModelLine throws INVALID_ARGUMENT when holdback model line is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineHoldbackModelLine(
              setModelLineHoldbackModelLineRequest { name = MODEL_LINE_NAME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `setHoldbackModelLine throws UNAUTHENTICATED when no principal is found`() {
    val request = setModelLineHoldbackModelLineRequest {
      name = MODEL_LINE_NAME
      holdbackModelLine = MODEL_LINE_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.setModelLineHoldbackModelLine(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `setHoldbackModelLine throws PERMISSION_DENIED when principal is data provider`() {
    val request = setModelLineHoldbackModelLineRequest {
      name = MODEL_LINE_NAME
      holdbackModelLine = MODEL_LINE_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.setModelLineHoldbackModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setHoldbackModelLine throws PERMISSION_DENIED when principal is duchy`() {
    val request = setModelLineHoldbackModelLineRequest {
      name = MODEL_LINE_NAME
      holdbackModelLine = MODEL_LINE_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) {
          runBlocking { service.setModelLineHoldbackModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setHoldbackModelLine throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = setModelLineHoldbackModelLineRequest {
      name = MODEL_LINE_NAME
      holdbackModelLine = MODEL_LINE_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.setModelLineHoldbackModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setHoldbackModelLine throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = setModelLineHoldbackModelLineRequest {
      name = MODEL_LINE_NAME
      holdbackModelLine = MODEL_LINE_NAME_2
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.setModelLineHoldbackModelLine(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelLines with parent uses filter with parent succeeds for model provider caller`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      filter = filter {
        type += Type.PROD
        type += Type.DEV
        type += Type.HOLDBACK
      }
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listModelLines(request) }
      }

    val expected = listModelLinesResponse {
      modelLine += MODEL_LINE.copy { type = Type.PROD }
      modelLine +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_2
          type = Type.DEV
        }
      modelLine +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_3
          type = Type.HOLDBACK
        }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelLinesRequest> {
        verify(internalModelLinesMock).streamModelLines(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelLinesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            type += TYPES
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelLines with parent uses filter with parent succeeds for data provider caller`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      filter = filter {
        type += Type.PROD
        type += Type.DEV
        type += Type.HOLDBACK
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelLines(request) }
      }

    val expected = listModelLinesResponse {
      modelLine += MODEL_LINE.copy { type = Type.PROD }
      modelLine +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_2
          type = Type.DEV
        }
      modelLine +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_3
          type = Type.HOLDBACK
        }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelLinesRequest> {
        verify(internalModelLinesMock).streamModelLines(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelLinesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            type += TYPES
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelLines throws UNAUTHENTICATED when no principal is found`() {
    val request = listModelLinesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listModelLines(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listModelLines throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = listModelLinesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.listModelLines(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelLines throws INVALID_ARGUMENT when parent is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelLines(ListModelLinesRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelLines throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.listModelLines(
              listModelLinesRequest {
                parent = MODEL_PROVIDER_NAME
                pageSize = -1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelLines throws invalid argument when parent doesn't match parent in page token`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = 987
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      }
      pageToken = listModelLinesPageToken.toByteArray().base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.listModelLines(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listModelLines throws PERMISSION_DENIED when principal is duchy`() {
    val request = listModelLinesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) { runBlocking { service.listModelLines(request) } }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelLines throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = listModelLinesRequest { parent = MODEL_SUITE_NAME }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.listModelLines(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listModelSuites with page token gets the next page`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      pageSize = 2
      filter = filter {
        type += Type.PROD
        type += Type.DEV
        type += Type.HOLDBACK
      }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        types += Type.PROD
        types += Type.DEV
        types += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      }
      pageToken = listModelLinesPageToken.toByteArray().base64UrlEncode()
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelLines(request) }
      }

    val expected = listModelLinesResponse {
      modelLine += MODEL_LINE.copy { type = Type.PROD }
      modelLine +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_2
          type = Type.DEV
        }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        types += Type.PROD
        types += Type.DEV
        types += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelLineId = EXTERNAL_MODEL_LINE_ID_2
        }
      }
      nextPageToken = listModelLinesPageToken.toByteArray().base64UrlEncode()
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelLinesRequest> {
        verify(internalModelLinesMock).streamModelLines(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelLinesRequest {
          limit = request.pageSize + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            type += TYPES
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
            }
          }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listModelLines with new page size replaces page size in page token`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      pageSize = 4
      filter = filter {
        type += Type.PROD
        type += Type.DEV
        type += Type.HOLDBACK
      }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        types += Type.PROD
        types += Type.DEV
        types += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      }
      pageToken = listModelLinesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelLines(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelLinesRequest> {
        verify(internalModelLinesMock).streamModelLines(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelLinesRequest {
          limit = request.pageSize + 1
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            type += TYPES
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
            }
          }
        }
      )
  }

  @Test
  fun `listModelLines with no page size uses page size in page token`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      filter = filter {
        type += Type.PROD
        type += Type.DEV
        type += Type.HOLDBACK
      }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        types += Type.PROD
        types += Type.DEV
        types += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      }
      pageToken = listModelLinesPageToken.toByteArray().base64UrlEncode()
    }

    withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
      runBlocking { service.listModelLines(request) }
    }

    val streamModelLinesRequest =
      captureFirst<StreamModelLinesRequest> {
        verify(internalModelLinesMock).streamModelLines(capture())
      }

    assertThat(streamModelLinesRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        internalStreamModelLinesRequest {
          limit = 3
          filter = internalFilter {
            externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
            externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
            type += TYPES
            after = afterFilter {
              createTime = CREATE_TIME
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
            }
          }
        }
      )
  }
}
