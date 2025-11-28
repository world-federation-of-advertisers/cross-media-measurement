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
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.interval
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
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.doThrow
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.api.v2alpha.DataProviderKey
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
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesRequest
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesPageToken
import org.wfanet.measurement.api.v2alpha.listModelLinesRequest
import org.wfanet.measurement.api.v2alpha.listModelLinesResponse
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.setModelLineActiveEndTimeRequest
import org.wfanet.measurement.api.v2alpha.setModelLineHoldbackModelLineRequest
import org.wfanet.measurement.api.v2alpha.setModelLineTypeRequest
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ErrorCode
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequest
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.afterFilter
import org.wfanet.measurement.internal.kingdom.StreamModelLinesRequestKt.filter as internalFilter
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesRequest as internalEnumerateValidModelLinesRequest
import org.wfanet.measurement.internal.kingdom.enumerateValidModelLinesResponse as internalEnumerateValidModelLinesResponse
import org.wfanet.measurement.internal.kingdom.getModelLineRequest as internalGetModelLineRequest
import org.wfanet.measurement.internal.kingdom.modelLine as internalModelLine
import org.wfanet.measurement.internal.kingdom.setActiveEndTimeRequest as internalSetActiveEndTimeRequest
import org.wfanet.measurement.internal.kingdom.setModelLineHoldbackModelLineRequest as internalSetModelLineHoldbackModelLineRequest
import org.wfanet.measurement.internal.kingdom.setModelLineTypeRequest as internalSetModelLineTypeRequest
import org.wfanet.measurement.internal.kingdom.streamModelLinesRequest as internalStreamModelLinesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.InvalidFieldValueException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineInvalidArgsException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelLineTypeIllegalException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ModelSuiteNotFoundException

private const val DEFAULT_LIMIT = 50
private val TYPES: Set<InternalModelLine.Type> =
  setOf(InternalModelLine.Type.PROD, InternalModelLine.Type.DEV, InternalModelLine.Type.HOLDBACK)

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
  type = InternalModelLine.Type.PROD
}

private val MODEL_LINE: ModelLine = modelLine {
  name = MODEL_LINE_NAME
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  activeStartTime = ACTIVE_START_TIME
  createTime = CREATE_TIME
  updateTime = UPDATE_TIME
  type = Type.PROD
}

@RunWith(JUnit4::class)
class ModelLinesServiceTest {

  private val internalModelLinesMock: ModelLinesCoroutineImplBase = mockService {
    onBlocking { createModelLine(any()) }
      .thenAnswer {
        val request = it.getArgument<InternalModelLine>(0)
        if (request.externalModelSuiteId != EXTERNAL_MODEL_SUITE_ID) {
          failGrpc(Status.NOT_FOUND) { "ModelProvider not found" }
        } else {
          when (request.type) {
            InternalModelLine.Type.DEV ->
              INTERNAL_MODEL_LINE.copy { type = InternalModelLine.Type.DEV }
            InternalModelLine.Type.PROD ->
              INTERNAL_MODEL_LINE.copy { type = InternalModelLine.Type.PROD }
            InternalModelLine.Type.HOLDBACK ->
              INTERNAL_MODEL_LINE.copy { type = InternalModelLine.Type.HOLDBACK }
            InternalModelLine.Type.TYPE_UNSPECIFIED,
            InternalModelLine.Type.UNRECOGNIZED ->
              INTERNAL_MODEL_LINE.copy { type = InternalModelLine.Type.TYPE_UNSPECIFIED }
            else -> INTERNAL_MODEL_LINE.copy { type = InternalModelLine.Type.TYPE_UNSPECIFIED }
          }
        }
      }
    onBlocking { setActiveEndTime(any()) }
      .thenReturn(INTERNAL_MODEL_LINE.copy { activeEndTime = ACTIVE_END_TIME })
    onBlocking { setModelLineHoldbackModelLine(any()) }
      .thenReturn(
        INTERNAL_MODEL_LINE.copy { externalHoldbackModelLineId = EXTERNAL_HOLDBACK_MODEL_LINE_ID }
      )
    onBlocking { setModelLineType(any()) } doReturn INTERNAL_MODEL_LINE
    onBlocking { streamModelLines(any()) }
      .thenReturn(
        flowOf(
          INTERNAL_MODEL_LINE.copy { type = InternalModelLine.Type.PROD },
          INTERNAL_MODEL_LINE.copy {
            externalModelLineId = EXTERNAL_MODEL_LINE_ID_2
            type = InternalModelLine.Type.DEV
          },
          INTERNAL_MODEL_LINE.copy {
            externalModelLineId = EXTERNAL_MODEL_LINE_ID_3
            type = InternalModelLine.Type.HOLDBACK
          },
        )
      )
  }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelLinesMock) }

  private lateinit var service: ModelLinesService

  @Before
  fun initService() {
    service = ModelLinesService(ModelLinesCoroutineStub(grpcTestServerRule.channel))
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
          type = InternalModelLine.Type.PROD
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
  fun `getModelLine returns model line successfully`() {
    wheneverBlocking { internalModelLinesMock.getModelLine(any()) }.thenReturn(INTERNAL_MODEL_LINE)

    val modelLine =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking { service.getModelLine(getModelLineRequest { name = MODEL_LINE_NAME }) }
      }

    assertThat(modelLine).isEqualTo(MODEL_LINE)

    verifyProtoArgument(internalModelLinesMock, ModelLinesCoroutineImplBase::getModelLine)
      .isEqualTo(
        internalGetModelLineRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      )
  }

  @Test
  fun `getModelLine throws INVALID_ARGUMENT when name invalid`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.getModelLine(getModelLineRequest { name = "123" }) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.INVALID_ARGUMENT.code)
  }

  @Test
  fun `getModelLine throws UNAUTHENTICATED when missing principal`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          service.getModelLine(
            getModelLineRequest { name = "modelProviders/1/modelSuites/2/modelLines/3" }
          )
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.UNAUTHENTICATED.code)
  }

  @Test
  fun `getModelLine throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal("modelProviders/2") {
          runBlocking {
            service.getModelLine(
              getModelLineRequest { name = "modelProviders/1/modelSuites/2/modelLines/3" }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setModelLineActiveEndTime returns model line with active end time`() {
    val request = setModelLineActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.setModelLineActiveEndTime(request) }
      }

    val expected = MODEL_LINE.copy { activeEndTime = ACTIVE_END_TIME }

    verifyProtoArgument(internalModelLinesMock, ModelLinesCoroutineImplBase::setActiveEndTime)
      .isEqualTo(
        internalSetActiveEndTimeRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          activeEndTime = ACTIVE_END_TIME
        }
      )

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `setModelLineActiveEndTime throws INVALID_ARGUMENT when name is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineActiveEndTime(
              setModelLineActiveEndTimeRequest { activeEndTime = ACTIVE_END_TIME }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `setModelLineActiveEndTime throws UNAUTHENTICATED when no principal is found`() {
    val request = setModelLineActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.setModelLineActiveEndTime(request) }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `setModelLineActiveEndTime throws PERMISSION_DENIED when principal is data provider`() {
    val request = setModelLineActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DATA_PROVIDER_NAME) {
          runBlocking { service.setModelLineActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setModelLineActiveEndTime throws PERMISSION_DENIED when principal is duchy`() {
    val request = setModelLineActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) {
          runBlocking { service.setModelLineActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setModelLineActiveEndTime throws PERMISSION_DENIED when principal is measurement consumer`() {
    val request = setModelLineActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
          runBlocking { service.setModelLineActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setModelLineActiveEndTime throws PERMISSION_DENIED when model provider caller doesn't match`() {
    val request = setModelLineActiveEndTimeRequest {
      name = MODEL_LINE_NAME
      activeEndTime = ACTIVE_END_TIME
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking { service.setModelLineActiveEndTime(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `setHoldbackModelLine calls internal service method`() {
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
        ModelLinesCoroutineImplBase::setModelLineHoldbackModelLine,
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
  fun `setHoldbackModelLine calls internal service method to clear value`() {
    // Return a value with no holdback set.
    wheneverBlocking { internalModelLinesMock.setModelLineHoldbackModelLine(any()) } doReturn
      INTERNAL_MODEL_LINE
    // Request with no holdback.
    val request = setModelLineHoldbackModelLineRequest { name = MODEL_LINE_NAME }

    val response =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.setModelLineHoldbackModelLine(request) }
      }

    verifyProtoArgument(
        internalModelLinesMock,
        ModelLinesCoroutineImplBase::setModelLineHoldbackModelLine,
      )
      .isEqualTo(
        internalSetModelLineHoldbackModelLineRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
        }
      )
    assertThat(response).isEqualTo(MODEL_LINE)
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
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
      }
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.listModelLines(request) }
      }

    val expected = listModelLinesResponse {
      modelLines += MODEL_LINE.copy { type = Type.PROD }
      modelLines +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_2
          type = Type.DEV
        }
      modelLines +=
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
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
      }
    }

    val result =
      withDataProviderPrincipal(DATA_PROVIDER_NAME) {
        runBlocking { service.listModelLines(request) }
      }

    val expected = listModelLinesResponse {
      modelLines += MODEL_LINE.copy { type = Type.PROD }
      modelLines +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_2
          type = Type.DEV
        }
      modelLines +=
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
  fun `listModelLines requests ModelLines filtered by active interval`() {
    val request = listModelLinesRequest {
      parent = "modelProviders/-/modelSuites/-"
      filter = filter {
        activeIntervalContains = interval {
          startTime = ACTIVE_START_TIME
          endTime = ACTIVE_END_TIME
        }
      }
    }

    withDataProviderPrincipal(DATA_PROVIDER_NAME) {
      runBlocking { service.listModelLines(request) }
    }

    verifyProtoArgument(internalModelLinesMock, ModelLinesCoroutineImplBase::streamModelLines)
      .isEqualTo(
        internalStreamModelLinesRequest {
          limit = DEFAULT_LIMIT + 1
          filter = internalFilter { activeIntervalContains = request.filter.activeIntervalContains }
        }
      )
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
        externalModelProviderId = 987L
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
  fun `listModelLines with page token gets the next page`() {
    val request = listModelLinesRequest {
      parent = MODEL_SUITE_NAME
      pageSize = 2
      filter = filter {
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
      }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
      modelLines += MODEL_LINE.copy { type = Type.PROD }
      modelLines +=
        MODEL_LINE.copy {
          name = MODEL_LINE_NAME_2
          type = Type.DEV
        }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = request.pageSize
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
      }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
      }
      val listModelLinesPageToken = listModelLinesPageToken {
        pageSize = 2
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
        externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
        typeIn += Type.PROD
        typeIn += Type.DEV
        typeIn += Type.HOLDBACK
        lastModelLine = previousPageEnd {
          createTime = CREATE_TIME
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
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
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
              externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
              externalModelLineId = EXTERNAL_MODEL_LINE_ID
            }
          }
        }
      )
  }

  @Test
  fun `createModelLine throws NOT_FOUND with model suite name when model suite not found`() {
    internalModelLinesMock.stub {
      onBlocking { createModelLine(any()) }
        .thenThrow(
          ModelSuiteNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelSuite not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelLine(
              createModelLineRequest {
                parent = MODEL_SUITE_NAME
                modelLine = MODEL_LINE.copy { type = Type.PROD }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelSuite", MODEL_SUITE_NAME)
  }

  @Test
  fun `createModelLine throws INVALID_ARGUMENT with model line name and type when model line type illegal`() {
    internalModelLinesMock.stub {
      onBlocking { createModelLine(any()) }
        .thenThrow(
          ModelLineTypeIllegalException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              InternalModelLine.Type.DEV,
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT, "ModelLine type illegal")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelLine(
              createModelLineRequest {
                parent = MODEL_SUITE_NAME
                modelLine =
                  MODEL_LINE.copy {
                    type = Type.DEV
                    holdbackModelLine = MODEL_LINE_NAME_2
                  }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLineType", Type.DEV.toString())
  }

  @Test
  fun `createModelLine throws INVALID_ARGUMENT when field has invalid value`() {
    val fieldName = "active_start_time"
    internalModelLinesMock.stub {
      onBlocking { createModelLine(any()) }
        .thenThrow(
          InvalidFieldValueException(fieldName)
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.createModelLine(
              createModelLineRequest {
                parent = MODEL_SUITE_NAME
                modelLine = MODEL_LINE.copy { type = Type.DEV }
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = "halo.wfanet.org"
          reason = ErrorCode.INVALID_FIELD_VALUE.name
          metadata["fieldName"] = fieldName
        }
      )
  }

  @Test
  fun `setModelLineActiveEndTime throws NOT_FOUND with model line name when model line not found`() {
    internalModelLinesMock.stub {
      onBlocking { setActiveEndTime(any()) }
        .thenThrow(
          ModelLineNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelLine not found")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineActiveEndTime(
              setModelLineActiveEndTimeRequest {
                name = MODEL_LINE_NAME
                activeEndTime = ACTIVE_END_TIME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
  }

  @Test
  fun `setModelLineActiveEndTime throws INVALID_ARGUEMENT with model line name when model line args invalid`() {
    internalModelLinesMock.stub {
      onBlocking { setActiveEndTime(any()) }
        .thenThrow(
          ModelLineInvalidArgsException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              "ModelLine args invalid",
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineActiveEndTime(
              setModelLineActiveEndTimeRequest {
                name = MODEL_LINE_NAME
                activeEndTime = ACTIVE_END_TIME
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
  }

  @Test
  fun `setModelLineHoldbackModelLine throws NOT_FOUND with model line name when model line not found`() {
    internalModelLinesMock.stub {
      onBlocking { setModelLineHoldbackModelLine(any()) }
        .thenThrow(
          ModelLineNotFoundException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "ModelLine not found")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineHoldbackModelLine(
              setModelLineHoldbackModelLineRequest {
                name = MODEL_LINE_NAME
                holdbackModelLine = MODEL_LINE_NAME_2
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
  }

  @Test
  fun `setModelLineHoldbackModelLine throws INVALID_ARGUMENT with model line name and type when model line type illegal`() {
    internalModelLinesMock.stub {
      onBlocking { setModelLineHoldbackModelLine(any()) }
        .thenThrow(
          ModelLineTypeIllegalException(
              ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
              ExternalId(EXTERNAL_MODEL_SUITE_ID),
              ExternalId(EXTERNAL_MODEL_LINE_ID),
              InternalModelLine.Type.HOLDBACK,
            )
            .asStatusRuntimeException(Status.Code.INVALID_ARGUMENT, "ModelLine type illegal")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.setModelLineHoldbackModelLine(
              setModelLineHoldbackModelLineRequest {
                name = MODEL_LINE_NAME
                holdbackModelLine = MODEL_LINE_NAME_2
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo?.metadataMap).containsEntry("modelLine", MODEL_LINE_NAME)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("modelLineType", Type.HOLDBACK.toString())
  }

  @Test
  fun `setModelLineType calls internal service method`() {
    val request = setModelLineTypeRequest {
      name = MODEL_LINE_NAME
      type = Type.PROD
    }

    val response =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.setModelLineType(request) }
      }

    verifyProtoArgument(internalModelLinesMock, ModelLinesCoroutineImplBase::setModelLineType)
      .isEqualTo(
        internalSetModelLineTypeRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalModelLineId = EXTERNAL_MODEL_LINE_ID
          type = InternalModelLine.Type.PROD
        }
      )
    assertThat(response).isEqualTo(MODEL_LINE)
  }

  @Test
  fun `setModelLineType throws INVALID_ARGUMENT when request type is HOLDBACK`() {
    val request = setModelLineTypeRequest {
      name = MODEL_LINE_NAME
      type = Type.HOLDBACK
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.setModelLineType(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("type")
  }

  @Test
  fun `setModelLineType throws MODEL_LINE_NOT_FOUND when ModelLine is not found`() {
    wheneverBlocking { internalModelLinesMock.setModelLineType(any()) } doThrow
      ModelLineNotFoundException(
          ExternalId(EXTERNAL_MODEL_PROVIDER_ID),
          ExternalId(EXTERNAL_MODEL_SUITE_ID),
          ExternalId(EXTERNAL_MODEL_LINE_ID),
        )
        .asStatusRuntimeException(Status.Code.NOT_FOUND)
    val request = setModelLineTypeRequest {
      name = MODEL_LINE_NAME
      type = Type.PROD
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking { service.setModelLineType(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = ErrorCode.MODEL_LINE_NOT_FOUND.name
          metadata[Errors.Metadata.MODEL_LINE.key] = MODEL_LINE_NAME
        }
      )
  }

  @Test
  fun `enumerateValidModelLines throws PERMISSION_DENIED when wrong model provider`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME_2) {
          runBlocking {
            service.enumerateValidModelLines(
              enumerateValidModelLinesRequest {
                parent = MODEL_SUITE_NAME
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                dataProviders += DATA_PROVIDER_NAME
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `enumerateValidModelLines returns model lines`() = runBlocking {
    whenever(internalModelLinesMock.enumerateValidModelLines(any()))
      .thenReturn(internalEnumerateValidModelLinesResponse { modelLines += INTERNAL_MODEL_LINE })

    val response =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking {
          service.enumerateValidModelLines(
            enumerateValidModelLinesRequest {
              parent = MODEL_SUITE_NAME
              timeInterval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              dataProviders += DATA_PROVIDER_NAME
            }
          )
        }
      }

    assertThat(response).isEqualTo(enumerateValidModelLinesResponse { modelLines += MODEL_LINE })

    verifyProtoArgument(
        internalModelLinesMock,
        ModelLinesCoroutineImplBase::enumerateValidModelLines,
      )
      .isEqualTo(
        internalEnumerateValidModelLinesRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalDataProviderIds +=
            apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
          timeInterval = interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          types += InternalModelLine.Type.PROD
        }
      )
  }

  @Test
  fun `enumerateValidModelLines is successful with mc principal`(): Unit = runBlocking {
    whenever(internalModelLinesMock.enumerateValidModelLines(any()))
      .thenReturn(internalEnumerateValidModelLinesResponse { modelLines += INTERNAL_MODEL_LINE })

    val response =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        runBlocking {
          service.enumerateValidModelLines(
            enumerateValidModelLinesRequest {
              parent = MODEL_SUITE_NAME
              timeInterval = interval {
                startTime = timestamp { seconds = 100 }
                endTime = timestamp { seconds = 200 }
              }
              dataProviders += DATA_PROVIDER_NAME
            }
          )
        }
      }

    assertThat(response).isEqualTo(enumerateValidModelLinesResponse { modelLines += MODEL_LINE })

    verifyProtoArgument(
        internalModelLinesMock,
        ModelLinesCoroutineImplBase::enumerateValidModelLines,
      )
      .isEqualTo(
        internalEnumerateValidModelLinesRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
          externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
          externalDataProviderIds +=
            apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
          timeInterval = interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          types += InternalModelLine.Type.PROD
        }
      )
  }

  @Test
  fun `enumerateValidModelLines requests ModelLines across providers and suites`() = runBlocking {
    val request = enumerateValidModelLinesRequest {
      parent = "modelProviders/-/modelSuites/-"
      timeInterval = interval {
        startTime = timestamp { seconds = 100 }
        endTime = timestamp { seconds = 200 }
      }
      dataProviders += DATA_PROVIDER_NAME
    }

    runBlocking {
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_NAME) {
        service.enumerateValidModelLines(request)
      }
    }

    verifyProtoArgument(
        internalModelLinesMock,
        ModelLinesCoroutineImplBase::enumerateValidModelLines,
      )
      .isEqualTo(
        internalEnumerateValidModelLinesRequest {
          externalDataProviderIds +=
            apiIdToExternalId(DataProviderKey.fromName(DATA_PROVIDER_NAME)!!.dataProviderId)
          timeInterval = request.timeInterval
          types += InternalModelLine.Type.PROD
        }
      )
  }

  @Test
  fun `enumerateValidModelLines throws PERMISSION_DENIED when wrong principal`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDuchyPrincipal(DUCHY_NAME) {
          runBlocking {
            service.enumerateValidModelLines(
              enumerateValidModelLinesRequest {
                parent = MODEL_SUITE_NAME
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                dataProviders += DATA_PROVIDER_NAME
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.enumerateValidModelLines(
              enumerateValidModelLinesRequest {
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                dataProviders += DATA_PROVIDER_NAME
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("parent")
  }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when parent is invalid`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
          runBlocking {
            service.enumerateValidModelLines(
              enumerateValidModelLinesRequest {
                parent = "invalid_name"
                timeInterval = interval {
                  startTime = timestamp { seconds = 100 }
                  endTime = timestamp { seconds = 200 }
                }
                dataProviders += DATA_PROVIDER_NAME
              }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("parent")
  }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  dataProviders += DATA_PROVIDER_NAME
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time_interval")
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval start_time is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  timeInterval = interval {
                    startTime = timestamp { seconds = Long.MAX_VALUE }
                    endTime = timestamp { seconds = 200 }
                  }
                  dataProviders += DATA_PROVIDER_NAME
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time_interval.start_time")
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval end_time is invalid`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = Long.MAX_VALUE }
                  }
                  dataProviders += DATA_PROVIDER_NAME
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time_interval.end_time")
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval start equals end`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 100 }
                  }
                  dataProviders += DATA_PROVIDER_NAME
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time_interval")
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when time_interval start greater than end`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  timeInterval = interval {
                    startTime = timestamp { seconds = 200 }
                    endTime = timestamp { seconds = 100 }
                  }
                  dataProviders += DATA_PROVIDER_NAME
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("time_interval")
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when data_providers is missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("data_providers")
    }

  @Test
  fun `enumerateValidModelLines throws INVALID_ARGUMENT when dataProviders has invalid name`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
            runBlocking {
              service.enumerateValidModelLines(
                enumerateValidModelLinesRequest {
                  parent = MODEL_SUITE_NAME
                  timeInterval = interval {
                    startTime = timestamp { seconds = 100 }
                    endTime = timestamp { seconds = 200 }
                  }
                  dataProviders += "invalid_name"
                }
              )
            }
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("data_providers")
    }
}
