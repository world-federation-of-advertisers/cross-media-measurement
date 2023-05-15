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
import com.google.protobuf.Timestamp
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLine.Type
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.withDuchyPrincipal
import org.wfanet.measurement.api.v2alpha.withMeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.ModelLine.Type as InternalType
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.modelLine as internalModelLine

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

private const val DISPLAY_NAME = "Display name"
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()
private val ACTIVE_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()

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
          println(request)
          println("--------------------------------")
          println(request.externalModelSuiteId)
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
}
