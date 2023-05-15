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

import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelLine as InternalModelLine
import org.wfanet.measurement.internal.kingdom.modelLine as internalModelLine
import com.google.protobuf.Timestamp
import java.time.Instant
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLine.Type
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createModelLineRequest
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.toProtoTime

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAHs"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAHs"
private const val MODEL_LINE_NAME = "$MODEL_SUITE_NAME/modelLines/AAAAAAAAAHs"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_LINE_ID =
  apiIdToExternalId(ModelLineKey.fromName(MODEL_LINE_NAME)!!.modelLineId)

private const val DISPLAY_NAME = "Display name"
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()
private val ACTIVE_START_TIME: Timestamp = Instant.ofEpochSecond(456).toProtoTime()

private val INTERNAL_MODEL_LINE: InternalModelLine = internalModelLine {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  externalModelLineId = EXTERNAL_MODEL_LINE_ID
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  activeStartTime = ACTIVE_START_TIME
  createTime = CREATE_TIME
}

private val MODEL_LINE: ModelLine = modelLine {
  name = MODEL_SUITE_NAME
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  activeStartTime = ACTIVE_START_TIME
  createTime = CREATE_TIME
}

@RunWith(JUnit4::class)
class ModelLinesServiceTest {

  private val internalModelLinesMock: ModelLinesCoroutineImplBase =
    mockService() {
      onBlocking { createModelLine(any()) }
        .thenReturn(INTERNAL_MODEL_LINE)
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule { addService(internalModelLinesMock) }

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
      modelLine = MODEL_LINE.copy {
        type = Type.PROD
      }
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelLine(request) }
      }

    /*val expected = MODEL_SUITE

    verifyProtoArgument(internalModelSuitesMock, ModelSuitesCoroutineImplBase::createModelSuite)
      .isEqualTo(
        INTERNAL_MODEL_SUITE.copy {
          clearCreateTime()
          clearExternalModelSuiteId()
        }
      )

    assertThat(result).isEqualTo(expected)*/
  }

}
