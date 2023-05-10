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

import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.protobuf.Timestamp
import java.time.Instant
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuiteKey
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.api.v2alpha.withModelProviderPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ModelSuite as InternalModelSuite
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.modelSuite as internalModelSuite

private const val MODEL_PROVIDER_NAME = "modelProviders/AAAAAAAAAA1"
private const val MODEL_PROVIDER_NAME_2 = "measurementConsumers/AAAAAAAAAA2"
private const val MODEL_SUITE_NAME = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAA1"
private const val MODEL_SUITE_NAME_2 = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAA2"
private const val MODEL_SUITE_NAME_3 = "$MODEL_PROVIDER_NAME/modelSuites/AAAAAAAAAA3"
private val EXTERNAL_MODEL_PROVIDER_ID =
  apiIdToExternalId(ModelProviderKey.fromName(MODEL_PROVIDER_NAME)!!.modelProviderId)
private val EXTERNAL_MODEL_SUITE_ID =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME)!!.modelSuiteId)
private val EXTERNAL_MODEL_SUITE_ID_2 =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME_2)!!.modelSuiteId)
private val EXTERNAL_MODEL_SUITE_ID_3 =
  apiIdToExternalId(ModelSuiteKey.fromName(MODEL_SUITE_NAME_3)!!.modelSuiteId)
private const val DISPLAY_NAME = "Display name"
private const val DESCRIPTION = "Description"
private val CREATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val INTERNAL_MODEL_SUITE: InternalModelSuite = internalModelSuite {
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID
  displayName = DISPLAY_NAME
  description = DESCRIPTION
  createTime = CREATE_TIME
}

private val MODEL_SUITE: ModelSuite = modelSuite {
  name = MODEL_SUITE_NAME
  displayName = DISPLAY_NAME
  description = DESCRIPTION
}

@RunWith(JUnit4::class)
class ModelSuitesServiceTest {

  private val internalModelSuitesMock: ModelSuitesCoroutineImplBase =
    mockService() {
      onBlocking { createModelSuite(any()) }.thenReturn(INTERNAL_MODEL_SUITE)
      onBlocking { getModelSuite(any()) }.thenReturn(INTERNAL_MODEL_SUITE)
      onBlocking { streamModelSuites(any()) }
        .thenReturn(
          flowOf(
            INTERNAL_MODEL_SUITE,
            INTERNAL_MODEL_SUITE.copy { externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_2 },
            INTERNAL_MODEL_SUITE.copy { externalModelSuiteId = EXTERNAL_MODEL_SUITE_ID_3 }
          )
        )
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalModelSuitesMock) }

  private lateinit var service: ModelSuitesService

  @Before
  fun initService() {
    service =
      ModelSuitesService(
        ModelSuitesGrpcKt.ModelSuitesCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `createModelSuite returns model suite`() {
    val request = createModelSuiteRequest {
      parent = MODEL_PROVIDER_NAME
      modelSuite = MODEL_SUITE
    }

    val result =
      withModelProviderPrincipal(MODEL_PROVIDER_NAME) {
        runBlocking { service.createModelSuite(request) }
      }

    val expected = MODEL_SUITE

    verifyProtoArgument(internalModelSuitesMock, ModelSuitesCoroutineImplBase::createModelSuite)
      .isEqualTo(
        INTERNAL_MODEL_SUITE.copy {
          clearCreateTime()
          clearExternalModelSuiteId()
        }
      )

    ProtoTruth.assertThat(result).isEqualTo(expected)
  }
}
