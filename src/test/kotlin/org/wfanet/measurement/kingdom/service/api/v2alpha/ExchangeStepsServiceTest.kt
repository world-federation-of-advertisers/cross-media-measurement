// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequestKt
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.GetExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.testing.makeDataProvider
import org.wfanet.measurement.api.v2alpha.testing.makeModelProvider
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as InternalExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest as internalClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse as internalClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.exchangeStep as internalExchangeStep
import org.wfanet.measurement.internal.kingdom.streamExchangeStepsRequest

private const val DATA_PROVIDER_ID = 12345L
private const val MODEL_PROVIDER_ID = 23456L
private val DATA_PROVIDER = makeDataProvider(DATA_PROVIDER_ID)
private val MODEL_PROVIDER = makeModelProvider(MODEL_PROVIDER_ID)
private const val DEFAULT_LIMIT = 50
private const val EXCHANGE_NAME = "recurringExchanges/AAAAAAAAAHs/exchanges/2021-03-14"
private const val RECURRING_EXCHANGE_ID = 1L
private val DATE = date {
  year = 2021
  month = 3
  day = 14
}
private const val EXCHANGE_ID = "2021-03-14"
private const val STEP_INDEX = 123
private const val ATTEMPT_NUMBER = 5
private val SERIALIZED_WORKFLOW = ByteString.copyFromUtf8("some-serialized-exchange-workflow")
private val UPDATE_TIME: Timestamp = Instant.ofEpochSecond(123).toProtoTime()

private val EXCHANGE_STEP = exchangeStep {
  val exchangeStepKey =
    ExchangeStepKey(
      recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID),
      exchangeId = EXCHANGE_ID,
      exchangeStepId = STEP_INDEX.toString()
    )
  name = exchangeStepKey.toName()
  state = ExchangeStep.State.READY_FOR_RETRY
  stepIndex = STEP_INDEX
  exchangeDate = DATE
  serializedExchangeWorkflow = SERIALIZED_WORKFLOW
}

private val EXCHANGE_STEP_ATTEMPT: String =
  ExchangeStepAttemptKey(
      recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID),
      exchangeId = EXCHANGE_ID,
      exchangeStepId = STEP_INDEX.toString(),
      exchangeStepAttemptId = ATTEMPT_NUMBER.toString()
    )
    .toName()

private val CLAIM_READY_EXCHANGE_STEP_RESPONSE: ClaimReadyExchangeStepResponse =
  claimReadyExchangeStepResponse {
    exchangeStep = EXCHANGE_STEP
    exchangeStepAttempt = EXCHANGE_STEP_ATTEMPT
  }

private val INTERNAL_EXCHANGE_STEP: InternalExchangeStep = internalExchangeStep {
  externalRecurringExchangeId = RECURRING_EXCHANGE_ID
  date = DATE
  stepIndex = STEP_INDEX
  state = InternalExchangeStep.State.READY_FOR_RETRY
  serializedExchangeWorkflow = SERIALIZED_WORKFLOW
  updateTime = UPDATE_TIME
}

@RunWith(JUnit4::class)
class ExchangeStepsServiceTest {

  private val internalService: InternalExchangeStepsCoroutineImplBase =
    mockService() {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(
          internalClaimReadyExchangeStepResponse {
            exchangeStep = INTERNAL_EXCHANGE_STEP
            attemptNumber = ATTEMPT_NUMBER
          }
        )
      onBlocking { streamExchangeSteps(any()) }.thenReturn(flowOf(INTERNAL_EXCHANGE_STEP))
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  private val service =
    ExchangeStepsService(InternalExchangeStepsCoroutineStub(grpcTestServerRule.channel))

  @Test
  fun getExchangeStep() =
    runBlocking<Unit> {
      assertFailsWith(NotImplementedError::class) {
        service.getExchangeStep(GetExchangeStepRequest.getDefaultInstance())
      }
    }

  private fun claimReadyExchangeStep(
    init: ClaimReadyExchangeStepRequestKt.Dsl.() -> Unit
  ): ClaimReadyExchangeStepResponse = runBlocking {
    service.claimReadyExchangeStep(claimReadyExchangeStepRequest(init))
  }

  private fun listExchangeSteps(
    init: ListExchangeStepsRequestKt.Dsl.() -> Unit
  ): ListExchangeStepsResponse = runBlocking {
    service.listExchangeSteps(listExchangeStepsRequest(init))
  }

  @Test
  fun `claimReadyExchangeStep unauthenticated`() {
    val e =
      assertFailsWith<StatusRuntimeException> {
        claimReadyExchangeStep { dataProvider = DATA_PROVIDER }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `claimReadyExchangeStep for DataProvider`() {
    val principal = DataProviderPrincipal(DataProviderKey(externalIdToApiId(DATA_PROVIDER_ID)))
    val provider = provider {
      type = Provider.Type.DATA_PROVIDER
      externalId = DATA_PROVIDER_ID
    }
    val response =
      withPrincipal(principal) { claimReadyExchangeStep { dataProvider = DATA_PROVIDER } }

    assertThat(response).isEqualTo(CLAIM_READY_EXCHANGE_STEP_RESPONSE)

    verifyProtoArgument(
        internalService,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep
      )
      .isEqualTo(internalClaimReadyExchangeStepRequest { this.provider = provider })
  }

  @Test
  fun `claimReadyExchangeStep for DataProvider with wrong parent in Request`() {
    val principal = DataProviderPrincipal(DataProviderKey(externalIdToApiId(DATA_PROVIDER_ID)))

    withPrincipal(principal) {
      assertFails { claimReadyExchangeStep { modelProvider = MODEL_PROVIDER } }
    }
  }

  @Test
  fun `listExchangeSteps unauthenticated`() {
    val e =
      assertFailsWith<StatusRuntimeException> {
        listExchangeSteps { filter = filter { dataProvider = MODEL_PROVIDER } }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `claimReadyExchangeStep for ModelProvider`() {
    val principal = ModelProviderPrincipal(ModelProviderKey(externalIdToApiId(MODEL_PROVIDER_ID)))
    val provider = provider {
      type = Provider.Type.MODEL_PROVIDER
      externalId = MODEL_PROVIDER_ID
    }

    val response =
      withPrincipal(principal) { claimReadyExchangeStep { modelProvider = MODEL_PROVIDER } }

    assertThat(response).isEqualTo(CLAIM_READY_EXCHANGE_STEP_RESPONSE)

    verifyProtoArgument(
        internalService,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep
      )
      .isEqualTo(internalClaimReadyExchangeStepRequest { this.provider = provider })
  }

  @Test
  fun `claimReadyExchangeStep for ModelProvider with wrong parent in Request`() {
    val principal = ModelProviderPrincipal(ModelProviderKey(externalIdToApiId(MODEL_PROVIDER_ID)))

    withPrincipal(principal) {
      assertFails { claimReadyExchangeStep { dataProvider = DATA_PROVIDER } }
    }
  }

  @Test
  fun `listExchangeSteps with page token uses filter with timestamp from page token`() {
    val principal = ModelProviderPrincipal(ModelProviderKey(externalIdToApiId(MODEL_PROVIDER_ID)))
    val provider = provider {
      type = Provider.Type.MODEL_PROVIDER
      externalId = MODEL_PROVIDER_ID
    }

    val result =
      withPrincipal(principal) {
        listExchangeSteps {
          parent = EXCHANGE_NAME
          pageToken = UPDATE_TIME.toByteArray().base64UrlEncode()
          filter = filter {
            modelProvider = MODEL_PROVIDER
            states += listOf(ExchangeStep.State.READY, ExchangeStep.State.READY_FOR_RETRY)
          }
        }
      }

    val expected = listExchangeStepsResponse {
      exchangeStep += EXCHANGE_STEP
      nextPageToken = UPDATE_TIME.toByteArray().base64UrlEncode()
    }

    val streamExchangeStepsRequest =
      captureFirst<StreamExchangeStepsRequest> {
        verify(internalService).streamExchangeSteps(capture())
      }

    assertThat(streamExchangeStepsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamExchangeStepsRequest {
          limit = DEFAULT_LIMIT
          filter =
            StreamExchangeStepsRequestKt.filter {
              this.principal = provider
              stepProvider = provider
              externalRecurringExchangeIds +=
                apiIdToExternalId(ExchangeKey.fromName(EXCHANGE_NAME)!!.recurringExchangeId)
              updatedAfter = UPDATE_TIME
              states +=
                listOf(InternalExchangeStep.State.READY, InternalExchangeStep.State.READY_FOR_RETRY)
              dates += DATE
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listExchangeSteps throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          listExchangeSteps {
            parent = EXCHANGE_NAME
            filter = filter { dataProvider = DATA_PROVIDER }
            pageSize = -1
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Page size cannot be less than 0")
  }
}
