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
import com.google.protobuf.any
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.mockito.kotlin.verify
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepKey
import org.wfanet.measurement.api.v2alpha.CanonicalRecurringExchangeKey
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequestKt
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsPageToken
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsPageTokenKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt.filter
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.listExchangeStepsPageToken
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsResponse
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.captureFirst
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as InternalExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase as InternalRecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub as InternalRecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest as internalClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse as internalClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.exchangeStep as internalExchangeStep
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.streamExchangeStepsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RecurringExchangeNotFoundException

private const val DEFAULT_LIMIT = 50
private const val EXCHANGE_NAME = "recurringExchanges/AAAAAAAAAHs/exchanges/2021-03-14"
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

@RunWith(JUnit4::class)
class ExchangeStepsServiceTest {
  private val internalRecurringExchangesServiceMock: InternalRecurringExchangesCoroutineImplBase =
    mockService {
      onBlocking { getRecurringExchange(any()) }.thenReturn(INTERNAL_RECURRING_EXCHANGE)
    }

  private val internalServiceMock: InternalExchangeStepsCoroutineImplBase = mockService {
    onBlocking { claimReadyExchangeStep(any()) }
      .thenReturn(
        internalClaimReadyExchangeStepResponse {
          exchangeStep = INTERNAL_EXCHANGE_STEP
          attemptNumber = ATTEMPT_NUMBER
        }
      )
    onBlocking { streamExchangeSteps(any()) }
      .thenReturn(flowOf(INTERNAL_EXCHANGE_STEP, INTERNAL_EXCHANGE_STEP_2))
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalRecurringExchangesServiceMock)
    addService(internalServiceMock)
  }

  private val service =
    ExchangeStepsService(
      InternalRecurringExchangesCoroutineStub(grpcTestServerRule.channel),
      InternalExchangeStepsCoroutineStub(grpcTestServerRule.channel),
    )

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
  fun `claimReadyExchangeStep throws UNAUTHENTICATED when principal is missing`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        claimReadyExchangeStep { parent = DATA_PROVIDER_KEY.toName() }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `claimReadyExchangeStep throws INVALID_ARGUMENT when parent is missing`() {
    val principal = DataProviderPrincipal(DATA_PROVIDER_KEY)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(principal) { claimReadyExchangeStep {} }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `claimReadyExchangeStep for DataProvider`() {
    val principal = DataProviderPrincipal(DATA_PROVIDER_KEY)
    val response =
      withPrincipal(principal) { claimReadyExchangeStep { parent = DATA_PROVIDER_KEY.toName() } }

    assertThat(response).isEqualTo(CLAIM_READY_EXCHANGE_STEP_RESPONSE)

    verifyProtoArgument(
        internalServiceMock,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep,
      )
      .isEqualTo(
        internalClaimReadyExchangeStepRequest {
          externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
        }
      )
  }

  @Test
  fun `claimReadyExchangeStep throws PERMISSION_DENIED when principal mismatches`() {
    val principal = DataProviderPrincipal(DATA_PROVIDER_KEY)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(principal) { claimReadyExchangeStep { parent = MODEL_PROVIDER_KEY.toName() } }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `claimReadyExchangeStep for ModelProvider`() {
    val principal = ModelProviderPrincipal(MODEL_PROVIDER_KEY)

    val response =
      withPrincipal(principal) { claimReadyExchangeStep { parent = MODEL_PROVIDER_KEY.toName() } }

    assertThat(response).isEqualTo(CLAIM_READY_EXCHANGE_STEP_RESPONSE)

    verifyProtoArgument(
        internalServiceMock,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep,
      )
      .isEqualTo(
        internalClaimReadyExchangeStepRequest {
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value
        }
      )
  }

  @Test
  fun `listExchangeSteps unauthenticated`() {
    val e =
      assertFailsWith<StatusRuntimeException> {
        listExchangeSteps { filter = filter { dataProvider = MODEL_PROVIDER_KEY.toName() } }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listExchangeSteps returns next page token when there are more results`() {
    val principal = ModelProviderPrincipal(MODEL_PROVIDER_KEY)
    val states = listOf(ExchangeStep.State.READY, ExchangeStep.State.READY_FOR_RETRY)

    val response: ListExchangeStepsResponse =
      withPrincipal(principal) {
        listExchangeSteps {
          parent = EXCHANGE_NAME
          filter = filter {
            modelProvider = MODEL_PROVIDER_KEY.toName()
            this.states += states
          }
          pageSize = 1
        }
      }

    val streamExchangeStepsRequest: StreamExchangeStepsRequest = captureFirst {
      verify(internalServiceMock).streamExchangeSteps(capture())
    }
    assertThat(streamExchangeStepsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamExchangeStepsRequest {
          limit = 2
          filter =
            StreamExchangeStepsRequestKt.filter {
              externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value
              this.states += InternalExchangeStep.State.READY
              this.states += InternalExchangeStep.State.READY_FOR_RETRY
              dates += DATE
            }
        }
      )

    assertThat(response)
      .ignoringFields(ListExchangeStepsResponse.NEXT_PAGE_TOKEN_FIELD_NUMBER)
      .isEqualTo(listExchangeStepsResponse { exchangeSteps += EXCHANGE_STEP })
    assertThat(ListExchangeStepsPageToken.parseFrom(response.nextPageToken.base64UrlDecode()))
      .isEqualTo(
        listExchangeStepsPageToken {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
          dates += DATE
          this.states += states
          externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value

          lastExchangeStep =
            ListExchangeStepsPageTokenKt.previousPageEnd {
              externalRecurringExchangeId = INTERNAL_EXCHANGE_STEP.externalRecurringExchangeId
              date = INTERNAL_EXCHANGE_STEP.date
              stepIndex = INTERNAL_EXCHANGE_STEP.stepIndex
            }
        }
      )
  }

  @Test
  fun `listExchangeSteps sets after filter when request has page token`() {
    val principal = ModelProviderPrincipal(MODEL_PROVIDER_KEY)
    val states = listOf(ExchangeStep.State.READY, ExchangeStep.State.READY_FOR_RETRY)
    val pageToken = listExchangeStepsPageToken {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
      dates += DATE
      this.states += states
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value

      lastExchangeStep =
        ListExchangeStepsPageTokenKt.previousPageEnd {
          externalRecurringExchangeId = INTERNAL_EXCHANGE_STEP.externalRecurringExchangeId
          date = INTERNAL_EXCHANGE_STEP.date
          stepIndex = INTERNAL_EXCHANGE_STEP.stepIndex
        }
    }
    internalServiceMock.stub {
      onBlocking { streamExchangeSteps(any()) }.thenReturn(flowOf(INTERNAL_EXCHANGE_STEP_2))
    }

    val response: ListExchangeStepsResponse =
      withPrincipal(principal) {
        listExchangeSteps {
          parent = EXCHANGE_NAME
          this.pageToken = pageToken.toByteString().base64UrlEncode()
          filter = filter {
            modelProvider = MODEL_PROVIDER_KEY.toName()
            this.states += states
          }
        }
      }

    val streamExchangeStepsRequest: StreamExchangeStepsRequest = captureFirst {
      verify(internalServiceMock).streamExchangeSteps(capture())
    }
    assertThat(streamExchangeStepsRequest)
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        streamExchangeStepsRequest {
          limit = DEFAULT_LIMIT + 1
          filter =
            StreamExchangeStepsRequestKt.filter {
              externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
              externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value
              this.states += InternalExchangeStep.State.READY
              this.states += InternalExchangeStep.State.READY_FOR_RETRY
              dates += DATE

              after =
                StreamExchangeStepsRequestKt.orderedKey {
                  externalRecurringExchangeId = INTERNAL_EXCHANGE_STEP.externalRecurringExchangeId
                  date = INTERNAL_EXCHANGE_STEP.date
                  stepIndex = INTERNAL_EXCHANGE_STEP.stepIndex
                }
            }
        }
      )

    assertThat(response).isEqualTo(listExchangeStepsResponse { exchangeSteps += EXCHANGE_STEP_2 })
  }

  @Test
  fun `listExchangeSteps throws INVALID_ARGUMENT when page size is less than 0`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        runBlocking {
          listExchangeSteps {
            parent = EXCHANGE_NAME
            filter = filter { dataProvider = DATA_PROVIDER_KEY.toName() }
            pageSize = -1
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.status.description).isEqualTo("Page size cannot be less than 0")
  }

  @Test
  fun `listExchangeSteps throws PERMISSION_DENIED with recurring exchange name when recurring exchange not found`() {
    internalRecurringExchangesServiceMock.stub {
      onBlocking { getRecurringExchange(any()) }
        .thenThrow(
          RecurringExchangeNotFoundException(EXTERNAL_RECURRING_EXCHANGE_ID)
            .asStatusRuntimeException(Status.Code.NOT_FOUND, "RecurringExchange not found.")
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(ModelProviderPrincipal(MODEL_PROVIDER_KEY)) {
          runBlocking {
            service.listExchangeSteps(
              listExchangeStepsRequest {
                parent = EXCHANGE_NAME
                filter = filter {
                  modelProvider = MODEL_PROVIDER_KEY.toName()
                  this.states += states
                }
                pageSize = 1
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("recurringExchange", RECURRING_EXCHANGE_KEY.toName())
  }

  companion object {
    private val EXTERNAL_DATA_PROVIDER_ID = ExternalId(12345L)
    private val EXTERNAL_MODEL_PROVIDER_ID = ExternalId(23456L)
    private val EXTERNAL_RECURRING_EXCHANGE_ID = ExternalId(1L)

    private val DATA_PROVIDER_KEY = DataProviderKey(EXTERNAL_DATA_PROVIDER_ID.apiId.value)
    private val MODEL_PROVIDER_KEY = ModelProviderKey(EXTERNAL_MODEL_PROVIDER_ID.apiId.value)
    private val RECURRING_EXCHANGE_KEY =
      CanonicalRecurringExchangeKey(EXTERNAL_RECURRING_EXCHANGE_ID.apiId.value)
    private val EXCHANGE_STEP_KEY =
      CanonicalExchangeStepKey(
        recurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.apiId.value,
        exchangeId = EXCHANGE_ID,
        exchangeStepId = STEP_INDEX.toString(),
      )
    private val EXCHANGE_STEP_2_KEY =
      CanonicalExchangeStepKey(
        recurringExchangeId = EXCHANGE_STEP_KEY.recurringExchangeId,
        exchangeId = EXCHANGE_STEP_KEY.exchangeId,
        exchangeStepId = (STEP_INDEX + 1).toString(),
      )
    private val EXCHANGE_STEP_ATTEMPT_KEY =
      CanonicalExchangeStepAttemptKey(
        recurringExchangeId = EXCHANGE_STEP_KEY.recurringExchangeId,
        exchangeId = EXCHANGE_STEP_KEY.exchangeId,
        exchangeStepId = EXCHANGE_STEP_KEY.exchangeStepId,
        exchangeStepAttemptId = ATTEMPT_NUMBER.toString(),
      )

    private val EXCHANGE_STEP = exchangeStep {
      name = EXCHANGE_STEP_KEY.toName()
      state = ExchangeStep.State.READY_FOR_RETRY
      stepIndex = STEP_INDEX
      exchangeDate = DATE
      dataProvider = DATA_PROVIDER_KEY.toName()
      exchangeWorkflow = any {
        value = SERIALIZED_WORKFLOW
        typeUrl = ProtoReflection.getTypeUrl(ExchangeWorkflow.getDescriptor())
      }
    }
    private val EXCHANGE_STEP_2 =
      EXCHANGE_STEP.copy {
        name = EXCHANGE_STEP_2_KEY.toName()
        stepIndex += 1
        modelProvider = MODEL_PROVIDER_KEY.toName()
      }

    private val CLAIM_READY_EXCHANGE_STEP_RESPONSE: ClaimReadyExchangeStepResponse =
      claimReadyExchangeStepResponse {
        exchangeStep = EXCHANGE_STEP
        exchangeStepAttempt = EXCHANGE_STEP_ATTEMPT_KEY.toName()
      }

    private val INTERNAL_RECURRING_EXCHANGE = recurringExchange {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID.value
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value
    }

    private val INTERNAL_EXCHANGE_STEP: InternalExchangeStep = internalExchangeStep {
      externalRecurringExchangeId = INTERNAL_RECURRING_EXCHANGE.externalRecurringExchangeId
      date = DATE
      stepIndex = STEP_INDEX
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value
      state = InternalExchangeStep.State.READY_FOR_RETRY
      serializedExchangeWorkflow = SERIALIZED_WORKFLOW
      updateTime = UPDATE_TIME
      apiVersion = Version.V2_ALPHA.string
    }
    private val INTERNAL_EXCHANGE_STEP_2 =
      INTERNAL_EXCHANGE_STEP.copy {
        stepIndex += 1
        externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID.value
      }
  }
}
