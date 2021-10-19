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
import com.google.type.date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.UseConstructor
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.GetExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as InternalExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.Provider
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest as internalClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse as internalClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.exchangeStep as internalExchangeStep
import org.wfanet.measurement.internal.kingdom.provider

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
}

@RunWith(JUnit4::class)
class ExchangeStepsServiceTest {

  private val internalService: InternalExchangeStepsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(
          internalClaimReadyExchangeStepResponse {
            exchangeStep = INTERNAL_EXCHANGE_STEP
            attemptNumber = ATTEMPT_NUMBER
          }
        )
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

  private fun claimReadyExchangeStep(): ClaimReadyExchangeStepResponse = runBlocking {
    service.claimReadyExchangeStep(claimReadyExchangeStepRequest {})
  }

  @Test
  fun `claimReadyExchangeStep unauthenticated`() {
    val e = assertFailsWith<StatusRuntimeException> { claimReadyExchangeStep() }
    assertThat(e.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  private fun testClaimReadyExchangeStepWithPrincipal(principal: Principal<*>, provider: Provider) {
    assertThat(withPrincipal(principal) { claimReadyExchangeStep() })
      .isEqualTo(CLAIM_READY_EXCHANGE_STEP_RESPONSE)

    verifyProtoArgument(
        internalService,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep
      )
      .isEqualTo(internalClaimReadyExchangeStepRequest { this.provider = provider })
  }

  @Test
  fun `claimReadyExchangeStep for DataProvider`() {
    val principal = Principal.DataProvider(DataProviderKey(externalIdToApiId(12345L)))
    val provider = provider {
      type = Provider.Type.DATA_PROVIDER
      externalId = 12345L
    }
    testClaimReadyExchangeStepWithPrincipal(principal, provider)
  }

  @Test
  fun `claimReadyExchangeStep for ModelProvider`() {
    val principal = Principal.ModelProvider(ModelProviderKey(externalIdToApiId(12345L)))
    val provider = provider {
      type = Provider.Type.MODEL_PROVIDER
      externalId = 12345L
    }
    testClaimReadyExchangeStepWithPrincipal(principal, provider)
  }
}
