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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.Date
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import kotlin.test.assertFails
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.GetExchangeStepRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepRequest as InternalClaimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse as InternalClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.ExchangeStep as InternalExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as InternalExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub

private const val RECURRING_EXCHANGE_ID = 1L
private val DATE = Date.newBuilder().setYear(2021).setMonth(3).setDay(14).build()
private const val EXCHANGE_ID = "2021-03-14"
private const val STEP_INDEX = 123
private const val ATTEMPT_NUMBER = 5

private val EXCHANGE_STEP =
  ExchangeStep.newBuilder()
    .apply {
      keyBuilder.apply {
        recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID)
        exchangeId = EXCHANGE_ID
        exchangeStepId = externalIdToApiId(STEP_INDEX.toLong())
      }
      state = ExchangeStep.State.READY_FOR_RETRY
    }
    .build()

private val EXCHANGE_STEP_ATTEMPT_KEY =
  ExchangeStepAttempt.Key.newBuilder()
    .apply {
      recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID)
      exchangeId = EXCHANGE_ID
      stepId = externalIdToApiId(STEP_INDEX.toLong())
      exchangeStepAttemptId = externalIdToApiId(ATTEMPT_NUMBER.toLong())
    }
    .build()

private val INTERNAL_EXCHANGE_STEP =
  InternalExchangeStep.newBuilder()
    .apply {
      externalRecurringExchangeId = RECURRING_EXCHANGE_ID
      date = DATE
      stepIndex = STEP_INDEX
      state = InternalExchangeStep.State.READY_FOR_RETRY
    }
    .build()

@RunWith(JUnit4::class)
class ExchangeStepsServiceTest {

  private val internalService: InternalExchangeStepsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(
          InternalClaimReadyExchangeStepResponse.newBuilder()
            .apply {
              exchangeStep = INTERNAL_EXCHANGE_STEP
              attemptNumber = ATTEMPT_NUMBER
            }
            .build()
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

  @Test
  fun `claimReadyExchangeStep for DataProvider`() {
    val id = 12345L
    val request =
      ClaimReadyExchangeStepRequest.newBuilder()
        .apply { dataProviderBuilder.dataProviderId = externalIdToApiId(id) }
        .build()
    val response = runBlocking { service.claimReadyExchangeStep(request) }
    assertThat(response)
      .isEqualTo(
        ClaimReadyExchangeStepResponse.newBuilder()
          .apply {
            exchangeStep = EXCHANGE_STEP
            exchangeStepAttempt = EXCHANGE_STEP_ATTEMPT_KEY
          }
          .build()
      )

    verifyProtoArgument(
        internalService,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep
      )
      .isEqualTo(
        InternalClaimReadyExchangeStepRequest.newBuilder()
          .apply { externalDataProviderId = id }
          .build()
      )
  }

  @Test
  fun `claimReadyExchangeStep for ModelProvider`() {
    val id = 12345L
    val request =
      ClaimReadyExchangeStepRequest.newBuilder()
        .apply { modelProviderBuilder.modelProviderId = externalIdToApiId(id) }
        .build()
    val response = runBlocking { service.claimReadyExchangeStep(request) }
    assertThat(response)
      .isEqualTo(
        ClaimReadyExchangeStepResponse.newBuilder()
          .apply {
            exchangeStep = EXCHANGE_STEP
            exchangeStepAttempt = EXCHANGE_STEP_ATTEMPT_KEY
          }
          .build()
      )

    verifyProtoArgument(
        internalService,
        InternalExchangeStepsCoroutineImplBase::claimReadyExchangeStep
      )
      .isEqualTo(
        InternalClaimReadyExchangeStepRequest.newBuilder()
          .apply { externalModelProviderId = id }
          .build()
      )
  }

  @Test
  fun `claimReadyExchangeStep without party`() {
    assertFails {
      runBlocking {
        service.claimReadyExchangeStep(ClaimReadyExchangeStepRequest.getDefaultInstance())
      }
    }
  }
}
