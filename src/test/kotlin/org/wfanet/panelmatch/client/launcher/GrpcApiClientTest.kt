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

package org.wfanet.panelmatch.client.launcher

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.argumentCaptor
import com.nhaarman.mockitokotlin2.mock
import com.nhaarman.mockitokotlin2.stub
import com.nhaarman.mockitokotlin2.times
import com.nhaarman.mockitokotlin2.verifyBlocking
import java.time.Clock
import java.time.Instant
import java.time.ZoneOffset
import kotlin.test.assertNotNull
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule

private const val DATA_PROVIDER_ID = "some-data-provider-id"
private const val MODEL_PROVIDER_ID = "some-model-provider-id"
private val DATA_PROVIDER_IDENTITY = Identity(DATA_PROVIDER_ID, Party.DATA_PROVIDER)
private val MODEL_PROVIDER_IDENTITY = Identity(MODEL_PROVIDER_ID, Party.MODEL_PROVIDER)

private val EXCHANGE_STEP: ExchangeStep =
  ExchangeStep.newBuilder()
    .apply {
      keyBuilder.apply {
        recurringExchangeId = "some-recurring-exchange-id"
        exchangeId = "some-exchange-id"
        exchangeStepId = "some-step-id"
      }
      state = ExchangeStep.State.READY_FOR_RETRY
    }
    .build()

private val EXCHANGE_STEP_ATTEMPT_KEY: ExchangeStepAttempt.Key =
  ExchangeStepAttempt.Key.newBuilder()
    .apply {
      recurringExchangeId = EXCHANGE_STEP.key.recurringExchangeId
      exchangeId = EXCHANGE_STEP.key.exchangeId
      stepId = EXCHANGE_STEP.key.exchangeStepId
      exchangeStepAttemptId = "some-attempt-id"
    }
    .build()

private val FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE =
  ClaimReadyExchangeStepResponse.newBuilder()
    .apply {
      exchangeStep = EXCHANGE_STEP
      exchangeStepAttempt = EXCHANGE_STEP_ATTEMPT_KEY
    }
    .build()

private val EMPTY_CLAIM_READY_EXCHANGE_STEP_RESPONSE =
  ClaimReadyExchangeStepResponse.getDefaultInstance()

@RunWith(JUnit4::class)
class GrpcApiClientTest {
  private val exchangeStepsServiceMock: ExchangeStepsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless())

  private val exchangeStepsAttemptsServiceMock: ExchangeStepAttemptsCoroutineImplBase =
    mock(useConstructor = UseConstructor.parameterless()) {}

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(exchangeStepsServiceMock)
    addService(exchangeStepsAttemptsServiceMock)
  }

  private val exchangeStepsStub = ExchangeStepsCoroutineStub(grpcTestServerRule.channel)
  private val exchangeStepAttemptsStub =
    ExchangeStepAttemptsCoroutineStub(grpcTestServerRule.channel)

  private val clock = Clock.fixed(Instant.ofEpochSecond(123456789), ZoneOffset.UTC)

  private fun makeClient(identity: Identity = DATA_PROVIDER_IDENTITY): GrpcApiClient {
    return GrpcApiClient(identity, exchangeStepsStub, exchangeStepAttemptsStub, clock)
  }

  private fun makeLogEntry(message: String): ExchangeStepAttempt.DebugLog {
    return ExchangeStepAttempt.DebugLog.newBuilder()
      .also {
        it.time = clock.instant().toProtoTime()
        it.message = message
      }
      .build()
  }

  @Test
  fun `claimExchangeStep as DataProvider with result`() {
    exchangeStepsServiceMock.stub {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE)
    }

    val client = makeClient(DATA_PROVIDER_IDENTITY)

    val result: ApiClient.ClaimedExchangeStep? = runBlocking { client.claimExchangeStep() }
    assertNotNull(result)
    val (exchangeStep, attemptKey) = result

    assertThat(exchangeStep).isEqualTo(EXCHANGE_STEP)
    assertThat(attemptKey).isEqualTo(EXCHANGE_STEP_ATTEMPT_KEY)

    argumentCaptor<ClaimReadyExchangeStepRequest> {
      verifyBlocking(exchangeStepsServiceMock) { claimReadyExchangeStep(capture()) }
      assertThat(firstValue)
        .isEqualTo(
          ClaimReadyExchangeStepRequest.newBuilder()
            .apply { dataProviderBuilder.dataProviderId = DATA_PROVIDER_ID }
            .build()
        )
    }
  }

  @Test
  fun `claimExchangeStep as DataProvider without result`() {
    exchangeStepsServiceMock.stub {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(EMPTY_CLAIM_READY_EXCHANGE_STEP_RESPONSE)
    }

    val client = makeClient(DATA_PROVIDER_IDENTITY)

    val result: ApiClient.ClaimedExchangeStep? = runBlocking { client.claimExchangeStep() }
    assertNull(result)
  }

  @Test
  fun `claimExchangeStep as ModelProvider with result`() {
    exchangeStepsServiceMock.stub {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE)
    }

    val client = makeClient(MODEL_PROVIDER_IDENTITY)

    val result: ApiClient.ClaimedExchangeStep? = runBlocking { client.claimExchangeStep() }
    assertNotNull(result)
    val (exchangeStep, attemptKey) = result

    assertThat(exchangeStep).isEqualTo(EXCHANGE_STEP)
    assertThat(attemptKey).isEqualTo(EXCHANGE_STEP_ATTEMPT_KEY)

    argumentCaptor<ClaimReadyExchangeStepRequest> {
      verifyBlocking(exchangeStepsServiceMock) { claimReadyExchangeStep(capture()) }
      assertThat(firstValue)
        .isEqualTo(
          ClaimReadyExchangeStepRequest.newBuilder()
            .apply { modelProviderBuilder.modelProviderId = MODEL_PROVIDER_ID }
            .build()
        )
    }
  }

  @Test
  fun appendLogEntry() {
    exchangeStepsAttemptsServiceMock.stub {
      onBlocking { appendLogEntry(any()) }.thenReturn(ExchangeStepAttempt.getDefaultInstance())
    }

    runBlocking {
      makeClient().appendLogEntry(EXCHANGE_STEP_ATTEMPT_KEY, listOf("message-1", "message-2"))
    }

    argumentCaptor<AppendLogEntryRequest> {
      verifyBlocking(exchangeStepsAttemptsServiceMock) { appendLogEntry(capture()) }
      assertThat(firstValue)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          AppendLogEntryRequest.newBuilder()
            .apply {
              key = EXCHANGE_STEP_ATTEMPT_KEY
              addLogEntries(makeLogEntry("message-1"))
              addLogEntries(makeLogEntry("message-2"))
            }
            .build()
        )
    }
  }

  @Test
  fun finishExchangeStepAttempt() {
    exchangeStepsAttemptsServiceMock.stub {
      onBlocking { finishExchangeStepAttempt(any()) }
        .thenReturn(ExchangeStepAttempt.getDefaultInstance())
    }

    runBlocking {
      makeClient()
        .finishExchangeStepAttempt(
          EXCHANGE_STEP_ATTEMPT_KEY,
          ExchangeStepAttempt.State.SUCCEEDED,
          listOf("message-1", "message-2")
        )
      makeClient()
        .finishExchangeStepAttempt(EXCHANGE_STEP_ATTEMPT_KEY, ExchangeStepAttempt.State.FAILED_STEP)
    }

    argumentCaptor<FinishExchangeStepAttemptRequest> {
      verifyBlocking(exchangeStepsAttemptsServiceMock, times(2)) {
        finishExchangeStepAttempt(capture())
      }
      assertThat(firstValue)
        .isEqualTo(
          FinishExchangeStepAttemptRequest.newBuilder()
            .apply {
              key = EXCHANGE_STEP_ATTEMPT_KEY
              finalState = ExchangeStepAttempt.State.SUCCEEDED
              addLogEntries(makeLogEntry("message-1"))
              addLogEntries(makeLogEntry("message-2"))
            }
            .build()
        )
      assertThat(secondValue)
        .isEqualTo(
          FinishExchangeStepAttemptRequest.newBuilder()
            .apply {
              key = EXCHANGE_STEP_ATTEMPT_KEY
              finalState = ExchangeStepAttempt.State.FAILED_STEP
            }
            .build()
        )
    }
  }
}
