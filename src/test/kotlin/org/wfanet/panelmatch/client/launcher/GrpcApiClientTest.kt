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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
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
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt.debugLog
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow.Party
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.appendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.panelmatch.client.common.Identity

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-attempt-id"
private const val DATA_PROVIDER_ID = "some-data-provider-id"
private const val MODEL_PROVIDER_ID = "some-model-provider-id"
private val DATA_PROVIDER_IDENTITY = Identity(DATA_PROVIDER_ID, Party.DATA_PROVIDER)
private val MODEL_PROVIDER_IDENTITY = Identity(MODEL_PROVIDER_ID, Party.MODEL_PROVIDER)

private val EXCHANGE_STEP_KEY =
  ExchangeStepKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = EXCHANGE_STEP_ID
  )

private val EXCHANGE_STEP: ExchangeStep = exchangeStep {
  name = EXCHANGE_STEP_KEY.toName()
  state = ExchangeStep.State.READY_FOR_RETRY
}

private val EXCHANGE_STEP_ATTEMPT_KEY: ExchangeStepAttemptKey =
  ExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = EXCHANGE_STEP_ID,
    exchangeStepAttemptId = EXCHANGE_STEP_ATTEMPT_ID
  )

private val FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE = claimReadyExchangeStepResponse {
  exchangeStep = EXCHANGE_STEP
  exchangeStepAttempt = EXCHANGE_STEP_ATTEMPT_KEY.toName()
}

private val EMPTY_CLAIM_READY_EXCHANGE_STEP_RESPONSE = claimReadyExchangeStepResponse {}

@RunWith(JUnit4::class)
class GrpcApiClientTest {
  private val exchangeStepsServiceMock: ExchangeStepsCoroutineImplBase = mockService {}

  private val exchangeStepsAttemptsServiceMock: ExchangeStepAttemptsCoroutineImplBase =
      mockService {}

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
    return debugLog {
      time = clock.instant().toProtoTime()
      this.message = message
    }
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
          claimReadyExchangeStepRequest {
            dataProvider = DataProviderKey(DATA_PROVIDER_ID).toName()
          }
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
          claimReadyExchangeStepRequest {
            modelProvider = ModelProviderKey(MODEL_PROVIDER_ID).toName()
          }
        )
    }
  }

  @Test
  fun appendLogEntry() {
    exchangeStepsAttemptsServiceMock.stub {
      onBlocking { appendLogEntry(any()) }.thenReturn(exchangeStepAttempt {})
    }

    runBlocking {
      makeClient().appendLogEntry(EXCHANGE_STEP_ATTEMPT_KEY, listOf("message-1", "message-2"))
    }

    argumentCaptor<AppendLogEntryRequest> {
      verifyBlocking(exchangeStepsAttemptsServiceMock) { appendLogEntry(capture()) }
      assertThat(firstValue)
        .ignoringRepeatedFieldOrder()
        .isEqualTo(
          appendLogEntryRequest {
            name = EXCHANGE_STEP_ATTEMPT_KEY.toName()
            logEntries += makeLogEntry("message-1")
            logEntries += makeLogEntry("message-2")
          }
        )
    }
  }

  @Test
  fun finishExchangeStepAttempt() {
    exchangeStepsAttemptsServiceMock.stub {
      onBlocking { finishExchangeStepAttempt(any()) }.thenReturn(exchangeStepAttempt {})
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
          finishExchangeStepAttemptRequest {
            name = EXCHANGE_STEP_ATTEMPT_KEY.toName()
            finalState = ExchangeStepAttempt.State.SUCCEEDED
            logEntries += makeLogEntry("message-1")
            logEntries += makeLogEntry("message-2")
          }
        )
      assertThat(secondValue)
        .isEqualTo(
          finishExchangeStepAttemptRequest {
            name = EXCHANGE_STEP_ATTEMPT_KEY.toName()
            finalState = ExchangeStepAttempt.State.FAILED_STEP
          }
        )
    }
  }
}
