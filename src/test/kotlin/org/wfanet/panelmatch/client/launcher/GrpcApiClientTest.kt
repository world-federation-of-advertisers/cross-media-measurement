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
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertNull
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.stub
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepKey
import org.wfanet.measurement.api.v2alpha.ClaimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt as V2AlphaExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKt.debugLogEntry
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow as V2AlphaExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.StepKt.generateRandomBytesStep as v2AlphaGenerateRandomBytesStep
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.exchangeIdentifiers as v2AlphaExchangeIdentifiers
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.schedule as v2AlphaSchedule
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflowKt.step as v2AlphaStep
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.appendExchangeStepAttemptLogEntryRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepRequest
import org.wfanet.measurement.api.v2alpha.claimReadyExchangeStepResponse
import org.wfanet.measurement.api.v2alpha.exchangeStep
import org.wfanet.measurement.api.v2alpha.exchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.exchangeWorkflow as v2AlphaExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.pack
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.panelmatch.client.common.ExchangeStepAttemptKey
import org.wfanet.panelmatch.client.common.Identity
import org.wfanet.panelmatch.client.internal.ExchangeStepAttempt
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow
import org.wfanet.panelmatch.client.internal.ExchangeWorkflow.Party
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.StepKt.generateRandomBytesStep
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.exchangeIdentifiers
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.schedule
import org.wfanet.panelmatch.client.internal.ExchangeWorkflowKt.step
import org.wfanet.panelmatch.client.internal.exchangeWorkflow
import org.wfanet.panelmatch.common.Fingerprinters.sha256

private const val RECURRING_EXCHANGE_ID = "some-recurring-exchange-id"
private const val EXCHANGE_ID = "some-exchange-id"
private const val EXCHANGE_STEP_ID = "some-step-id"
private const val EXCHANGE_STEP_ATTEMPT_ID = "some-attempt-id"
private const val DATA_PROVIDER_ID = "some-data-provider-id"
private const val MODEL_PROVIDER_ID = "some-model-provider-id"
private val DATA_PROVIDER_IDENTITY = Identity(DATA_PROVIDER_ID, Party.DATA_PROVIDER)
private val MODEL_PROVIDER_IDENTITY = Identity(MODEL_PROVIDER_ID, Party.MODEL_PROVIDER)
private val EXCHANGE_DATE = LocalDate.of(2024, 1, 1)

private val V2ALPHA_EXCHANGE_STEP_KEY =
  CanonicalExchangeStepKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = EXCHANGE_STEP_ID,
  )

private val V2ALPHA_WORKFLOW = v2AlphaExchangeWorkflow {
  exchangeIdentifiers = v2AlphaExchangeIdentifiers {
    dataProvider = "dataProviders/$DATA_PROVIDER_ID"
    modelProvider = "modelProviders/$MODEL_PROVIDER_ID"
    storage = V2AlphaExchangeWorkflow.StorageType.AMAZON_S3
    sharedStorageOwner = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
  }
  firstExchangeDate = EXCHANGE_DATE.minusDays(10L).toProtoDate()
  repetitionSchedule = v2AlphaSchedule { cronExpression = "@daily" }
  steps += v2AlphaStep {
    party = V2AlphaExchangeWorkflow.Party.DATA_PROVIDER
    generateRandomBytesStep = v2AlphaGenerateRandomBytesStep { byteCount = 8 }
  }
}

private val V2ALPHA_EXCHANGE_STEP: ExchangeStep = exchangeStep {
  name = V2ALPHA_EXCHANGE_STEP_KEY.toName()
  state = ExchangeStep.State.READY_FOR_RETRY
  stepIndex = 0
  exchangeDate = EXCHANGE_DATE.toProtoDate()
  exchangeWorkflow = V2ALPHA_WORKFLOW.pack()
}

private val V2ALPHA_EXCHANGE_STEP_ATTEMPT_KEY: CanonicalExchangeStepAttemptKey =
  CanonicalExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = EXCHANGE_STEP_ID,
    exchangeStepAttemptId = EXCHANGE_STEP_ATTEMPT_ID,
  )

private val EXCHANGE_STEP_ATTEMPT_KEY =
  ExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_ID,
    exchangeId = EXCHANGE_ID,
    stepId = EXCHANGE_STEP_ID,
    attemptId = EXCHANGE_STEP_ATTEMPT_ID,
  )

private val EXCHANGE_WORKFLOW = exchangeWorkflow {
  exchangeIdentifiers = exchangeIdentifiers {
    dataProviderId = DATA_PROVIDER_ID
    modelProviderId = MODEL_PROVIDER_ID
    storage = ExchangeWorkflow.StorageType.AMAZON_S3
    sharedStorageOwner = Party.DATA_PROVIDER
  }
  firstExchangeDate = V2ALPHA_WORKFLOW.firstExchangeDate
  repetitionSchedule = schedule { cronExpression = "@daily" }
  steps += step {
    party = Party.DATA_PROVIDER
    generateRandomBytesStep = generateRandomBytesStep { byteCount = 8 }
  }
}

private val CLAIMED_EXCHANGE_STEP =
  ApiClient.ClaimedExchangeStep(
    attemptKey = EXCHANGE_STEP_ATTEMPT_KEY,
    exchangeDate = EXCHANGE_DATE,
    stepIndex = 0,
    workflow = EXCHANGE_WORKFLOW,
    workflowFingerprint = sha256(V2ALPHA_WORKFLOW.toByteString()),
  )

private val FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE = claimReadyExchangeStepResponse {
  exchangeStep = V2ALPHA_EXCHANGE_STEP
  exchangeStepAttempt = V2ALPHA_EXCHANGE_STEP_ATTEMPT_KEY.toName()
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

  private fun makeLimitedClient(identity: Identity = DATA_PROVIDER_IDENTITY): ApiClient {
    return GrpcApiClient(identity, exchangeStepsStub, exchangeStepAttemptsStub, clock)
      .withMaxParallelClaimedExchangeSteps(1)
  }

  private fun makeLogEntry(message: String): V2AlphaExchangeStepAttempt.DebugLogEntry {
    return debugLogEntry {
      entryTime = clock.instant().toProtoTime()
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

    assertThat(result).isEqualTo(CLAIMED_EXCHANGE_STEP)

    verifyProtoArgument(
        exchangeStepsServiceMock,
        ExchangeStepsCoroutineImplBase::claimReadyExchangeStep,
      )
      .isEqualTo(
        claimReadyExchangeStepRequest { parent = DataProviderKey(DATA_PROVIDER_ID).toName() }
      )
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

    assertThat(result).isEqualTo(CLAIMED_EXCHANGE_STEP)

    verifyProtoArgument(
        exchangeStepsServiceMock,
        ExchangeStepsCoroutineImplBase::claimReadyExchangeStep,
      )
      .isEqualTo(
        claimReadyExchangeStepRequest { parent = ModelProviderKey(MODEL_PROVIDER_ID).toName() }
      )
  }

  @Test
  fun appendLogEntry() {
    exchangeStepsAttemptsServiceMock.stub {
      onBlocking { appendExchangeStepAttemptLogEntry(any()) }.thenReturn(exchangeStepAttempt {})
    }

    runBlocking {
      makeClient().appendLogEntry(EXCHANGE_STEP_ATTEMPT_KEY, listOf("message-1", "message-2"))
    }

    verifyProtoArgument(
        exchangeStepsAttemptsServiceMock,
        ExchangeStepAttemptsCoroutineImplBase::appendExchangeStepAttemptLogEntry,
      )
      .isEqualTo(
        appendExchangeStepAttemptLogEntryRequest {
          name = V2ALPHA_EXCHANGE_STEP_ATTEMPT_KEY.toName()
          logEntries += makeLogEntry("message-1")
          logEntries += makeLogEntry("message-2")
        }
      )
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
          listOf("message-1", "message-2"),
        )
      makeClient()
        .finishExchangeStepAttempt(EXCHANGE_STEP_ATTEMPT_KEY, ExchangeStepAttempt.State.FAILED_STEP)
    }

    val requestCaptor: KArgumentCaptor<FinishExchangeStepAttemptRequest> = argumentCaptor {
      verifyBlocking(exchangeStepsAttemptsServiceMock, times(2)) {
        finishExchangeStepAttempt(capture())
      }
    }
    assertThat(requestCaptor.firstValue)
      .isEqualTo(
        finishExchangeStepAttemptRequest {
          name = V2ALPHA_EXCHANGE_STEP_ATTEMPT_KEY.toName()
          finalState = V2AlphaExchangeStepAttempt.State.SUCCEEDED
          logEntries += makeLogEntry("message-1")
          logEntries += makeLogEntry("message-2")
        }
      )
    assertThat(requestCaptor.secondValue)
      .isEqualTo(
        finishExchangeStepAttemptRequest {
          name = V2ALPHA_EXCHANGE_STEP_ATTEMPT_KEY.toName()
          finalState = V2AlphaExchangeStepAttempt.State.FAILED_STEP
        }
      )
  }

  @Test
  fun `claimExchangeStep skips for multiple requests to limited client`() {
    exchangeStepsServiceMock.stub {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE)
    }

    val client = makeLimitedClient(DATA_PROVIDER_IDENTITY)

    runBlocking {
      client.claimExchangeStep()
      client.claimExchangeStep()
    }

    val stepCaptor = argumentCaptor<ClaimReadyExchangeStepRequest>()
    verifyBlocking(exchangeStepsServiceMock, times(1)) {
      claimReadyExchangeStep(stepCaptor.capture())
    }
  }

  @Test
  fun `claimExchangeStep succeeds out for multiple requests to unlimited client`() {
    exchangeStepsServiceMock.stub {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE)
    }

    val client = makeClient(DATA_PROVIDER_IDENTITY)

    runBlocking {
      client.claimExchangeStep()
      client.claimExchangeStep()
    }

    val stepCaptor = argumentCaptor<ClaimReadyExchangeStepRequest>()
    verifyBlocking(exchangeStepsServiceMock, times(2)) {
      claimReadyExchangeStep(stepCaptor.capture())
    }
  }

  @Test
  fun `claimExchangeStep succeeds for multiple claims and finishes to limited client`() {
    exchangeStepsServiceMock.stub {
      onBlocking { claimReadyExchangeStep(any()) }
        .thenReturn(FULL_CLAIM_READY_EXCHANGE_STEP_RESPONSE)
    }

    val client = makeLimitedClient(DATA_PROVIDER_IDENTITY)

    runBlocking {
      client.claimExchangeStep()
      client.finishExchangeStepAttempt(
        EXCHANGE_STEP_ATTEMPT_KEY,
        ExchangeStepAttempt.State.SUCCEEDED,
        listOf("message-1", "message-2"),
      )
      client.claimExchangeStep()
    }

    val stepCaptor = argumentCaptor<ClaimReadyExchangeStepRequest>()
    verifyBlocking(exchangeStepsServiceMock, times(2)) {
      claimReadyExchangeStep(stepCaptor.capture())
    }
  }
}
