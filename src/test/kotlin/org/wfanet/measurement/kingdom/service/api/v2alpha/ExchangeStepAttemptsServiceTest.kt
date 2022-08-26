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
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.stub
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.copy
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.AppendLogEntryRequest as InternalAppendLogEntryRequest
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase as InternalExchangeStepAttempts
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as InternalExchangeSteps
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.exchangeStep
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest as internalFinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepRequest

private const val RECURRING_EXCHANGE_ID = 1L
private const val STEP_INDEX = 1
private const val ATTEMPT_NUMBER = 3
private val ARBITRARY_BYTES = ByteString.copyFromUtf8("some arbitrary bytes")

private val DEBUG_LOG_1_TIME = Timestamp.newBuilder().setSeconds(1010101).build()
private const val DEBUG_LOG_1_MESSAGE = "some message"
private val DEBUG_LOG_2_TIME = Timestamp.newBuilder().setSeconds(2020202).build()
private const val DEBUG_LOG_2_MESSAGE = "some other message"

private val DATE = Date.newBuilder().setYear(2021).setMonth(3).setDay(14).build()
private const val EXCHANGE_ID = "2021-03-14"

private val INTERNAL_EXCHANGE_STEP_ATTEMPT: InternalExchangeStepAttempt =
  InternalExchangeStepAttempt.newBuilder()
    .apply {
      externalRecurringExchangeId = RECURRING_EXCHANGE_ID
      date = DATE
      stepIndex = STEP_INDEX
      attemptNumber = ATTEMPT_NUMBER
      state = InternalExchangeStepAttempt.State.ACTIVE
      detailsBuilder.apply {
        startTimeBuilder.seconds = 123
        updateTimeBuilder.seconds = 456
        addDebugLogEntriesBuilder().apply {
          time = DEBUG_LOG_1_TIME
          message = DEBUG_LOG_1_MESSAGE
        }
        addDebugLogEntriesBuilder().apply {
          time = DEBUG_LOG_2_TIME
          message = DEBUG_LOG_2_MESSAGE
        }
        addSharedOutputs(ARBITRARY_BYTES)
      }
    }
    .build()

private fun toV2AlphaName(): String {
  return ExchangeStepAttemptKey(
      recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID),
      exchangeId = EXCHANGE_ID,
      exchangeStepId = STEP_INDEX.toString(),
      exchangeStepAttemptId = ATTEMPT_NUMBER.toString()
    )
    .toName()
}

private val EXCHANGE_STEP_ATTEMPT: ExchangeStepAttempt =
  ExchangeStepAttempt.newBuilder()
    .apply {
      name = toV2AlphaName()
      state = ExchangeStepAttempt.State.ACTIVE
      attemptNumber = ATTEMPT_NUMBER
      startTime = INTERNAL_EXCHANGE_STEP_ATTEMPT.details.startTime
      updateTime = INTERNAL_EXCHANGE_STEP_ATTEMPT.details.updateTime
      addDebugLogEntriesBuilder().apply {
        time = DEBUG_LOG_1_TIME
        message = DEBUG_LOG_1_MESSAGE
      }
      addDebugLogEntriesBuilder().apply {
        time = DEBUG_LOG_2_TIME
        message = DEBUG_LOG_2_MESSAGE
      }
    }
    .build()

@RunWith(JUnit4::class)
class ExchangeStepAttemptsServiceTest {

  private val internalExchangeStepAttempts: InternalExchangeStepAttempts = mockService {
    onBlocking { appendLogEntry(any()) }.thenReturn(INTERNAL_EXCHANGE_STEP_ATTEMPT)
    onBlocking { finishExchangeStepAttempt(any()) }.thenReturn(INTERNAL_EXCHANGE_STEP_ATTEMPT)
  }

  private val internalExchangeSteps: InternalExchangeSteps = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalExchangeStepAttempts)
    addService(internalExchangeSteps)
  }

  private val service =
    ExchangeStepAttemptsService(
      ExchangeStepAttemptsCoroutineStub(grpcTestServerRule.channel),
      ExchangeStepsCoroutineStub(grpcTestServerRule.channel)
    )

  @Test
  fun appendLogEntry() {
    val request =
      AppendLogEntryRequest.newBuilder()
        .apply {
          name = EXCHANGE_STEP_ATTEMPT.name
          addAllLogEntries(EXCHANGE_STEP_ATTEMPT.debugLogEntriesList)
        }
        .build()

    assertThat(runBlocking { service.appendLogEntry(request) }).isEqualTo(EXCHANGE_STEP_ATTEMPT)

    verifyProtoArgument(internalExchangeStepAttempts, InternalExchangeStepAttempts::appendLogEntry)
      .isEqualTo(
        InternalAppendLogEntryRequest.newBuilder()
          .apply {
            externalRecurringExchangeId = RECURRING_EXCHANGE_ID
            date = INTERNAL_EXCHANGE_STEP_ATTEMPT.date
            stepIndex = STEP_INDEX
            attemptNumber = ATTEMPT_NUMBER
            addAllDebugLogEntries(INTERNAL_EXCHANGE_STEP_ATTEMPT.details.debugLogEntriesList)
          }
          .build()
      )
  }

  @Test
  fun `finishExchangeStepAttempt unauthenticated`() {
    val request = finishExchangeStepAttemptRequest {
      name = EXCHANGE_STEP_ATTEMPT.name
      finalState = ExchangeStepAttempt.State.FAILED
      logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
    }

    val e =
      assertFailsWith<StatusRuntimeException> {
        runBlocking { service.finishExchangeStepAttempt(request) }
      }
    assertThat(e.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `finishExchangeStepAttempt unauthorized to read`() {
    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }.thenThrow(Status.NOT_FOUND.asRuntimeException())
    }

    val request = finishExchangeStepAttemptRequest {
      name = EXCHANGE_STEP_ATTEMPT.name
      finalState = ExchangeStepAttempt.State.FAILED
      logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
    }

    val dataProviderKey = DataProviderKey(externalIdToApiId(12345))
    val principal = DataProviderPrincipal(dataProviderKey)
    val e =
      withPrincipal(principal) {
        assertFailsWith<StatusRuntimeException> {
          runBlocking { service.finishExchangeStepAttempt(request) }
        }
      }
    assertWithMessage(e.status.toString())
      .that(e.status.code)
      .isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `finishExchangeStepAttempt unauthorized to write`() {
    val externalDataProviderId = 12345L
    val dataProviderKey = DataProviderKey(externalIdToApiId(externalDataProviderId))
    val principal = DataProviderPrincipal(dataProviderKey)

    val provider = provider {
      type = Provider.Type.DATA_PROVIDER
      externalId = externalDataProviderId
    }

    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenReturn(
          exchangeStep { this.provider = provider.copy { type = Provider.Type.MODEL_PROVIDER } }
        )
    }

    val request = finishExchangeStepAttemptRequest {
      name = EXCHANGE_STEP_ATTEMPT.name
      finalState = ExchangeStepAttempt.State.FAILED
      logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
    }

    val e =
      withPrincipal(principal) {
        assertFailsWith<StatusRuntimeException> {
          runBlocking { service.finishExchangeStepAttempt(request) }
        }
      }
    assertWithMessage(e.status.toString())
      .that(e.status.code)
      .isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun finishExchangeStepAttempt() {
    val externalDataProviderId = 12345L
    val dataProviderKey = DataProviderKey(externalIdToApiId(externalDataProviderId))
    val principal = DataProviderPrincipal(dataProviderKey)
    val provider = provider {
      type = Provider.Type.DATA_PROVIDER
      externalId = externalDataProviderId
    }

    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }.thenReturn(exchangeStep { this.provider = provider })
    }

    val request = finishExchangeStepAttemptRequest {
      name = EXCHANGE_STEP_ATTEMPT.name
      finalState = ExchangeStepAttempt.State.FAILED
      logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
    }

    val response =
      withPrincipal(principal) { runBlocking { service.finishExchangeStepAttempt(request) } }

    assertThat(response).isEqualTo(EXCHANGE_STEP_ATTEMPT)

    verifyProtoArgument(internalExchangeSteps, InternalExchangeSteps::getExchangeStep)
      .isEqualTo(
        getExchangeStepRequest {
          externalRecurringExchangeId = RECURRING_EXCHANGE_ID
          date = INTERNAL_EXCHANGE_STEP_ATTEMPT.date
          stepIndex = STEP_INDEX
          this.provider = provider
        }
      )

    verifyProtoArgument(
        internalExchangeStepAttempts,
        InternalExchangeStepAttempts::finishExchangeStepAttempt
      )
      .ignoringFieldAbsence()
      .isEqualTo(
        internalFinishExchangeStepAttemptRequest {
          this.provider = provider
          externalRecurringExchangeId = RECURRING_EXCHANGE_ID
          date = INTERNAL_EXCHANGE_STEP_ATTEMPT.date
          stepIndex = STEP_INDEX
          attemptNumber = ATTEMPT_NUMBER
          state = InternalExchangeStepAttempt.State.FAILED
          debugLogEntries += INTERNAL_EXCHANGE_STEP_ATTEMPT.details.debugLogEntriesList
        }
      )
  }
}
