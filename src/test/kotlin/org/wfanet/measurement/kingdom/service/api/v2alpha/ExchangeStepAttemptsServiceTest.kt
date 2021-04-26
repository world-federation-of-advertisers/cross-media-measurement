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
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.type.Date
import com.nhaarman.mockitokotlin2.UseConstructor
import com.nhaarman.mockitokotlin2.any
import com.nhaarman.mockitokotlin2.mock
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.AppendLogEntryRequest
import org.wfanet.measurement.api.v2alpha.CreateExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.AppendLogEntryRequest as InternalAppendLogEntryRequest
import org.wfanet.measurement.internal.kingdom.CreateExchangeStepAttemptRequest as InternalCreateExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase as InternalExchangeStepAttempts
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub

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

private val EXCHANGE_STEP_ATTEMPT: ExchangeStepAttempt =
  ExchangeStepAttempt.newBuilder()
    .apply {
      keyBuilder.apply {
        recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID)
        exchangeId = EXCHANGE_ID
        stepId = externalIdToApiId(STEP_INDEX.toLong())
        exchangeStepAttemptId = externalIdToApiId(ATTEMPT_NUMBER.toLong())
      }
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
      addSharedOutputs(ARBITRARY_BYTES)
    }
    .build()

@RunWith(JUnit4::class)
class ExchangeStepAttemptsServiceTest {

  private val internalService: InternalExchangeStepAttempts =
    mock(useConstructor = UseConstructor.parameterless()) {
      onBlocking { appendLogEntry(any()) }.thenReturn(INTERNAL_EXCHANGE_STEP_ATTEMPT)
      onBlocking { createExchangeStepAttempt(any()) }.thenReturn(INTERNAL_EXCHANGE_STEP_ATTEMPT)
    }

  @get:Rule val grpcTestServerRule = GrpcTestServerRule { addService(internalService) }

  private val service =
    ExchangeStepAttemptsService(ExchangeStepAttemptsCoroutineStub(grpcTestServerRule.channel))

  @Test
  fun appendLogEntry() {
    val request =
      AppendLogEntryRequest.newBuilder()
        .apply {
          key = EXCHANGE_STEP_ATTEMPT.key
          addAllLogEntries(EXCHANGE_STEP_ATTEMPT.debugLogEntriesList)
        }
        .build()

    assertThat(runBlocking { service.appendLogEntry(request) }).isEqualTo(EXCHANGE_STEP_ATTEMPT)

    verifyProtoArgument(internalService, InternalExchangeStepAttempts::appendLogEntry)
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
  fun `createExchangeStepAttempt with minimal request`() {
    val request =
      CreateExchangeStepAttemptRequest.newBuilder()
        .apply { exchangeStepAttemptBuilder.apply { key = EXCHANGE_STEP_ATTEMPT.key } }
        .build()

    assertThat(runBlocking { service.createExchangeStepAttempt(request) })
      .isEqualTo(EXCHANGE_STEP_ATTEMPT)

    verifyProtoArgument(internalService, InternalExchangeStepAttempts::createExchangeStepAttempt)
      .ignoringFieldAbsence()
      .isEqualTo(
        InternalCreateExchangeStepAttemptRequest.newBuilder()
          .apply {
            exchangeStepAttemptBuilder.apply {
              externalRecurringExchangeId = RECURRING_EXCHANGE_ID
              date = INTERNAL_EXCHANGE_STEP_ATTEMPT.date
              stepIndex = STEP_INDEX
              attemptNumber = 1
              state = InternalExchangeStepAttempt.State.ACTIVE
            }
          }
          .build()
      )
  }

  @Test
  fun `createExchangeStepAttempt with extra fields set`() {
    val request =
      CreateExchangeStepAttemptRequest.newBuilder()
        .apply {
          exchangeStepAttemptBuilder.apply {
            key = EXCHANGE_STEP_ATTEMPT.key
            state = ExchangeStepAttempt.State.FAILED
            addSharedOutputs(ByteString.copyFromUtf8("some-shared-output"))
            addAllDebugLogEntries(EXCHANGE_STEP_ATTEMPT.debugLogEntriesList)
          }
        }
        .build()

    assertThat(runBlocking { service.createExchangeStepAttempt(request) })
      .isEqualTo(EXCHANGE_STEP_ATTEMPT)

    verifyProtoArgument(internalService, InternalExchangeStepAttempts::createExchangeStepAttempt)
      .ignoringFieldAbsence()
      .isEqualTo(
        InternalCreateExchangeStepAttemptRequest.newBuilder()
          .apply {
            exchangeStepAttemptBuilder.apply {
              externalRecurringExchangeId = RECURRING_EXCHANGE_ID
              date = INTERNAL_EXCHANGE_STEP_ATTEMPT.date
              stepIndex = STEP_INDEX
              attemptNumber = 1
              state = InternalExchangeStepAttempt.State.ACTIVE
              detailsBuilder.addAllDebugLogEntries(
                INTERNAL_EXCHANGE_STEP_ATTEMPT.details.debugLogEntriesList
              )
            }
          }
          .build()
      )
  }

  @Test
  fun finishExchangeStepAttempt() =
    runBlocking<Unit> {
      assertFailsWith(NotImplementedError::class) {
        service.finishExchangeStepAttempt(FinishExchangeStepAttemptRequest.getDefaultInstance())
      }
    }
}
