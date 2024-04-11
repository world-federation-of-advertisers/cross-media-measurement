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
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepAttemptKey
import org.wfanet.measurement.api.v2alpha.CanonicalExchangeStepKey
import org.wfanet.measurement.api.v2alpha.CanonicalRecurringExchangeKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ExchangeStepAttempt
import org.wfanet.measurement.api.v2alpha.appendExchangeStepAttemptLogEntryRequest
import org.wfanet.measurement.api.v2alpha.finishExchangeStepAttemptRequest
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.internal.kingdom.AppendLogEntryRequest as InternalAppendLogEntryRequest
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt as InternalExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase as InternalExchangeStepAttempts
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase as InternalExchangeSteps
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.exchangeStep as internalExchangeStep
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest as internalFinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepAttemptNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.ExchangeStepNotFoundException
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.RecurringExchangeNotFoundException

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
private val EXTERNAL_DATA_PROVIDER_ID = ExternalId(12345L)
private val DATA_PROVIDER_KEY = DataProviderKey(EXTERNAL_DATA_PROVIDER_ID.apiId.value)
private val RECURRING_EXCHANGE_KEY =
  CanonicalRecurringExchangeKey(ExternalId(RECURRING_EXCHANGE_ID).apiId.value)
private val EXCHANGE_STEP_KEY =
  CanonicalExchangeStepKey(
    recurringExchangeId = RECURRING_EXCHANGE_KEY.recurringExchangeId,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = STEP_INDEX.toString(),
  )
private val EXCHANGE_STEP_ATTEMPT_KEY =
  CanonicalExchangeStepAttemptKey(
    recurringExchangeId = RECURRING_EXCHANGE_KEY.recurringExchangeId,
    exchangeId = EXCHANGE_ID,
    exchangeStepId = STEP_INDEX.toString(),
    exchangeStepAttemptId = ATTEMPT_NUMBER.toString(),
  )

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
  return CanonicalExchangeStepAttemptKey(
      recurringExchangeId = externalIdToApiId(RECURRING_EXCHANGE_ID),
      exchangeId = EXCHANGE_ID,
      exchangeStepId = STEP_INDEX.toString(),
      exchangeStepAttemptId = ATTEMPT_NUMBER.toString(),
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
        entryTime = DEBUG_LOG_1_TIME
        message = DEBUG_LOG_1_MESSAGE
      }
      addDebugLogEntriesBuilder().apply {
        entryTime = DEBUG_LOG_2_TIME
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
      ExchangeStepsCoroutineStub(grpcTestServerRule.channel),
    )

  @Test
  fun appendLogEntry() {
    val externalDataProviderId = ExternalId(12345L)
    val principal = DataProviderPrincipal(DataProviderKey(externalDataProviderId.apiId.value))
    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenReturn(
          internalExchangeStep { this.externalDataProviderId = externalDataProviderId.value }
        )
    }
    val request = appendExchangeStepAttemptLogEntryRequest {
      name = EXCHANGE_STEP_ATTEMPT.name
      logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
    }

    val response: ExchangeStepAttempt =
      withPrincipal(principal) {
        runBlocking { service.appendExchangeStepAttemptLogEntry(request) }
      }

    assertThat(response).isEqualTo(EXCHANGE_STEP_ATTEMPT)
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
  fun `finishExchangeStepAttempt throws PERMISSION_DENIED when ExchangeStep not found`() {
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
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(principal) { runBlocking { service.finishExchangeStepAttempt(request) } }
      }
    assertWithMessage(e.status.toString())
      .that(e.status.code)
      .isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `finishExchangeStepAttempt throws PERMISSION_DENIED when ExchangeStep party mismatches`() {
    val externalDataProviderId = ExternalId(12345L)
    val principal = DataProviderPrincipal(DataProviderKey(externalDataProviderId.apiId.value))
    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenReturn(internalExchangeStep { externalModelProviderId = externalDataProviderId.value })
    }
    val request = finishExchangeStepAttemptRequest {
      name = EXCHANGE_STEP_ATTEMPT.name
      finalState = ExchangeStepAttempt.State.FAILED
      logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(principal) { runBlocking { service.finishExchangeStepAttempt(request) } }
      }

    assertWithMessage(exception.status.toString())
      .that(exception.status.code)
      .isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun finishExchangeStepAttempt() {
    val externalDataProviderId = ExternalId(12345L)
    val dataProviderKey = DataProviderKey(externalDataProviderId.apiId.value)
    val principal = DataProviderPrincipal(dataProviderKey)

    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenReturn(
          internalExchangeStep { this.externalDataProviderId = externalDataProviderId.value }
        )
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
        }
      )

    verifyProtoArgument(
        internalExchangeStepAttempts,
        InternalExchangeStepAttempts::finishExchangeStepAttempt,
      )
      .ignoringFieldAbsence()
      .isEqualTo(
        internalFinishExchangeStepAttemptRequest {
          externalRecurringExchangeId = RECURRING_EXCHANGE_ID
          date = INTERNAL_EXCHANGE_STEP_ATTEMPT.date
          stepIndex = STEP_INDEX
          attemptNumber = ATTEMPT_NUMBER
          state = InternalExchangeStepAttempt.State.FAILED
          debugLogEntries += INTERNAL_EXCHANGE_STEP_ATTEMPT.details.debugLogEntriesList
        }
      )
  }

  @Test
  fun `finishExchangeStepAttempt throws NOT_FOUND with recurring exchange name when recurring exchange not found`() {
    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenReturn(
          internalExchangeStep { this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value }
        )
    }
    internalExchangeStepAttempts.stub {
      onBlocking { finishExchangeStepAttempt(any()) }
        .thenThrow(
          RecurringExchangeNotFoundException(ExternalId(RECURRING_EXCHANGE_ID))
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(DataProviderPrincipal(DATA_PROVIDER_KEY)) {
          runBlocking {
            service.finishExchangeStepAttempt(
              finishExchangeStepAttemptRequest {
                name = EXCHANGE_STEP_ATTEMPT.name
                finalState = ExchangeStepAttempt.State.FAILED
                logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("recurringExchange", RECURRING_EXCHANGE_KEY.toName())
  }

  @Test
  fun `finishExchangeStepAttempt throws PERMISSION_DENIED with exchange step name when exchange step not found`() {
    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenThrow(
          ExchangeStepNotFoundException(ExternalId(RECURRING_EXCHANGE_ID), DATE, STEP_INDEX)
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(DataProviderPrincipal(DATA_PROVIDER_KEY)) {
          runBlocking {
            service.finishExchangeStepAttempt(
              finishExchangeStepAttemptRequest {
                name = EXCHANGE_STEP_ATTEMPT.name
                finalState = ExchangeStepAttempt.State.FAILED
                logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("exchangeStep", EXCHANGE_STEP_KEY.toName())
  }

  @Test
  fun `finishExchangeStepAttempt throws NOT_FOUND with exchange step attempt name when exchange step attempt not found`() {
    internalExchangeSteps.stub {
      onBlocking { getExchangeStep(any()) }
        .thenReturn(
          internalExchangeStep { this.externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID.value }
        )
    }
    internalExchangeStepAttempts.stub {
      onBlocking { finishExchangeStepAttempt(any()) }
        .thenThrow(
          ExchangeStepAttemptNotFoundException(
              ExternalId(RECURRING_EXCHANGE_ID),
              DATE,
              STEP_INDEX,
              ATTEMPT_NUMBER,
            )
            .asStatusRuntimeException(Status.Code.NOT_FOUND)
        )
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipal(DataProviderPrincipal(DATA_PROVIDER_KEY)) {
          runBlocking {
            service.finishExchangeStepAttempt(
              finishExchangeStepAttemptRequest {
                name = EXCHANGE_STEP_ATTEMPT.name
                finalState = ExchangeStepAttempt.State.FAILED
                logEntries += EXCHANGE_STEP_ATTEMPT.debugLogEntriesList
              }
            )
          }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.metadataMap)
      .containsEntry("exchangeStepAttempt", EXCHANGE_STEP_ATTEMPT_KEY.toName())
  }
}
