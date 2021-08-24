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

package org.wfanet.measurement.kingdom.service.internal.testing

import com.google.cloud.spanner.Value
import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.lang.IllegalArgumentException
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt.debugLog
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ModelProvider
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Provider
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.exchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.exchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.provider

private const val INTERNAL_RECURRING_EXCHANGE_ID = 111L
private const val EXTERNAL_RECURRING_EXCHANGE_ID = 222L
private val RECURRING_EXCHANGE_ID_GENERATOR =
  FixedIdGenerator(
    InternalId(INTERNAL_RECURRING_EXCHANGE_ID),
    ExternalId(EXTERNAL_RECURRING_EXCHANGE_ID)
  )

private const val INTERNAL_DATA_PROVIDER_ID = 333L
private const val EXTERNAL_DATA_PROVIDER_ID = 444L
private val DATA_PROVIDER_ID_GENERATOR =
  FixedIdGenerator(InternalId(INTERNAL_DATA_PROVIDER_ID), ExternalId(EXTERNAL_DATA_PROVIDER_ID))

private const val INTERNAL_MODEL_PROVIDER_ID = 555L
private const val EXTERNAL_MODEL_PROVIDER_ID = 666L
private val MODEL_ID_GENERATOR =
  FixedIdGenerator(InternalId(INTERNAL_MODEL_PROVIDER_ID), ExternalId(EXTERNAL_MODEL_PROVIDER_ID))

private const val FIXED_GENERATED_INTERNAL_ID = 2345L
private const val FIXED_GENERATED_EXTERNAL_ID = 6789L
private val idGenerator =
  FixedIdGenerator(InternalId(FIXED_GENERATED_INTERNAL_ID), ExternalId(FIXED_GENERATED_EXTERNAL_ID))

private const val STEP_INDEX = 1
private val EXCHANGE_WORKFLOW: ExchangeWorkflow =
  ExchangeWorkflow.newBuilder()
    .apply {
      addSteps(
        ExchangeWorkflow.Step.newBuilder().apply {
          party = ExchangeWorkflow.Party.MODEL_PROVIDER
          stepIndex = STEP_INDEX
        }
      )
    }
    .build()

private val DATE: Date =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 8
      day = 5
    }
    .build()

private val RECURRING_EXCHANGE: RecurringExchange =
  RecurringExchange.newBuilder()
    .apply {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
      externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
      state = RecurringExchange.State.ACTIVE
      detailsBuilder.apply {
        cronSchedule = "@daily"
        exchangeWorkflow = EXCHANGE_WORKFLOW
      }
      nextExchangeDate = DATE
    }
    .build()

private val DATA_PROVIDER: DataProvider =
  DataProvider.newBuilder()
    .apply {
      certificateBuilder.apply {
        notValidBeforeBuilder.seconds = 12345
        notValidAfterBuilder.seconds = 23456
        detailsBuilder.x509Der = ByteString.copyFromUtf8("This is a certificate der.")
      }
      detailsBuilder.apply {
        apiVersion = "2"
        publicKey = ByteString.copyFromUtf8("This is a  public key.")
        publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
      }
    }
    .build()

@RunWith(JUnit4::class)
abstract class ExchangeStepAttemptsServiceTest {

  /** Creates a /RecurringExchanges service implementation using [idGenerator]. */
  protected abstract fun newRecurringExchangesService(
    idGenerator: IdGenerator
  ): RecurringExchangesCoroutineImplBase

  /** Creates a test subject. */
  protected abstract fun newDataProvidersService(
    idGenerator: IdGenerator
  ): DataProvidersCoroutineImplBase

  /** Creates a test subject. */
  protected abstract fun newModelProvidersService(
    idGenerator: IdGenerator
  ): ModelProvidersCoroutineImplBase

  /** Creates a /ExchangeSteps service implementation using [idGenerator]. */
  protected abstract fun newExchangeStepsService(
    idGenerator: IdGenerator
  ): ExchangeStepsCoroutineImplBase

  /** Creates a /ExchangeStepAttempts service implementation using [idGenerator]. */
  protected abstract fun newExchangeStepAttemptsService(
    idGenerator: IdGenerator
  ): ExchangeStepAttemptsCoroutineImplBase

  private lateinit var exchangeStepAttemptsService: ExchangeStepAttemptsCoroutineImplBase
  private lateinit var exchangeStepsService: ExchangeStepsCoroutineImplBase

  @Before
  fun initServices() {
    exchangeStepAttemptsService = newExchangeStepAttemptsService(idGenerator)
    exchangeStepsService = newExchangeStepsService(idGenerator)
    val dataProvidersService = newDataProvidersService(DATA_PROVIDER_ID_GENERATOR)
    val modelProvidersService = newModelProvidersService(MODEL_ID_GENERATOR)
    val recurringExchangesService = newRecurringExchangesService(RECURRING_EXCHANGE_ID_GENERATOR)
    runBlocking {
      dataProvidersService.createDataProvider(DATA_PROVIDER)
      modelProvidersService.createModelProvider(ModelProvider.getDefaultInstance())
      recurringExchangesService.createRecurringExchange(
        createRecurringExchangeRequest { recurringExchange = RECURRING_EXCHANGE }
      )
    }
  }

  @Test
  fun `finishExchangeStepAttempt fails for missing date`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(finishExchangeStepAttemptRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Date must be provided in the request.")
  }

  @Test
  fun `finishExchangeStepAttempt fails without exchange step attempt`() = runBlocking {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(
          finishExchangeStepAttemptRequest {
            externalRecurringExchangeId = 1L
            stepIndex = STEP_INDEX
            attemptNumber = 1
            date = DATE
          }
        )
      }

    assertThat(exception).hasMessageThat().contains("Attempt for Step: $STEP_INDEX not found.")
  }

  @Test
  fun `finishExchangeStepAttempt succeeds`() = runBlocking {
    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest {
        provider =
          provider {
            externalId = EXTERNAL_MODEL_PROVIDER_ID
            type = Provider.Type.MODEL_PROVIDER
          }
      }
    )

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        finishExchangeStepAttemptRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = DATE
          stepIndex = STEP_INDEX
          attemptNumber = 1
          state = ExchangeStepAttempt.State.SUCCEEDED
        }
      )

    val expected = exchangeStepAttempt {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = DATE
      stepIndex = STEP_INDEX
      attemptNumber = 1
      state = ExchangeStepAttempt.State.SUCCEEDED
      details =
        exchangeStepAttemptDetails {
          startTime = response.details.startTime
          updateTime = Value.COMMIT_TIMESTAMP.toProto()
          debugLogEntries +=
            debugLog {
              message = "Attempt for Step: 1 Succeeded."
              time = Value.COMMIT_TIMESTAMP.toProto()
            }
        }
    }

    assertThat(response).isEqualTo(expected)
  }

  @Test
  fun `finishExchangeStepAttempt fails temporarily`() = runBlocking {
    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest {
        provider =
          provider {
            externalId = EXTERNAL_MODEL_PROVIDER_ID
            type = Provider.Type.MODEL_PROVIDER
          }
      }
    )

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        finishExchangeStepAttemptRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = DATE
          stepIndex = STEP_INDEX
          attemptNumber = 1
          state = ExchangeStepAttempt.State.FAILED
        }
      )

    val expected = exchangeStepAttempt {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = DATE
      stepIndex = STEP_INDEX
      attemptNumber = 1
      state = ExchangeStepAttempt.State.FAILED
      details =
        exchangeStepAttemptDetails {
          startTime = response.details.startTime
          updateTime = Value.COMMIT_TIMESTAMP.toProto()
          debugLogEntries +=
            debugLog {
              message = "Attempt for Step: 1 Failed."
              time = Value.COMMIT_TIMESTAMP.toProto()
            }
        }
    }

    assertThat(response).isEqualTo(expected)
  }

  @Test
  fun `finishExchangeStepAttempt fails permanently`() = runBlocking {
    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest {
        provider =
          provider {
            externalId = EXTERNAL_MODEL_PROVIDER_ID
            type = Provider.Type.MODEL_PROVIDER
          }
      }
    )

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        finishExchangeStepAttemptRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = DATE
          stepIndex = STEP_INDEX
          attemptNumber = 1
          state = ExchangeStepAttempt.State.FAILED_STEP
        }
      )

    val expected = exchangeStepAttempt {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = DATE
      stepIndex = STEP_INDEX
      attemptNumber = 1
      state = ExchangeStepAttempt.State.FAILED_STEP
      details =
        exchangeStepAttemptDetails {
          startTime = response.details.startTime
          updateTime = Value.COMMIT_TIMESTAMP.toProto()
          debugLogEntries +=
            debugLog {
              message = "Attempt for Step: 1 Failed."
              time = Value.COMMIT_TIMESTAMP.toProto()
            }
        }
    }

    assertThat(response).isEqualTo(expected)
  }
}
