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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.rpc.ErrorInfo
import io.grpc.Status
import io.grpc.Status.Code.INVALID_ARGUMENT
import io.grpc.StatusRuntimeException
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.identity.testing.FixedIdGenerator
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProviderKt.details
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetailsKt.debugLog
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt.step
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.exchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.exchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.kingdom.deploy.common.testing.DuchyIdSetter
import org.wfanet.measurement.kingdom.service.internal.testing.Population.Companion.DUCHIES

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
private val EXCHANGE_WORKFLOW = exchangeWorkflow {
  steps += step {
    party = ExchangeWorkflow.Party.MODEL_PROVIDER
    stepIndex = STEP_INDEX
  }
}

private val RECURRING_EXCHANGE = recurringExchange {
  externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  state = RecurringExchange.State.ACTIVE
  details = recurringExchangeDetails {
    cronSchedule = "@daily"
    exchangeWorkflow = EXCHANGE_WORKFLOW
  }
  nextExchangeDate = EXCHANGE_DATE
}

private val DATA_PROVIDER = dataProvider {
  certificate = certificate {
    notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
    notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
    details =
      CertificateKt.details { x509Der = ByteString.copyFromUtf8("This is a certificate der.") }
  }
  details = details {
    apiVersion = "2"
    publicKey = ByteString.copyFromUtf8("This is a  public key.")
    publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
  }
  requiredExternalDuchyIds += DUCHIES.map { it.externalDuchyId }
}

private val MODEL_PROVIDER = modelProvider { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }

@RunWith(JUnit4::class)
abstract class ExchangeStepAttemptsServiceTest {
  @get:Rule val duchyIdSetter = DuchyIdSetter(DUCHIES)

  /** Creates a /RecurringExchanges service implementation using [idGenerator]. */
  protected abstract fun newRecurringExchangesService(
    idGenerator: IdGenerator
  ): RecurringExchangesCoroutineImplBase

  /** Creates a /Exchanges service implementation using [idGenerator]. */
  protected abstract fun newExchangesService(idGenerator: IdGenerator): ExchangesCoroutineImplBase

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

  private lateinit var recurringExchangesService: RecurringExchangesCoroutineImplBase
  private lateinit var exchangesService: ExchangesCoroutineImplBase
  private lateinit var exchangeStepsService: ExchangeStepsCoroutineImplBase
  private lateinit var exchangeStepAttemptsService: ExchangeStepAttemptsCoroutineImplBase

  @Before
  fun initServices() {
    recurringExchangesService = newRecurringExchangesService(RECURRING_EXCHANGE_ID_GENERATOR)
    exchangesService = newExchangesService(idGenerator)
    exchangeStepsService = newExchangeStepsService(idGenerator)
    exchangeStepAttemptsService = newExchangeStepAttemptsService(idGenerator)
    val dataProvidersService = newDataProvidersService(DATA_PROVIDER_ID_GENERATOR)
    val modelProvidersService = newModelProvidersService(MODEL_ID_GENERATOR)
    runBlocking {
      dataProvidersService.createDataProvider(DATA_PROVIDER)
      modelProvidersService.createModelProvider(MODEL_PROVIDER)
    }
  }

  @Test
  fun `finishExchangeStepAttempt with multiple steps succeeds`() = runBlocking {
    createRecurringExchange(
      RECURRING_EXCHANGE.copy {
        details =
          RECURRING_EXCHANGE.details.copy {
            exchangeWorkflow = exchangeWorkflow {
              addNextStep(ExchangeWorkflow.Party.MODEL_PROVIDER, emptyList())
              addNextStep(ExchangeWorkflow.Party.MODEL_PROVIDER, listOf(1))
              addNextStep(ExchangeWorkflow.Party.MODEL_PROVIDER, listOf(2))
            }
          }
      }
    )

    val claimReadyExchangeStepResponse =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse.exchangeStep.stepIndex).isEqualTo(1L)
    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.SUCCEEDED)
      )
    assertThat(response.attemptNumber).isEqualTo(1L)
    val expected = makeExchangeStepAttempt(ExchangeStepAttempt.State.SUCCEEDED)
    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
    assertThat(claimReadyExchangeStepResponse.attemptNumber).isEqualTo(1L)

    val claimReadyExchangeStepResponse2 =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse2.exchangeStep.stepIndex).isEqualTo(2L)
    val response2 =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(
          attemptState = ExchangeStepAttempt.State.SUCCEEDED,
          attemptNumber = 1,
          stepProvider = PROVIDER,
          stepIndex = 2
        )
      )
    assertThat(response2.attemptNumber).isEqualTo(1L)

    val claimReadyExchangeStepResponse3 =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse3.exchangeStep.stepIndex).isEqualTo(3L)
    val response3 =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(
          attemptState = ExchangeStepAttempt.State.SUCCEEDED,
          attemptNumber = 1,
          stepProvider = PROVIDER,
          stepIndex = 3
        )
      )
    assertThat(response3.attemptNumber).isEqualTo(1L)

    exchangesService.assertTestExchangeHasState(Exchange.State.SUCCEEDED)
    exchangeStepsService.assertTestExchangeStepHasState(
      exchangeStepState = ExchangeStep.State.SUCCEEDED,
      exchangeStepIndex = 1
    )
    exchangeStepsService.assertTestExchangeStepHasState(
      exchangeStepState = ExchangeStep.State.SUCCEEDED,
      exchangeStepIndex = 2
    )
    exchangeStepsService.assertTestExchangeStepHasState(
      exchangeStepState = ExchangeStep.State.SUCCEEDED,
      exchangeStepIndex = 3
    )
  }

  @Test
  fun `finishExchangeStepAttempt with deadlock multiple steps fails`() = runBlocking {
    createRecurringExchange(
      RECURRING_EXCHANGE.copy {
        details =
          RECURRING_EXCHANGE.details.copy {
            exchangeWorkflow = exchangeWorkflow {
              addNextStep(ExchangeWorkflow.Party.MODEL_PROVIDER, emptyList())
              addNextStep(ExchangeWorkflow.Party.MODEL_PROVIDER, listOf(3))
              addNextStep(ExchangeWorkflow.Party.MODEL_PROVIDER, listOf(2))
            }
          }
      }
    )

    val claimReadyExchangeStepResponse =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse.exchangeStep.stepIndex).isEqualTo(1L)
    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.SUCCEEDED)
      )
    assertThat(response.attemptNumber).isEqualTo(1L)
    val expected = makeExchangeStepAttempt(ExchangeStepAttempt.State.SUCCEEDED)
    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
    assertThat(claimReadyExchangeStepResponse.attemptNumber).isEqualTo(1L)

    val claimReadyExchangeStepResponse2 =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse2)
      .isEqualTo(org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse {})

    exchangesService.assertTestExchangeHasState(Exchange.State.ACTIVE)
    exchangeStepsService.assertTestExchangeStepHasState(
      exchangeStepState = ExchangeStep.State.SUCCEEDED,
      exchangeStepIndex = 1
    )
    exchangeStepsService.assertTestExchangeStepHasState(
      exchangeStepState = ExchangeStep.State.BLOCKED,
      exchangeStepIndex = 2
    )
    exchangeStepsService.assertTestExchangeStepHasState(
      exchangeStepState = ExchangeStep.State.BLOCKED,
      exchangeStepIndex = 3
    )
  }

  @Test
  fun `finishExchangeStepAttempt fails for missing date`() = runBlocking {
    createRecurringExchange()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(finishExchangeStepAttemptRequest {})
      }

    assertThat(exception.status.code).isEqualTo(INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Date must be provided in the request.")
  }

  @Test
  fun `finishExchangeStepAttempt fails without exchange step attempt`() = runBlocking {
    createRecurringExchange()

    val request: FinishExchangeStepAttemptRequest = makeRequest(ExchangeStepAttempt.State.SUCCEEDED)
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    val errorInfo: ErrorInfo = checkNotNull(exception.errorInfo)
    assertThat(errorInfo.reason).isEqualTo("EXCHANGE_STEP_ATTEMPT_NOT_FOUND")
  }

  @Test
  fun `finishExchangeStepAttempt fails for wrong provider`() = runBlocking {
    createRecurringExchange()

    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest { provider = PROVIDER }
    )

    val request: FinishExchangeStepAttemptRequest =
      makeRequest(
        ExchangeStepAttempt.State.SUCCEEDED,
        1,
        provider {
          externalId = EXTERNAL_DATA_PROVIDER_ID
          type = Provider.Type.DATA_PROVIDER
        }
      )
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(request)
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    val errorInfo: ErrorInfo = checkNotNull(exception.errorInfo)
    assertThat(errorInfo.reason).isEqualTo("EXCHANGE_STEP_NOT_FOUND")
  }

  @Test
  fun `getExchangeStepAttempt succeeds`() = runBlocking {
    createRecurringExchange()

    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest { provider = PROVIDER }
    )

    val response =
      exchangeStepAttemptsService.getExchangeStepAttempt(
        getExchangeStepAttemptRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = EXCHANGE_DATE
          stepIndex = 1
          attemptNumber = 1
          provider = PROVIDER
        }
      )

    val expected = exchangeStepAttempt {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = EXCHANGE_DATE
      stepIndex = STEP_INDEX
      attemptNumber = 1
      state = ExchangeStepAttempt.State.ACTIVE
      details = exchangeStepAttemptDetails {}
    }

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
  }

  @Test
  fun `getExchangeStepAttempt fails with wrong provider`() = runBlocking {
    createRecurringExchange()

    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest { provider = PROVIDER }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.getExchangeStepAttempt(
          getExchangeStepAttemptRequest {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
            date = EXCHANGE_DATE
            stepIndex = 1
            attemptNumber = 1
            provider = provider {
              externalId = EXTERNAL_DATA_PROVIDER_ID
              type = Provider.Type.DATA_PROVIDER
            }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    val errorInfo: ErrorInfo = checkNotNull(exception.errorInfo)
    assertThat(errorInfo.reason).isEqualTo("EXCHANGE_STEP_ATTEMPT_NOT_FOUND")
  }

  @Test
  fun `finishExchangeStepAttempt succeeds`() = runBlocking {
    createRecurringExchange()

    val claimReadyExchangeStepResponse =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.SUCCEEDED)
      )
    assertThat(response.attemptNumber).isEqualTo(1L)

    val expected = makeExchangeStepAttempt(ExchangeStepAttempt.State.SUCCEEDED)
    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
    assertThat(claimReadyExchangeStepResponse.attemptNumber).isEqualTo(1L)

    exchangesService.assertTestExchangeHasState(Exchange.State.SUCCEEDED)
    exchangeStepsService.assertTestExchangeStepHasState(ExchangeStep.State.SUCCEEDED)
  }

  @Test
  fun `finishExchangeStepAttempt fails temporarily`() = runBlocking {
    createRecurringExchange()

    val claimReadyExchangeStepResponse =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.FAILED)
      )
    assertThat(response.attemptNumber).isEqualTo(1L)

    val expected = makeExchangeStepAttempt(ExchangeStepAttempt.State.FAILED)

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
    assertThat(claimReadyExchangeStepResponse.attemptNumber).isEqualTo(1L)
    exchangesService.assertTestExchangeHasState(Exchange.State.ACTIVE)
    exchangeStepsService.assertTestExchangeStepHasState(ExchangeStep.State.READY_FOR_RETRY)
  }

  @Test
  fun `finishExchangeStepAttempt fails permanently`() = runBlocking {
    createRecurringExchange()

    val claimReadyExchangeStepResponse =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.FAILED_STEP)
      )
    assertThat(response.attemptNumber).isEqualTo(1L)

    val expected = makeExchangeStepAttempt(ExchangeStepAttempt.State.FAILED_STEP)

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
    assertThat(claimReadyExchangeStepResponse.attemptNumber).isEqualTo(1L)
    exchangesService.assertTestExchangeHasState(Exchange.State.FAILED)
    exchangeStepsService.assertTestExchangeStepHasState(ExchangeStep.State.FAILED)
  }

  @Test
  fun `finishExchangeStepAttempt succeeds on second try`() = runBlocking {
    createRecurringExchange()

    val claimReadyExchangeStepResponse =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse.attemptNumber).isEqualTo(1L)

    val failedAttempt =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.FAILED)
      )
    assertThat(failedAttempt.attemptNumber).isEqualTo(1L)
    exchangesService.assertTestExchangeHasState(Exchange.State.ACTIVE)
    exchangeStepsService.assertTestExchangeStepHasState(ExchangeStep.State.READY_FOR_RETRY)

    val claimReadyExchangeStepResponse2 =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest { provider = PROVIDER }
      )
    assertThat(claimReadyExchangeStepResponse.exchangeStep)
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(claimReadyExchangeStepResponse2.exchangeStep)
    assertThat(claimReadyExchangeStepResponse2.exchangeStep.updateTime.toGcloudTimestamp())
      .isGreaterThan(claimReadyExchangeStepResponse.exchangeStep.updateTime.toGcloudTimestamp())
    assertThat(claimReadyExchangeStepResponse2.attemptNumber).isEqualTo(2L)

    val response =
      exchangeStepAttemptsService.finishExchangeStepAttempt(
        makeRequest(ExchangeStepAttempt.State.SUCCEEDED, 2)
      )
    assertThat(response.attemptNumber).isEqualTo(2L)

    val expected = makeExchangeStepAttempt(ExchangeStepAttempt.State.SUCCEEDED, 2)

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(expected)
    exchangesService.assertTestExchangeHasState(Exchange.State.SUCCEEDED)
    exchangeStepsService.assertTestExchangeStepHasState(ExchangeStep.State.SUCCEEDED)
  }

  private suspend fun createRecurringExchange(recExchange: RecurringExchange = RECURRING_EXCHANGE) {
    recurringExchangesService.createRecurringExchange(
      createRecurringExchangeRequest { recurringExchange = recExchange }
    )
  }

  private fun makeRequest(
    attemptState: ExchangeStepAttempt.State,
    attemptNumber: Int = 1,
    stepProvider: Provider = PROVIDER,
    stepIndex: Int = STEP_INDEX
  ): FinishExchangeStepAttemptRequest {
    return finishExchangeStepAttemptRequest {
      provider = stepProvider
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = EXCHANGE_DATE
      this.stepIndex = stepIndex
      this.attemptNumber = attemptNumber
      state = attemptState
    }
  }

  private fun makeExchangeStepAttempt(
    attemptState: ExchangeStepAttempt.State,
    attemptNo: Int = 1
  ): ExchangeStepAttempt {
    val debugLogMessage =
      when (attemptState) {
        ExchangeStepAttempt.State.SUCCEEDED -> "Attempt for Step: 1 Succeeded."
        else -> "Attempt for Step: 1 Failed."
      }
    return exchangeStepAttempt {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = EXCHANGE_DATE
      stepIndex = STEP_INDEX
      attemptNumber = attemptNo
      state = attemptState
      details = exchangeStepAttemptDetails {
        debugLogEntries += debugLog { message = debugLogMessage }
      }
    }
  }

  private fun ExchangeWorkflowKt.Dsl.addNextStep(
    party: ExchangeWorkflow.Party,
    prerequisites: Iterable<Int>
  ) {
    steps += step {
      stepIndex = this@addNextStep.steps.size + 1
      this.party = party
      prerequisiteStepIndices += prerequisites
    }
  }
}
