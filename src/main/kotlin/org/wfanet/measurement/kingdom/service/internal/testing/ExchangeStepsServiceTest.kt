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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.common.testing.TestClockWithNamedInstants
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.ClaimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.DataProviderKt.details
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt.step
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequestKt.filter
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepResponse
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.exchangeStep
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow
import org.wfanet.measurement.internal.kingdom.finishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails
import org.wfanet.measurement.internal.kingdom.streamExchangeStepsRequest
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

private val EXCHANGE_STEP = exchangeStep {
  externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
  date = EXCHANGE_DATE
  state = ExchangeStep.State.IN_PROGRESS
  stepIndex = STEP_INDEX
  provider = PROVIDER
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

@RunWith(JUnit4::class)
abstract class ExchangeStepsServiceTest {

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
    idGenerator: IdGenerator,
    serviceClock: Clock
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
    exchangeStepsService = newExchangeStepsService(idGenerator, Clock.systemUTC())
    exchangeStepAttemptsService = newExchangeStepAttemptsService(idGenerator)
    val dataProvidersService = newDataProvidersService(DATA_PROVIDER_ID_GENERATOR)
    val modelProvidersService = newModelProvidersService(MODEL_ID_GENERATOR)
    runBlocking {
      dataProvidersService.createDataProvider(DATA_PROVIDER)
      modelProvidersService.createModelProvider(modelProvider {})
    }
  }

  private suspend fun claimReadyExchangeStep(): ClaimReadyExchangeStepResponse {
    return exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest { provider = PROVIDER }
    )
  }

  @Test
  fun `claimReadyExchangeStepRequest fails for missing Provider`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepsService.claimReadyExchangeStep(claimReadyExchangeStepRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Invalid Provider")
  }

  @Test
  fun `claimReadyExchangeStepRequest returns empty for wrong Provider`() = runBlocking {
    createRecurringExchange()

    val response =
      exchangeStepsService.claimReadyExchangeStep(
        claimReadyExchangeStepRequest {
          provider = provider {
            externalId = EXTERNAL_DATA_PROVIDER_ID
            type = Provider.Type.DATA_PROVIDER
          }
        }
      )

    assertThat(response).isEqualToDefaultInstance()
  }

  @Test
  fun `claimReadyExchangeStepRequest fails without recurring exchange`() = runBlocking {
    assertThat(claimReadyExchangeStep()).isEqualToDefaultInstance()
  }

  @Test
  fun `claimReadyExchangeStepRequest succeeds`() = runBlocking {
    createRecurringExchange()

    assertThat(claimReadyExchangeStep())
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(
        claimReadyExchangeStepResponse {
          exchangeStep = EXCHANGE_STEP
          attemptNumber = 1
        }
      )

    exchangesService.assertTestExchangeHasState(Exchange.State.ACTIVE)
    exchangeStepsService.assertTestExchangeStepHasState(ExchangeStep.State.IN_PROGRESS)
    exchangeStepAttemptsService.assertTestExchangeStepAttemptHasState(
      ExchangeStepAttempt.State.ACTIVE
    )

    assertThat(claimReadyExchangeStep()).isEqualToDefaultInstance()
  }

  @Test
  fun `claimReadyExchangeStepRequest expires ExchangeStepAttempt`() = runBlocking {
    createRecurringExchange()

    // Initiate the clock as yesterday and create an ExchangeStepAttempt.
    val expirationDuration = Duration.ofDays(1).seconds
    val clock = TestClockWithNamedInstants(Instant.now().minusSeconds(expirationDuration))
    exchangeStepsService = newExchangeStepsService(idGenerator, clock)

    assertThat(claimReadyExchangeStep())
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(
        claimReadyExchangeStepResponse {
          exchangeStep = EXCHANGE_STEP
          attemptNumber = 1
        }
      )

    exchangesService.assertTestExchangeHasState(Exchange.State.ACTIVE)

    clock.tickSeconds("Next Day", expirationDuration)

    assertThat(claimReadyExchangeStep())
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(
        claimReadyExchangeStepResponse {
          exchangeStep = EXCHANGE_STEP
          attemptNumber = 2
        }
      )

    exchangesService.assertTestExchangeHasState(Exchange.State.ACTIVE)
    exchangeStepAttemptsService.assertTestExchangeStepAttemptHasState(
      ExchangeStepAttempt.State.FAILED,
      attemptIndex = 1
    )
    exchangeStepAttemptsService.assertTestExchangeStepAttemptHasState(
      ExchangeStepAttempt.State.ACTIVE,
      attemptIndex = 2
    )
  }

  @Test
  fun `claimReadyExchangeStepRequest without any step`() =
    runBlocking {
      // TODO(yunyeng): Add test once underlying services complete.
    }

  @Test
  fun `claimReadyExchangeStep creates multiple Exchanges`() = runBlocking {
    val date = LocalDate.now(ZoneOffset.UTC).minusDays(3)

    recurringExchangesService.createRecurringExchange(
      createRecurringExchangeRequest {
        recurringExchange = RECURRING_EXCHANGE.copy { nextExchangeDate = date.toProtoDate() }
      }
    )

    fun com.google.type.Date.toLocalDateOrNull(): LocalDate? {
      return if (year == 0 && month == 0 && day == 0) null else toLocalDate()
    }

    val responses = (0 until 5).map { claimReadyExchangeStep() }
    val dates = responses.map { it.exchangeStep.date.toLocalDateOrNull() }

    assertThat(dates)
      .containsExactly(date, date.plusDays(1), date.plusDays(2), date.plusDays(3), null)
      .inOrder()
  }

  @Test
  fun `claimReadyExchangeStep does not create new Exchanges if one is failed`() = runBlocking {
    val exchangeDate = LocalDate.now(ZoneOffset.UTC).minusDays(3).toProtoDate()

    recurringExchangesService.createRecurringExchange(
      createRecurringExchangeRequest {
        recurringExchange = RECURRING_EXCHANGE.copy { nextExchangeDate = exchangeDate }
      }
    )

    val response = claimReadyExchangeStep()
    exchangeStepAttemptsService.finishExchangeStepAttempt(
      finishExchangeStepAttemptRequest {
        provider = response.exchangeStep.provider
        externalRecurringExchangeId = response.exchangeStep.externalRecurringExchangeId
        date = response.exchangeStep.date
        stepIndex = response.exchangeStep.stepIndex
        attemptNumber = response.attemptNumber
        state = ExchangeStepAttempt.State.FAILED_STEP
      }
    )

    val exchange =
      exchangesService.getExchange(
        getExchangeRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = exchangeDate
          provider = response.exchangeStep.provider
        }
      )
    assertThat(exchange.state).isEqualTo(Exchange.State.FAILED)

    assertThat(claimReadyExchangeStep()).isEqualToDefaultInstance()
  }

  @Test
  fun `getExchangeStepRequest succeeds`() = runBlocking {
    createRecurringExchange()

    claimReadyExchangeStep()

    val response =
      exchangeStepsService.getExchangeStep(
        getExchangeStepRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = EXCHANGE_DATE
          stepIndex = 1
          provider = PROVIDER
        }
      )

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .isEqualTo(EXCHANGE_STEP)
  }

  @Test
  fun `getExchangeStepRequest fails with wrong provider`() = runBlocking {
    createRecurringExchange()
    claimReadyExchangeStep()

    val request = getExchangeStepRequest {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = EXCHANGE_DATE
      stepIndex = 1
      provider = provider {
        externalId = 555L
        type = Provider.Type.DATA_PROVIDER
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> { exchangeStepsService.getExchangeStep(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `streamExchangeSteps returns empty with wrong step provider`(): Unit = runBlocking {
    createRecurringExchange()
    claimReadyExchangeStep()

    val response =
      exchangeStepsService
        .streamExchangeSteps(
          streamExchangeStepsRequest {
            filter = filter {
              stepProvider = provider {
                externalId = EXTERNAL_DATA_PROVIDER_ID
                type = Provider.Type.DATA_PROVIDER
              }
              principal = PROVIDER
              externalRecurringExchangeIds += EXTERNAL_RECURRING_EXCHANGE_ID
            }
          }
        )
        .toList()

    assertThat(response).isEmpty()
  }

  @Test
  fun `streamExchangeSteps returns only step provider's steps`(): Unit = runBlocking {
    createRecurringExchangeWithMultipleSteps()
    claimReadyExchangeStep()

    val response =
      exchangeStepsService
        .streamExchangeSteps(
          streamExchangeStepsRequest {
            filter = filter {
              stepProvider = PROVIDER
              principal = PROVIDER
              externalRecurringExchangeIds += EXTERNAL_RECURRING_EXCHANGE_ID
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .containsExactly(
        EXCHANGE_STEP.copy {
          state = ExchangeStep.State.BLOCKED
          stepIndex = 3
        },
        EXCHANGE_STEP
      )
      .inOrder()
  }

  @Test
  fun `streamExchangeSteps returns all exchangeSteps in order`(): Unit = runBlocking {
    createRecurringExchangeWithMultipleSteps()
    claimReadyExchangeStep()

    val response =
      exchangeStepsService
        .streamExchangeSteps(
          streamExchangeStepsRequest {
            filter = filter {
              principal = PROVIDER
              externalRecurringExchangeIds += EXTERNAL_RECURRING_EXCHANGE_ID
            }
          }
        )
        .toList()

    assertThat(response)
      .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
      .containsExactly(
        EXCHANGE_STEP.copy {
          state = ExchangeStep.State.BLOCKED
          stepIndex = 2
          provider = provider {
            type = Provider.Type.DATA_PROVIDER
            externalId = EXTERNAL_DATA_PROVIDER_ID
          }
        },
        EXCHANGE_STEP.copy {
          state = ExchangeStep.State.BLOCKED
          stepIndex = 3
        },
        EXCHANGE_STEP
      )
      .inOrder()
  }

  @Test
  fun `streamExchangeSteps respects limit`(): Unit = runBlocking {
    createRecurringExchangeWithMultipleSteps()
    claimReadyExchangeStep()

    val response =
      exchangeStepsService
        .streamExchangeSteps(
          streamExchangeStepsRequest {
            filter = filter {
              principal = PROVIDER
              externalRecurringExchangeIds += EXTERNAL_RECURRING_EXCHANGE_ID
            }
            limit = 1
          }
        )
        .toList()

    assertThat(response).hasSize(1)
  }

  private suspend fun createRecurringExchange(recExchange: RecurringExchange = RECURRING_EXCHANGE) {
    recurringExchangesService.createRecurringExchange(
      createRecurringExchangeRequest { recurringExchange = recExchange }
    )
  }

  private suspend fun createRecurringExchangeWithMultipleSteps() {
    val workflow = exchangeWorkflow {
      steps += step {
        party = ExchangeWorkflow.Party.MODEL_PROVIDER
        stepIndex = 1
      }
      steps += step {
        party = ExchangeWorkflow.Party.DATA_PROVIDER
        stepIndex = 2
        prerequisiteStepIndices += 1
      }
      steps += step {
        party = ExchangeWorkflow.Party.MODEL_PROVIDER
        stepIndex = 3
        prerequisiteStepIndices += 1
        prerequisiteStepIndices += 2
      }
    }
    createRecurringExchange(
      RECURRING_EXCHANGE.copy { details = details.copy { exchangeWorkflow = workflow } }
    )
  }
}
