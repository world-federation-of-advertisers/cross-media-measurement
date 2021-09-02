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
import com.google.common.truth.extensions.proto.FieldScope
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.ByteString
import com.google.type.Date
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.lang.IllegalArgumentException
import java.time.Instant
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
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.kingdom.CertificateKt
import org.wfanet.measurement.internal.kingdom.DataProviderKt.details
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflowKt.step
import org.wfanet.measurement.internal.kingdom.FinishExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.Provider
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.certificate
import org.wfanet.measurement.internal.kingdom.claimReadyExchangeStepRequest
import org.wfanet.measurement.internal.kingdom.createRecurringExchangeRequest
import org.wfanet.measurement.internal.kingdom.dataProvider
import org.wfanet.measurement.internal.kingdom.exchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.exchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.exchangeWorkflow
import org.wfanet.measurement.internal.kingdom.getExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.modelProvider
import org.wfanet.measurement.internal.kingdom.provider
import org.wfanet.measurement.internal.kingdom.recurringExchange
import org.wfanet.measurement.internal.kingdom.recurringExchangeDetails

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
  steps +=
    step {
      party = ExchangeWorkflow.Party.MODEL_PROVIDER
      stepIndex = STEP_INDEX
    }
}

private val DATE: Date =
  Date.newBuilder()
    .apply {
      year = 2021
      month = 8
      day = 5
    }
    .build()

private val RECURRING_EXCHANGE = recurringExchange {
  externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
  externalDataProviderId = EXTERNAL_DATA_PROVIDER_ID
  externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID
  state = RecurringExchange.State.ACTIVE
  details =
    recurringExchangeDetails {
      cronSchedule = "@daily"
      exchangeWorkflow = EXCHANGE_WORKFLOW
    }
  nextExchangeDate = DATE
}

private val DATA_PROVIDER = dataProvider {
  certificate =
    certificate {
      notValidBefore = Instant.ofEpochSecond(12345).toProtoTime()
      notValidAfter = Instant.ofEpochSecond(23456).toProtoTime()
      details =
        CertificateKt.details { x509Der = ByteString.copyFromUtf8("This is a certificate der.") }
    }
  details =
    details {
      apiVersion = "2"
      publicKey = ByteString.copyFromUtf8("This is a  public key.")
      publicKeySignature = ByteString.copyFromUtf8("This is a  public key signature.")
    }
}

private val MODEL_PROVIDER = modelProvider { externalModelProviderId = EXTERNAL_MODEL_PROVIDER_ID }

private val EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS: FieldScope =
  FieldScopes.allowingFieldDescriptors(
    ExchangeStepAttemptDetails.getDescriptor().findFieldByName("start_time"),
    ExchangeStepAttemptDetails.getDescriptor().findFieldByName("update_time"),
    ExchangeStepAttemptDetails.DebugLog.getDescriptor().findFieldByName("time")
  )

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
      modelProvidersService.createModelProvider(MODEL_PROVIDER)
      recurringExchangesService.createRecurringExchange(
        createRecurringExchangeRequest { recurringExchange = RECURRING_EXCHANGE }
      )
    }
  }

  @Test
  fun `finishExchangeStepAttempt fails for missing date`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(
          FinishExchangeStepAttemptRequest.getDefaultInstance()
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Date must be provided in the request.")
  }

  @Test
  fun `finishExchangeStepAttempt fails without exchange step attempt`() = runBlocking {
    val exception =
      assertFailsWith<IllegalArgumentException> {
        exchangeStepAttemptsService.finishExchangeStepAttempt(
          FinishExchangeStepAttemptRequest.newBuilder()
            .apply {
              externalRecurringExchangeId = 1L
              stepIndex = STEP_INDEX
              attemptNumber = 1
              date = DATE
            }
            .build()
        )
      }

    assertThat(exception).hasMessageThat().contains("Attempt for Step: $STEP_INDEX not found.")
  }

  @Test
  fun `getExchangeStepAttempt succeeds`() = runBlocking {
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
      exchangeStepAttemptsService.getExchangeStepAttempt(
        getExchangeStepAttemptRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = DATE
          stepIndex = 1
          attemptNumber = 1
          provider =
            provider {
              externalId = EXTERNAL_MODEL_PROVIDER_ID
              type = Provider.Type.MODEL_PROVIDER
            }
        }
      )

    val expected = exchangeStepAttempt {
      externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
      date = DATE
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
    exchangeStepsService.claimReadyExchangeStep(
      claimReadyExchangeStepRequest {
        provider =
          provider {
            externalId = EXTERNAL_MODEL_PROVIDER_ID
            type = Provider.Type.MODEL_PROVIDER
          }
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        exchangeStepAttemptsService.getExchangeStepAttempt(
          getExchangeStepAttemptRequest {
            externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
            date = DATE
            stepIndex = 1
            attemptNumber = 1
            provider =
              provider {
                externalId = EXTERNAL_DATA_PROVIDER_ID
                type = Provider.Type.DATA_PROVIDER
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception).hasMessageThat().contains("Exchange Step Attempt not found.")
  }

  @Test
  fun `finishExchangeStepAttempt succeeds`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
    // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/204.
  }

  @Test
  fun `finishExchangeStepAttempt fails temporarily`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
    // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/204.
  }

  @Test
  fun `finishExchangeStepAttempt fails permanently`() = runBlocking {
    // TODO(yunyeng): Add test once underlying services complete.
    // See https://github.com/world-federation-of-advertisers/cross-media-measurement/issues/204.
  }
}
