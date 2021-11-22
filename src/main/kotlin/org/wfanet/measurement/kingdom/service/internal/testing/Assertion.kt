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

import com.google.common.truth.extensions.proto.FieldScope
import com.google.common.truth.extensions.proto.FieldScopes
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.time.LocalDate
import java.time.ZoneOffset
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.common.Provider
import org.wfanet.measurement.internal.common.provider
import org.wfanet.measurement.internal.kingdom.Exchange
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.exchange
import org.wfanet.measurement.internal.kingdom.exchangeDetails
import org.wfanet.measurement.internal.kingdom.exchangeStep
import org.wfanet.measurement.internal.kingdom.exchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.exchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.getExchangeRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepAttemptRequest
import org.wfanet.measurement.internal.kingdom.getExchangeStepRequest

private const val EXTERNAL_RECURRING_EXCHANGE_ID = 222L
private const val EXTERNAL_MODEL_PROVIDER_ID = 666L
private const val STEP_INDEX = 1

internal val EXCHANGE_DATE = LocalDate.now(ZoneOffset.UTC).toProtoDate()

internal val PROVIDER = provider {
  externalId = EXTERNAL_MODEL_PROVIDER_ID
  type = Provider.Type.MODEL_PROVIDER
}

internal val EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS: FieldScope =
  FieldScopes.allowingFieldDescriptors(ExchangeStep.getDescriptor().findFieldByName("update_time"))

internal val EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS: FieldScope =
  FieldScopes.allowingFieldDescriptors(
    ExchangeStepAttempt.getDescriptor().findFieldByName("details")
  )

internal suspend fun ExchangesCoroutineImplBase.assertTestExchangeHasState(
  exchangeState: Exchange.State
) {
  assertThat(
      getExchange(
        getExchangeRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = EXCHANGE_DATE
          provider = PROVIDER
        }
      )
    )
    .isEqualTo(
      exchange {
        externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
        date = EXCHANGE_DATE
        state = exchangeState
        details = exchangeDetails {}
      }
    )
}

internal suspend fun ExchangeStepsCoroutineImplBase.assertTestExchangeStepHasState(
  exchangeStepState: ExchangeStep.State,
  exchangeStepIndex: Int = STEP_INDEX
) {
  assertThat(
      getExchangeStep(
        getExchangeStepRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = EXCHANGE_DATE
          stepIndex = exchangeStepIndex
          provider = PROVIDER
        }
      )
    )
    .ignoringFieldScope(EXCHANGE_STEP_RESPONSE_IGNORED_FIELDS)
    .isEqualTo(
      exchangeStep {
        externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
        date = EXCHANGE_DATE
        stepIndex = exchangeStepIndex
        state = exchangeStepState
        provider = PROVIDER
      }
    )
}

internal suspend fun ExchangeStepAttemptsCoroutineImplBase.assertTestExchangeStepAttemptHasState(
  exchangeStepAttemptState: ExchangeStepAttempt.State,
  attemptIndex: Int = 1
) {
  assertThat(
      getExchangeStepAttempt(
        getExchangeStepAttemptRequest {
          externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
          date = EXCHANGE_DATE
          stepIndex = STEP_INDEX
          attemptNumber = attemptIndex
          provider = PROVIDER
        }
      )
    )
    .ignoringFieldScope(EXCHANGE_STEP_ATTEMPT_RESPONSE_IGNORED_FIELDS)
    .isEqualTo(
      exchangeStepAttempt {
        externalRecurringExchangeId = EXTERNAL_RECURRING_EXCHANGE_ID
        date = EXCHANGE_DATE
        stepIndex = STEP_INDEX
        attemptNumber = attemptIndex
        state = exchangeStepAttemptState
        details = exchangeStepAttemptDetails {}
      }
    )
}
