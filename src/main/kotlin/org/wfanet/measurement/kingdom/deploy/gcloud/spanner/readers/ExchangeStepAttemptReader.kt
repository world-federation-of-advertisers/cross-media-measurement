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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers

import com.google.cloud.spanner.Struct
import com.google.type.Date
import java.time.Clock
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails
import org.wfanet.measurement.internal.kingdom.ExchangeWorkflow
import org.wfanet.measurement.internal.kingdom.exchangeStepAttempt

/** Reads [ExchangeStepAttempt] protos from Spanner. */
class ExchangeStepAttemptReader : SpannerReader<ExchangeStepAttemptReader.Result>() {

  data class Result(val exchangeStepAttempt: ExchangeStepAttempt, val recurringExchangeId: Long)

  override val baseSql: String =
    """
    SELECT
      ExchangeStepAttempts.RecurringExchangeId,
      ExchangeStepAttempts.Date,
      ExchangeStepAttempts.StepIndex,
      ExchangeStepAttempts.AttemptIndex,
      ExchangeStepAttempts.State,
      ExchangeStepAttempts.ExchangeStepAttemptDetails,
      RecurringExchanges.ExternalRecurringExchangeId
    FROM ExchangeStepAttempts
    JOIN RecurringExchanges USING (RecurringExchangeId)
    JOIN ExchangeSteps USING (RecurringExchangeId, Date, StepIndex)
    """
      .trimIndent()

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchangeStepAttempt =
        exchangeStepAttempt {
          externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
          date = struct.getDate("Date").toProtoDate()
          stepIndex = struct.getLong("StepIndex").toInt()
          attemptNumber = struct.getLong("AttemptIndex").toInt()
          state = struct.getProtoEnum("State", ExchangeStepAttempt.State::forNumber)
          details =
            struct.getProtoMessage(
              "ExchangeStepAttemptDetails",
              ExchangeStepAttemptDetails.getDefaultInstance(),
            )
        },
      recurringExchangeId = struct.getLong("RecurringExchangeId"),
    )
  }

  suspend fun readByExternalIds(
    readContext: AsyncDatabaseClient.ReadContext,
    externalRecurringExchangeId: ExternalId,
    exchangeDate: Date,
    stepIndex: Int,
    attemptNumber: Int,
  ): Result? {
    fillStatementBuilder {
      appendClause(
        """
        WHERE
          RecurringExchanges.ExternalRecurringExchangeId = @external_recurring_exchange_id
          AND ExchangeStepAttempts.Date = @date
          AND ExchangeStepAttempts.StepIndex = @step_index
          AND ExchangeStepAttempts.AttemptIndex = @attempt_index
        """
          .trimIndent()
      )
      bind("external_recurring_exchange_id" to externalRecurringExchangeId)
      bind("date" to exchangeDate.toCloudDate())
      bind("step_index" to stepIndex.toLong())
      bind("attempt_index" to attemptNumber.toLong())
    }

    return execute(readContext).singleOrNullIfEmpty()
  }

  private object Params {
    const val EXCHANGE_STEP_STATE = "exchange_step_state"
    const val EXCHANGE_STEP_ATTEMPT_STATE = "exchange_step_attempt_state"
    const val NOW = "now"
    const val EXTERNAL_PARTY_ID = "externalPartyId"
  }

  companion object {
    fun forExpiredAttempts(
      party: ExchangeWorkflow.Party,
      externalPartyId: ExternalId,
      clock: Clock,
      limit: Long = 10,
    ): SpannerReader<Result> {
      return ExchangeStepAttemptReader().fillStatementBuilder {
        val conjuncts =
          mutableListOf(
            "ExchangeSteps.State = @${Params.EXCHANGE_STEP_STATE}",
            "ExchangeStepAttempts.State = @${Params.EXCHANGE_STEP_ATTEMPT_STATE}",
            "ExchangeStepAttempts.ExpirationTime <= @${Params.NOW}",
          )
        bind(Params.EXCHANGE_STEP_STATE).toInt64(ExchangeStep.State.IN_PROGRESS)
        bind(Params.EXCHANGE_STEP_ATTEMPT_STATE).toInt64(ExchangeStepAttempt.State.ACTIVE)
        // Due to the fact that we set ExpirationTime using the application clock, we should to be
        // consistent and use the application clock for comparisons rather than DB time.
        bind(Params.NOW).to(clock.instant().toGcloudTimestamp())

        when (party) {
          ExchangeWorkflow.Party.MODEL_PROVIDER -> {
            conjuncts.add(
              """
              ExchangeSteps.ModelProviderId = (
                SELECT ModelProviderId
                FROM ModelProviders
                WHERE ExternalModelProviderId = @${Params.EXTERNAL_PARTY_ID}
              )
              """
                .trimIndent()
            )
          }
          ExchangeWorkflow.Party.DATA_PROVIDER -> {
            conjuncts.add(
              """
              ExchangeSteps.DataProviderId = (
                SELECT DataProviderId
                FROM DataProviders
                WHERE ExternalDataProviderId = @${Params.EXTERNAL_PARTY_ID}
              )
              """
                .trimIndent()
            )
          }
          ExchangeWorkflow.Party.PARTY_UNSPECIFIED,
          ExchangeWorkflow.Party.UNRECOGNIZED ->
            throw IllegalArgumentException("Invalid party $party")
        }
        bind(Params.EXTERNAL_PARTY_ID to externalPartyId)

        appendClause("WHERE " + conjuncts.joinToString(" AND "))
        appendClause("LIMIT @limit")
        bind("limit").to(limit)
      }
    }
  }
}
