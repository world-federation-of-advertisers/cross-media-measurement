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
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.getProtoEnum
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttempt
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptDetails

/** Reads [ExchangeStepAttempt] protos from Spanner. */
class ExchangeStepAttemptReader : SpannerReader<ExchangeStepAttemptReader.Result>() {

  data class Result(val exchangeStepAttempt: ExchangeStepAttempt, val recurringExchangeId: Long)

  override val baseSql: String =
    """
    SELECT $SELECT_COLUMNS_SQL
    FROM ExchangeStepAttempts
    JOIN RecurringExchanges USING (RecurringExchangeId)
    JOIN ExchangeSteps USING (RecurringExchangeId, Date, StepIndex)
    """.trimIndent()

  override val externalIdColumn: String
    get() = error("This isn't supported.")

  override suspend fun translate(struct: Struct): Result {
    return Result(
      exchangeStepAttempt =
        ExchangeStepAttempt.newBuilder()
          .apply {
            externalRecurringExchangeId = struct.getLong("ExternalRecurringExchangeId")
            date = struct.getDate("Date").toProtoDate()
            stepIndex = struct.getLong("StepIndex").toInt()
            attemptNumber = struct.getLong("AttemptIndex").toInt()
            state = struct.getProtoEnum("State", ExchangeStepAttempt.State::forNumber)
            details =
              struct.getProtoMessage(
                "ExchangeStepAttemptDetails",
                ExchangeStepAttemptDetails.parser()
              )
          }
          .build(),
      recurringExchangeId = struct.getLong("RecurringExchangeId")
    )
  }

  companion object {
    private val SELECT_COLUMNS =
      listOf(
        "ExchangeStepAttempts.RecurringExchangeId",
        "ExchangeStepAttempts.Date",
        "ExchangeStepAttempts.StepIndex",
        "ExchangeStepAttempts.AttemptIndex",
        "ExchangeStepAttempts.State",
        "ExchangeStepAttempts.ExchangeStepAttemptDetails",
        "ExchangeStepAttempts.ExchangeStepAttemptDetailsJson",
        "RecurringExchanges.ExternalRecurringExchangeId"
      )

    val SELECT_COLUMNS_SQL = SELECT_COLUMNS.joinToString(", ")

    fun forExpiredAttempts(
      externalModelProviderId: Long?,
      externalDataProviderId: Long?,
      limit: Long = 10
    ): SpannerReader<Result> {
      require((externalModelProviderId == null) != (externalDataProviderId == null)) {
        "Specify exactly one of `externalDataProviderId` and `externalModelProviderId`"
      }

      return ExchangeStepAttemptReader().withBuilder {
        appendClause(
          """
            WHERE ExchangeSteps.State = @exchange_step_state
              AND ExchangeStepAttempts.State = @exchange_step_attempt_state
              AND ExchangeStepAttempts.ExpirationTime <= CURRENT_TIMESTAMP()
            """.trimIndent()
        )
        bind("exchange_step_state").toProtoEnum(ExchangeStep.State.IN_PROGRESS)
        bind("exchange_step_attempt_state").toProtoEnum(ExchangeStepAttempt.State.ACTIVE)

        if (externalModelProviderId != null) {
          appendClause(
            """
              AND ExchangeSteps.ModelProviderId = (
                SELECT ModelProviderId
                FROM ModelProviders
                WHERE ExternalModelProviderId = @external_model_provider_id
              )
              """.trimIndent()
          )
          bind("external_model_provider_id").to(externalModelProviderId)
        }

        if (externalDataProviderId != null) {
          appendClause(
            """
              AND ExchangeSteps.DataProviderId = (
                SELECT DataProviderId
                FROM DataProviders
                WHERE ExternalDataProviderId = @external_data_provider_id
              )
              """.trimIndent()
          )
          bind("external_data_provider_id").to(externalDataProviderId)
        }

        appendClause("LIMIT @limit")
        bind("limit").to(limit)
      }
    }
  }
}
