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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.gcloud.spanner.toInt64Array
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader

/**
 * Streams [ExchangeStep]s matching [filter] from Spanner ordered by ascending updateTime.
 *
 * @param filter a filter to control which [ExchangeStep]s to return
 * @param limit how many [ExchangeStep]s to return -- if zero, there is no limit
 */
class StreamExchangeSteps(requestFilter: StreamExchangeStepsRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<ExchangeStepReader.Result>() {

  override val reader =
    ExchangeStepReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause(
        "ORDER BY ExchangeSteps.RecurringExchangeId, ExchangeSteps.Date, ExchangeSteps.StepIndex"
      )
      if (limit > 0) {
        appendClause("LIMIT @${Params.LIMIT}")
        bind(Params.LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamExchangeStepsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.externalRecurringExchangeId != 0L) {
      conjuncts.add("ExternalRecurringExchangeId = @${Params.EXTERNAL_RECURRING_EXCHANGE_ID}")
      bind(Params.EXTERNAL_RECURRING_EXCHANGE_ID).to(filter.externalRecurringExchangeId)
    }

    if (filter.datesList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.Date IN UNNEST(@${Params.DATES})")
      bind(Params.DATES).toDateArray(filter.datesList.map { it.toCloudDate() })
    }

    if (filter.statesList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.State IN UNNEST(@${Params.STATES})")
      bind(Params.STATES).toInt64Array(filter.statesList)
    }

    if (filter.externalDataProviderId != 0L) {
      conjuncts.add(
        "(ExchangeSteps.DataProviderId = RecurringExchanges.DataProviderId AND " +
          "ExternalDataProviderId = @${Params.EXTERNAL_DATA_PROVIDER_ID})"
      )
      bind(Params.EXTERNAL_DATA_PROVIDER_ID).to(filter.externalDataProviderId)
    }

    if (filter.externalModelProviderId != 0L) {
      conjuncts.add(
        "ExchangeSteps.ModelProviderId = RecurringExchanges.ModelProviderId AND " +
          "ExternalModelProviderId = @${Params.EXTERNAL_MODEL_PROVIDER_ID}"
      )
      bind(Params.EXTERNAL_MODEL_PROVIDER_ID).to(filter.externalModelProviderId)
    }

    if (filter.hasAfter()) {
      val conjunct =
        """
        ExternalRecurringExchangeId > @${Params.AFTER_EXTERNAL_RECURRING_EXCHANGE_ID} OR (
          ExternalRecurringExchangeId = @${Params.AFTER_EXTERNAL_RECURRING_EXCHANGE_ID} AND (
            ExchangeSteps.Date > @${Params.AFTER_DATE} OR (
              ExchangeSteps.Date = @${Params.AFTER_DATE} AND
                ExchangeSteps.StepIndex > @${Params.AFTER_STEP_INDEX}
            )
          )
        )
        """
          .trimIndent()
      conjuncts.add(conjunct)
      bind(Params.AFTER_EXTERNAL_RECURRING_EXCHANGE_ID).to(filter.after.externalRecurringExchangeId)
      bind(Params.AFTER_DATE).to(filter.after.date.toCloudDate())
      bind(Params.AFTER_STEP_INDEX).to(filter.after.stepIndex.toLong())
    }

    check(conjuncts.isNotEmpty())
    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  private object Params {
    const val LIMIT = "limit"
    const val EXTERNAL_RECURRING_EXCHANGE_ID = "externalRecurringExchangeId"
    const val DATES = "dates"
    const val STATES = "states"
    const val EXTERNAL_DATA_PROVIDER_ID = "externalDataProviderId"
    const val EXTERNAL_MODEL_PROVIDER_ID = "externalModelProviderId"
    const val AFTER_EXTERNAL_RECURRING_EXCHANGE_ID = "afterExternalRecurringExchangeId"
    const val AFTER_DATE = "afterDate"
    const val AFTER_STEP_INDEX = "afterStepIndex"
  }
}
