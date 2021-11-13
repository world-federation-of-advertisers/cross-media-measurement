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
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamExchangeStepsRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.PROVIDER_PARAM
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.providerFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.stepIsOwnedByProviderTypeFilter
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
      appendClause("ORDER BY ExchangeSteps.UpdateTime ASC, Date ASC, StepIndex ASC")
      if (limit > 0) {
        appendClause("LIMIT @${Params.LIMIT}")
        bind(Params.LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamExchangeStepsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    conjuncts.add(providerFilter(filter.principal))
    bind(PROVIDER_PARAM to filter.principal.externalId)

    if (filter.hasStepProvider()) {
      conjuncts.add(stepIsOwnedByProviderTypeFilter(filter.stepProvider.type))
      bind(Params.EXTERNAL_STEP_PROVIDER_ID to filter.stepProvider.externalId)
    }

    if (filter.externalRecurringExchangeIdsList.isNotEmpty()) {
      conjuncts.add(
        "RecurringExchanges.ExternalRecurringExchangeId IN " +
          "UNNEST(@${Params.EXTERNAL_RECURRING_EXCHANGE_ID})"
      )
      bind(Params.EXTERNAL_RECURRING_EXCHANGE_ID)
        .toInt64Array(filter.externalRecurringExchangeIdsList.map { it.toLong() })
    }

    if (filter.datesList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.Date IN UNNEST(@${Params.DATES})")
      bind(Params.DATES).toDateArray(filter.datesList.map { it.toCloudDate() })
    }

    if (filter.statesValueList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.State IN UNNEST(@${Params.STATES})")
      bind(Params.STATES).toInt64Array(filter.statesValueList.map { it.toLong() })
    }

    if (filter.stepIndicesList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.StepIndex IN UNNEST(@${Params.STEP_INDICES})")
      bind(Params.STEP_INDICES).toInt64Array(filter.stepIndicesList.map { it.toLong() })
    }

    if (filter.hasUpdatedAfter()) {
      conjuncts.add("ExchangeSteps.UpdateTime > @${Params.UPDATED_AFTER}")
      bind(Params.UPDATED_AFTER to filter.updatedAfter.toGcloudTimestamp())
    }

    check(conjuncts.isNotEmpty())
    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  private object Params {
    const val LIMIT = "limit"
    const val EXTERNAL_STEP_PROVIDER_ID = "externalStepProviderId"
    const val EXTERNAL_RECURRING_EXCHANGE_ID = "externalRecurringExchangeId"
    const val DATES = "dates"
    const val STATES = "states"
    const val STEP_INDICES = "stepIndices"
    const val UPDATED_AFTER = "updatedAfter"
  }
}
