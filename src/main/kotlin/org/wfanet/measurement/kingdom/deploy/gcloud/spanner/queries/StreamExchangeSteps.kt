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
import io.grpc.Status
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.Provider
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
      appendClause("ORDER BY ExchangeSteps.UpdateTime, Date ASC")
      if (limit > 0) {
        appendClause("LIMIT @${Params.LIMIT}")
        bind(Params.LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamExchangeStepsRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (!filter.hasProvider()) {
      failGrpc(Status.INVALID_ARGUMENT) { "Provider must be provided." }
    }
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (filter.provider.type) {
      Provider.Type.DATA_PROVIDER ->
        conjuncts.add("DataProviders.ExternalDataProviderId = @${Params.EXTERNAL_PROVIDER_ID}")
      Provider.Type.MODEL_PROVIDER ->
        conjuncts.add("ModelProviders.ExternalModelProviderId = @${Params.EXTERNAL_PROVIDER_ID}")
      Provider.Type.TYPE_UNSPECIFIED, Provider.Type.UNRECOGNIZED ->
        failGrpc(Status.INVALID_ARGUMENT) {
          "external_data_provider_id or external_model_provider_id must be provided."
        }
    }
    bind(Params.EXTERNAL_PROVIDER_ID to filter.provider.externalId)

    if (filter.externalRecurringExchangeId != 0L) {
      conjuncts.add(
        "RecurringExchanges.ExternalRecurringExchangeId = @${Params.EXTERNAL_RECURRING_EXCHANGE_ID}"
      )
      bind(Params.EXTERNAL_RECURRING_EXCHANGE_ID to filter.externalRecurringExchangeId)
    }

    if (filter.datesList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.Date IN UNNEST(@${Params.DATES})")
      bind(Params.DATES).toDateArray(filter.datesList.map { it.toCloudDate() })
    }

    if (filter.statesValueList.isNotEmpty()) {
      conjuncts.add("ExchangeSteps.State IN UNNEST(@${Params.STATES})")
      bind(Params.STATES).toInt64Array(filter.statesValueList.map { it.toLong() })
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
    const val EXTERNAL_PROVIDER_ID = "externalModelProviderId"
    const val EXTERNAL_RECURRING_EXCHANGE_ID = "externalRecurringExchangeId"
    const val DATES = "dates"
    const val STATES = "states"
    const val UPDATED_AFTER = "updatedAfter"
  }
}
