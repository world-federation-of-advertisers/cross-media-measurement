/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import com.google.cloud.spanner.Statement
import org.wfanet.measurement.gcloud.common.toCloudDate
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bind
import org.wfanet.measurement.internal.kingdom.StreamExchangesRequest
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeReader

class StreamExchanges(requestFilter: StreamExchangesRequest.Filter, limit: Int = 0) :
  SimpleSpannerQuery<ExchangeReader.Result>() {

  override val reader =
    ExchangeReader().fillStatementBuilder {
      appendWhereClause(requestFilter)
      appendClause("ORDER BY Exchanges.Date ASC")
      if (limit > 0) {
        appendClause("LIMIT @${Params.LIMIT}")
        bind(Params.LIMIT to limit.toLong())
      }
    }

  private fun Statement.Builder.appendWhereClause(filter: StreamExchangesRequest.Filter) {
    val conjuncts = mutableListOf<String>()

    if (filter.hasDateBefore()) {
      conjuncts.add("Exchanges.Date < @${Params.EXCHANGED_BEFORE}")
      bind(Params.EXCHANGED_BEFORE to filter.dateBefore.toCloudDate())
    }

    if (conjuncts.isEmpty()) {
      return
    }

    appendClause("WHERE ")
    append(conjuncts.joinToString(" AND "))
  }

  private object Params {
    const val LIMIT = "limit"
    const val EXCHANGED_BEFORE = "exchangedBefore"
  }
}
