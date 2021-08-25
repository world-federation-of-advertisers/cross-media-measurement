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

import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.RecurringExchange
import org.wfanet.measurement.kingdom.db.StreamRecurringExchangesFilter
import org.wfanet.measurement.kingdom.db.hasDataProviderFilter
import org.wfanet.measurement.kingdom.db.hasModelProviderFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.StreamRecurringExchangesFilterSqlConverter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.toSql
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RecurringExchangeReader

/**
 * Streams [RecurringExchange]s matching [filter] from Spanner ordered by ascending updateTime.
 *
 * @param filter a filter to control which [RecurringExchange]s to return
 * @param limit how many [RecurringExchange]s to return -- if zero, there is no limit
 */
class StreamRecurringExchanges(filter: StreamRecurringExchangesFilter, limit: Long = 0) :
  SimpleSpannerQuery<RecurringExchangeReader.Result>() {

  override val reader: BaseSpannerReader<RecurringExchangeReader.Result> by lazy {
    RecurringExchangeReader(forcedIndex).withBuilder {
      if (!filter.empty) {
        appendClause("WHERE ")
        filter.toSql(this, StreamRecurringExchangesFilterSqlConverter)
      }

      appendClause("ORDER BY NextExchangeDate ASC")

      if (limit > 0) {
        appendClause("LIMIT @limit")
        bind("limit").to(limit)
      }
    }
  }

  private val forcedIndex: RecurringExchangeReader.Index by lazy {
    if (filter.hasDataProviderFilter()) {
      RecurringExchangeReader.Index.DATA_PROVIDER_ID
    } else if (filter.hasModelProviderFilter()) {
      RecurringExchangeReader.Index.MODEL_PROVIDER_ID
    } else {
      RecurringExchangeReader.Index.NONE
    }
  }
}
