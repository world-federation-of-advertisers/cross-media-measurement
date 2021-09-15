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
import org.wfanet.measurement.internal.kingdom.ExchangeStep
import org.wfanet.measurement.kingdom.db.GetExchangeStepFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.GetExchangeStepFilterSqlConverter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.toSql
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ExchangeStepReader

/**
 * Get an [ExchangeStep]s matching [filter] from Spanner ordered by ascending updateTime.
 *
 * @param filter a filter to control which [ExchangeStep] to return.
 */
class GetExchangeStep(filter: GetExchangeStepFilter) :
  SimpleSpannerQuery<ExchangeStepReader.Result>() {

  override val reader: BaseSpannerReader<ExchangeStepReader.Result> by lazy {
    ExchangeStepReader(ExchangeStepReader.Index.NONE).fillStatementBuilder {
      require(!filter.empty) { "Filter not provided for GetExchangeStep." }

      appendClause("WHERE ")
      filter.toSql(this, GetExchangeStepFilterSqlConverter)

      appendClause("ORDER BY Date, ExchangeSteps.UpdateTime ASC")
      appendClause("LIMIT 1")
    }
  }
}
