// Copyright 2020 The Cross-Media Measurement Authors
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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.kingdom.db.StreamReportsFilter
import org.wfanet.measurement.kingdom.db.hasStateFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.StreamReportsFilterSqlConverter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.toSql
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader

/**
 * Streams [Report]s matching [filter] from Spanner ordered by ascending updateTime.
 *
 * @param filter a filter to control which [Report]s to return
 * @param limit how many [Report]s to return -- if zero, there is no limit
 */
class StreamReports(
  filter: StreamReportsFilter,
  limit: Long
) : SpannerQuery<ReportReader.Result, Report>() {
  override val reader: BaseSpannerReader<ReportReader.Result> by lazy {
    ReportReader(forcedIndex).withBuilder {
      if (!filter.empty) {
        appendClause("WHERE ")
        filter.toSql(this, StreamReportsFilterSqlConverter)
      }

      appendClause("ORDER BY UpdateTime ASC")

      if (limit > 0) {
        appendClause("LIMIT @limit")
        bind("limit").to(limit)
      }
    }
  }

  private val forcedIndex: ReportReader.Index by lazy {
    if (filter.hasStateFilter()) ReportReader.Index.STATE else ReportReader.Index.NONE
  }

  override fun Flow<ReportReader.Result>.transform(): Flow<Report> = map { it.report }
}
