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
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.kingdom.db.StreamRequisitionsFilter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.StreamRequisitionsFilterSqlConverter
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.common.toSql
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.RequisitionReader

/**
 * Streams [Requisition]s matching [filter] from Spanner ordered by ascending CreateTime.
 *
 * @param filter a filter to control which [Requisition]s to return
 * @param limit how many [Requisition]s to return -- if zero, there is no limit
 */
class StreamRequisitions(
  filter: StreamRequisitionsFilter,
  limit: Long
) : SpannerQuery<RequisitionReader.Result, Requisition>() {

  override val reader: BaseSpannerReader<RequisitionReader.Result> by lazy {
    RequisitionReader().withBuilder {
      if (!filter.empty) {
        appendClause("WHERE")
        filter.toSql(this, StreamRequisitionsFilterSqlConverter)
      }

      appendClause("ORDER BY CreateTime ASC")
      appendClause("LIMIT @limit")
      bind("limit").to(limit)
    }
  }

  override fun Flow<RequisitionReader.Result>.transform(): Flow<Requisition> {
    return map { it.requisition }
  }
}
