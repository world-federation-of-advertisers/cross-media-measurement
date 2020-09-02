// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.db.kingdom.gcp.queries

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

/**
 * Query for finding [Report]s with no unfulfilled [Requisition]s.
 *
 * Since this is a very specific use case and involves a more complex query than that of
 * [StreamReportsQuery], we keep it separate for simplicity.
 */
class StreamReadyReportsQuery {
  /**
   * Streams [Report]s without unfulfilled [Requisition]s.
   *
   * @param readContext the context in which to perform Spanner reads
   * @param limit how many [Report]s to return -- if zero, there is no limit
   * @return a [Flow] of [Report]s in an arbitrary order
   */
  fun execute(
    readContext: ReadContext,
    limit: Long
  ): Flow<Report> {
    val sql =
      """
      JOIN ReportRequisitions USING (AdvertiserId, ReportConfigId, ScheduleId, ReportId)
      JOIN Requisitions USING (DataProviderId, CampaignId, RequisitionId)
      WHERE Requisitions.State = @requisition_state
        AND Reports.State = @report_state
      GROUP BY ${ReportReader.SELECT_COLUMNS_SQL}, ReportConfigs.NumRequisitions
      HAVING COUNT(ReportRequisitions.RequisitionId) = ReportConfigs.NumRequisitions
      """.trimIndent()

    return ReportReader()
      .withBuilder {
        appendClause(sql)
        bind("requisition_state").toProtoEnum(RequisitionState.FULFILLED)

        // TODO: sort out the right state here -- are we representing
        // AWAITING_REQUISITION_FULFILLMENT explicitly in the DB?
        bind("report_state").toProtoEnum(ReportState.AWAITING_REQUISITION_CREATION)

        if (limit > 0) {
          appendClause("LIMIT @limit")
          bind("limit").to(limit)
        }
      }
      .execute(readContext)
      .map { it.report }
  }
}
