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

package org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.BaseSpannerReader
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.readers.ReportReader

/**
 * SpannerQuery for finding [Report]s with no unfulfilled Requisitions.
 *
 * Since this is a very specific use case and involves a more complex query than that of
 * [StreamReports], we keep it separate for simplicity.
 */
class StreamReadyReports(limit: Long) : SpannerQuery<ReportReader.Result, Report>() {
  override val reader: BaseSpannerReader<ReportReader.Result> by lazy {
    val sql =
      """
      JOIN ReportRequisitions USING (AdvertiserId, ReportConfigId, ScheduleId, ReportId)
      JOIN Requisitions USING (DataProviderId, CampaignId, RequisitionId)
      WHERE Requisitions.State = @requisition_state
        AND Reports.State = @report_state
      GROUP BY ${ReportReader.SELECT_COLUMNS_SQL}, ReportConfigs.NumRequisitions
      HAVING COUNT(ReportRequisitions.RequisitionId) = ReportConfigs.NumRequisitions
      """.trimIndent()

    ReportReader(index = ReportReader.Index.STATE)
      .withBuilder {
        appendClause(sql)
        bind("requisition_state").toProtoEnum(RequisitionState.FULFILLED)
        bind("report_state").toProtoEnum(ReportState.AWAITING_REQUISITION_CREATION)

        if (limit > 0) {
          appendClause("LIMIT @limit")
          bind("limit").to(limit)
        }
      }
  }

  override fun Flow<ReportReader.Result>.transform(): Flow<Report> = map { it.report }
}
