package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Query for finding [Report]s with no unfulfilled [Requisition]s.
 *
 * Since this is a very specific use case and involves a more complex query than that of
 * [StreamReportsQuery], we keep it separate for simplicity.
 */
class StreamReadyReportsQuery {
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
      LIMIT @limit
      """.trimIndent()

    return ReportReader()
      .withBuilder {
        appendClause(sql)
        bind("requisition_state").toProtoEnum(RequisitionState.FULFILLED)
        bind("report_state").toProtoEnum(ReportState.AWAITING_REQUISITION_FULFILLMENT)
        bind("limit").to(limit)
      }
      .execute(readContext)
      .map { it.report }
  }
}
