package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.ReadContext
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.Requisition

/**
 * Confirms a Duchy's readiness for a [Report].
 *
 * If all Duchies are ready, the [Report] is put into state [ReportState.IN_PROGRESS].
 */
class ConfirmDuchyReadinessTransaction {
  /**
   * Runs the transaction.
   *
   * @param[transactionContext] the transaction to use
   * @param[externalReportId] the [Report]
   * @param[duchyId] the Duchy
   * @param[externalRequisitionIds] all [Requisition]s the Duchy is providing for the computation
   * @throws[IllegalArgumentException] if [externalRequisitionIds] is not what is expected
   */
  fun execute(
    transactionContext: TransactionContext,
    externalReportId: ExternalId,
    duchyId: String,
    externalRequisitionIds: Set<ExternalId>
  ) = runBlocking(spannerDispatcher()) {
    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    require(reportReadResult.report.state == ReportState.AWAITING_DUCHY_CONFIRMATION) {
      "Report $externalReportId is in wrong state: ${reportReadResult.report.state}"
    }

    val requisitions = readRequisitionsForReportAndDuchy(
      transactionContext, reportReadResult.reportId, duchyId
    )
    val expectedIds = requisitions.map(::ExternalId).toSet()
    validateRequisitions(externalRequisitionIds, expectedIds)

    transactionContext.buffer(updateReportDetailsMutation(reportReadResult, duchyId))
  }

  private fun readRequisitionsForReportAndDuchy(
    readContext: ReadContext,
    reportId: Long,
    duchyId: String
  ): List<Long> {
    val sql =
      """
      SELECT Requisitions.ExternalRequisitionId
      FROM ReportRequisitions
      JOIN Requisitions USING (DataProviderId, CampaignId, RequisitionId)
      WHERE ReportRequisitions.ReportId = @report_id
        AND Requisitions.DuchyId = @duchy_id
      """.trimIndent()
    val statement =
      Statement.newBuilder(sql)
        .bind("report_id").to(reportId)
        .bind("duchy_id").to(duchyId)
        .build()
    return readContext
      .executeQuery(statement)
      .asSequence()
      .map { it.getLong("ExternalRequisitionId") }
      .toList()
  }

  private fun validateRequisitions(providedIds: Set<ExternalId>, expectedIds: Set<ExternalId>) {
    require(providedIds == expectedIds) {
      """
      Provided Requisitions do not match what's expected:
        - Matching external ids: ${providedIds intersect expectedIds}
        - Missing external ids: ${expectedIds subtract providedIds}
        - Extra external ids: ${providedIds subtract expectedIds}
      """.trimIndent()
    }
  }

  private fun updateReportDetailsMutation(
    reportReadResult: ReportReader.Result,
    duchyId: String
  ): Mutation {
    require(duchyId in DuchyIds.ALL) {
      "Duchy id '$duchyId' not in list of valid duchies: ${DuchyIds.ALL}"
    }

    val reportDetails =
      reportReadResult.report.reportDetails.toBuilder()
        .addConfirmedDuchies(duchyId)
        .build()

    return Mutation.newUpdateBuilder("Reports").apply {
      set("AdvertiserId").to(reportReadResult.advertiserId)
      set("ReportConfigId").to(reportReadResult.reportConfigId)
      set("ScheduleId").to(reportReadResult.scheduleId)
      set("ReportId").to(reportReadResult.reportId)
      set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      set("ReportDetails").toProtoBytes(reportDetails)
      set("ReportDetailsJson").toProtoJson(reportDetails)
      if (reportDetails.confirmedDuchiesCount == DuchyIds.size) {
        set("State").toProtoEnum(ReportState.IN_PROGRESS)
      }
    }.build()
  }
}
