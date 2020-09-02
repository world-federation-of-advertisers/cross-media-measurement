package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.identity.DuchyIds
import org.wfanet.measurement.db.gcp.asSequence
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/**
 * Confirms a Duchy's readiness for a [Report].
 *
 * If all Duchies are ready, the [Report] is put into state [ReportState.IN_PROGRESS].
 */
class ConfirmDuchyReadiness(
  private val externalReportId: ExternalId,
  private val duchyId: String,
  private val externalRequisitionIds: Set<ExternalId>
) : SpannerWriter<Report, Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    require(duchyId in DuchyIds.ALL) {
      "Duchy id '$duchyId' not in list of valid duchies: ${DuchyIds.ALL}"
    }

    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    require(reportReadResult.report.state == ReportState.AWAITING_DUCHY_CONFIRMATION) {
      "Report $externalReportId is in wrong state: ${reportReadResult.report.state}"
    }
    if (duchyId in reportReadResult.report.reportDetails.confirmedDuchiesList) {
      return reportReadResult.report
    }

    val requisitions = readRequisitionsForReportAndDuchy(reportReadResult.reportId)
    val expectedIds = requisitions.map(::ExternalId).toSet()
    validateRequisitions(externalRequisitionIds, expectedIds)

    val newReport = reportReadResult.report.toBuilder().apply {
      reportDetailsBuilder.addConfirmedDuchies(duchyId)
      if (reportDetails.confirmedDuchiesCount == DuchyIds.size) {
        state = ReportState.IN_PROGRESS
      }
    }.build()

    updateReport(reportReadResult, newReport)

    return newReport
  }

  override fun ResultScope<Report>.computeResult(): Report {
    return checkNotNull(transactionResult)
  }

  private fun TransactionScope.readRequisitionsForReportAndDuchy(reportId: Long): List<Long> {
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
    return transactionContext
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

  private fun TransactionScope.updateReport(
    reportReadResult: ReportReader.Result,
    newReport: Report
  ) {
    transactionContext.buffer(
      Mutation.newUpdateBuilder("Reports").apply {
        set("AdvertiserId").to(reportReadResult.advertiserId)
        set("ReportConfigId").to(reportReadResult.reportConfigId)
        set("ScheduleId").to(reportReadResult.scheduleId)
        set("ReportId").to(reportReadResult.reportId)
        set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        set("ReportDetails").toProtoBytes(newReport.reportDetails)
        set("ReportDetailsJson").toProtoJson(newReport.reportDetails)
        set("State").toProtoEnum(newReport.state)
      }.build()
    )
  }
}
