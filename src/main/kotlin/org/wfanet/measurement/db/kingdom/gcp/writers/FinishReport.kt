package org.wfanet.measurement.db.kingdom.gcp.writers

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Value
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.bufferTo
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails

class FinishReport(
  private val externalReportId: ExternalId,
  private val result: ReportDetails.Result
) : SpannerWriter<Report, Report>() {
  override suspend fun TransactionScope.runTransaction(): Report {
    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    require(reportReadResult.report.state == ReportState.IN_PROGRESS) {
      "Report can't be finished because it is not IN_PROGRESS: ${reportReadResult.report}"
    }

    val newReportDetails =
      reportReadResult.report.reportDetails.toBuilder()
        .setResult(result)
        .build()

    Mutation.newUpdateBuilder("Reports")
      .set("AdvertiserId").to(reportReadResult.advertiserId)
      .set("ReportConfigId").to(reportReadResult.reportConfigId)
      .set("ScheduleId").to(reportReadResult.scheduleId)
      .set("ReportId").to(reportReadResult.reportId)
      .set("State").toProtoEnum(ReportState.SUCCEEDED)
      .set("ReportDetails").toProtoBytes(newReportDetails)
      .set("ReportDetailsJson").toProtoJson(newReportDetails)
      .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
      .build()
      .bufferTo(transactionContext)

    return reportReadResult.report.toBuilder().apply {
      state = ReportState.SUCCEEDED
      reportDetails = newReportDetails
      reportDetailsJson = newReportDetails.toJson()
    }.build()
  }

  override fun ResultScope<Report>.buildResult(): Report {
    return checkNotNull(transactionResult).toBuilder().apply {
      updateTime = commitTimestamp.toProto()
    }.build()
  }
}
