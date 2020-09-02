package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.db.kingdom.gcp.readers.ReportReader
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportDetails

class FinishReportTransaction {
  fun execute(
    transactionContext: TransactionContext,
    externalReportId: ExternalId,
    result: ReportDetails.Result
  ): Report = runBlocking(spannerDispatcher()) {
    val reportReadResult = ReportReader().readExternalId(transactionContext, externalReportId)
    require(reportReadResult.report.state == ReportState.IN_PROGRESS) {
      "Report can't be finished because it is not IN_PROGRESS: ${reportReadResult.report}"
    }

    val newReportDetails =
      reportReadResult.report.reportDetails.toBuilder()
        .setResult(result)
        .build()

    val mutation: Mutation =
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

    transactionContext.buffer(mutation)

    // TODO: this does not have the correct updateTime because the commitTimestamp is not available.
    reportReadResult.report.toBuilder().apply {
      state = ReportState.SUCCEEDED
      reportDetails = newReportDetails
      reportDetailsJson = newReportDetails.toJson()
    }.build()
  }
}
