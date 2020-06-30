package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

class UpdateReportStateTransaction {
  fun execute(
    transactionContext: TransactionContext,
    externalReportId: ExternalId,
    state: ReportState
  ): Report {
    val reportReadResult = runBlocking {
      ReportReader.forExternalId(transactionContext, externalReportId)
        ?: error("ExternalReportId $externalReportId not found")
    }

    if (reportReadResult.report.state == state) {
      return reportReadResult.report
    }

    val mutation: Mutation =
      Mutation.newUpdateBuilder("Reports")
        .set("AdvertiserId").to(reportReadResult.advertiserId)
        .set("ReportConfigId").to(reportReadResult.reportConfigId)
        .set("ScheduleId").to(reportReadResult.scheduleId)
        .set("ReportId").to(reportReadResult.reportId)
        .set("State").toProtoEnum(state)
        .set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
        .build()

    transactionContext.buffer(mutation)

    return reportReadResult.report.toBuilder().setState(state).build()
  }
}
