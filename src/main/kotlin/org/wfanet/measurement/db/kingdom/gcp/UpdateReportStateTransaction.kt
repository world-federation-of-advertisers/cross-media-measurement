package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.toProtoEnum
import org.wfanet.measurement.internal.kingdom.Report

class UpdateReportStateTransaction {
  fun execute(
    transactionContext: TransactionContext,
    externalReportId: ExternalId,
    state: Report.ReportState
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
        .build()

    transactionContext.buffer(mutation)

    return reportReadResult.report.toBuilder().setState(state).build()
  }
}
