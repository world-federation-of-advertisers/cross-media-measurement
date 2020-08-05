package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.TransactionContext
import com.google.cloud.spanner.Value
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.spannerDispatcher
import org.wfanet.measurement.db.gcp.toProtoBytes
import org.wfanet.measurement.db.gcp.toProtoJson
import org.wfanet.measurement.internal.kingdom.ReportLogEntry

class CreateReportLogEntryTransaction {
  fun execute(
    transactionContext: TransactionContext,
    reportLogEntry: ReportLogEntry
  ) = runBlocking(spannerDispatcher()) {
    val externalId = ExternalId(reportLogEntry.externalReportId)
    val reportReadResult =
      requireNotNull(ReportReader.forExternalId(transactionContext, externalId)) {
        "Report missing: $externalId"
      }
    transactionContext.buffer(reportLogEntry.toInsertMutation(reportReadResult))
  }
}

private fun ReportLogEntry.toInsertMutation(reportReadResult: ReportReadResult): Mutation =
  Mutation.newInsertBuilder("ReportLogEntries")
    .set("AdvertiserId").to(reportReadResult.advertiserId)
    .set("ReportConfigId").to(reportReadResult.reportConfigId)
    .set("ScheduleId").to(reportReadResult.scheduleId)
    .set("ReportId").to(reportReadResult.reportId)
    .set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    .set("ReportLogDetails").toProtoBytes(reportLogDetails)
    .set("ReportLogDetailsJson").toProtoJson(reportLogDetails)
    .build()
