package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.Report

class GetReportQuery {
  fun execute(readContext: ReadContext, externalReportId: ExternalId): Report = runBlocking {
    ReportReader()
      .withBuilder {
        appendClause("WHERE Reports.ExternalReportId = @external_report_id")
        bind("external_report_id").to(externalReportId.value)
      }
      .execute(readContext)
      .single()
      .report
  }
}
