package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.Report

class ReadLatestReportByScheduleQuery {
  fun execute(readContext: ReadContext, externalScheduleId: ExternalId): Report = runBlocking {
    val whereClause =
      """
      WHERE ReportConfigSchedules.ExternalScheduleId = @external_schedule_id
      ORDER BY CreateTime DESC
      LIMIT 1
      """.trimIndent()

    ReportReader()
      .withBuilder {
        appendClause(whereClause)
        bind("external_schedule_id").to(externalScheduleId.value)
      }
      .execute(readContext)
      .single()
      .report
  }
}
