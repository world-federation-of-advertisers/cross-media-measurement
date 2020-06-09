package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.single
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.Report

class ReadReportQuery {
  fun execute(readContext: ReadContext, externalScheduleId: ExternalId): Report {
    val whereClause =
      """
      WHERE ReportConfigSchedules.ExternalScheduleId = @external_schedule_id
      ORDER BY CreateTime DESC
      LIMIT 1
      """.trimIndent()

    val reader = ReportReader()

    reader.builder
      .appendClause(whereClause)
      .bind("external_schedule_id").to(externalScheduleId.value)

    return runBlocking { reader.execute(readContext).single() }
  }
}
