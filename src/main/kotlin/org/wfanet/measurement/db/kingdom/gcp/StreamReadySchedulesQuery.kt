package org.wfanet.measurement.db.kingdom.gcp

import com.google.cloud.spanner.ReadContext
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.db.gcp.appendClause
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/**
 * Query for finding [ReportConfigSchedule]s with nextReportStartTimes in the past.
 */
class StreamReadySchedulesQuery {
  fun execute(
    readContext: ReadContext,
    limit: Long
  ): Flow<ReportConfigSchedule> {
    return ScheduleReader()
      .withBuilder {
        appendClause("WHERE ReportConfigSchedules.NextReportStartTime < CURRENT_TIMESTAMP()")

        if (limit > 0) {
          appendClause("LIMIT @limit")
          bind("limit").to(limit)
        }
      }
      .execute(readContext)
      .map { it.schedule }
  }
}
