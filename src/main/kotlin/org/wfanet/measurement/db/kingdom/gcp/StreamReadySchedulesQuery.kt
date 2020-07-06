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
  /**
   * Streams [ReportConfigSchedule]s with nextReportStartTimes in the past.
   *
   * @param[readContext] the context in which to perform Spanner reads
   * @param[limit] how many results to return -- if zero, there is no limit
   * @return a [Flow] of [ReportConfigSchedule]s in an arbitrary order
   */
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
