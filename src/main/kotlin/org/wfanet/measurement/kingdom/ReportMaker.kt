package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/**
 * Streams ReportConfigSchedules that need Reports and creates them.
 *
 * The next Report for a ReportConfigSchedule is created at the beginning of the period of time that
 * the Report covers.
 */
suspend fun Daemon.runReportMaker() {
  streamReadySchedules()
    .onEach { logger.info("Schedule is ready: $it") }
    .collect { schedule ->
      throttleAndLog {
        logger.info("Creating next report for schedule: $schedule")
        daemonDatabaseServicesClient.createNextReport(schedule)
      }
    }
}

private fun Daemon.streamReadySchedules(): Flow<ReportConfigSchedule> = retryLoop {
  daemonDatabaseServicesClient.streamReadySchedules()
}
