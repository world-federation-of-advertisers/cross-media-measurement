package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.onEach
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/** Streams ReportConfigs that need Reports and creates them in parallel. */
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
