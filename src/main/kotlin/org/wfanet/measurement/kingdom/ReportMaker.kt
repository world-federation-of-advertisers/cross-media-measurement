package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.parallelCollect
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/** Streams ReportConfigs that need Reports and creates them in parallel. */
suspend fun Daemon.runReportMaker() {
  streamReadySchedules()
    .parallelCollect(maxParallelism) { schedule ->
      throttleAndLog {
        reportStarterClient.createNextReport(schedule)
      }
    }
}

private fun Daemon.streamReadySchedules(): Flow<ReportConfigSchedule> =
  retryLoop { reportStarterClient.streamReadySchedules() }
