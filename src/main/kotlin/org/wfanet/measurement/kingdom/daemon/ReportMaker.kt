// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.daemon

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
suspend fun Daemon.runReportMaker(combinedPublicKeyResourceId: String) {
  streamReadySchedules()
    .onEach { logger.info("Schedule is ready: $it") }
    .collect { schedule ->
      throttleAndLog {
        logger.info("Creating next report for schedule: $schedule")
        daemonDatabaseServicesClient.createNextReport(schedule, combinedPublicKeyResourceId)
      }
    }
}

private fun Daemon.streamReadySchedules(): Flow<ReportConfigSchedule> = retryLoop {
  daemonDatabaseServicesClient.streamReadySchedules()
}
