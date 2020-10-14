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
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/**
 * Streams Reports with no unfulfilled Requisitions and updates them to await Duchy confirmation.
 *
 * At present, this consists of Reports in state AWAITING_REQUISITION_CREATION with the correct
 * number of associated, fulfilled Requisitions.
 */
suspend fun Daemon.runReportStarter() {
  streamReadyReports()
    .onEach { logger.info("Report is ready: $it") }
    .collect { report ->
      throttleAndLog {
        daemonDatabaseServicesClient.updateReportState(
          report,
          ReportState.AWAITING_DUCHY_CONFIRMATION
        )
      }
    }
}

private fun Daemon.streamReadyReports(): Flow<Report> = retryLoop {
  daemonDatabaseServicesClient.streamReadyReports()
}
