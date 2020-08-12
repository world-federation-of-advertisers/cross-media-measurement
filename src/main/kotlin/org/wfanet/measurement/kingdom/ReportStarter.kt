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

package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.parallelCollect
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/** Streams Reports with no unfulfilled Requisitions and marks them as ready to start in parallel */
suspend fun Daemon.runReportStarter() {
  streamReadyReports()
    .parallelCollect(maxParallelism) { report ->
      throttleAndLog {
        reportStarterClient.updateReportState(report, ReportState.AWAITING_DUCHY_CONFIRMATION)
      }
    }
}

private fun Daemon.streamReadyReports(): Flow<Report> =
  retryLoop { reportStarterClient.streamReadyReports() }
