// Copyright 2020 The Cross-Media Measurement Authors
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
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.mapConcurrently
import org.wfanet.measurement.common.pairAll
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/**
 * Associates Requisitions to Reports, creating the Requisitions if necessary.
 *
 * Because the number of Requisitions could be large, this allows up to [Daemon.maxConcurrency]
 * in-flight Requisition creations or associations.
 */
suspend fun Daemon.runRequisitionLinker() {
  streamReportsAwaitingRequisitionCreation()
    .pairAll { report -> daemonDatabaseServicesClient.buildRequisitionsForReport(report).asFlow() }
    .mapConcurrently(this, maxConcurrency) { (report, requisition) ->
      throttleAndLog {
        logger.info("Creating requisition: $requisition")
        val actualRequisition = daemonDatabaseServicesClient.createRequisition(requisition)

        logger.info("Associating requisition $actualRequisition to report $report")
        daemonDatabaseServicesClient.associateRequisitionToReport(actualRequisition, report)
      }
    }
    .collect()
}

private fun Daemon.streamReportsAwaitingRequisitionCreation(): Flow<Report> =
  retryLoop {
    daemonDatabaseServicesClient.streamReportsInState(ReportState.AWAITING_REQUISITION_CREATION)
  }
