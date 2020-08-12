package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import org.wfanet.measurement.common.pairAll
import org.wfanet.measurement.common.parallelCollect
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/** Streams Reports that are lacking requisitions and creates them in parallel. */
suspend fun Daemon.runRequisitionLinker() {
  streamReportsAwaitingRequisitionCreation()
    .pairAll { report -> daemonDatabaseServicesClient.buildRequisitionsForReport(report).asFlow() }
    .parallelCollect(maxParallelism) { (report, requisition) ->
      throttleAndLog {
        daemonDatabaseServicesClient.createRequisition(requisition)
        daemonDatabaseServicesClient.associateRequisitionToReport(requisition, report)
      }
    }
}

private fun Daemon.streamReportsAwaitingRequisitionCreation(): Flow<Report> =
  retryLoop {
    daemonDatabaseServicesClient.streamReportsInState(ReportState.AWAITING_REQUISITION_CREATION)
  }
