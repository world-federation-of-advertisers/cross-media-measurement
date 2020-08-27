package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import org.wfanet.measurement.common.pairAll
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState

/** Streams Reports that are lacking requisitions and creates them in parallel. */
suspend fun Daemon.runRequisitionLinker() {
  streamReportsAwaitingRequisitionCreation()
    .pairAll { report -> daemonDatabaseServicesClient.buildRequisitionsForReport(report).asFlow() }
    .collect { (report, requisition) ->
      throttleAndLog {
        logger.info("Creating requisition: $requisition")
        val actualRequisition = daemonDatabaseServicesClient.createRequisition(requisition)

        logger.info("Associating requisition $actualRequisition to report $report")
        daemonDatabaseServicesClient.associateRequisitionToReport(actualRequisition, report)
      }
    }
}

private fun Daemon.streamReportsAwaitingRequisitionCreation(): Flow<Report> =
  retryLoop {
    daemonDatabaseServicesClient.streamReportsInState(ReportState.AWAITING_REQUISITION_CREATION)
  }
