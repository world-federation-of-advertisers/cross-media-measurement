package org.wfanet.measurement.provider.reports

import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.Throttler
import org.wfanet.measurement.common.onReadyGrpc
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.Requisition

class ReportApiImpl(private val throttler: Throttler) : ReportApi {
  override suspend fun streamReadyScheduledReportConfigs(): Flow<ReportConfigSchedule> =
    retryLoop { TODO() }

  override suspend fun streamReportsInState(state: Report.ReportState): Flow<Report> =
    retryLoop { TODO() }

  override suspend fun streamFulfilledPendingReports(): Flow<Report> =
    retryLoop { TODO() }

  override suspend fun streamMissingRequisitionsForReport(report: Report): Flow<Requisition> =
    retryLoop { TODO() }

  override suspend fun createReport(externalScheduleId: ExternalId) {
    logExceptions { throttler.onReadyGrpc { TODO() } }
  }

  override suspend fun startReport(report: Report) {
    logExceptions { throttler.onReadyGrpc { TODO() } }
  }

  override suspend fun maybeAddRequisition(requisition: Requisition) {
    logExceptions { throttler.onReadyGrpc { TODO() } }
  }

  private suspend fun <T> retryLoop(block: suspend () -> Flow<T>): Flow<T> =
    renewedFlow(0, 0) {
      logExceptions {
        throttler.onReadyGrpc(block)
      } ?: emptyFlow()
    }

  private suspend fun <T> logExceptions(block: suspend () -> T): T? {
    try {
      return block()
    } catch (e: Exception) {
      logger.warning(e.toString())
    }
    return null
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}
