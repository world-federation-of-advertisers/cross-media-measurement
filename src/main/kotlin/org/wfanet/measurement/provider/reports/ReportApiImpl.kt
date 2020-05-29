package org.wfanet.measurement.provider.reports

import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.util.logging.Logger
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.ThrottledException
import org.wfanet.measurement.common.Throttler
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.ScheduledReportConfig

class ReportApiImpl(private val throttler: Throttler) : ReportApi {
  override suspend fun streamReadyScheduledReportConfigs(): Flow<ScheduledReportConfig> =
    retryLoop { TODO() }

  override suspend fun streamReportsInState(state: Report.ReportState): Flow<Report> =
    retryLoop { TODO() }

  override suspend fun streamFulfilledPendingReports(): Flow<Report> =
    retryLoop { TODO() }

  override suspend fun streamMissingRequisitionsForReport(report: Report): Flow<Requisition> =
    retryLoop { TODO() }

  override suspend fun createReport(reportConfig: ScheduledReportConfig) {
    logExceptions { withThrottler { TODO() } }
  }

  override suspend fun startReport(report: Report) {
    logExceptions { withThrottler { TODO() } }
  }

  override suspend fun maybeAddRequisition(requisition: Requisition) {
    logExceptions { withThrottler { TODO() } }
  }

  private suspend fun <T> retryLoop(block: suspend () -> Flow<T>): Flow<T> =
    renewedFlow(0, 0) {
      logExceptions {
        withThrottler(block)
      } ?: emptyFlow<T>()
    }

  private suspend fun <T> logExceptions(block: suspend () -> T): T? {
    try {
      return block()
    } catch (e: Exception) {
      logger.warning(e.toString())
    }
    return null
  }

  private suspend fun <T> withThrottler(block: suspend () -> T): T =
    throttler.onReady {
      try {
        block()
      } catch (e: StatusRuntimeException) {
        when (e.status.code) {
          Status.Code.DEADLINE_EXCEEDED,
          Status.Code.RESOURCE_EXHAUSTED,
          Status.Code.UNAVAILABLE -> throw ThrottledException("gRPC back-off: ${e.status}", e)
          else -> throw e
        }
      }
    }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}
