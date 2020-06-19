package org.wfanet.measurement.kingdom

import java.time.Duration
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.transform
import org.wfanet.measurement.common.Throttler
import org.wfanet.measurement.common.logAndSuppressExceptionSuspend
import org.wfanet.measurement.common.onReadyGrpc
import org.wfanet.measurement.common.parallelCollect
import org.wfanet.measurement.common.renewedFlow
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule

/**
 * Utility class for managing the [Report] lifecycle.
 *
 * The public methods never terminate.
 *
 * @property[throttler] a throttler to rate-limit gRPCs
 * @property[maxParallelism] the maximum number of sub-coroutines to use per public API method
 * @property[reportStarterClient] a wrapper around stubs for internal services
 */
@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class) // For Flow extension functions.
class ReportStarter(
  private val throttler: Throttler,
  private val maxParallelism: Int,
  private val reportStarterClient: ReportStarterClient
) {
  /**
   * Streams ReportConfigs that need Reports and creates them in parallel.
   *
   * Note: this will never terminate by itself; it should be cancelled when done.
   */
  suspend fun createReports() {
    streamReadySchedules()
      .parallelCollect(maxParallelism) { schedule ->
        throttleAndLogExceptions {
          reportStarterClient.createNextReport(schedule)
        }
      }
  }

  /**
   * Streams Reports that are lacking requisitions and creates them in parallel.
   *
   * Note: this will never terminate by itself; it should be cancelled when done.
   */
  suspend fun createRequisitions() {
    streamReportsAwaitingRequisitionCreation()
      .transform { report ->
        for (requisition in reportStarterClient.buildRequisitionsForReport(report)) {
          emit(Pair(report, requisition))
        }
      }
      .parallelCollect(maxParallelism) { (report, requisition) ->
        throttleAndLogExceptions {
          reportStarterClient.createRequisition(requisition)
          reportStarterClient.associateRequisitionToReport(requisition, report)
        }
      }
  }

  /**
   * Streams Reports with no unfulfilled Requisitions and marks them as ready to start in parallel.
   *
   * Note: this will never terminate by itself; it should be cancelled when done.
   */
  suspend fun startReports() {
    streamReadyReports()
      .parallelCollect(maxParallelism) { report ->
        throttleAndLogExceptions {
          reportStarterClient.updateReportState(report, ReportState.READY_TO_START)
        }
      }
  }

  private fun streamReadySchedules(): Flow<ReportConfigSchedule> =
    retryLoop { reportStarterClient.streamReadySchedules() }

  private fun streamReportsAwaitingRequisitionCreation(): Flow<Report> =
    retryLoop {
      reportStarterClient.streamReportsInState(ReportState.AWAITING_REQUISITION_CREATION)
    }

  private fun streamReadyReports(): Flow<Report> =
    retryLoop { reportStarterClient.streamReadyReports() }

  private fun <T> retryLoop(block: suspend () -> Flow<T>): Flow<T> =
    renewedFlow(Duration.ofMinutes(10).toMillis(), 0) {
      throttleAndLogExceptions(block) ?: emptyFlow()
    }

  private suspend fun <T> throttleAndLogExceptions(block: suspend () -> T): T? =
    logAndSuppressExceptionSuspend { throttler.onReadyGrpc(block) }
}
