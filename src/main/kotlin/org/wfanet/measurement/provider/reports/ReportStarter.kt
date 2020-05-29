package org.wfanet.measurement.provider.reports

import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.transform
import org.wfanet.measurement.common.AdaptiveThrottler
import org.wfanet.measurement.common.throttledCollect
import org.wfanet.measurement.internal.kingdom.Report

@OptIn(kotlinx.coroutines.ExperimentalCoroutinesApi::class)
class ReportStarter(
  private val reportApi: ReportApi,
  private val max_parallelism: Int,
  private val throttler: AdaptiveThrottler
) {

  /**
   * Streams ReportConfigs that need Reports and creates them in parallel.
   *
   * Note: this will never terminate by itself; it should be cancelled when done.
   */
  suspend fun createReports() = coroutineScope {
    reportApi.streamReadyScheduledReportConfigs()
      .throttledCollect(max_parallelism, throttler, block = reportApi::createReport)
  }

  /**
   * Streams Reports that are lacking requisitions and creates them in parallel.
   *
   * Note: this will never terminate by itself; it should be cancelled when done.
   */
  suspend fun createRequisitions() {
    reportApi.streamReportsInState(Report.ReportState.AWAITING_REQUISITIONS)
      .transform { emitAll(reportApi.streamMissingRequisitionsForReport(it)) }
      .throttledCollect(max_parallelism, throttler, block = reportApi::maybeAddRequisition)
  }

  /**
   * Streams Reports with no unfulfilled Requisitions and starts the computations in parallel.
   *
   * Note: this will never terminate by itself; it should be cancelled when done.
   */
  suspend fun startComputations() {
    reportApi.streamFulfilledPendingReports()
      .throttledCollect(max_parallelism, throttler, block = reportApi::startReport)
  }
}
