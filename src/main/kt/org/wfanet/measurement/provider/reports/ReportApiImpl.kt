package org.wfanet.measurement.provider.reports

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfa.measurement.internal.kingdom.Requisition

class ReportApiImpl : ReportApi {
  override suspend fun streamReadyScheduledReportConfigs(): Flow<ScheduledReportConfig> = flow {
    TODO("Not yet implemented")
  }

  override suspend fun streamReportsInState(state: ReportState): Flow<Report> = flow {
    TODO("Not yet implemented")
  }

  override suspend fun streamFulfilledPendingReports(): Flow<Report> = flow {
    TODO("Not yet implemented")
  }

  override suspend fun createReport(reportConfig: ScheduledReportConfig) {
    TODO("Not yet implemented")
  }

  override suspend fun startReport(report: Report) {
    TODO("Not yet implemented")
  }

  override suspend fun maybeAddRequisition(requisition: Requisition) {
    TODO("Not yet implemented")
  }
}
