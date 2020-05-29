package org.wfanet.measurement.provider.reports

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.ScheduledReportConfig

class ReportApiImpl : ReportApi {
  override suspend fun streamReadyScheduledReportConfigs(): Flow<ScheduledReportConfig> = flow {
    TODO("Not yet implemented")
  }

  override suspend fun streamReportsInState(state: Report.ReportState): Flow<Report> = flow {
    TODO("Not yet implemented")
  }

  override suspend fun streamFulfilledPendingReports(): Flow<Report> = flow {
    TODO("Not yet implemented")
  }

  override suspend fun streamMissingRequisitionsForReport(report: Report): Flow<Requisition> {
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
