package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.Requisition

interface ReportStarterClient {
  suspend fun createNextReport(reportConfigSchedule: ReportConfigSchedule)
  suspend fun buildRequisitionsForReport(report: Report): List<Requisition>
  suspend fun createRequisition(requisition: Requisition): Requisition
  suspend fun associateRequisitionToReport(requisition: Requisition, report: Report)
  suspend fun updateReportState(report: Report, newState: ReportState)
  fun streamReportsInState(state: ReportState): Flow<Report>
  fun streamReadyReports(): Flow<Report>
  fun streamReadySchedules(): Flow<ReportConfigSchedule>
}
