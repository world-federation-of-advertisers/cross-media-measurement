package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.Report.ReportState
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest

class ReportStarterClientImpl(
  private val reportStorage: ReportStorageCoroutineStub,
  private val requisitionStorage: RequisitionStorageCoroutineStub
) : ReportStarterClient {
  override suspend fun createNextReport(reportConfigSchedule: ReportConfigSchedule) {
    val request =
      CreateNextReportRequest.newBuilder()
        .setExternalScheduleId(reportConfigSchedule.externalScheduleId)
        .build()
    reportStorage.createNextReport(request)
  }

  override suspend fun buildRequisitionsForReport(report: Report): List<Requisition> {
    // TODO
    return emptyList()
  }

  override suspend fun createRequisition(requisition: Requisition): Requisition =
    requisitionStorage.createRequisition(requisition)

  override suspend fun associateRequisitionToReport(requisition: Requisition, report: Report) {
    reportStorage.associateRequisition(
      AssociateRequisitionRequest.newBuilder()
        .setExternalReportId(report.externalReportId)
        .setExternalRequisitionId(requisition.externalRequisitionId)
        .build()
    )
  }

  override suspend fun updateReportState(report: Report, newState: ReportState) {
    reportStorage.updateReportState(
      UpdateReportStateRequest.newBuilder()
        .setExternalReportId(report.externalReportId)
        .setState(newState)
        .build()
    )
  }

  override fun streamReportsInState(state: ReportState): Flow<Report> =
    reportStorage.streamReports(
      StreamReportsRequest.newBuilder().apply {
        filterBuilder.addStates(state)
      }.build()
    )

  override fun streamReadyReports(): Flow<Report> =
    reportStorage.streamReadyReports(StreamReadyReportsRequest.getDefaultInstance())

  override fun streamReadySchedules(): Flow<ReportConfigSchedule> { TODO() }
}
