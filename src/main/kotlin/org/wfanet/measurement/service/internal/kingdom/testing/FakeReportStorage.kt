package org.wfanet.measurement.service.internal.kingdom.testing

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.testing.ServiceMocker
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionRequest
import org.wfanet.measurement.internal.kingdom.AssociateRequisitionResponse
import org.wfanet.measurement.internal.kingdom.CreateNextReportRequest
import org.wfanet.measurement.internal.kingdom.GetReportRequest
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamReadyReportsRequest
import org.wfanet.measurement.internal.kingdom.StreamReportsRequest
import org.wfanet.measurement.internal.kingdom.UpdateReportStateRequest

class FakeReportStorage : ReportStorageCoroutineImplBase() {
  val mocker = ServiceMocker<ReportStorageCoroutineImplBase>()

  override suspend fun getReport(request: GetReportRequest): Report =
    mocker.handleCall(request)

  override suspend fun associateRequisition(
    request: AssociateRequisitionRequest
  ): AssociateRequisitionResponse =
    mocker.handleCall(request)

  override suspend fun createNextReport(request: CreateNextReportRequest): Report =
    mocker.handleCall(request)

  override fun streamReadyReports(request: StreamReadyReportsRequest): Flow<Report> =
    mocker.handleCall(request)

  override fun streamReports(request: StreamReportsRequest): Flow<Report> =
    mocker.handleCall(request)

  override suspend fun updateReportState(request: UpdateReportStateRequest): Report =
    mocker.handleCall(request)
}
