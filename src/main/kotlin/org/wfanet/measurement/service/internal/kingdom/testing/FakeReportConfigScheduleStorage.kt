package org.wfanet.measurement.service.internal.kingdom.testing

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.testing.ServiceMocker
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest

class FakeReportConfigScheduleStorage : ReportConfigScheduleStorageCoroutineImplBase() {
  val mocker = ServiceMocker<ReportConfigScheduleStorageCoroutineImplBase>()

  override fun streamReadyReportConfigSchedules(
    request: StreamReadyReportConfigSchedulesRequest
  ): Flow<ReportConfigSchedule> = mocker.handleCall(request)
}
