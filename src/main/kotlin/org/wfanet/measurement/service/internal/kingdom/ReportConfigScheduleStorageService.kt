package org.wfanet.measurement.service.internal.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ReportConfigSchedule
import org.wfanet.measurement.internal.kingdom.ReportConfigScheduleStorageGrpcKt.ReportConfigScheduleStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamReadyReportConfigSchedulesRequest

class ReportConfigScheduleStorageService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : ReportConfigScheduleStorageCoroutineImplBase() {

  override fun streamReadyReportConfigSchedules(
    request: StreamReadyReportConfigSchedulesRequest
  ): Flow<ReportConfigSchedule> {
    TODO()
  }
}
