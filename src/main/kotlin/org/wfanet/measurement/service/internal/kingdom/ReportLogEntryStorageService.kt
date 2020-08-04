package org.wfanet.measurement.service.internal.kingdom

import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.internal.kingdom.ReportLogEntry
import org.wfanet.measurement.internal.kingdom.ReportLogEntryStorageGrpcKt.ReportLogEntryStorageCoroutineImplBase

class ReportLogEntryStorageService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : ReportLogEntryStorageCoroutineImplBase() {
  override suspend fun createReportLogEntry(request: ReportLogEntry): ReportLogEntry {
    return kingdomRelationalDatabase.addReportLogEntry(request)
  }
}
