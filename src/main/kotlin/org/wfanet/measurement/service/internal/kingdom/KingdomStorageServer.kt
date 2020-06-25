package org.wfanet.measurement.service.internal.kingdom

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase

/**
 * Makes a [CommonServer] on [port] exporting all of the internal Kingdom storage-layer services.
 */
fun buildKingdomStorageServer(
  relationalDatabase: KingdomRelationalDatabase,
  port: Int,
  nameForLogging: String = "KingdomStorageServer"
): CommonServer =
  CommonServer(
    nameForLogging,
    port,
    ReportConfigScheduleStorageService(relationalDatabase),
    ReportConfigStorageService(relationalDatabase),
    ReportStorageService(relationalDatabase),
    RequisitionStorageService(relationalDatabase)
  )
