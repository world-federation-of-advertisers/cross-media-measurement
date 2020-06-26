package org.wfanet.measurement.service.internal.kingdom

import io.grpc.BindableService
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase

/** Builds a list of all the Kingdom's internal storage services. */
fun buildStorageServices(relationalDatabase: KingdomRelationalDatabase): List<BindableService> =
  listOf(
    ReportConfigScheduleStorageService(relationalDatabase),
    ReportConfigStorageService(relationalDatabase),
    ReportStorageService(relationalDatabase),
    RequisitionStorageService(relationalDatabase)
  )

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
    *buildStorageServices(relationalDatabase).toTypedArray()
  )
