package org.wfanet.measurement.service.v1alpha.requisition

import java.time.Clock
import java.util.logging.Logger
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.CommonServerType
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.RandomIdGeneratorImpl
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import org.wfanet.measurement.kingdom.RequisitionManagerImpl

fun main(args: Array<String>) {
  val logger = Logger.getLogger("MainKt")
  val spanner = SpannerFromFlags()
  Flags.parse(args.toList())

  logger.info("Kingdom Spanner Database: ${spanner.databaseId}")

  val idGen = RandomIdGeneratorImpl(Clock.systemUTC())
  val database = GcpKingdomRelationalDatabase(idGen, spanner.databaseClient)
  val requisitionManager = RequisitionManagerImpl(database)
  CommonServer(CommonServerType.REQUISITION, RequisitionService(requisitionManager))
    .start()
    .blockUntilShutdown()
}
