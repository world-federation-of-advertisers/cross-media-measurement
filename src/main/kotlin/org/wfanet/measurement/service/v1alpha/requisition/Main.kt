package org.wfanet.measurement.service.v1alpha.requisition

import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.CommonServerType
import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.RandomIdGeneratorImpl
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import org.wfanet.measurement.kingdom.RequisitionManagerImpl
import java.time.Clock
import java.util.logging.Logger

fun main(args: Array<String>) {
  val logger = Logger.getLogger("MainKt")
  val spanner = SpannerFromFlags()
  Flags.parse(args.toList())

  logger.info("Kingdom Spanner Database: ${spanner.databaseId}")

  val database = GcpKingdomRelationalDatabase(spanner.databaseClient)
  val idGen = RandomIdGeneratorImpl(Clock.systemUTC())
  val requisitionManager = RequisitionManagerImpl(idGen, database)
  CommonServer(CommonServerType.REQUISITION, RequisitionService(requisitionManager))
    .start()
    .blockUntilShutdown()
}
