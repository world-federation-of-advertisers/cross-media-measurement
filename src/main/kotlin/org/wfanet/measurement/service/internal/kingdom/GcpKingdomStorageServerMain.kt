package org.wfanet.measurement.service.internal.kingdom

import org.wfanet.measurement.common.Flags
import org.wfanet.measurement.common.RandomIdGeneratorImpl
import org.wfanet.measurement.common.intFlag
import org.wfanet.measurement.common.stringFlag
import org.wfanet.measurement.db.gcp.SpannerFromFlags
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase
import java.time.Clock

/** Runs the internal Kingdom storage services in a single server with a Spanner backend. */
fun main(args: Array<String>) {
  val port = intFlag("port", 8080)
  val nameForLogging = stringFlag("name-for-logging", "KingdomStorageServer")
  val spannerFromFlags = SpannerFromFlags()
  Flags.parse(args.asIterable())

  val clock = Clock.systemUTC()

  val relationalDatabase = GcpKingdomRelationalDatabase(
    clock,
    RandomIdGeneratorImpl(clock),
    spannerFromFlags.databaseClient
  )

  val server = buildKingdomStorageServer(
    relationalDatabase,
    port.value,
    nameForLogging.value
  )

  server.start().blockUntilShutdown()
}
