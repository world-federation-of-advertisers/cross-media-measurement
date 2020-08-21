package org.wfanet.measurement.kingdom

import com.google.cloud.spanner.DatabaseClient
import java.time.Clock
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase

private const val SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/kingdom.sdl"

class GcpInProcessKingdomIntegrationTest : InProcessKingdomIntegrationTest() {
  private val spannerDatabase = SpannerEmulatorDatabaseRule(SCHEMA_RESOURCE_PATH)

  val databaseClient: DatabaseClient
    get() = spannerDatabase.databaseClient

  override val kingdomRelationalDatabase: KingdomRelationalDatabase by lazy {
    GcpKingdomRelationalDatabase(
      Clock.systemUTC(),
      RandomIdGenerator(Clock.systemUTC()),
      databaseClient
    )
  }

  override val rules = listOf(spannerDatabase)
}
