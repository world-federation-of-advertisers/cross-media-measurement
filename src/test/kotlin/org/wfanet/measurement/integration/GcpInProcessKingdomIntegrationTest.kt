package org.wfanet.measurement.integration

import java.time.Clock
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.RandomIdGenerator
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.db.gcp.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.gcp.GcpKingdomRelationalDatabase

private const val SCHEMA_RESOURCE_PATH = "/src/main/db/gcp/kingdom.sdl"

class GcpInProcessKingdomIntegrationTest : InProcessKingdomIntegrationTest() {
  override val kingdomRelationalDatabaseRule = object : ProviderRule<KingdomRelationalDatabase> {
    private val spannerDatabase = SpannerEmulatorDatabaseRule(SCHEMA_RESOURCE_PATH)

    override val value: KingdomRelationalDatabase by lazy {
      GcpKingdomRelationalDatabase(
        Clock.systemUTC(),
        RandomIdGenerator(Clock.systemUTC()),
        spannerDatabase.databaseClient
      )
    }

    override fun apply(base: Statement, description: Description): Statement {
      return spannerDatabase.apply(base, description)
    }
  }
}
