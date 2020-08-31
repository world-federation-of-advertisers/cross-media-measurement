package org.wfanet.measurement.integration

import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase

val DUCHY_IDS = listOf("duchy1", "duchy2", "duchy3")

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessKingdomAndDuchyIntegrationTest {
  abstract val kingdomRelationalDatabaseRule: ProviderRule<KingdomRelationalDatabase>
  abstract val duchyDependenciesRule: ProviderRule<(String) -> InProcessDuchy.DuchyDependencies>

  private val kingdomRelationalDatabase: KingdomRelationalDatabase
    get() = kingdomRelationalDatabaseRule.value

  private val kingdom = InProcessKingdom { kingdomRelationalDatabase }

  private val duchies: List<InProcessDuchy> = DUCHY_IDS.map { duchyId ->
    val dependencies = duchyDependenciesRule.value(duchyId)
    InProcessDuchy(
      duchyId = duchyId,
      otherDuchyIds = (DUCHY_IDS.toSet() - duchyId).toList(),
      kingdomChannel = kingdom.publicApiChannel,
      duchyDependencies = dependencies
    )
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      DuchyIdSetter(DUCHY_IDS),
      kingdomRelationalDatabaseRule,
      kingdom,
      duchyDependenciesRule,
      *duchies.toTypedArray()
    )
  }

  @Test
  fun noop() {
    // Ensure everything loads OK.
  }
}
