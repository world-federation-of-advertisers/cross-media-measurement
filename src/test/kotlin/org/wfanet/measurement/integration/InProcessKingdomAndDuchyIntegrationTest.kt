package org.wfanet.measurement.integration

import java.math.BigInteger
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.DuchyOrder
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.duchy.testing.TestKeys

val DUCHY_IDS = listOf("duchy1", "duchy2", "duchy3")

val EL_GAMAL_KEYS = DUCHY_IDS.zip(TestKeys.EL_GAMAL_KEYS).toMap()
val CLIENT_PUBLIC_KEY = TestKeys.COMBINED_EL_GAMAL_PUBLIC_KEY
const val CURVE_ID = TestKeys.CURVE_ID

val DUCHIES = EL_GAMAL_KEYS.map {
  Duchy(it.key, BigInteger(it.value.elGamalPk.toByteArray()))
}

val DUCHY_ORDER = DuchyOrder(DUCHIES.toSet())

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessKingdomAndDuchyIntegrationTest {
  abstract val kingdomRelationalDatabaseRule: ProviderRule<KingdomRelationalDatabase>

  abstract val duchyDependenciesRule: ProviderRule<(Duchy) -> InProcessDuchy.DuchyDependencies>

  private val kingdomRelationalDatabase: KingdomRelationalDatabase
    get() = kingdomRelationalDatabaseRule.value

  private val kingdom = InProcessKingdom { kingdomRelationalDatabase }

  private val duchies: List<InProcessDuchy> by lazy {
    DUCHIES.map { duchy ->
      InProcessDuchy(
        duchyId = duchy.name,
        otherDuchyIds = (DUCHY_IDS.toSet() - duchy.name).toList(),
        kingdomChannel = kingdom.publicApiChannel,
        duchyDependenciesProvider = { duchyDependenciesRule.value(duchy) }
      )
    }
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
