// Copyright 2020 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.common

import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.Ignore
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.integration.common.InProcessDuchy.DuchyDependencies
import org.wfanet.measurement.kingdom.db.testing.KingdomDatabases

val DUCHY_IDS = listOf("a", "b", "c")

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessKingdomAndDuchyIntegrationTestBase {
  /** Provides database wrappers to the test. */
  abstract val kingdomDatabasesRule: ProviderRule<KingdomDatabases>

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  abstract val duchyDependenciesRule: ProviderRule<(String) -> DuchyDependencies>

  private val kingdomDatabases: KingdomDatabases
    get() = kingdomDatabasesRule.value

  private val kingdom =
    InProcessKingdom(verboseGrpcLogging = true, databasesProvider = { kingdomDatabases })

  private val duchies: List<InProcessDuchy> by lazy {
    DUCHY_IDS.map { duchy ->
      InProcessDuchy(
        verboseGrpcLogging = true,
        duchyId = duchy,
        otherDuchyIds = (DUCHY_IDS.toSet() - duchy).toList(),
        kingdomChannel = kingdom.publicApiChannel,
        duchyDependenciesProvider = { duchyDependenciesRule.value(duchy) }
      )
    }
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      DuchyIdSetter(DUCHY_IDS),
      kingdomDatabasesRule,
      kingdom,
      duchyDependenciesRule,
      *duchies.toTypedArray()
    )
  }

  @Test
  @Ignore
  fun `LiquidLegionV2 computation, 1 requisition per duchy`() = runBlocking {
    // TODO(wangyaopw): add test for this when v2alpha kingdom is done.
  }

  @Test
  @Ignore
  fun `LiquidLegionV2 computation, all requisitions at the same duchy`() = runBlocking {
    // TODO(wangyaopw): add test for this when v2alpha kingdom is done.
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
