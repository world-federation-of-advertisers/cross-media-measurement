// Copyright 2020 The Measurement System Authors
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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import java.math.BigInteger
import java.util.logging.Logger
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.runBlocking
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.common.Duchy
import org.wfanet.measurement.common.identity.testing.DuchyIdSetter
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.pollFor
import org.wfanet.measurement.duchy.testing.DUCHY_IDS
import org.wfanet.measurement.duchy.testing.DUCHY_PUBLIC_KEYS
import org.wfanet.measurement.duchy.toDuchyOrder
import org.wfanet.measurement.integration.common.InProcessDuchy.DuchyDependencies
import org.wfanet.measurement.internal.kingdom.Report
import org.wfanet.measurement.kingdom.db.KingdomRelationalDatabase
import org.wfanet.measurement.kingdom.db.streamReportsFilter

val DUCHY_IDS = DUCHY_IDS

val DUCHIES = DUCHY_PUBLIC_KEYS.latest.map {
  Duchy(it.key, BigInteger(it.value.toByteArray()))
}

val DUCHY_ORDER = DUCHY_PUBLIC_KEYS.latest.toDuchyOrder()

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessKingdomAndDuchyIntegrationTestBase {
  /** Provides a [KingdomRelationalDatabase] to the test. */
  abstract val kingdomRelationalDatabaseRule: ProviderRule<KingdomRelationalDatabase>

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  abstract val duchyDependenciesRule: ProviderRule<(Duchy) -> DuchyDependencies>

  private val kingdomRelationalDatabase: KingdomRelationalDatabase
    get() = kingdomRelationalDatabaseRule.value

  private val kingdom = InProcessKingdom(verboseGrpcLogging = true) { kingdomRelationalDatabase }

  private val duchies: List<InProcessDuchy> by lazy {
    DUCHIES.map { duchy ->
      InProcessDuchy(
        verboseGrpcLogging = true,
        duchyId = duchy.name,
        otherDuchyIds = (DUCHY_IDS.toSet() - duchy.name).toList(),
        kingdomChannel = kingdom.publicApiChannel,
        duchyDependenciesProvider = { duchyDependenciesRule.value(duchy) }
      )
    }
  }

  private val dataProviderRule = FakeDataProviderRule()

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      DuchyIdSetter(DUCHY_IDS),
      dataProviderRule,
      kingdomRelationalDatabaseRule,
      kingdom,
      duchyDependenciesRule,
      *duchies.toTypedArray()
    )
  }

  @Test
  fun `LiquidLegionV2 computation, 1 requisition per duchy`() = runBlocking<Unit> {
    val (dataProviders, campaigns) = kingdom.populateKingdomRelationalDatabase()

    logger.info("Starting first data provider")
    dataProviderRule.startDataProviderForCampaign(
      dataProviders[0],
      campaigns[0],
      duchies[0].newPublisherDataProviderStub()
    )

    logger.info("Starting second data provider")
    dataProviderRule.startDataProviderForCampaign(
      dataProviders[1],
      campaigns[1],
      duchies[1].newPublisherDataProviderStub()
    )

    logger.info("Starting third data provider")
    dataProviderRule.startDataProviderForCampaign(
      dataProviders[1],
      campaigns[2],
      duchies[2].newPublisherDataProviderStub()
    )

    // Now wait until the computation is done.
    val doneReport: Report = pollFor(timeoutMillis = 300_000) {
      kingdomRelationalDatabase
        .streamReports(
          filter = streamReportsFilter(states = listOf(Report.ReportState.SUCCEEDED)),
          limit = 1
        )
        .singleOrNull()
    }

    logger.info("Final Report: $doneReport")

    assertThat(doneReport)
      .comparingExpectedFieldsOnly()
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        Report.newBuilder().apply {
          reportDetailsBuilder.apply {
            addAllConfirmedDuchies(DUCHY_IDS)
            reportDetailsBuilder.resultBuilder.apply {
              reach = 13L
              putFrequency(6L, 0.3)
              putFrequency(3L, 0.7)
            }
          }
        }.build()
      )
  }

  @Test
  fun `LiquidLegionV2 computation, all requisitions at the same duchy`() = runBlocking<Unit> {
    val (dataProviders, campaigns) = kingdom.populateKingdomRelationalDatabase()

    logger.info("Starting first data provider")
    dataProviderRule.startDataProviderForCampaign(
      dataProviders[0],
      campaigns[0],
      duchies[0].newPublisherDataProviderStub()
    )

    logger.info("Starting second data provider")
    dataProviderRule.startDataProviderForCampaign(
      dataProviders[1],
      campaigns[1],
      duchies[0].newPublisherDataProviderStub()
    )

    logger.info("Starting third data provider")
    dataProviderRule.startDataProviderForCampaign(
      dataProviders[1],
      campaigns[2],
      duchies[0].newPublisherDataProviderStub()
    )

    // Now wait until the computation is done.
    val doneReport: Report = pollFor(timeoutMillis = 300_000) {
      kingdomRelationalDatabase
        .streamReports(
          filter = streamReportsFilter(states = listOf(Report.ReportState.SUCCEEDED)),
          limit = 1
        )
        .singleOrNull()
    }

    logger.info("Final Report: $doneReport")

    assertThat(doneReport)
      .comparingExpectedFieldsOnly()
      .ignoringRepeatedFieldOrder()
      .isEqualTo(
        Report.newBuilder().apply {
          reportDetailsBuilder.apply {
            addAllConfirmedDuchies(DUCHY_IDS)
            reportDetailsBuilder.resultBuilder.apply {
              reach = 13L
              putFrequency(6L, 0.3)
              putFrequency(3L, 0.7)
            }
          }
        }.build()
      )
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
