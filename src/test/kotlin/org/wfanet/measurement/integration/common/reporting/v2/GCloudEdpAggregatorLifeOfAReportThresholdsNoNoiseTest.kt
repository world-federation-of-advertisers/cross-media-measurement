// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.Timeout
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.ProtocolConfig as PublicProtocolConfig
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_THRESHOLDS_NO_NOISE
import org.wfanet.measurement.integration.deploy.gcloud.InternalReportingServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.SpannerAccessServicesFactory
import org.wfanet.measurement.integration.deploy.gcloud.SpannerDuchyDependencyProviderRule
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.hmssProtocolConfigConfig
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH as POSTGRES_REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.v2alpha.BasicReport

/**
 * Implementation of [InProcessEdpAggregatorTrusTeeThresholdTest] for GCloud backends with Spanner
 * database. Uses no-noise TrusTee protocol config with small-cell suppression and applies EDP-local
 * Direct thresholds to one publisher.
 */
class GCloudEdpAggregatorLifeOfAReportThresholdsNoNoiseTest :
  InProcessEdpAggregatorTrusTeeThresholdTest(
    kingdomDataServicesRule = KingdomDataServicesProviderRule(spannerEmulator),
    duchyDependenciesRule = SpannerDuchyDependencyProviderRule(spannerEmulator, ALL_DUCHY_NAMES),
    secureComputationDatabaseAdmin = spannerEmulator,
    accessServicesFactory = SpannerAccessServicesFactory(spannerEmulator),
    reportingDataServicesProviderRule =
      InternalReportingServicesProviderRule(
        spannerEmulator,
        reportingPostgresDatabaseProvider,
        IMPRESSION_QUALIFICATION_FILTER_MAPPING,
      ),
    resultMinimumThresholdsByEdp =
      mapOf(
        "edp2" to
          ResultsFulfillerParamsKt.kAnonymityParams {
            minUsers = 100
            minImpressions = 4000
            reachMaxFrequencyPerUser = 5
          }
      ),
  ) {

  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  override fun assertTrusTeeMetricResults(basicReport: BasicReport) {
    assertNoNoiseResults(
      basicReport,
      expectedCrossPublisherReach = EXPECTED_TRUSTEE_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_TRUSTEE_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_TRUSTEE_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_TRUSTEE_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_TRUSTEE_EDP_SPEC2_REACH,
    )

    val components =
      basicReport.resultGroupsList.single().resultsList.single().metricSet.componentsList
    val thresholdedComponent =
      components.single { it.value.cumulative.reach == EXPECTED_TRUSTEE_EDP_SPEC1_REACH }
    assertThat(thresholdedComponent.value.cumulative.kPlusReachList)
      .containsExactly(EXPECTED_TRUSTEE_EDP_SPEC1_REACH, 0L, 0L, 0L, 0L)
      .inOrder()

    val unthresholdedComponent =
      components.single { it.value.cumulative.reach == EXPECTED_TRUSTEE_EDP_SPEC2_REACH }
    assertThat(unthresholdedComponent.value.cumulative.kPlusReachList[1]).isGreaterThan(0L)
  }

  override fun assertTrusTeeMeasurementResults(measurements: List<Measurement>) {
    val thresholdedMeasurements =
      measurements.filter {
        it.dataProvidersList.singleOrNull()?.key == dataProviderName(THRESHOLDED_EDP_DISPLAY_NAME)
      }
    val thresholdedResults = decryptedResults(thresholdedMeasurements)

    val thresholdedResult = thresholdedResults.single { it.hasReach() && it.hasFrequency() }
    assertThat(thresholdedResult.reach.value).isEqualTo(EXPECTED_TRUSTEE_EDP_SPEC1_REACH)
    assertThat(thresholdedResult.reach.noiseMechanism)
      .isEqualTo(PublicProtocolConfig.NoiseMechanism.NONE)
    assertThat(thresholdedResult.frequency.noiseMechanism)
      .isEqualTo(PublicProtocolConfig.NoiseMechanism.NONE)
    val thresholdedDistribution = thresholdedResult.frequency.relativeFrequencyDistributionMap
    // Without threshold uncertainty, these zeros are treated as exact and conflict with the
    // positive reach, making report correction infeasible.
    assertThat(thresholdedDistribution).isNotEmpty()
    assertThat(thresholdedDistribution.values.all { it == 0.0 }).isTrue()
    val thresholdedImpression = thresholdedResults.single { it.hasImpression() }.impression
    assertThat(thresholdedImpression.value).isEqualTo(EXPECTED_TRUSTEE_EDP_SPEC1_IMPRESSIONS)
    assertThat(thresholdedImpression.noiseMechanism)
      .isEqualTo(PublicProtocolConfig.NoiseMechanism.NONE)

    val unthresholdedMeasurements =
      measurements.filter {
        it.dataProvidersList.singleOrNull()?.key == dataProviderName(UNTHRESHOLDED_EDP_DISPLAY_NAME)
      }
    val unthresholdedResults = decryptedResults(unthresholdedMeasurements)
    val unthresholdedResult = unthresholdedResults.single { it.hasReach() && it.hasFrequency() }
    assertThat(unthresholdedResult.reach.value).isEqualTo(EXPECTED_TRUSTEE_EDP_SPEC2_REACH)
    assertThat(unthresholdedResult.frequency.relativeFrequencyDistributionMap.getValue(2L))
      .isGreaterThan(0.0)
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig = TRUSTEE_PROTOCOL_CONFIG_CONFIG_THRESHOLDS_NO_NOISE,
        hmssProtocolConfigConfig =
          hmssProtocolConfigConfig {
            protocolConfig =
              ProtocolConfigKt.honestMajorityShareShuffle {
                noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
                reachAndFrequencyRingModulus = 127
                reachRingModulus = 127
              }
            firstNonAggregatorDuchyId = "worker1"
            secondNonAggregatorDuchyId = "worker2"
            aggregatorDuchyId = "aggregator"
          },
      )
    }

    private const val EXPECTED_TRUSTEE_CROSS_PUBLISHER_REACH = 5330L
    private const val EXPECTED_TRUSTEE_CROSS_PUBLISHER_IMPRESSIONS = 8860L
    private val EXPECTED_TRUSTEE_K_PLUS_REACH = listOf(5330L, 2572L, 647L, 311L, 0L)
    private const val EXPECTED_TRUSTEE_EDP_SPEC1_REACH = 3937L
    private const val EXPECTED_TRUSTEE_EDP_SPEC1_IMPRESSIONS = 4584L
    private const val EXPECTED_TRUSTEE_EDP_SPEC2_REACH = 3638L
    private const val THRESHOLDED_EDP_DISPLAY_NAME = "edp2"
    private const val UNTHRESHOLDED_EDP_DISPLAY_NAME = "edp1"
  }
}
