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

import com.google.common.truth.Truth.assertWithMessage
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.Timeout
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_THRESHOLDS_LOW
import org.wfanet.measurement.integration.deploy.gcloud.InternalReportingServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.SpannerAccessServicesFactory
import org.wfanet.measurement.integration.deploy.gcloud.SpannerDuchyDependencyProviderRule
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.hmssProtocolConfigConfig
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH as POSTGRES_REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec

/**
 * Integration test for TrusTee with Gaussian noise and low small-cell suppression thresholds
 * (min_users=100, min_impressions=1000).
 *
 * With these thresholds, all results pass through essentially unchanged: cross-publisher reach
 * (~5330) exceeds min_users (100), total impressions (~8860) exceed min_impressions (1000), and
 * every per-frequency bucket has more than 100 users so no fold-down occurs. The Gaussian noise
 * (epsilon=1.0, delta=1e-15 from the MetricSpecConfig) adds ~8 users standard deviation, which is
 * negligible against the signal. The noise correction post-processor should find a consistent
 * solution and the BasicReport should SUCCEED with values approximately matching the no-noise case.
 */
class GCloudEdpAggregatorLifeOfAReportNoiseThresholdsLowTest :
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
  ) {

  override val useNoisyAssertions: Boolean
    get() = true

  override fun assertTrusTeeMetricResults(basicReport: BasicReport) {
    val resultGroup = basicReport.resultGroupsList.single()
    val totalResults =
      resultGroup.resultsList.filter {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    val result = totalResults.single()
    val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

    assertWithMessage("population size").that(result.metricSet.populationSize).isGreaterThan(0)

    // Noise std dev ~8 users (epsilon=1.0, delta=1e-15). Allow 5% tolerance.
    assertWithMessage("cross-publisher reach (~5330)")
      .that(reportingUnitCumulative.reach)
      .isIn(5064L..5597L)
    assertWithMessage("cross-publisher impressions (~8860)")
      .that(reportingUnitCumulative.impressions)
      .isIn(8417L..9303L)

    assertWithMessage("cross-publisher percent reach")
      .that(reportingUnitCumulative.percentReach)
      .isGreaterThan(0f)
    assertWithMessage("cross-publisher average frequency")
      .that(reportingUnitCumulative.averageFrequency)
      .isGreaterThan(0f)
    assertWithMessage("cross-publisher grps").that(reportingUnitCumulative.grps).isGreaterThan(0f)

    assertWithMessage("cross-publisher k+ reach list is not empty")
      .that(reportingUnitCumulative.kPlusReachList)
      .isNotEmpty()
    assertWithMessage("cross-publisher k+ reach is monotonically non-increasing")
      .that(reportingUnitCumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
      .isTrue()

    assertWithMessage("stacked incremental reach is not empty")
      .that(result.metricSet.reportingUnit.stackedIncrementalReachList)
      .isNotEmpty()

    assertWithMessage("number of components").that(result.metricSet.componentsCount).isEqualTo(2)

    val componentReaches = mutableListOf<Long>()
    result.metricSet.componentsList.forEach { component ->
      val cumulative = component.value.cumulative
      componentReaches.add(cumulative.reach)

      assertWithMessage("component ${component.key} reach").that(cumulative.reach).isGreaterThan(0L)
      assertWithMessage("component ${component.key} impressions")
        .that(cumulative.impressions)
        .isGreaterThan(0L)
      assertWithMessage("component ${component.key} k+ reach is monotonically non-increasing")
        .that(cumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
        .isTrue()

      assertWithMessage("cross-publisher reach > component ${component.key} reach")
        .that(reportingUnitCumulative.reach)
        .isGreaterThan(cumulative.reach)
      assertWithMessage("cross-publisher impressions >= component ${component.key} impressions")
        .that(reportingUnitCumulative.impressions)
        .isAtLeast(cumulative.impressions)
    }

    // Per-EDP reaches should be approximately 3937 and 3638 (allow 10% tolerance for noise).
    for (reach in componentReaches) {
      assertWithMessage("per-EDP reach is in expected range").that(reach).isIn(3274L..4331L)
    }

    assertWithMessage("cross-publisher reach < sum of individual EDP reaches")
      .that(reportingUnitCumulative.reach)
      .isLessThan(componentReaches.sum())
  }

  @get:Rule val timeout: Timeout = Timeout.seconds(180)

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
        trusTeeProtocolConfigConfig = TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_THRESHOLDS_LOW,
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
  }
}
