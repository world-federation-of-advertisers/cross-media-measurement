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

import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.Timeout
import org.wfanet.measurement.api.v2alpha.ProtocolConfig as PublicProtocolConfig
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_DETERMINISTIC_NOISE_THRESHOLDS_HIGH
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
 * Input suppression under DETERMINISTIC_TRUNCATED_LAPLACE.
 *
 * Under this mechanism the TEE drops a contribution whose own reach is below `min_users` before it
 * enters the aggregate, so that its marginal cannot be recovered by differencing overlapping
 * regions. No other mechanism does this.
 *
 * `min_users` is 3800, between the two EDPs' own reach on this synthetic data. edp2's 3638 is below
 * it, so that contribution is dropped and the aggregate holds edp1 alone; edp1's 3937 is above it,
 * so it survives both the input check and the threshold applied to the aggregate. Their combined
 * reach is 5330, so a report that only applied the thresholds to the aggregated output would report
 * roughly that. Landing on edp1's reach instead is what distinguishes input suppression from output
 * thresholding.
 *
 * Setting the threshold above both EDPs is not a usable alternative: every contribution is then
 * dropped, the aggregate is empty, and the report fails rather than returning zero.
 */
class GCloudEdpAggregatorTrusTeeDeterministicNoiseThresholdsHighReportTest :
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
    deterministicTruncatedLaplaceSupported = true,
  ) {

  override val expectedTrusTeeNoiseMechanism: PublicProtocolConfig.NoiseMechanism
    get() = PublicProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE

  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  override fun assertTrusTeeMetricResults(basicReport: BasicReport) {
    val resultGroup = basicReport.resultGroupsList.single()
    val result =
      resultGroup.resultsList.single {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

    // Only edp1 reaches the aggregate, so the result is its own reach rather than the combined
    // reach of both EDPs, which is EXPECTED_CROSS_PUBLISHER_REACH.
    assertWithinNoiseBound(
      "cross-publisher reach with edp2 suppressed on input",
      reportingUnitCumulative.reach,
      EXPECTED_EDP_SPEC1_REACH,
      UNIT_SENSITIVITY,
    )
  }

  companion object {
    /** One VID moves reach by 1. */
    private const val UNIT_SENSITIVITY = 1.0

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig =
          TRUSTEE_PROTOCOL_CONFIG_CONFIG_DETERMINISTIC_NOISE_THRESHOLDS_HIGH,
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
