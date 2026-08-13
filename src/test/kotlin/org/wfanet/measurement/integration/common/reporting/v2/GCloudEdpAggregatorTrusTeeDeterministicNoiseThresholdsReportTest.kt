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
import org.wfanet.measurement.api.v2alpha.ProtocolConfig as PublicProtocolConfig
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_DETERMINISTIC_NOISE_THRESHOLDS
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
 * Below-threshold suppression and fold-down under DETERMINISTIC_TRUNCATED_LAPLACE.
 *
 * `min_users` is 500, below each EDP's own reach, so no contribution is dropped on input and the
 * thresholds act on the noised output histogram. On this synthetic data the two highest frequency
 * buckets hold 311 and 0 users, both under the threshold, so each is zeroed and folded into the
 * bucket below it. The surviving buckets stay populated.
 *
 * The suppressed buckets are exactly zero whatever the draw, so this needs no tolerance.
 */
class GCloudEdpAggregatorTrusTeeDeterministicNoiseThresholdsReportTest :
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
    val kPlusReach = result.metricSet.reportingUnit.cumulative.kPlusReachList

    assertWithMessage("k+ reach buckets").that(kPlusReach).hasSize(EXPECTED_BUCKET_COUNT)
    for (index in SUPPRESSED_BUCKET_INDICES) {
      assertWithMessage("$index+ reach is suppressed").that(kPlusReach[index]).isEqualTo(0L)
    }
    for (index in 0 until SUPPRESSED_BUCKET_INDICES.first) {
      assertWithMessage("$index+ reach survives").that(kPlusReach[index]).isGreaterThan(0L)
    }
    assertWithMessage("k+ reach is non-increasing")
      .that(kPlusReach)
      .isInOrder(Comparator.reverseOrder<Long>())
  }

  companion object {
    private const val EXPECTED_BUCKET_COUNT = 5

    // The 4+ and 5+ buckets hold 311 and 0 users before noise, both under min_users, so both fold
    // down. The 3+ bucket holds 647 and survives.
    private val SUPPRESSED_BUCKET_INDICES = 3..4

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig = TRUSTEE_PROTOCOL_CONFIG_CONFIG_DETERMINISTIC_NOISE_THRESHOLDS,
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
