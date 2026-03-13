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
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_K_ANON_HIGH_THRESHOLD
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
 * Implementation of [InProcessEdpAggregatorLifeOfAReportTest] for GCloud backends with Spanner
 * database. Uses Gaussian noise TrusTee protocol config with high k-anonymity thresholds that cause
 * some component metrics to be zeroed.
 */
class GCloudEdpAggregatorLifeOfAReportNoiseKAnonHighThresholdTest :
  InProcessEdpAggregatorLifeOfAReportTest(
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

  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  override fun assertTrusTeeResults(basicReport: BasicReport) {
    assertKAnonFilteredResults(basicReport, expectedNonZeroKPlusReachCount = 0)
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
        trusTeeProtocolConfigConfig =
          TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_K_ANON_HIGH_THRESHOLD,
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
