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
import com.google.common.truth.Truth.assertWithMessage
import kotlinx.coroutines.runBlocking
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import org.wfanet.measurement.api.v2alpha.ProtocolConfig as PublicProtocolConfig
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.TRUSTEE_PROTOCOL_CONFIG_CONFIG_DETERMINISTIC_NOISE
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
import org.wfanet.measurement.reporting.v2alpha.ResultGroup
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest

/**
 * TrusTEE with DETERMINISTIC_TRUNCATED_LAPLACE, end to end.
 *
 * Exercises the whole path the mechanism has to survive: the Kingdom offering it, gated on every
 * DataProvider reporting the capability; the herald carrying it to the Duchy; the TEE drawing the
 * noise; and the reporting server mapping the mechanism and deriving a variance for it. Any of
 * those missing fails the report rather than the assertion.
 */
class GCloudEdpAggregatorTrusTeeDeterministicNoiseReportTest :
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

  /**
   * The draw is a pure function of the aggregated frequency vector and the contribution count, so
   * two reports over the same event groups must agree exactly. A stochastic mechanism fails this.
   */
  @Test
  fun `rerunning the same report returns identical results`() = runBlocking {
    val firstMetricSet = runTotalReport("trustee-rerun-campaign-1", "trustee-rerun-1")
    val secondMetricSet = runTotalReport("trustee-rerun-campaign-2", "trustee-rerun-2")

    assertThat(secondMetricSet).isEqualTo(firstMetricSet)
  }

  /** Runs a BasicReport over the multi-EDP event groups and returns its total metric set. */
  private suspend fun runTotalReport(
    campaignGroupId: String,
    basicReportId: String,
  ): ResultGroup.MetricSet {
    val eventGroups = getMultiEdpEventGroups()
    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        campaignGroupId,
        basicReportId,
        includeIqfFilter = false,
      )
    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
    assertWithMessage("state of $basicReportId")
      .that(completedBasicReport.state)
      .isEqualTo(BasicReport.State.SUCCEEDED)

    val resultGroup = completedBasicReport.resultGroupsList.single()
    return resultGroup.resultsList
      .single { it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL }
      .metricSet
  }

  override fun assertTrusTeeMetricResults(basicReport: BasicReport) {
    val resultGroup = basicReport.resultGroupsList.single()
    val totalResults =
      resultGroup.resultsList.filter {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    val result = totalResults.single()
    val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

    // Reach carries a single draw of L1 sensitivity 1 over the no-noise ground truth, so it
    // cannot move further than the truncation bound.
    assertWithinNoiseBound(
      "cross-publisher reach",
      reportingUnitCumulative.reach,
      EXPECTED_CROSS_PUBLISHER_REACH,
      UNIT_SENSITIVITY,
    )

    // Impressions carry no draw under this configuration: the harness sets every EDP's direct
    // noise to NONE, and the TrusTEE processor noises reach and frequency only.
    assertWithMessage("cross-publisher impressions")
      .that(reportingUnitCumulative.impressions)
      .isEqualTo(EXPECTED_CROSS_PUBLISHER_IMPRESSIONS)

    // Each k+ bucket carries its own draw and is then normalized against a noised denominator, so
    // the error compounds across the histogram.
    assertWithinNoiseBound(
      "3+ reach",
      reportingUnitCumulative.kPlusReachList[2],
      EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH[2],
      K_PLUS_SENSITIVITY,
    )
  }

  companion object {
    /** One VID moves reach, or a single frequency bucket, by 1. */
    private const val UNIT_SENSITIVITY = 1.0

    /**
     * A k+ value is a sum over frequency buckets, each carrying its own draw, normalized against a
     * denominator that carries all of them. Bounding it by the histogram width is conservative.
     */
    private const val K_PLUS_SENSITIVITY = 5.0

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig = TRUSTEE_PROTOCOL_CONFIG_CONFIG_DETERMINISTIC_NOISE,
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
