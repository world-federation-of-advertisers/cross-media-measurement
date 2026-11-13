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
import kotlin.math.round
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.deploy.gcloud.InternalReportingServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.SpannerAccessServicesFactory
import org.wfanet.measurement.integration.deploy.gcloud.SpannerDuchyDependencyProviderRule
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH as POSTGRES_REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest

/** End-to-end coverage for postprocessing inconsistent, deterministically noised Direct results. */
class GCloudEdpAggregatorPostProcessorFlagsTest :
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
    hmssEnabled = false,
    trusTeeEnabled = false,
    directNoiseType = ResultsFulfillerParams.NoiseParams.NoiseType.DETERMINISTIC_TRUNCATED_LAPLACE,
  ) {

  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  @Test
  fun `inconsistent deterministic noisy results succeed with correction disabled`() = runBlocking {
    assertCompletedReport(enableValidation = true, enableCorrection = false)
  }

  @Test
  fun `inconsistent deterministic noisy results succeed with validation and correction disabled`() =
    runBlocking {
      assertCompletedReport(enableValidation = false, enableCorrection = false)
    }

  @Test
  fun `inconsistent deterministic noisy results are corrected with validation disabled`() =
    runBlocking {
      assertCompletedReport(enableValidation = false, enableCorrection = true)
    }

  private suspend fun assertCompletedReport(enableValidation: Boolean, enableCorrection: Boolean) {
    val eventGroups =
      listReportingEventGroups().filter { it.eventGroupReferenceId == "campaign/sparse-frequency" }
    assertThat(eventGroups).hasSize(1)

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(
          buildCreateBasicReportRequest(
            eventGroups,
            "postprocessor-flags-campaign",
            "postprocessor-flags-basicreport",
            includeIqfFilter = false,
          )
        )

    executeBasicReportsReportsJob(createdBasicReport.name)

    val measurements = getMeasurementsForBasicReport(createdBasicReport.name)
    assertExpectedProtocolUsed(measurements)
    val directResults =
      decryptedResults(
        measurements.filter { it.dataProvidersList.singleOrNull()?.key == dataProviderName("edp2") }
      )
    val reachAndFrequency = directResults.single { it.hasReach() && it.hasFrequency() }
    val impression = directResults.single { it.hasImpression() }.impression
    val deterministicNoise = ProtocolConfig.NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE
    assertThat(reachAndFrequency.reach.noiseMechanism).isEqualTo(deterministicNoise)
    assertThat(reachAndFrequency.frequency.noiseMechanism).isEqualTo(deterministicNoise)
    assertThat(impression.noiseMechanism).isEqualTo(deterministicNoise)

    val distribution = reachAndFrequency.frequency.relativeFrequencyDistributionMap
    assertThat(reachAndFrequency.reach.value).isGreaterThan(0L)
    assertThat(distribution).isNotEmpty()
    val frequencyCounts =
      distribution.toSortedMap().values.map { it * reachAndFrequency.reach.value }
    val requestedKPlusReach =
      createdBasicReport.resultGroupSpecsList
        .single()
        .resultGroupMetricSpec
        .reportingUnit
        .cumulative
        .kPlusReach
    val expectedKPlusReach =
      (0 until requestedKPlusReach).map { index ->
        round(frequencyCounts.drop(index).sum()).toLong()
      }
    // Independently drawn noise violates the impressions >= sum(k+ reach) identity.
    assertThat(expectedKPlusReach.sum()).isGreaterThan(impression.value)

    executeReportProcessorJob(enableValidation, enableCorrection)

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)

    val metricSet =
      completedBasicReport.resultGroupsList
        .single()
        .resultsList
        .single {
          it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
        }
        .metricSet
    val cumulative = metricSet.reportingUnit.cumulative
    if (enableCorrection) {
      assertThat(cumulative.reach).isGreaterThan(0L)
      assertThat(cumulative.kPlusReachList).hasSize(requestedKPlusReach)
      assertThat(cumulative.kPlusReachList.first()).isEqualTo(cumulative.reach)
      for ((lowerFrequencyReach, higherFrequencyReach) in cumulative.kPlusReachList.zipWithNext()) {
        assertThat(lowerFrequencyReach).isAtLeast(higherFrequencyReach)
      }
      assertThat(cumulative.kPlusReachList.last()).isAtLeast(0L)
      assertThat(cumulative.kPlusReachList.sum()).isAtMost(cumulative.impressions)
    } else {
      assertThat(cumulative.reach).isEqualTo(reachAndFrequency.reach.value)
      assertThat(cumulative.impressions).isEqualTo(impression.value)
      assertThat(cumulative.kPlusReachList).containsExactlyElementsIn(expectedKPlusReach).inOrder()
      assertThat(cumulative.kPlusReachList.sum()).isGreaterThan(cumulative.impressions)
    }
    assertThat(cumulative.percentReach)
      .isWithin(FLOAT_TOLERANCE)
      .of((cumulative.reach.toDouble() / metricSet.populationSize * 100).toFloat())
    assertThat(cumulative.averageFrequency)
      .isWithin(FLOAT_TOLERANCE)
      .of((cumulative.impressions.toDouble() / cumulative.reach).toFloat())
  }

  companion object {
    private const val FLOAT_TOLERANCE = 1e-5f

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)
  }
}
