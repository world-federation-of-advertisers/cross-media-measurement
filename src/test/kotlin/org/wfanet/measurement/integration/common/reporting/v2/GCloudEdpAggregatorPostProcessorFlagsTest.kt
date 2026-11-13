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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import kotlin.math.round
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.config.reporting.copy
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

  override val metricSpecConfigOverride: MetricSpecConfig
    get() =
      super.metricSpecConfigOverride.copy {
        impressionCountParams =
          impressionCountParams.copy {
            // The frequency measurement still counts multiple impressions per person.
            maximumFrequencyPerUser = 1
          }
      }

  @Test
  fun `report rejected with both flags enabled succeeds with correction disabled`() = runBlocking {
    val rejectedReport =
      createAndProcessReport(
        enableValidation = true,
        enableCorrection = true,
        basicReportId = "flags-enabled-basicreport",
      )
    assertThat(rejectedReport.basicReport.state).isEqualTo(BasicReport.State.FAILED)

    val processedReport =
      createAndProcessReport(
        enableValidation = true,
        enableCorrection = false,
        basicReportId = "flags-disabled-basicreport",
      )
    assertThat(processedReport.basicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(processedReport.reachAndFrequency).isEqualTo(rejectedReport.reachAndFrequency)
    assertThat(processedReport.impression).isEqualTo(rejectedReport.impression)
    assertAssembledResults(processedReport, enableCorrection = false)
  }

  @Test
  fun `report rejected with both flags enabled succeeds with validation and correction disabled`() =
    runBlocking {
      val rejectedReport =
        createAndProcessReport(
          enableValidation = true,
          enableCorrection = true,
          basicReportId = "flags-enabled-basicreport",
        )
      assertThat(rejectedReport.basicReport.state).isEqualTo(BasicReport.State.FAILED)

      val processedReport =
        createAndProcessReport(
          enableValidation = false,
          enableCorrection = false,
          basicReportId = "flags-disabled-basicreport",
        )
      assertThat(processedReport.basicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(processedReport.reachAndFrequency).isEqualTo(rejectedReport.reachAndFrequency)
      assertThat(processedReport.impression).isEqualTo(rejectedReport.impression)
      assertAssembledResults(processedReport, enableCorrection = false)
    }

  @Test
  fun `report rejected with both flags enabled is corrected with validation disabled`() =
    runBlocking {
      val rejectedReport =
        createAndProcessReport(
          enableValidation = true,
          enableCorrection = true,
          basicReportId = "flags-enabled-basicreport",
        )
      assertThat(rejectedReport.basicReport.state).isEqualTo(BasicReport.State.FAILED)

      val processedReport =
        createAndProcessReport(
          enableValidation = false,
          enableCorrection = true,
          basicReportId = "flags-disabled-basicreport",
        )
      assertThat(processedReport.basicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(processedReport.reachAndFrequency).isEqualTo(rejectedReport.reachAndFrequency)
      assertThat(processedReport.impression).isEqualTo(rejectedReport.impression)
      assertAssembledResults(processedReport, enableCorrection = true)
    }

  private fun assertAssembledResults(processedReport: ProcessedReport, enableCorrection: Boolean) {
    val completedBasicReport = processedReport.basicReport
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
      assertThat(cumulative.kPlusReachList).hasSize(processedReport.expectedKPlusReach.size)
      assertThat(cumulative.kPlusReachList.first()).isEqualTo(cumulative.reach)
      for ((lowerFrequencyReach, higherFrequencyReach) in cumulative.kPlusReachList.zipWithNext()) {
        assertThat(lowerFrequencyReach).isAtLeast(higherFrequencyReach)
      }
      assertThat(cumulative.kPlusReachList.last()).isAtLeast(0L)
      assertThat(cumulative.kPlusReachList.sum()).isAtMost(cumulative.impressions)
    } else {
      assertThat(cumulative.reach).isEqualTo(processedReport.reachAndFrequency.reach.value)
      assertThat(cumulative.impressions).isEqualTo(processedReport.impression.impression.value)
      assertThat(cumulative.kPlusReachList)
        .containsExactlyElementsIn(processedReport.expectedKPlusReach)
        .inOrder()
      assertThat(cumulative.kPlusReachList.sum()).isGreaterThan(cumulative.impressions)
    }
    assertThat(cumulative.percentReach)
      .isWithin(FLOAT_TOLERANCE)
      .of((cumulative.reach.toDouble() / metricSet.populationSize * 100).toFloat())
    assertThat(cumulative.averageFrequency)
      .isWithin(FLOAT_TOLERANCE)
      .of((cumulative.impressions.toDouble() / cumulative.reach).toFloat())
  }

  private suspend fun createAndProcessReport(
    enableValidation: Boolean,
    enableCorrection: Boolean,
    basicReportId: String,
  ): ProcessedReport {
    val eventGroups =
      listReportingEventGroups().filter { it.eventGroupReferenceId == "campaign/sparse-frequency" }
    assertThat(eventGroups).hasSize(1)

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(
          buildCreateBasicReportRequest(
            eventGroups,
            "$basicReportId-campaign",
            basicReportId,
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
    val impressionResult = directResults.single { it.hasImpression() }
    val impression = impressionResult.impression
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
    // The capped impression count is below the frequency histogram's weighted total.
    assertThat(expectedKPlusReach.sum()).isGreaterThan(impression.value)

    executeReportProcessorJob(enableValidation, enableCorrection)

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
    return ProcessedReport(
      completedBasicReport,
      reachAndFrequency,
      impressionResult,
      expectedKPlusReach,
    )
  }

  private data class ProcessedReport(
    val basicReport: BasicReport,
    val reachAndFrequency: Measurement.Result,
    val impression: Measurement.Result,
    val expectedKPlusReach: List<Long>,
  )

  companion object {
    private const val FLOAT_TOLERANCE = 1e-5f

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)
  }
}
