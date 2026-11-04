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
import kotlinx.coroutines.runBlocking
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.Timeout
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParamsKt
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.IMPRESSION_QUALIFICATION_FILTER_MAPPING
import org.wfanet.measurement.integration.deploy.gcloud.InternalReportingServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.KingdomDataServicesProviderRule
import org.wfanet.measurement.integration.deploy.gcloud.SpannerAccessServicesFactory
import org.wfanet.measurement.integration.deploy.gcloud.SpannerDuchyDependencyProviderRule
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata.REPORTING_CHANGELOG_PATH as POSTGRES_REPORTING_CHANGELOG_PATH
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest

/** End-to-end test for a Direct frequency result with a suppressed 1+ bucket. */
class GCloudEdpAggregatorDirectFrequencyThresholdTest :
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
    resultMinimumThresholdsByEdp =
      mapOf(
        "edp2" to
          ResultsFulfillerParamsKt.kAnonymityParams {
            minUsers = 11
            minImpressions = 5
            reachMaxFrequencyPerUser = 5
          }
      ),
  ) {

  @get:Rule val timeout: Timeout = Timeout.seconds(180)

  @Test
  fun `Direct report corrects a suppressed one-plus bucket without changing higher buckets`() =
    runBlocking {
      val eventGroups =
        listReportingEventGroups().filter {
          it.eventGroupReferenceId == "campaign/sparse-frequency"
        }
      assertThat(eventGroups).hasSize(1)

      val createBasicReportRequest =
        buildCreateBasicReportRequest(
          eventGroups,
          "direct-frequency-threshold-campaign",
          "direct-frequency-threshold-basicreport",
          includeIqfFilter = false,
        )
      val createdBasicReport =
        reportingBasicReportsClient
          .withCallCredentials(credentials)
          .createBasicReport(createBasicReportRequest)

      executeBasicReportsReportsJob(createdBasicReport.name)

      val measurements = getMeasurementsForBasicReport(createdBasicReport.name)
      val directResults =
        decryptedResults(
          measurements.filter {
            it.dataProvidersList.singleOrNull()?.key == dataProviderName("edp2")
          }
        )
      val reachAndFrequencyResult = directResults.single { it.hasReach() && it.hasFrequency() }
      assertThat(reachAndFrequencyResult.reach.value).isEqualTo(110)
      val distribution = reachAndFrequencyResult.frequency.relativeFrequencyDistributionMap
      assertThat(distribution.getValue(1L)).isEqualTo(0.0)
      assertThat(distribution.getValue(2L)).isWithin(FLOAT_TOLERANCE).of(30.0 / 110.0)
      assertThat(distribution.getValue(3L)).isWithin(FLOAT_TOLERANCE).of(70.0 / 110.0)
      assertThat(distribution.values.sum()).isWithin(FLOAT_TOLERANCE).of(100.0 / 110.0)
      assertThat(directResults.single { it.hasImpression() }.impression.value).isEqualTo(280)

      executeReportProcessorJob()

      val completedBasicReport =
        reportingBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertStructuralResults(completedBasicReport)
      assertSingleEdpNoNoiseResults(
        completedBasicReport,
        expectedReach = 110,
        expectedImpressions = 280,
        expectedKPlusReach = listOf(110L, 100L, 70L, 0L, 0L),
      )
    }

  companion object {
    private const val FLOAT_TOLERANCE = 1e-9

    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val reportingPostgresDatabaseProvider =
      PostgresDatabaseProviderRule(POSTGRES_REPORTING_CHANGELOG_PATH)
  }
}
