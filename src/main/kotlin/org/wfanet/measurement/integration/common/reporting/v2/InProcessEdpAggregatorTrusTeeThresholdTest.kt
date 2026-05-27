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
import org.junit.Test
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Integration tests for TrusTee EDPA reports with threshold configurations.
 *
 * Subclasses configure different noise and small-cell suppression settings via
 * [InProcessCmmsComponents.initConfig] and override assertion methods to match expected results.
 */
abstract class InProcessEdpAggregatorTrusTeeThresholdTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  duchyNames: List<String> = ALL_DUCHY_NAMES,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    duchyNames,
    hmssEnabled = false,
    trusTeeEnabled = true,
    multiEdpDisplayNames = setOf("edp1", "edp2"),
  ) {

  // Noisy tests use approximate assertions instead of exact equality. Subclasses override
  // this when testing configurations that add differential privacy noise.
  open val useNoisyAssertions: Boolean
    get() = false

  // Small-cell suppression fold-down can zero out high-frequency buckets, changing the expected
  // k+ reach distribution. Subclasses override this for threshold configurations.
  open val expectedCrossPublisherKPlusReach: List<Long>
    get() = EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH

  // Reports generally succeed, but some server-side conditions (e.g. infeasible noise
  // correction) cause reports to fail. Subclasses override this to test those failure paths.
  open val expectedTrusTeeBasicReportState: BasicReport.State
    get() = BasicReport.State.SUCCEEDED

  // Subclasses override this to use approximate assertions for noisy results or to check
  // different expected values for threshold configurations.
  open fun assertTrusTeeMetricResults(basicReport: BasicReport) {
    assertNoNoiseResults(
      basicReport,
      expectedCrossPublisherReach = EXPECTED_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_EDP_SPEC2_REACH,
    )
  }

  @Test
  fun `TrusTee basic report has the expected result`() = runBlocking {
    val eventGroups = getMultiEdpEventGroups()
    check(eventGroups.size > 1)

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        "trustee-campaign",
        "trustee-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    val retrievedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertRunningBasicReport(createBasicReportRequest, createdBasicReport, retrievedBasicReport)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(expectedTrusTeeBasicReportState)

    val measurements = listMeasurements()
    val trusTeeProtocolMeasurements =
      measurements.filter { measurement ->
        measurement.protocolConfig.protocolsList.any { it.hasTrusTee() }
      }
    assertWithMessage("at least one measurement used TrusTee protocol")
      .that(trusTeeProtocolMeasurements)
      .isNotEmpty()

    if (expectedTrusTeeBasicReportState == BasicReport.State.SUCCEEDED) {
      assertStructuralResults(completedBasicReport)
      assertTrusTeeMetricResults(completedBasicReport)
    }
  }
}
