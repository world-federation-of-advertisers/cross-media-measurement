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
import org.junit.Test
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Integration tests for multi-EDP EDPA report operations.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests.
 * The protocol (HMSS or TrusTee) is determined by the [hmssEnabled] and [trusTeeEnabled] params.
 */
abstract class InProcessEdpAggregatorMultiEdpReportTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  duchyNames: List<String> = ALL_DUCHY_NAMES,
  hmssEnabled: Boolean,
  trusTeeEnabled: Boolean,
) :
  InProcessEdpAggregatorLifeOfAReportTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    secureComputationDatabaseAdmin,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    duchyNames,
    hmssEnabled,
    trusTeeEnabled,
    multiEdpDisplayNames = setOf("edp1", "edp2"),
  ) {

  @Test
  fun `no noise basic report has the expected result`() = runBlocking {
    val eventGroups = getMultiEdpEventGroups()
    check(eventGroups.size > 1)

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        "multi-edp-campaign",
        "multi-edp-basicreport",
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

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

    assertStructuralResults(completedBasicReport)
    assertNoNoiseResults(
      completedBasicReport,
      expectedCrossPublisherReach = EXPECTED_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_EDP_SPEC2_REACH,
    )
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `basic report with cross-publisher creative-id event groups succeeds`() = runBlocking {
    val creativeIdEventGroups = getCreativeIdOnlyEventGroups()
    check(creativeIdEventGroups.size >= 2) {
      "Expected at least 2 creative-id event groups, got ${creativeIdEventGroups.size}"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        creativeIdEventGroups,
        "creative-id-cross-pub-campaign",
        "creative-id-cross-pub-basicreport",
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

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertStructuralResults(completedBasicReport)
    assertNoNoiseResults(
      completedBasicReport,
      expectedCrossPublisherReach = EXPECTED_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_EDP_SPEC2_REACH,
    )
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `no noise basic report fails when EDP requires Gaussian noise`() = runBlocking {
    val eventGroups = getMultiEdpEventGroupsIncludingRestrictedEdp()
    check(eventGroups.size > 1)

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        eventGroups,
        "gaussian-campaign",
        "gaussian-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    executeBasicReportsReportsJob(createdBasicReport.name)

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.FAILED)
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `basic report with two result_group_specs different component orderings both succeed`() =
    runBlocking {
      val eventGroups = getMultiEdpEventGroups()
      check(eventGroups.size > 1)
      val distinctDps = eventGroups.map { it.cmmsDataProvider }.distinct()
      check(distinctDps.size == 2) {
        "Test requires exactly 2 distinct DPs, got ${distinctDps.size}"
      }

      // Build the standard single-spec request, then splice in a second result_group_spec
      // that references the same DPs in reversed order. Both request stackedIncrementalReach
      // -> different first-component anchors in one BasicReport.
      val baseRequest =
        buildCreateBasicReportRequest(
          eventGroups,
          "multi-edp-two-anchor-campaign",
          "multi-edp-two-anchor-basicreport",
          includeIqfFilter = false,
        )
      val originalSpec = baseRequest.basicReport.resultGroupSpecsList.single()
      val reversedSpec =
        originalSpec.copy {
          title = "reversed"
          reportingUnit =
            reportingUnit.copy {
              components.clear()
              components += distinctDps.reversed()
            }
        }
      val originalSpecTitled = originalSpec.copy { title = "original" }
      val createBasicReportRequest =
        baseRequest.copy {
          basicReport =
            basicReport.copy {
              resultGroupSpecs.clear()
              resultGroupSpecs += originalSpecTitled
              resultGroupSpecs += reversedSpec
            }
        }

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

      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(completedBasicReport.resultGroupsList).hasSize(2)

      val resultGroupsByTitle =
        completedBasicReport.resultGroupsList.associate { it.title to it.resultsList.single() }
      val originalResult = resultGroupsByTitle.getValue("original")
      val reversedResult = resultGroupsByTitle.getValue("reversed")

      val originalStacked = originalResult.metricSet.reportingUnit.stackedIncrementalReachList
      val reversedStacked = reversedResult.metricSet.reportingUnit.stackedIncrementalReachList

      // Both curves have one point per component.
      assertThat(originalStacked).hasSize(2)
      assertThat(reversedStacked).hasSize(2)

      // Each curve sums to the full union reach (regardless of anchor order).
      assertThat(originalStacked.sum()).isEqualTo(EXPECTED_CROSS_PUBLISHER_REACH)
      assertThat(reversedStacked.sum()).isEqualTo(EXPECTED_CROSS_PUBLISHER_REACH)

      // The two curves are anchored on different first-components -> different first values.
      assertThat(originalStacked[0]).isNotEqualTo(reversedStacked[0])

      // Anchor values are the two individual per-EDP reach values (order-independent check).
      // Confirms each curve's leading point is the reach of *its own* first component,
      // not a bleed-over from the sibling spec's ordering.
      assertThat(listOf(originalStacked[0], reversedStacked[0]).sorted())
        .containsExactly(EXPECTED_EDP_SPEC2_REACH, EXPECTED_EDP_SPEC1_REACH)
        .inOrder()

      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }
}
