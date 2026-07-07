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
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
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
  fun `no noise basic report with ReportingSet components has the expected result`() = runBlocking {
    // getMultiEdpEventGroups returns exactly one EventGroup per EDP (edp1, edp2).
    val eventGroups = getMultiEdpEventGroups()
    check(eventGroups.size == 2) { "Expected exactly 2 EDP event groups, got ${eventGroups.size}" }

    // One primitive ReportingSet component per EDP. Each spans a single EDP's EventGroup, so the
    // components have disjoint EventGroups and therefore distinct DataProvider sets -- what
    // validation requires. Together they span the same universe as the DataProvider-component
    // `no noise` test, so the expected results are identical.
    val reportingSetComponents: List<ReportingSet> =
      eventGroups.mapIndexed { index, eventGroup ->
        createPrimitiveReportingSet(
          reportingSetId = "reportingset$index",
          displayName = "ReportingSet component $index",
          cmmsEventGroups = listOf(eventGroup.cmmsEventGroup),
        )
      }

    // Reuse the scaffolding from the DataProvider-component request (model line, reporting interval,
    // metric spec), but omit campaign_group so the server synthesizes it, and set the ReportingUnit
    // components to ReportingSets instead of DataProviders.
    val createBasicReportRequest =
      buildCreateBasicReportRequest(
          eventGroups,
          "reportingset-component-campaign",
          "reportingset-component-basicreport",
          includeIqfFilter = false,
        )
        .copy {
          basicReport =
            basicReport.copy {
              campaignGroup = ""
              campaignGroupDisplayName = ""
              val reportingSetComponentSpec =
                resultGroupSpecs.single().copy {
                  reportingUnit = reportingUnit {
                    components += reportingSetComponents.map { it.name }
                  }
                }
              resultGroupSpecs.clear()
              resultGroupSpecs += reportingSetComponentSpec
            }
        }

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    // The caller omitted campaign_group, so the server synthesized one, surfaced only via
    // effective_campaign_group.
    assertThat(createdBasicReport.campaignGroup).isEmpty()
    assertThat(createdBasicReport.effectiveCampaignGroup).isNotEmpty()

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(completedBasicReport.campaignGroup).isEmpty()
    assertThat(completedBasicReport.effectiveCampaignGroup).isNotEmpty()

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
}
