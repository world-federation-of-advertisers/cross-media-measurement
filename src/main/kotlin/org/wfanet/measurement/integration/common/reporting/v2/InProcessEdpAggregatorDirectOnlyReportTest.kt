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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
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
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Integration tests for single-EDP (direct protocol) EDPA report operations.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests.
 */
abstract class InProcessEdpAggregatorDirectOnlyReportTest(
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
  ) {

  @Test
  fun `basic report with reference-id-only event groups succeeds`() = runBlocking {
    val refIdOnlyEventGroups = getReferenceIdOnlyEventGroups()
    check(refIdOnlyEventGroups.isNotEmpty()) { "No reference-ID-only event groups found" }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        refIdOnlyEventGroups,
        "ref-id-only-campaign",
        "ref-id-only-basicreport",
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
    assertSingleEdpNoNoiseResults(
      completedBasicReport,
      expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
      expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
    )
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `basic report with single-edp creative-id event group succeeds`() = runBlocking {
    val allEventGroups = listReportingEventGroups()
    val singleCreativeIdEventGroup =
      allEventGroups.filter {
        it.eventGroupReferenceId == "$CREATIVE_ID_ENTITY_TYPE/$EDP1_CREATIVE_EVENT_GROUP_REF_ID"
      }
    check(singleCreativeIdEventGroup.isNotEmpty()) {
      "No single creative-id event group found for edp1"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        singleCreativeIdEventGroup,
        "creative-id-single-edp-campaign",
        "creative-id-single-edp-basicreport",
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
    assertSingleEdpNoNoiseResults(
      completedBasicReport,
      expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
      expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
    )
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }

  @Test
  fun `basic report with mixed reference-id and entity-key event groups fails`() = runBlocking {
    val mixedEventGroups = getMixedEntityKeyEventGroups()

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        mixedEventGroups,
        "mixed-selectors-campaign",
        "mixed-selectors-basicreport",
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
  fun `basic report with multi-entity-key blob filtering to one entity key succeeds`() =
    runBlocking {
      val singleEntityKeyEventGroup =
        listReportingEventGroups().filter {
          it.eventGroupReferenceId == "$CREATIVE_ID_ENTITY_TYPE/$EDP1_MULTI_CREATIVE_A_ID"
        }
      check(singleEntityKeyEventGroup.isNotEmpty()) { "No multi-entity-key event group found" }

      val createBasicReportRequest =
        buildCreateBasicReportRequest(
          singleEntityKeyEventGroup,
          "multi-entity-key-campaign",
          "multi-entity-key-basicreport",
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
      assertSingleEdpNoNoiseResults(
        completedBasicReport,
        expectedReach = EXPECTED_SINGLE_EDP_SPEC2_REACH,
        expectedImpressions = EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS,
        expectedKPlusReach = EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH,
      )
      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }

  @Test
  fun `basic report with two same-edp entity-key event groups succeeds`() = runBlocking {
    val bothEntityKeyEventGroups =
      listReportingEventGroups().filter {
        it.eventGroupReferenceId in EDP1_MULTI_ENTITY_KEY_REF_IDS
      }
    check(bothEntityKeyEventGroups.size == 2) {
      "Expected 2 multi-entity-key event groups, got ${bothEntityKeyEventGroups.size}"
    }

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        bothEntityKeyEventGroups,
        "two-entity-key-campaign",
        "two-entity-key-basicreport",
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

    val resultGroup = completedBasicReport.resultGroupsList.single()
    val totalResult =
      resultGroup.resultsList.single {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    val reportingUnitCumulative = totalResult.metricSet.reportingUnit.cumulative

    assertWithMessage("reach")
      .that(reportingUnitCumulative.reach)
      .isEqualTo(EXPECTED_SINGLE_EDP_SPEC2_REACH)
    assertWithMessage("impressions")
      .that(reportingUnitCumulative.impressions)
      .isEqualTo(EXPECTED_TWO_ENTITY_KEY_IMPRESSIONS)
    assertWithMessage("k+ reach")
      .that(reportingUnitCumulative.kPlusReachList)
      .containsExactlyElementsIn(EXPECTED_TWO_ENTITY_KEY_K_PLUS_REACH)
      .inOrder()
    assertWithMessage("population size").that(totalResult.metricSet.populationSize).isGreaterThan(0)
    assertWithMessage("number of components")
      .that(totalResult.metricSet.componentsCount)
      .isEqualTo(1)
    assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
  }
}
