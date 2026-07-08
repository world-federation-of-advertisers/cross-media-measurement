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
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import com.google.type.DayOfWeek
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
  // Regression for #4132. Under the shape:
  //   metric_frequency: { weekly: MONDAY }
  //   reporting_unit: cumulative only (NO non_cumulative)
  //   component:      non_cumulative (per-EDP weekly)
  // the write side stored per-primitive weekly non_cumulative RSRs in one window bucket and
  // the union whole-report cumulative RSR in a separate bucket, each carrying only its own
  // external ReportingSet IDs. `BasicReportProcessedResultsTransformation.buildResults`
  // unconditionally called both `buildReportingUnitMetricSet` and `buildComponentMetricSet`
  // on every window, so `Map.getValue` on the missing key threw NoSuchElementException:
  //   - via `GetBasicReport`: surfaced as gRPC INTERNAL with no error text.
  //   - via `ListBasicReports` result assembly: swallowed by outer machinery, report
  //     landed in `state=SUCCEEDED` with `resultGroups=[]` (silent data loss).
  //
  // Post-fix: per-window `containsKey` guards in `buildResults` skip the metric slice
  // whose ReportingSet is not in the current window's bucket. Under the reproducer shape:
  //   - The cumulative bucket renders the union `reporting_unit.cumulative` slice and
  //     silently skips the per-EDP component slice (whose RSs live elsewhere).
  //   - The per-primitive weekly bucket renders the per-EDP `component.non_cumulative`
  //     slice and silently skips the union reporting_unit slice.
  //
  // Report window is ~4 days (Sun 2021-03-14 17:00 PT -> Thu 2021-03-18), so weekly Monday
  // cadence collapses to exactly one weekly bucket covering the full window: per-week per-EDP
  // non_cumulative reach == the deterministic total per-EDP reach.
  @Test
  fun `weekly report with reporting_unit cumulative and component non_cumulative succeeds`() =
    runBlocking {
      val eventGroups = getMultiEdpEventGroups()
      check(eventGroups.size > 1)

      val baseRequest =
        buildCreateBasicReportRequest(
          eventGroups,
          "weekly-cumulative-plus-component-campaign",
          "weekly-cumulative-plus-component-basicreport",
          includeIqfFilter = false,
        )
      val requestSpec =
        baseRequest.basicReport.resultGroupSpecsList.single().copy {
          title = "weekly-cumulative-plus-component"
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    impressions = true
                    averageFrequency = true
                  }
              }
          }
        }
      val createBasicReportRequest =
        baseRequest.copy {
          basicReport =
            basicReport.copy {
              resultGroupSpecs.clear()
              resultGroupSpecs += requestSpec
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

      // Pre-fix regression witnesses (all failed pre-fix):
      //   - state=SUCCEEDED (pre-fix: some code paths returned INTERNAL)
      //   - resultGroups not empty (pre-fix: empty on the ListBasicReports path)
      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(completedBasicReport.resultGroupsList).hasSize(1)
      val resultGroup = completedBasicReport.resultGroupsList.single()
      assertWithMessage("resultsList not empty").that(resultGroup.resultsList).isNotEmpty()

      // The response shape under weekly cadence + reporting_unit.cumulative +
      // component.non_cumulative carries multiple result entries:
      //   * Union `reporting_unit.cumulative` slices: `nonCumulativeMetricStartTime` is
      //     unset (0). The entry with the max `metricEndTime` covers the whole report.
      //   * Per-EDP `component.non_cumulative` slices: `nonCumulativeMetricStartTime` is
      //     set and `componentsList` is populated.
      // Pre-fix, one of these silently disappeared (per PR body: `Map.getValue` on the
      // missing per-window RS ID threw NoSuchElement, mapped either to gRPC INTERNAL on the
      // GetBasicReport path or to a silent empty `resultGroupsList` on the ListBasicReports
      // path). Post-fix, both must render.
      val unionCumulativeResults =
        resultGroup.resultsList.filter { it.metadata.nonCumulativeMetricStartTime.seconds == 0L }
      val perEdpNonCumulativeResults =
        resultGroup.resultsList.filter { it.metadata.nonCumulativeMetricStartTime.seconds > 0L }
      assertWithMessage("union cumulative slice present")
        .that(unionCumulativeResults)
        .isNotEmpty()
      assertWithMessage("per-EDP non_cumulative slice present")
        .that(perEdpNonCumulativeResults)
        .isNotEmpty()

      // Union cumulative reach at report end == deterministic cross-publisher reach
      // (`EXPECTED_CROSS_PUBLISHER_REACH` is pinned in `InProcessEdpAggregatorLifeOfAReportTest`
      // by the synthetic-data setup; reused by other no-noise assertions in this file).
      val wholeReportUnionCumulative =
        unionCumulativeResults.maxBy { it.metadata.metricEndTime.seconds }
      assertWithMessage("union cumulative reach at end of report")
        .that(wholeReportUnionCumulative.metricSet.reportingUnit.cumulative.reach)
        .isEqualTo(EXPECTED_CROSS_PUBLISHER_REACH)

      // Every per-EDP non_cumulative entry must carry a components list keyed by the two DPs
      // in the reporting unit. Pre-fix, the components map was populated but the entry itself
      // was dropped in one of the two code paths above.
      val allComponentKeys =
        perEdpNonCumulativeResults.flatMap { it.metricSet.componentsList.map { c -> c.key } }.toSet()
      assertWithMessage("per-EDP non_cumulative components cover both DPs")
        .that(allComponentKeys.size)
        .isEqualTo(2)

      // Correctness invariants that hold regardless of the weekly slicing:
      resultGroup.resultsList.forEach { r ->
        val cum = r.metricSet.reportingUnit.cumulative
        assertWithMessage("reporting_unit.cumulative.reach >= 0").that(cum.reach).isAtLeast(0L)
        r.metricSet.componentsList.forEach { component ->
          val nc = component.value.nonCumulative
          assertWithMessage("component ${component.key} non_cumulative reach >= 0")
            .that(nc.reach)
            .isAtLeast(0L)
          assertWithMessage("component ${component.key} non_cumulative impressions >= reach")
            .that(nc.impressions)
            .isAtLeast(nc.reach)
          if (nc.reach > 0) {
            // average_frequency == impressions / reach (within float tolerance).
            val expectedAvgFreq = nc.impressions.toFloat() / nc.reach.toFloat()
            assertWithMessage("component ${component.key} average_frequency")
              .that(nc.averageFrequency)
              .isWithin(1e-4f)
              .of(expectedAvgFreq)
          }
        }
      }

      // Union cumulative reach is monotonically non-decreasing across
      // increasing `metricEndTime`s.
      val cumulativeReachesByEndTime =
        unionCumulativeResults
          .sortedBy { it.metadata.metricEndTime.seconds }
          .map { it.metricSet.reportingUnit.cumulative.reach }
      assertWithMessage("union cumulative reach monotonically non-decreasing")
        .that(cumulativeReachesByEndTime.zipWithNext { a, b -> b >= a }.all { it })
        .isTrue()

      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }
}
