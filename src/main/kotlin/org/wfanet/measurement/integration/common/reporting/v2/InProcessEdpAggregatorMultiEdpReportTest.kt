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
import com.google.type.DayOfWeek
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
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
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

    // Reuse the scaffolding from the DataProvider-component request (model line, reporting
    // interval, metric spec), but omit campaign_group (campaignGroupId = null) so the server
    // synthesizes it, and set the ReportingUnit components to ReportingSets instead of
    // DataProviders.
    val createBasicReportRequest =
      buildCreateBasicReportRequest(
          eventGroups,
          campaignGroupId = null,
          basicReportId = "reportingset-component-basicreport",
          includeIqfFilter = false,
        )
        .copy {
          basicReport =
            basicReport.copy {
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

    // Verify the ReportingSet-component request round-trips via getBasicReport in the RUNNING state
    // before it is processed, so a field dropped on persist is caught here.
    val retrievedRunningBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
    assertRunningBasicReport(
      createBasicReportRequest,
      createdBasicReport,
      retrievedRunningBasicReport,
      expectedEffectiveCampaignGroup = createdBasicReport.effectiveCampaignGroup,
    )

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(completedBasicReport.campaignGroup).isEmpty()
    assertThat(completedBasicReport.effectiveCampaignGroup).isNotEmpty()

    // The results are keyed by the ReportingSet component resource names. This is what
    // distinguishes ReportingSet components from DataProvider components -- a regression that
    // resolved the ReportingSets back to their underlying DataProviders would key by DataProvider
    // name and still pass the numeric assertions below.
    val componentKeys =
      completedBasicReport.resultGroupsList
        .single()
        .resultsList
        .single {
          it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
        }
        .metricSet
        .componentsList
        .map { it.key }
    assertThat(componentKeys).containsExactlyElementsIn(reportingSetComponents.map { it.name })

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

  // Under the shape:
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
  // cadence produces TWO weekly buckets: a partial bucket from report_start to the first
  // Monday, and a second bucket from that Monday to report_end. Two buckets exercises the
  // per-window guard for both branches (per-primitive and union) with real multi-bucket
  // data; a single-bucket window would not distinguish a correct fix from a fix that only
  // handled the union-only case.
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
      // Pre-fix, one of these silently disappeared (`Map.getValue` on the
      // missing per-window RS ID threw NoSuchElement, mapped either to gRPC INTERNAL on the
      // GetBasicReport path or to a silent empty `resultGroupsList` on the ListBasicReports
      // path). Post-fix, both must render.
      val unionCumulativeResults =
        resultGroup.resultsList.filter { it.metadata.nonCumulativeMetricStartTime.seconds == 0L }
      val perEdpNonCumulativeResults =
        resultGroup.resultsList.filter { it.metadata.nonCumulativeMetricStartTime.seconds > 0L }
      assertWithMessage("union cumulative slice present").that(unionCumulativeResults).isNotEmpty()
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
        perEdpNonCumulativeResults
          .flatMap { it.metricSet.componentsList.map { c -> c.key } }
          .toSet()
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

      // "Adds up" invariant: sum of per-EDP per-week non_cumulative impressions across all
      // weekly buckets equals the whole-report cross-publisher impressions total. Reach cannot
      // be summed this way (a VID reached in two weeks is counted twice in the non_cumulative
      // sum but once in the union total); impressions add cleanly because each impression is
      // counted once at its actual time.
      val perEdpImpressionsSum: Long =
        perEdpNonCumulativeResults.sumOf { r ->
          r.metricSet.componentsList.sumOf { c -> c.value.nonCumulative.impressions }
        }
      assertWithMessage("sum of per-EDP per-week non_cumulative impressions")
        .that(perEdpImpressionsSum)
        .isEqualTo(EXPECTED_CROSS_PUBLISHER_IMPRESSIONS)

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

      // Cross-spec consistency assertions: The two specs reference the same
      // underlying components (just in different orderings), so the union metrics and
      // population size must be identical across specs. If the alias table over- or
      // under-fires, per-component data can leak between specs; these assertions detect that.
      val originalUnionCumulative = originalResult.metricSet.reportingUnit.cumulative
      val reversedUnionCumulative = reversedResult.metricSet.reportingUnit.cumulative
      assertWithMessage("cross-spec union cumulative reach equal")
        .that(originalUnionCumulative.reach)
        .isEqualTo(reversedUnionCumulative.reach)
      assertWithMessage("cross-spec union cumulative impressions equal")
        .that(originalUnionCumulative.impressions)
        .isEqualTo(reversedUnionCumulative.impressions)
      assertWithMessage("cross-spec population size equal")
        .that(originalResult.metricSet.populationSize)
        .isEqualTo(reversedResult.metricSet.populationSize)
      // Each spec must expose both DPs in its componentsList (component-scoped result must not
      // silently drop a DP under the reordering).
      assertWithMessage("original spec componentsList covers both DPs")
        .that(originalResult.metricSet.componentsList.map { it.key }.toSet())
        .containsExactlyElementsIn(distinctDps)
      assertWithMessage("reversed spec componentsList covers both DPs")
        .that(reversedResult.metricSet.componentsList.map { it.key }.toSet())
        .containsExactlyElementsIn(distinctDps)

      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }

  // A cross-publisher summary report: weekly cadence, requesting both
  // `reporting_unit.non_cumulative` (per-week incremental) and
  // `reporting_unit.cumulative` (running total across weeks), with NO per-EDP
  // `component` subfield. The result set carries one union `ReportingSetResult`
  // per week bucket, and a single whole-report union `ReportingSetResult` for
  // the cumulative side.
  @Test
  fun `weekly cross-publisher union reach and impressions succeeds with no per-EDP components`() =
    runBlocking {
      val eventGroups = getMultiEdpEventGroups()
      check(eventGroups.size > 1)

      val baseRequest =
        buildCreateBasicReportRequest(
          eventGroups,
          "weekly-union-no-components-campaign",
          "weekly-union-no-components-basicreport",
          includeIqfFilter = false,
        )
      val requestSpec =
        baseRequest.basicReport.resultGroupSpecsList.single().copy {
          title = "weekly-union-no-components"
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          resultGroupMetricSpec = resultGroupMetricSpec {
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    impressions = true
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

      assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(completedBasicReport.resultGroupsList).hasSize(1)

      // Under weekly cadence + reporting_unit.cumulative + reporting_unit.nonCumulative
      // (no per-EDP component), each weekly bucket produces one entry carrying BOTH the
      // union cumulative (through the bucket's metric_end_time) AND the union non_cumulative
      // (for that bucket alone). Report window is ~4 days -> two weekly buckets on Monday
      // cadence.
      //
      // Whole-report cumulative reach / impressions == deterministic cross-publisher totals
      // ( /  pinned in
      //  by the synthetic-data setup; reused by other
      // no-noise assertions in this file).
      val resultGroup = completedBasicReport.resultGroupsList.single()
      val weeklyResults =
        resultGroup.resultsList.filter {
          it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
        }
      assertWithMessage("weekly results").that(weeklyResults).isNotEmpty()

      val wholeReportEntry = weeklyResults.maxBy { it.metadata.metricEndTime.seconds }
      val wholeReportCumulative = wholeReportEntry.metricSet.reportingUnit.cumulative
      assertWithMessage("union cumulative reach at end of report")
        .that(wholeReportCumulative.reach)
        .isEqualTo(EXPECTED_CROSS_PUBLISHER_REACH)
      // Cumulative impressions is not asserted here because the API rejects
      // reporting_unit.cumulative.impressions when metric_frequency is weekly (returns
      // UNIMPLEMENTED). The whole-report impressions total is instead validated below via
      // the sum-of-weekly-non_cumulative-impressions == EXPECTED_CROSS_PUBLISHER_IMPRESSIONS
      // adds-up invariant.

      // "Adds up" invariant: sum of per-week union non_cumulative impressions across all
      // weekly buckets equals the whole-report impressions total. Impressions add cleanly
      // across time; reach does not (a VID reached in two weeks counts twice in the sum but
      // once in the union total).
      val nonCumulativeImpressionsSum =
        weeklyResults.sumOf { it.metricSet.reportingUnit.nonCumulative.impressions }
      assertWithMessage("sum of weekly non_cumulative union impressions")
        .that(nonCumulativeImpressionsSum)
        .isEqualTo(EXPECTED_CROSS_PUBLISHER_IMPRESSIONS)

      // Correctness invariants that hold regardless of the weekly slicing:
      // Cumulative impressions is not asserted here because the API rejects
      // reporting_unit.cumulative.impressions when metric_frequency is weekly (UNIMPLEMENTED),
      // so cumulative impressions stays at proto default (0) even though cumulative reach is
      // populated.
      weeklyResults.forEach { r ->
        val cum = r.metricSet.reportingUnit.cumulative
        val nc = r.metricSet.reportingUnit.nonCumulative
        assertWithMessage("non_cumulative impressions >= reach")
          .that(nc.impressions)
          .isAtLeast(nc.reach)
        assertWithMessage("cumulative reach >= non_cumulative reach in same bucket")
          .that(cum.reach)
          .isAtLeast(nc.reach)
      }
      // Union cumulative reach is monotonically non-decreasing across increasing
      // s.
      val cumulativeReachesByEndTime =
        weeklyResults
          .sortedBy { it.metadata.metricEndTime.seconds }
          .map { it.metricSet.reportingUnit.cumulative.reach }
      assertWithMessage("union cumulative reach monotonically non-decreasing")
        .that(cumulativeReachesByEndTime.zipWithNext { a, b -> b >= a }.all { it })
        .isTrue()

      // Union-only-request assertion: this request has NO component.* spec, so no result entry
      // should carry per-EDP components. Under the pre-fix bug, per-EDP subset enumeration
      // over-triggered and populated componentsList even when the caller did not request it.
      weeklyResults.forEach { r ->
        assertWithMessage("componentsList empty for union-only request")
          .that(r.metricSet.componentsList)
          .isEmpty()
      }

      // Sum-of-weekly-non_cumulative-reach >= whole-report union reach. VIDs that cross a
      // week boundary are counted in both weeks (>= relation); a purely non-overlapping VID
      // distribution would give equality.
      val nonCumulativeReachSum =
        weeklyResults.sumOf { it.metricSet.reportingUnit.nonCumulative.reach }
      assertWithMessage("sum of weekly non_cumulative reach >= whole-report reach")
        .that(nonCumulativeReachSum)
        .isAtLeast(EXPECTED_CROSS_PUBLISHER_REACH)

      assertExpectedProtocolUsed(getMeasurementsForBasicReport(completedBasicReport.name))
    }
}
