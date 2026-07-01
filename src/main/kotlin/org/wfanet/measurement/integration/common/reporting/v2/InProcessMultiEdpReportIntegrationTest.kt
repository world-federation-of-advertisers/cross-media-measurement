/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.DayOfWeek
import kotlin.math.max
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.ALL_EDP_DISPLAY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.MetricResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt

/**
 * Integration tests for multi-EDP (cross-publisher) report operations.
 *
 * This is abstract so that different cross-publisher protocols (HMSS, TrusTee) can each run the
 * same tests with their respective configurations.
 */
abstract class InProcessMultiEdpReportIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<
      (
        String, ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
      ) -> InProcessDuchy.DuchyDependencies
    >,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
  duchyNames: List<String> = ALL_DUCHY_NAMES,
  hmssEnabled: Boolean,
  trusTeeEnabled: Boolean,
) :
  InProcessLifeOfAReportIntegrationTest(
    kingdomDataServicesRule,
    duchyDependenciesRule,
    accessServicesFactory,
    reportingDataServicesProviderRule,
    duchyNames,
    hmssEnabled,
    trusTeeEnabled,
  ) {

  @Test
  fun `report with union reach across 2 edps has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val edpDisplayNames = ALL_EDP_DISPLAY_NAMES.take(2)
    val eventGroupsByEdpDisplayName: Map<String, List<EventGroup>> =
      listEventGroups().groupBy {
        inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(
          it.cmmsDataProvider
        )!!
      }
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroupsByEdpDisplayName.getValue(edpDisplayNames[0]).first() to
          "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroupsByEdpDisplayName.getValue(edpDisplayNames[1]).first() to
          "person.age_group == ${Person.AgeGroup.YEARS_55_PLUS_VALUE}",
      )
    val primitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)
    val compositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = reportingSet {
              displayName = "composite"
              composite =
                ReportingSetKt.composite {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = primitiveReportingSets[0].name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = primitiveReportingSets[1].name
                        }
                    }
                }
            }
            reportingSetId = "def"
          }
        )
    val metricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val reportName: String =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report {
              reportingMetricEntries +=
                ReportKt.reportingMetricEntry {
                  key = compositeReportingSet.name
                  value =
                    ReportKt.reportingMetricCalculationSpec {
                      metricCalculationSpecs += metricCalculationSpec.name
                    }
                }
              timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
            }
            reportId = "report"
          }
        )
        .name

    val report: Report = pollForCompletedReport(reportName)
    assertThat(report.state).isEqualTo(Report.State.SUCCEEDED)

    assertExpectedProtocolUsed(getMeasurementsForReport(reportName))

    val eventGroupSpecs: List<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult: Measurement.Result =
      calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult: MetricResult.ReachResult =
      report.metricCalculationResultsList.single().resultAttributesList.single().metricResult.reach
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      )
      .reachValue()
      .isWithin(tolerance)
      .of(expectedResult.reach.value)
  }

  @Test
  fun `getBasicReport returns SUCCEEDED multi edp basic report when basic report is completed`() =
    runBlocking {
      val eventGroups = getMultiEdpEventGroups()
      check(eventGroups.size > 1)

      val createBasicReportRequest =
        buildCreateBasicReportRequest(eventGroups).copy {
          basicReport =
            basicReport.copy {
              resultGroupSpecs.clear()
              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit {
                  components += eventGroups.map { it.cmmsDataProvider }
                }
                metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                dimensionSpec = dimensionSpec {
                  grouping =
                    DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "person.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                    }
                  }
                }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  populationSize = true
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      nonCumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                          kPlusReach = 5
                          percentKPlusReach = true
                          averageFrequency = true
                          impressions = true
                          grps = true
                        }
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                        }
                      stackedIncrementalReach = false
                    }
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      nonCumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                          kPlusReach = 5
                          percentKPlusReach = true
                          averageFrequency = true
                          impressions = true
                          grps = true
                        }
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                        }
                      nonCumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                      cumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                    }
                }
              }

              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit {
                  components += eventGroups.map { it.cmmsDataProvider }
                }
                metricFrequency = metricFrequencySpec { total = true }
                dimensionSpec = dimensionSpec {
                  grouping =
                    DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "person.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                    }
                  }
                }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  populationSize = true
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                          kPlusReach = 5
                          percentKPlusReach = true
                          averageFrequency = true
                          impressions = true
                          grps = true
                        }
                      stackedIncrementalReach = true
                    }
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                          averageFrequency = true
                          kPlusReach = 5
                          percentKPlusReach = true
                          impressions = true
                          grps = true
                        }
                    }
                }
              }
            }
        }

      val createdBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .createBasicReport(createBasicReportRequest)

      val retrievedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedBasicReport)
        .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(
          createBasicReportRequest.basicReport.copy {
            name = createdBasicReport.name
            state = BasicReport.State.RUNNING
            effectiveImpressionQualificationFilters +=
              retrievedBasicReport.impressionQualificationFiltersList
            effectiveModelLine = inProcessCmmsComponents.modelLineResourceName
            // Server-derived: equals the caller-supplied campaign_group in DataProvider mode.
            effectiveCampaignGroup = campaignGroup
            reportingInterval =
              reportingInterval.copy {
                effectiveReportStart =
                  createBasicReportRequest.basicReport.reportingInterval.reportStart
              }
          }
        )
      assertThat(retrievedBasicReport.createTime).isEqualTo(createdBasicReport.createTime)

      executeBasicReportsReportsJob(createdBasicReport.name)
      executeReportProcessorJob()

      val retrievedCompletedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedCompletedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

      assertExpectedProtocolUsed(getMeasurementsForBasicReport(createdBasicReport.name))

      // Check that non cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
              val reportingUnitMetricSet = result.metricSet.reportingUnit.nonCumulative
              val reportingUnitValuesCheck =
                reportingUnitMetricSet.reach > 0 &&
                  reportingUnitMetricSet.percentReach > 0 &&
                  reportingUnitMetricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it } &&
                  reportingUnitMetricSet.percentKPlusReachList
                    .zipWithNext { a, b -> b <= a }
                    .all { it } &&
                  reportingUnitMetricSet.averageFrequency > 0 &&
                  reportingUnitMetricSet.impressions > 0 &&
                  reportingUnitMetricSet.grps > 0

              var componentReach = 0L
              var componentPercentReach = 0.0f
              var componentAverageFrequency = 0.0f
              var componentKPlusReachExists = false
              var componentPercentKPlusReachExists = false
              var componentImpressions = 0L
              var componentGrps = 0.0f
              var componentUniqueReach = 0L

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.nonCumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentAverageFrequency =
                  max(componentAverageFrequency, metricSet.averageFrequency)
                componentKPlusReachExists =
                  componentKPlusReachExists ||
                    metricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentPercentKPlusReachExists =
                  componentPercentKPlusReachExists ||
                    metricSet.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentImpressions = max(componentImpressions, metricSet.impressions)
                componentGrps = max(componentGrps, metricSet.grps)
                componentUniqueReach =
                  max(componentUniqueReach, component.value.nonCumulativeUnique.reach)
              }

              val componentValuesCheck =
                componentReach > 0 &&
                  componentPercentReach > 0 &&
                  componentKPlusReachExists &&
                  componentPercentKPlusReachExists &&
                  componentAverageFrequency > 0 &&
                  componentImpressions > 0 &&
                  componentGrps > 0 &&
                  componentUniqueReach > 0

              reportingUnitValuesCheck &&
                componentValuesCheck &&
                result.metricSet.populationSize > 0
            }
        )
      }

      // Check that cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
              val reportingUnitCumulativeMetricSet = result.metricSet.reportingUnit.cumulative
              val reportingUnitValuesCheck =
                reportingUnitCumulativeMetricSet.reach > 0 &&
                  reportingUnitCumulativeMetricSet.percentReach > 0

              var componentReach = 0L
              var componentPercentReach = 0.0f
              var componentUniqueReach = 0L

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.cumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentUniqueReach =
                  max(componentUniqueReach, component.value.cumulativeUnique.reach)
              }

              val componentValuesCheck =
                componentReach > 0 && componentPercentReach > 0 && componentUniqueReach > 0

              reportingUnitValuesCheck &&
                componentValuesCheck &&
                result.metricSet.populationSize > 0
            }
        )
      }

      // Check that total results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
            }
            .firstOrNull { result ->
              val reportingUnitCumulativeMetricSet = result.metricSet.reportingUnit.cumulative
              val reportingUnitValuesCheck =
                reportingUnitCumulativeMetricSet.reach > 0 &&
                  reportingUnitCumulativeMetricSet.percentReach > 0 &&
                  reportingUnitCumulativeMetricSet.kPlusReachList
                    .zipWithNext { a, b -> b <= a }
                    .all { it } &&
                  reportingUnitCumulativeMetricSet.percentKPlusReachList
                    .zipWithNext { a, b -> b <= a }
                    .all { it } &&
                  reportingUnitCumulativeMetricSet.averageFrequency > 0 &&
                  reportingUnitCumulativeMetricSet.impressions > 0 &&
                  reportingUnitCumulativeMetricSet.grps > 0 &&
                  result.metricSet.reportingUnit.stackedIncrementalReachList
                    .zipWithNext { a, b -> b >= a }
                    .all { it }

              var componentReach = 0L
              var componentPercentReach = 0.0f
              var componentAverageFrequency = 0.0f
              var componentKPlusReachExists = false
              var componentPercentKPlusReachExists = false
              var componentImpressions = 0L
              var componentGrps = 0.0f

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.cumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentAverageFrequency =
                  max(componentAverageFrequency, metricSet.averageFrequency)
                componentKPlusReachExists =
                  componentKPlusReachExists ||
                    metricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentPercentKPlusReachExists =
                  componentPercentKPlusReachExists ||
                    metricSet.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentImpressions = max(componentImpressions, metricSet.impressions)
                componentGrps = max(componentGrps, metricSet.grps)
              }

              val componentValuesCheck =
                componentReach > 0 &&
                  componentPercentReach > 0 &&
                  componentKPlusReachExists &&
                  componentPercentKPlusReachExists &&
                  componentAverageFrequency > 0 &&
                  componentImpressions > 0 &&
                  componentGrps > 0

              reportingUnitValuesCheck &&
                componentValuesCheck &&
                result.metricSet.populationSize > 0
            }
        )
      }
    }

  @Test
  fun `getBasicReport returns SUCCEEDED custom group basic report when basic report is completed`() =
    runBlocking {
      val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
      val eventGroups = getMultiEdpEventGroups()
      check(eventGroups.size > 1)

      // One primitive custom group per EDP (empty campaign_group; disjoint DataProvider sets).
      val customGroups: List<ReportingSet> =
        eventGroups.mapIndexed { index, eventGroup ->
          publicReportingSetsClient
            .withCallCredentials(credentials)
            .createReportingSet(
              createReportingSetRequest {
                parent = measurementConsumerData.name
                reportingSet = reportingSet {
                  displayName = "custom group $index"
                  primitive =
                    ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
                }
                reportingSetId = "customgroup$index"
              }
            )
        }

      // Reuse the scaffolding (model line, reporting interval, IQF), but drop campaign_group so the
      // server synthesizes one, and bucket by the custom-group ReportingSets.
      val createBasicReportRequest =
        buildCreateBasicReportRequest(eventGroups).copy {
          basicReport =
            basicReport.copy {
              campaignGroup = ""
              campaignGroupDisplayName = ""
              resultGroupSpecs.clear()
              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit { components += customGroups.map { it.name } }
                metricFrequency = metricFrequencySpec { total = true }
                dimensionSpec = dimensionSpec {
                  grouping =
                    DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "person.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                    }
                  }
                }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  populationSize = true
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                        }
                      stackedIncrementalReach = true
                    }
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                        }
                      cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                    }
                }
              }
            }
        }

      val createdBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .createBasicReport(createBasicReportRequest)

      // campaign_group is empty; the synthesized one is surfaced only via effective_campaign_group.
      assertThat(createdBasicReport.campaignGroup).isEmpty()
      assertThat(createdBasicReport.effectiveCampaignGroup).isNotEmpty()

      executeBasicReportsReportsJob(createdBasicReport.name)
      executeReportProcessorJob()

      val retrievedCompletedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedCompletedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

      // Results are bucketed by the custom-group ReportingSet resource names.
      val componentKeys =
        retrievedCompletedBasicReport.resultGroupsList
          .flatMap { it.resultsList }
          .flatMap { it.metricSet.componentsList }
          .map { it.key }
          .toSet()
      assertThat(componentKeys).containsAtLeastElementsIn(customGroups.map { it.name })
    }

  private fun assertExpectedProtocolUsed(measurements: List<Measurement>) {
    assertWithMessage("measurements").that(measurements).isNotEmpty()
    var expectedProtocolFound = false
    for (measurement in measurements) {
      val protocol = measurement.protocolConfig.protocolsList.single()
      assertWithMessage("protocol for ${measurement.name}")
        .that(protocol.protocolCase)
        .isAnyOf(ProtocolConfig.Protocol.ProtocolCase.DIRECT, expectedProtocol)
      if (protocol.protocolCase == expectedProtocol) expectedProtocolFound = true
    }
    assertWithMessage("at least one $expectedProtocol measurement")
      .that(expectedProtocolFound)
      .isTrue()
  }

  private suspend fun getMeasurementsForReport(reportName: String): List<Measurement> {
    return listMeasurements().filter {
      it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.report == reportName
    }
  }

  private suspend fun getMeasurementsForBasicReport(basicReportName: String): List<Measurement> {
    val basicReportKey = BasicReportKey.fromName(basicReportName)!!
    val internalBasicReport =
      reportingServer.internalBasicReportsClient.getBasicReport(
        internalGetBasicReportRequest {
          cmmsMeasurementConsumerId = basicReportKey.cmmsMeasurementConsumerId
          externalBasicReportId = basicReportKey.basicReportId
        }
      )
    val reportName =
      ReportKey(internalBasicReport.cmmsMeasurementConsumerId, internalBasicReport.externalReportId)
        .toName()
    return getMeasurementsForReport(reportName)
  }
}
