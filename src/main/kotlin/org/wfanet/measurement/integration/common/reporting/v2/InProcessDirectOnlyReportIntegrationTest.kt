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
import com.google.protobuf.timestamp
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import java.time.LocalDate
import kotlin.math.max
import kotlin.test.assertNotNull
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.getImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.getReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.invalidateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt

/**
 * Integration tests for single-EDP (direct protocol) report and metric operations.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests.
 */
abstract class InProcessDirectOnlyReportIntegrationTest(
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

  private fun assertExpectedProtocolUsed(measurements: List<Measurement>) {
    assertWithMessage("measurements").that(measurements).isNotEmpty()
    for (measurement in measurements) {
      val protocol = measurement.protocolConfig.protocolsList.single()
      assertWithMessage("protocol for ${measurement.name}")
        .that(protocol.protocolCase)
        .isEqualTo(expectedProtocol)
    }
  }

  private suspend fun getMeasurementsForMetric(metricName: String): List<Measurement> {
    return listMeasurements().filter {
      it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.metric == metricName
    }
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

  @Test
  fun `population metric for union has correct result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroups[0] to "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}",
        eventGroups[1] to "person.age_group <= ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
      )

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            metricId = "population"
            metric = metric {
              reportingSet = createdCompositeReportingSet.name
              timeInterval = interval {
                startTime = timestamp { seconds = 1615791600 }
                endTime = timestamp { seconds = 1615964400 }
              }
              metricSpec = metricSpec {
                populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
              }
              filters += "person.gender == ${Person.Gender.MALE_VALUE}"
              modelLine = inProcessCmmsComponents.modelLineResourceName
            }
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)

    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val expectedResult =
      MeasurementResults.computePopulation(
        inProcessCmmsComponents.getPopulationData().populationSpec,
        "(person.gender == ${Person.Gender.MALE_VALUE}) && (person.age_group <= ${Person.AgeGroup.YEARS_35_TO_54_VALUE})",
        TestEvent.getDescriptor(),
      )
    assertThat(retrievedMetric.result.populationCount.value).isEqualTo(expectedResult)
  }

  @Test
  fun `population metric for difference has correct result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroups[0] to "person.age_group <= ${Person.AgeGroup.YEARS_35_TO_54_VALUE}",
        eventGroups[1] to "person.age_group <= ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
      )

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            metricId = "population"
            metric = metric {
              reportingSet = createdCompositeReportingSet.name
              timeInterval = interval {
                startTime = timestamp { seconds = 1615791600 }
                endTime = timestamp { seconds = 1615964400 }
              }
              metricSpec = metricSpec {
                populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
              }
              filters += "person.gender == ${Person.Gender.MALE_VALUE}"
              modelLine = inProcessCmmsComponents.modelLineResourceName
            }
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)

    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val expectedResult =
      MeasurementResults.computePopulation(
        inProcessCmmsComponents.getPopulationData().populationSpec,
        "(person.gender == ${Person.Gender.MALE_VALUE}) && (person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE})",
        TestEvent.getDescriptor(),
      )
    assertThat(retrievedMetric.result.populationCount.value).isEqualTo(expectedResult)
  }

  @Test
  fun `population metric with no reporting set filters has correct result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> = listOf(eventGroups[0] to "")

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            metricId = "population"
            metric = metric {
              reportingSet = createdPrimitiveReportingSets[0].name
              timeInterval = interval {
                startTime = timestamp { seconds = 1615791600 }
                endTime = timestamp { seconds = 1615964400 }
              }
              metricSpec = metricSpec {
                populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
              }
              filters += "person.gender == ${Person.Gender.MALE_VALUE}"
              modelLine = inProcessCmmsComponents.modelLineResourceName
            }
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)

    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val expectedResult =
      MeasurementResults.computePopulation(
        inProcessCmmsComponents.getPopulationData().populationSpec,
        "(person.gender == ${Person.Gender.MALE_VALUE})",
        TestEvent.getDescriptor(),
      )
    assertThat(retrievedMetric.result.populationCount.value).isEqualTo(expectedResult)
  }

  @Test
  fun `reporting set is created and then retrieved`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "composite"
      primitive = ReportingSetKt.primitive { cmmsEventGroups.add(eventGroups[0].cmmsEventGroup) }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "def"
          }
        )

    val retrievedPrimitiveReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .getReportingSet(getReportingSetRequest { name = createdPrimitiveReportingSet.name })

    assertThat(createdPrimitiveReportingSet).isEqualTo(retrievedPrimitiveReportingSet)
  }

  @Test
  fun `report with unique reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.gender == ${Person.Gender.MALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[0].name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[1].name
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "unique reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    val equivalentFilter =
      "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
        "person.gender == ${Person.Gender.FEMALE_VALUE}"
    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      listOf(buildEventGroupSpec(eventGroup, equivalentFilter, EVENT_RANGE.toInterval()))
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with intersection reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.gender == ${Person.Gender.FEMALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.INTERSECTION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "intersection reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    val equivalentFilter =
      "(${createdPrimitiveReportingSets[0].filter}) && (${createdPrimitiveReportingSets[1].filter})"
    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      listOf(buildEventGroupSpec(eventGroup, equivalentFilter, EVENT_RANGE.toInterval()))
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with 2 reporting metric entries has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
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

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    for (resultAttribute in
      retrievedReport.metricCalculationResultsList.single().resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report across two time intervals has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val eventRangeWithNoReach =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 18)..LocalDate.of(2021, 3, 19))

    val createdMetricCalculationSpec =
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

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += EVENT_RANGE.toInterval()
        timeIntervals += eventRangeWithNoReach.toInterval()
      }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          buildEventGroupSpec(eventGroup, filter, resultAttribute.timeInterval)
        }
      val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with invalidated Metric has state FAILED`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.gender == ${Person.Gender.MALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[0].name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[1].name
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "unique reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    publicMetricsClient
      .withCallCredentials(credentials)
      .invalidateMetric(
        invalidateMetricRequest {
          name = retrievedReport.metricCalculationResultsList[0].resultAttributesList[0].metric
        }
      )

    val failedReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .getReport(getReportRequest { name = retrievedReport.name })
    assertThat(failedReport.state).isEqualTo(Report.State.FAILED)
  }

  @Test
  fun `report with reporting interval has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
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
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2021
            month = 3
            day = 15
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2021
            month = 3
            day = 17
          }
        }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          buildEventGroupSpec(eventGroup, filter, resultAttribute.timeInterval)
        }
      val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with reporting interval doesn't create metric beyond report_end`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
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
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2024
            month = 1
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2024
            month = 1
            day = 17
          }
        }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    assertThat(retrievedReport.metricCalculationResultsList[0].resultAttributesList).hasSize(2)
    val sortedResults =
      retrievedReport.metricCalculationResultsList[0].resultAttributesList.sortedBy {
        it.timeInterval.startTime.seconds
      }
    assertThat(sortedResults[0].timeInterval)
      .isEqualTo(
        interval {
          startTime = timestamp {
            seconds = 1704268800 // January 3, 2024 at 12:00 AM, America/Los_Angeles
          }
          endTime = timestamp {
            seconds = 1704873600 // January 10, 2024 at 12:00 AM, America/Los_Angeles
          }
        }
      )
    assertThat(sortedResults[1].timeInterval)
      .isEqualTo(
        interval {
          startTime = timestamp {
            seconds = 1704873600 // January 10, 2024 at 12:00 AM, America/Los_Angeles
          }
          endTime = timestamp {
            seconds = 1705478400 // January 17, 2024 at 12:00 AM, America/Los_Angeles
          }
        }
      )
  }

  @Test
  fun `report with group by has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> = listOf(eventGroup to "")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val grouping1Predicate1 = "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}"
    val grouping1Predicate2 = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
    val grouping2Predicate1 = "person.gender == ${Person.Gender.FEMALE_VALUE}"
    val grouping2Predicate2 = "person.gender == ${Person.Gender.MALE_VALUE}"

    val createdMetricCalculationSpec =
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
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += grouping1Predicate1
                  predicates += grouping1Predicate2
                }
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += grouping2Predicate1
                  predicates += grouping2Predicate2
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForReport(retrievedReport.name))

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          val allFilters =
            (resultAttribute.groupingPredicatesList + filter)
              .filter { it.isNotBlank() }
              .joinToString(" && ")
          buildEventGroupSpec(eventGroup, allFilters, EVENT_RANGE.toInterval())
        }
      val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `reach metric result has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `reach metric with single edp params result has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams = DP_PARAMS
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            singleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams = SINGLE_DATA_PROVIDER_DP_PARAMS
                vidSamplingInterval = SINGLE_DATA_PROVIDER_VID_SAMPLING_INTERVAL
              }
          }
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `reach-and-frequency metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            reachPrivacyParams = DP_PARAMS
            frequencyPrivacyParams = DP_PARAMS
            maximumFrequency = 5
          }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult =
      calculateExpectedReachAndFrequencyMeasurementResult(
        eventGroupSpecs,
        metric.metricSpec.reachAndFrequency.maximumFrequency,
      )

    val reachAndFrequencyResult = retrievedMetric.result.reachAndFrequency
    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = reachAndFrequencyResult.reach.value }
        frequency =
          MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(
              reachAndFrequencyResult.frequencyHistogram.binsList.associate {
                Pair(it.label.toLong(), it.binResult.value / reachAndFrequencyResult.reach.value)
              }
            )
          }
      }
    val reachTolerance =
      computeErrorMargin(reachAndFrequencyResult.reach.univariateStatistics.standardDeviation)
    val frequencyToleranceMap: Map<Long, Double> =
      reachAndFrequencyResult.frequencyHistogram.binsList.associate { bin ->
        bin.label.toLong() to computeErrorMargin(bin.relativeUnivariateStatistics.standardDeviation)
      }

    assertThat(actualResult).reachValue().isWithin(reachTolerance).of(expectedResult.reach.value)
    assertThat(actualResult)
      .frequencyDistribution()
      .isWithin(frequencyToleranceMap)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)
  }

  @Test
  fun `reach-and-frequency metric with no data has a result of 0`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            reachPrivacyParams = DP_PARAMS
            frequencyPrivacyParams = DP_PARAMS
            maximumFrequency = 5
          }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val reachAndFrequencyResult = retrievedMetric.result.reachAndFrequency
    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = reachAndFrequencyResult.reach.value }
        frequency =
          MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(
              reachAndFrequencyResult.frequencyHistogram.binsList.associate {
                Pair(
                  it.label.toLong(),
                  if (reachAndFrequencyResult.reach.value == 0L) {
                    0.0
                  } else {
                    it.binResult.value / reachAndFrequencyResult.reach.value
                  },
                )
              }
            )
          }
      }
    val reachTolerance =
      computeErrorMargin(reachAndFrequencyResult.reach.univariateStatistics.standardDeviation)
    val frequencyToleranceMap: Map<Long, Double> =
      reachAndFrequencyResult.frequencyHistogram.binsList.associate { bin ->
        bin.label.toLong() to computeErrorMargin(bin.relativeUnivariateStatistics.standardDeviation)
      }

    val mapWithAllZeroFrequency = buildMap {
      reachAndFrequencyResult.frequencyHistogram.binsList.forEach { bin ->
        put(bin.label.toLong(), 0.0)
      }
    }

    assertThat(actualResult).reachValue().isWithin(reachTolerance).of(0)
    if (actualResult.reach.value == 0L) {
      assertThat(actualResult)
        .frequencyDistribution()
        .isWithin(frequencyToleranceMap)
        .of(mapWithAllZeroFrequency)
    }
  }

  @Test
  fun `impression count metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        impressionCount = MetricSpecKt.impressionCountParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult =
      calculateExpectedImpressionMeasurementResult(
        eventGroupSpecs,
        createdMetric.metricSpec.impressionCount.maximumFrequencyPerUser,
      )

    val impressionResult = retrievedMetric.result.impressionCount
    val actualResult =
      MeasurementKt.result {
        impression = MeasurementKt.ResultKt.impression { value = impressionResult.value }
      }
    val tolerance = computeErrorMargin(impressionResult.univariateStatistics.standardDeviation)

    assertThat(actualResult)
      .impressionValue()
      .isWithin(tolerance)
      .of(expectedResult.impression.value)
  }

  @Test
  fun `impression count metric with no data has a result of 0`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
      metricSpec = metricSpec {
        impressionCount = MetricSpecKt.impressionCountParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val impressionResult = retrievedMetric.result.impressionCount
    val actualResult =
      MeasurementKt.result {
        impression = MeasurementKt.ResultKt.impression { value = impressionResult.value }
      }
    val tolerance = computeErrorMargin(impressionResult.univariateStatistics.standardDeviation)

    assertThat(actualResult).impressionValue().isWithin(tolerance).of(0)
  }

  @Test
  fun `watch duration metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        watchDuration = MetricSpecKt.watchDurationParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    // TODO(@tristanvuong2021): Calculate watch duration using synthetic spec.
  }

  @Test
  fun `reach metric with filter has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
      filters += "person.gender == ${Person.Gender.MALE_VALUE}"
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        val allFilters =
          (metric.filtersList + filter).filter { it.isNotBlank() }.joinToString(" && ")
        buildEventGroupSpec(eventGroup, allFilters, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `reach metric with no data has a result of 0`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
      metricSpec = metricSpec {
        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
      filters += "person.gender == ${Person.Gender.MALE_VALUE}"
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)
    assertExpectedProtocolUsed(getMeasurementsForMetric(retrievedMetric.name))

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(0)
  }

  @Test
  fun `retrieving data provider succeeds`() = runBlocking {
    val eventGroups = listEventGroups()
    val dataProviderName = eventGroups.first().cmmsDataProvider

    val dataProvider =
      publicDataProvidersClient
        .withCallCredentials(credentials)
        .getDataProvider(getDataProviderRequest { name = dataProviderName })

    assertThat(DataProviderCertificateKey.fromName(dataProvider.certificate)).isNotNull()
  }

  @Test
  fun `getBasicReport returns SUCCEEDED single edp basic report when basic report is completed`() =
    runBlocking {
      val eventGroups = listEventGroups().take(1)

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

      // Check that non cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
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

              componentValuesCheck && result.metricSet.populationSize > 0
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

              componentValuesCheck && result.metricSet.populationSize > 0
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

              componentValuesCheck && result.metricSet.populationSize > 0
            }
        )
      }

      assertExpectedProtocolUsed(getMeasurementsForBasicReport(retrievedCompletedBasicReport.name))
    }

  @Test
  fun `getBasicReport returns basic report when model line system specified`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()

    val dataProvider =
      publicDataProvidersClient
        .withCallCredentials(credentials)
        .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })

    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumerData.name)!!

    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "abc123")
    val campaignGroup =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = reportingSet {
              displayName = "campaign group"
              campaignGroup = campaignGroupKey.toName()
              primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
            }
            reportingSetId = campaignGroupKey.reportingSetId
          }
        )

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
        basicReportId = "basicreport123",
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroup.name
      campaignGroupDisplayName = campaignGroup.displayName
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2021
          month = 3
          day = 14
          hours = 17
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2021
          month = 3
          day = 15
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += dataProvider.name }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
            }
        }
      }
    }

    val createdBasicReport =
      publicBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(
          createBasicReportRequest {
            parent = measurementConsumerData.name
            basicReportId = basicReportKey.basicReportId
            this.basicReport = basicReport
          }
        )

    val retrievedPublicBasicReport =
      publicBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = basicReportKey.toName() })

    assertThat(retrievedPublicBasicReport).isEqualTo(createdBasicReport)
    assertThat(retrievedPublicBasicReport.modelLine).isEmpty()
    assertThat(retrievedPublicBasicReport.effectiveModelLine)
      .isEqualTo(inProcessCmmsComponents.modelLineResourceName)
  }

  @Test
  fun `getImpressionQualificationFilter retrieves ImpressionQualificationFilter`() = runBlocking {
    val impressionQualificationFilter =
      publicImpressionQualificationFiltersClient
        .withCallCredentials(credentials)
        .getImpressionQualificationFilter(
          getImpressionQualificationFilterRequest { name = "impressionQualificationFilters/ami" }
        )

    assertThat(impressionQualificationFilter)
      .isEqualTo(
        impressionQualificationFilter {
          name = "impressionQualificationFilters/ami"
          displayName = "ami"
          filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
          filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
          filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
        }
      )
  }

  @Test
  fun `listImpressionQualificationFilters with page size and page token retrieves ImpressionQualificationFilter`() =
    runBlocking {
      val internalPageToken = listImpressionQualificationFiltersPageToken {
        after =
          ListImpressionQualificationFiltersPageTokenKt.after {
            externalImpressionQualificationFilterId = "ami"
          }
      }

      val listImpressionQualificationFiltersResponse =
        publicImpressionQualificationFiltersClient
          .withCallCredentials(credentials)
          .listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest {
              pageSize = 1
              pageToken = internalPageToken.toByteString().base64UrlEncode()
            }
          )

      assertThat(listImpressionQualificationFiltersResponse)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += impressionQualificationFilter {
              name = "impressionQualificationFilters/mrc"
              displayName = "mrc"
              filterSpecs += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
                  }
                }
              }
            }
          }
        )
    }
}
