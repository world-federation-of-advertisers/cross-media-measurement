/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import io.grpc.Status
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.DimensionSpec
import org.wfanet.measurement.internal.reporting.v2.DimensionSpecKt
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.univariateStatistics

@RunWith(JUnit4::class)
class BasicReportsReportsJobTest {
  private val basicReportsMock: BasicReportsCoroutineImplBase = mockService {
    onBlocking { listBasicReports(any()) }
      .thenReturn(listBasicReportsResponse { basicReports += INTERNAL_BASIC_REPORT })
  }
  private val reportsMock: ReportsCoroutineImplBase = mockService {
    onBlocking { getReport(any()) }.thenReturn(REPORT)
  }

  private val metricCalculationSpecsMock: MetricCalculationSpecsCoroutineImplBase = mockService {
    onBlocking { listMetricCalculationSpecs(any()) }
      .thenReturn(
        listMetricCalculationSpecsResponse {
          metricCalculationSpecs += NON_CUMULATIVE_METRIC_CALCULATION_SPEC
          metricCalculationSpecs += CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC
          metricCalculationSpecs += TOTAL_METRIC_CALCULATION_SPEC
          metricCalculationSpecs += POPULATION_METRIC_CALCULATION_SPEC
        }
      )
  }

  private val reportResultsMock: ReportResultsCoroutineImplBase = mockService {
    onBlocking { createReportResult(any()) } doAnswer
      {
        val request = it.arguments.first() as CreateReportResultRequest
        request.reportResult.copy {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          externalReportResultId = EXTERNAL_REPORT_RESULT_ID
        }
      }
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(basicReportsMock)
    addService(reportsMock)
    addService(metricCalculationSpecsMock)
    addService(reportResultsMock)
  }

  private lateinit var job: BasicReportsReportsJob

  @Before
  fun initJob() {
    job =
      BasicReportsReportsJob(
        MEASUREMENT_CONSUMER_CONFIGS,
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        ReportsCoroutineStub(grpcTestServerRule.channel),
        MetricCalculationSpecsCoroutineStub(grpcTestServerRule.channel),
        ReportResultsCoroutineStub(grpcTestServerRule.channel),
        TEST_EVENT_DESCRIPTOR,
      )
  }

  @Test
  fun `execute transforms and persists results when report for basic report is SUCCEEDED`(): Unit =
    runBlocking {
      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          reportingInterval =
            ReportKt.reportingInterval {
              reportStart = dateTime {
                year = 2025
                month = 1
                day = 6
                timeZone = timeZone { id = "America/Los_Angeles" }
              }
              reportEnd = date {
                year = 2025
                month = 1
                day = 2
              }
            }

          val reportingSetResult1Attribute =
            ReportKt.MetricCalculationResultKt.resultAttribute {
              groupingPredicates += "person.gender == 1"
              groupingPredicates += "person.age_group == 1"
              filter =
                "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1)"
            }

          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                reportingSetResult1Attribute.copy {
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }

              resultAttributes +=
                reportingSetResult1Attribute.copy {
                  metricSpec = metricSpec {
                    impressionCount = MetricSpecKt.impressionCountParams {}
                  }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    impressionCount = MetricResultKt.impressionCountResult { value = 1L }
                  }
                }

              resultAttributes +=
                reportingSetResult1Attribute.copy {
                  metricSpec = metricSpec {
                    impressionCount = MetricSpecKt.impressionCountParams {}
                  }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736755200 }
                    endTime = timestamp { seconds = 1737360000 }
                  }
                  metricResult = metricResult {
                    impressionCount = MetricResultKt.impressionCountResult { value = 1L }
                  }
                }

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  groupingPredicates += "person.gender == 1"
                  groupingPredicates += "person.age_group == 2"
                  filter =
                    "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 1)"
                  metricSpec = metricSpec {
                    impressionCount = MetricSpecKt.impressionCountParams {}
                  }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    impressionCount = MetricResultKt.impressionCountResult { value = 1L }
                  }
                }
            }

          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    POPULATION_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  groupingPredicates += "person.gender == 1"
                  groupingPredicates += "person.age_group == 1"
                  metricSpec = metricSpec {
                    populationCount = MetricSpecKt.populationCountParams {}
                  }
                  filter = "(person.age_group == 1)"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    populationCount = MetricResultKt.populationCountResult { value = 1000L }
                  }
                }

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  groupingPredicates += "person.gender == 1"
                  groupingPredicates += "person.age_group == 2"
                  metricSpec = metricSpec {
                    populationCount = MetricSpecKt.populationCountParams {}
                  }
                  filter = "(person.age_group == 1)"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    populationCount = MetricResultKt.populationCountResult { value = 2000L }
                  }
                }
            }

          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                reportingSetResult1Attribute.copy {
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }

              resultAttributes +=
                reportingSetResult1Attribute.copy {
                  metricSpec = metricSpec {
                    impressionCount = MetricSpecKt.impressionCountParams {}
                  }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    impressionCount = MetricResultKt.impressionCountResult { value = 1L }
                  }
                }

              resultAttributes +=
                reportingSetResult1Attribute.copy {
                  metricSpec = metricSpec {
                    populationCount = MetricSpecKt.populationCountParams {}
                  }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    populationCount = MetricResultKt.populationCountResult { value = 1000L }
                  }
                }
            }

          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    TOTAL_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  groupingPredicates += "person.gender == 1"
                  groupingPredicates += "person.age_group == 1"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  filter =
                    "((has(banner_ad.viewable) && banner_ad.viewable == true) || (has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0)) && (person.age_group == 2)"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }

          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  groupingPredicates += "person.gender == 1"
                  groupingPredicates += "person.age_group == 1"
                  metricSpec = metricSpec {
                    populationCount = MetricSpecKt.populationCountParams {}
                  }
                  filter = "(person.age_group == 2)"
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    populationCount = MetricResultKt.populationCountResult { value = 3000L }
                  }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportId = "1234"
        externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
        details = basicReportDetails {
          effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId = "IQF"
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
          effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
            filterSpecs += impressionQualificationFilterSpec {
              mediaType = MediaType.VIDEO
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "video_ad.viewed_fraction"
                  value = EventTemplateFieldKt.fieldValue { floatValue = 1.0f }
                }
              }
            }
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
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 1
              day = 6
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2025
              month = 1
              day = 2
            }
          }
          resultGroupSpecs += resultGroupSpec {
            dimensionSpec = dimensionSpec {
              grouping =
                DimensionSpecKt.grouping {
                  eventTemplateFields += "person.gender"
                  eventTemplateFields += "person.age_group"
                }
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                }
              }
            }
          }
          resultGroupSpecs += resultGroupSpec {
            dimensionSpec = dimensionSpec {
              grouping =
                DimensionSpecKt.grouping {
                  eventTemplateFields += "person.gender"
                  eventTemplateFields += "person.age_group"
                }
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
                }
              }
            }
          }
        }
      }

      whenever(basicReportsMock.listBasicReports(any()))
        .thenReturn(listBasicReportsResponse { basicReports += basicReport })

      job.execute()

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::listBasicReports)
        .isEqualTo(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = BasicReport.State.REPORT_CREATED
              }
            pageSize = BATCH_SIZE
          }
        )

      verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getReportRequest {
            name = ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, basicReport.externalReportId).toName()
          }
        )

      verify(basicReportsMock, times(0)).failBasicReport(any())

      verifyProtoArgument(reportResultsMock, ReportResultsCoroutineImplBase::createReportResult)
        .isEqualTo(
          createReportResultRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            reportResult = reportResult { reportStart = report.reportingInterval.reportStart }
          }
        )
      verifyProtoArgument(
          reportResultsMock,
          ReportResultsCoroutineImplBase::batchCreateReportingSetResults,
        )
        .ignoringRepeatedFieldOrderOfFieldDescriptors(REPORTING_SET_RESULT_UNORDERED_LISTS)
        .isEqualTo(
          batchCreateReportingSetResultsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = EXTERNAL_REPORT_RESULT_ID
            externalBasicReportId = basicReport.externalBasicReportId
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = EXTERNAL_REPORT_RESULT_ID
              reportingSetResult = reportingSetResult {
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
                    vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
                    custom = true
                    metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                    grouping =
                      ReportingSetResultKt.DimensionKt.grouping {
                        valueByPath["person.age_group"] =
                          EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                        valueByPath["person.gender"] =
                          EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }
                    eventFilters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  }
                populationSize = 1000
                reportingWindowResults +=
                  ReportingSetResultKt.reportingWindowEntry {
                    key =
                      ReportingSetResultKt.reportingWindow {
                        nonCumulativeStart = date {
                          year = 2025
                          month = 1
                          day = 6
                        }
                        end = date {
                          year = 2025
                          month = 1
                          day = 13
                        }
                      }

                    value =
                      ReportingSetResultKt.reportingWindowResult {
                        unprocessedReportResultValues =
                          ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                            cumulativeResults =
                              ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                                .noisyMetricSet {
                                  reach =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .reachResult { value = 1 }
                                  impressionCount =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .impressionCountResult { value = 1 }
                                }
                            nonCumulativeResults =
                              ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                                .noisyMetricSet {
                                  reach =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .reachResult { value = 1 }
                                  impressionCount =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .impressionCountResult { value = 1 }
                                }
                          }
                      }
                  }
                reportingWindowResults +=
                  ReportingSetResultKt.reportingWindowEntry {
                    key =
                      ReportingSetResultKt.reportingWindow {
                        nonCumulativeStart = date {
                          year = 2025
                          month = 1
                          day = 13
                        }
                        end = date {
                          year = 2025
                          month = 1
                          day = 20
                        }
                      }
                    value =
                      ReportingSetResultKt.reportingWindowResult {
                        unprocessedReportResultValues =
                          ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                            nonCumulativeResults =
                              ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                                .noisyMetricSet {
                                  impressionCount =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .impressionCountResult { value = 1 }
                                }
                          }
                      }
                  }
              }
            }
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = EXTERNAL_REPORT_RESULT_ID
              reportingSetResult = reportingSetResult {
                populationSize = 2000
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
                    vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
                    custom = true
                    metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                    grouping =
                      ReportingSetResultKt.DimensionKt.grouping {
                        valueByPath["person.age_group"] =
                          EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
                        valueByPath["person.gender"] =
                          EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }
                    eventFilters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  }
                reportingWindowResults +=
                  ReportingSetResultKt.reportingWindowEntry {
                    key =
                      ReportingSetResultKt.reportingWindow {
                        nonCumulativeStart = date {
                          year = 2025
                          month = 1
                          day = 6
                        }
                        end = date {
                          year = 2025
                          month = 1
                          day = 13
                        }
                      }
                    value =
                      ReportingSetResultKt.reportingWindowResult {
                        unprocessedReportResultValues =
                          ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                            nonCumulativeResults =
                              ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                                .noisyMetricSet {
                                  impressionCount =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .impressionCountResult { value = 1 }
                                }
                          }
                      }
                  }
              }
            }
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = EXTERNAL_REPORT_RESULT_ID
              reportingSetResult = reportingSetResult {
                populationSize = 3000
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = COMPOSITE_REPORTING_SET.externalReportingSetId
                    vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
                    custom = true
                    metricFrequencySpec = metricFrequencySpec { total = true }
                    grouping =
                      ReportingSetResultKt.DimensionKt.grouping {
                        valueByPath["person.age_group"] =
                          EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                        valueByPath["person.gender"] =
                          EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }
                    eventFilters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_35_TO_54" }
                      }
                    }
                  }
                reportingWindowResults +=
                  ReportingSetResultKt.reportingWindowEntry {
                    key =
                      ReportingSetResultKt.reportingWindow {
                        end = date {
                          year = 2025
                          month = 1
                          day = 13
                        }
                      }
                    value =
                      ReportingSetResultKt.reportingWindowResult {
                        unprocessedReportResultValues =
                          ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                            cumulativeResults =
                              ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                                .noisyMetricSet {
                                  reach =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .reachResult { value = 1 }
                                }
                          }
                      }
                  }
              }
            }
          }
        )
    }

  @Test
  fun `execute transforms and persists results when effective IQFs doesn't match original`(): Unit =
    runBlocking {
      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          reportingInterval =
            ReportKt.reportingInterval {
              reportStart = dateTime {
                year = 2025
                month = 1
                day = 6
                timeZone = timeZone { id = "America/Los_Angeles" }
              }
              reportEnd = date {
                year = 2025
                month = 1
                day = 2
              }
            }

          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    POPULATION_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  metricSpec = metricSpec {
                    populationCount = MetricSpecKt.populationCountParams {}
                  }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult {
                    populationCount = MetricResultKt.populationCountResult { value = 1000L }
                  }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportId = "1234"
        externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
        details = basicReportDetails {
          effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId = "IQF"
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
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 1
              day = 6
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2025
              month = 1
              day = 2
            }
          }
          resultGroupSpecs += resultGroupSpec { dimensionSpec = DimensionSpec.getDefaultInstance() }
        }
      }

      whenever(basicReportsMock.listBasicReports(any()))
        .thenReturn(listBasicReportsResponse { basicReports += basicReport })

      job.execute()

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::listBasicReports)
        .isEqualTo(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = BasicReport.State.REPORT_CREATED
              }
            pageSize = BATCH_SIZE
          }
        )

      verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getReportRequest {
            name = ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, basicReport.externalReportId).toName()
          }
        )

      verify(basicReportsMock, times(0)).failBasicReport(any())

      verifyProtoArgument(reportResultsMock, ReportResultsCoroutineImplBase::createReportResult)
        .isEqualTo(
          createReportResultRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            reportResult = reportResult { reportStart = report.reportingInterval.reportStart }
          }
        )
      verifyProtoArgument(
          reportResultsMock,
          ReportResultsCoroutineImplBase::batchCreateReportingSetResults,
        )
        .ignoringRepeatedFieldOrderOfFieldDescriptors(REPORTING_SET_RESULT_UNORDERED_LISTS)
        .isEqualTo(
          batchCreateReportingSetResultsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = EXTERNAL_REPORT_RESULT_ID
            externalBasicReportId = basicReport.externalBasicReportId
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = EXTERNAL_REPORT_RESULT_ID
              reportingSetResult = reportingSetResult {
                dimension =
                  ReportingSetResultKt.dimension {
                    grouping = ReportingSetResult.Dimension.Grouping.getDefaultInstance()
                    externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
                    vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
                    externalImpressionQualificationFilterId = "IQF"
                    metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  }
                populationSize = 1000
                reportingWindowResults +=
                  ReportingSetResultKt.reportingWindowEntry {
                    key =
                      ReportingSetResultKt.reportingWindow {
                        nonCumulativeStart = date {
                          year = 2025
                          month = 1
                          day = 6
                        }
                        end = date {
                          year = 2025
                          month = 1
                          day = 13
                        }
                      }

                    value =
                      ReportingSetResultKt.reportingWindowResult {
                        unprocessedReportResultValues =
                          ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                            nonCumulativeResults =
                              ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                                .noisyMetricSet {
                                  reach =
                                    ReportingSetResultKt.ReportingWindowResultKt
                                      .NoisyReportResultValuesKt
                                      .NoisyMetricSetKt
                                      .reachResult { value = 1 }
                                }
                          }
                      }
                  }
              }
            }
          }
        )
    }

  @Test
  fun `execute creates 2 reporting set results for 2 different reporting sets`(): Unit =
    runBlocking {
      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          metricCalculationResults.clear()
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    PRIMITIVE_REPORTING_SET_2.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      job.execute()

      val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
      verifyBlocking(reportResultsMock, times(1)) {
        batchCreateReportingSetResults(requestCaptor.capture())
      }
      assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(2)
    }

  @Test
  fun `execute sets custom when results for custom IQF`(): Unit = runBlocking {
    val basicReport =
      INTERNAL_BASIC_REPORT.copy {
        details =
          INTERNAL_BASIC_REPORT.details.copy {
            effectiveImpressionQualificationFilters.clear()
            effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
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
      }

    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(listBasicReportsResponse { basicReports += basicReport })

    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
    assertThat(requestCaptor.firstValue.requestsList.single().reportingSetResult.dimension.custom)
      .isTrue()
  }

  @Test
  fun `execute sets custom when results for non-custom IQF`(): Unit = runBlocking {
    val basicReport =
      INTERNAL_BASIC_REPORT.copy {
        details =
          INTERNAL_BASIC_REPORT.details.copy {
            effectiveImpressionQualificationFilters.clear()
            effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
              externalImpressionQualificationFilterId = "IQF"
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
      }

    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(listBasicReportsResponse { basicReports += basicReport })

    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
    assertThat(
        requestCaptor.firstValue.requestsList
          .single()
          .reportingSetResult
          .dimension
          .externalImpressionQualificationFilterId
      )
      .isEqualTo("IQF")
  }

  @Test
  fun `execute creates 2 reporting set results for 2 different IQFs`(): Unit = runBlocking {
    val basicReport =
      INTERNAL_BASIC_REPORT.copy {
        details =
          INTERNAL_BASIC_REPORT.details.copy {
            effectiveImpressionQualificationFilters.clear()
            effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
              externalImpressionQualificationFilterId = "IQF"
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
            effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
              filterSpecs += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "video_ad.viewed_fraction"
                    value = EventTemplateFieldKt.fieldValue { floatValue = 1.0f }
                  }
                }
              }
            }
          }
      }

    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(listBasicReportsResponse { basicReports += basicReport })

    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(video_ad.viewed_fraction) && video_ad.viewed_fraction == 1.0))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(2)
  }

  @Test
  fun `execute sets weekly metric frequency type when results for weekly spec`(): Unit =
    runBlocking {
      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          metricCalculationResults.clear()
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      job.execute()

      val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
      verifyBlocking(reportResultsMock, times(1)) {
        batchCreateReportingSetResults(requestCaptor.capture())
      }
      assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
      assertThat(
          requestCaptor.firstValue.requestsList
            .single()
            .reportingSetResult
            .dimension
            .metricFrequencySpec
        )
        .isEqualTo(metricFrequencySpec { weekly = DayOfWeek.MONDAY })
    }

  @Test
  fun `execute sets total metric frequency type when results for total spec`(): Unit = runBlocking {
    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  TOTAL_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
    assertThat(
        requestCaptor.firstValue.requestsList
          .single()
          .reportingSetResult
          .dimension
          .metricFrequencySpec
      )
      .isEqualTo(metricFrequencySpec { total = true })
  }

  @Test
  fun `execute sets groupings when results has several groupings`(): Unit = runBlocking {
    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                groupingPredicates += "person.gender == 1"
                groupingPredicates += "person.age_group == 1"
                groupingPredicates += "person.social_grade_group == 1"
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
    assertThat(
        requestCaptor.firstValue.requestsList
          .single()
          .reportingSetResult
          .dimension
          .grouping
          .valueByPathMap
      )
      .containsExactly(
        "person.age_group",
        EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" },
        "person.gender",
        EventTemplateFieldKt.fieldValue { enumValue = "MALE" },
        "person.social_grade_group",
        EventTemplateFieldKt.fieldValue { enumValue = "A_B_C1" },
      )
  }

  @Test
  fun `execute doesn't set groupings when no groupings`(): Unit = runBlocking {
    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
    assertThat(
        requestCaptor.firstValue.requestsList
          .single()
          .reportingSetResult
          .dimension
          .grouping
          .valueByPathMap
      )
      .isEmpty()
  }

  @Test
  fun `execute sets event filters when results has filter with dimension spec component`(): Unit =
    runBlocking {
      val basicReport =
        INTERNAL_BASIC_REPORT.copy {
          details =
            INTERNAL_BASIC_REPORT.details.copy {
              effectiveImpressionQualificationFilters.clear()
              effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
                filterSpecs += impressionQualificationFilterSpec {
                  mediaType = MediaType.VIDEO
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                  }
                }
              }
              resultGroupSpecs.clear()
              resultGroupSpecs += resultGroupSpec {
                dimensionSpec = dimensionSpec {
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "person.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                    }
                  }
                }
              }
            }
        }

      whenever(basicReportsMock.listBasicReports(any()))
        .thenReturn(listBasicReportsResponse { basicReports += basicReport })

      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          metricCalculationResults.clear()
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter =
                    "((has(banner_ad.viewable) && banner_ad.viewable == true)) && (person.age_group == 1)"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      job.execute()

      val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
      verifyBlocking(reportResultsMock, times(1)) {
        batchCreateReportingSetResults(requestCaptor.capture())
      }
      assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
      assertThat(
          requestCaptor.firstValue.requestsList
            .single()
            .reportingSetResult
            .dimension
            .eventFiltersList
        )
        .ignoringRepeatedFieldOrderOfFieldDescriptors(REPORTING_SET_RESULT_UNORDERED_LISTS)
        .containsExactly(
          eventFilter {
            terms += eventTemplateField {
              path = "person.age_group"
              value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
            }
          }
        )
    }

  @Test
  fun `execute doesn't set event filters when results has no dimension spec component`(): Unit =
    runBlocking {
      val basicReport =
        INTERNAL_BASIC_REPORT.copy {
          details =
            INTERNAL_BASIC_REPORT.details.copy {
              effectiveImpressionQualificationFilters.clear()
              effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
                filterSpecs += impressionQualificationFilterSpec {
                  mediaType = MediaType.VIDEO
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                  }
                }
              }
              resultGroupSpecs.clear()
              resultGroupSpecs += resultGroupSpec { dimensionSpec = dimensionSpec {} }
            }
        }

      whenever(basicReportsMock.listBasicReports(any()))
        .thenReturn(listBasicReportsResponse { basicReports += basicReport })

      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          metricCalculationResults.clear()
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      job.execute()

      val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
      verifyBlocking(reportResultsMock, times(1)) {
        batchCreateReportingSetResults(requestCaptor.capture())
      }
      assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
      assertThat(
          requestCaptor.firstValue.requestsList
            .single()
            .reportingSetResult
            .dimension
            .eventFiltersList
        )
        .isEmpty()
    }

  @Test
  fun `execute processes all supported metric types correctly`(): Unit = runBlocking {
    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  TOTAL_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult {
                  impressionCount =
                    MetricResultKt.impressionCountResult {
                      value = 5L
                      univariateStatistics = univariateStatistics { standardDeviation = 1.0 }
                    }
                }
              }
          }

        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult {
                  reach =
                    MetricResultKt.reachResult {
                      value = 1L
                      univariateStatistics = univariateStatistics { standardDeviation = 1.0 }
                    }
                }
              }
          }

        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { impressionCount = MetricSpecKt.impressionCountParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult {
                  impressionCount =
                    MetricResultKt.impressionCountResult {
                      value = 1L
                      univariateStatistics = univariateStatistics { standardDeviation = 1.0 }
                    }
                }
              }
            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec {
                  reachAndFrequency = MetricSpecKt.reachAndFrequencyParams {}
                }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult {
                  reachAndFrequency =
                    MetricResultKt.reachAndFrequencyResult {
                      reach =
                        MetricResultKt.reachResult {
                          value = 1L
                          univariateStatistics = univariateStatistics { standardDeviation = 1.0 }
                        }
                      frequencyHistogram =
                        MetricResultKt.histogramResult {
                          bins +=
                            MetricResultKt.HistogramResultKt.bin {
                              label = "1"
                              binResult = MetricResultKt.HistogramResultKt.binResult { value = 2.0 }
                              resultUnivariateStatistics = univariateStatistics {
                                standardDeviation = 1.0
                              }
                            }
                          bins +=
                            MetricResultKt.HistogramResultKt.bin {
                              label = "2"
                              binResult = MetricResultKt.HistogramResultKt.binResult { value = 4.0 }
                              resultUnivariateStatistics = univariateStatistics {
                                standardDeviation = 1.0
                              }
                            }
                          bins +=
                            MetricResultKt.HistogramResultKt.bin {
                              label = "3"
                              binResult = MetricResultKt.HistogramResultKt.binResult { value = 6.0 }
                              resultUnivariateStatistics = univariateStatistics {
                                standardDeviation = 1.0
                              }
                            }
                        }
                    }
                }
              }
          }

        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  POPULATION_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  PRIMITIVE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                metricSpec = metricSpec { populationCount = MetricSpecKt.populationCountParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult {
                  populationCount = MetricResultKt.populationCountResult { value = 1000L }
                }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(2)
    val totalReportingSetResult =
      requestCaptor.firstValue.requestsList
        .first {
          it.reportingSetResult.dimension.metricFrequencySpec.selectorCase ==
            MetricFrequencySpec.SelectorCase.TOTAL
        }
        .reportingSetResult

    assertThat(totalReportingSetResult.populationSize).isEqualTo(1000)
    assertThat(totalReportingSetResult.reportingWindowResultsList).hasSize(1)
    assertThat(totalReportingSetResult.reportingWindowResultsList[0])
      .isEqualTo(
        ReportingSetResultKt.reportingWindowEntry {
          key =
            ReportingSetResultKt.reportingWindow {
              end = date {
                year = 2025
                month = 1
                day = 13
              }
            }
          value =
            ReportingSetResultKt.reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  cumulativeResults =
                    ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                      .noisyMetricSet {
                        impressionCount =
                          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                            .NoisyMetricSetKt
                            .impressionCountResult {
                              value = 5
                              univariateStatistics =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .univariateStatistics { standardDeviation = 1.0 }
                            }
                      }
                }
            }
        }
      )

    val weeklyReportingSetResult =
      requestCaptor.firstValue.requestsList
        .first {
          it.reportingSetResult.dimension.metricFrequencySpec.selectorCase ==
            MetricFrequencySpec.SelectorCase.WEEKLY
        }
        .reportingSetResult
    assertThat(weeklyReportingSetResult.populationSize).isEqualTo(1000)
    assertThat(weeklyReportingSetResult.reportingWindowResultsList).hasSize(1)
    assertThat(weeklyReportingSetResult.reportingWindowResultsList[0])
      .isEqualTo(
        ReportingSetResultKt.reportingWindowEntry {
          key =
            ReportingSetResultKt.reportingWindow {
              nonCumulativeStart = date {
                year = 2025
                month = 1
                day = 6
              }
              end = date {
                year = 2025
                month = 1
                day = 13
              }
            }
          value =
            ReportingSetResultKt.reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  cumulativeResults =
                    ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                      .noisyMetricSet {
                        reach =
                          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                            .NoisyMetricSetKt
                            .reachResult {
                              value = 1
                              univariateStatistics =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .univariateStatistics { standardDeviation = 1.0 }
                            }
                      }
                  nonCumulativeResults =
                    ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                      .noisyMetricSet {
                        reach =
                          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                            .NoisyMetricSetKt
                            .reachResult {
                              value = 1
                              univariateStatistics =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .univariateStatistics { standardDeviation = 1.0 }
                            }
                        frequencyHistogram =
                          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                            .NoisyMetricSetKt
                            .histogramResult {
                              binResults[1] =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .HistogramResultKt
                                  .binResult {
                                    value = 2
                                    univariateStatistics =
                                      ReportingSetResultKt.ReportingWindowResultKt
                                        .NoisyReportResultValuesKt
                                        .NoisyMetricSetKt
                                        .univariateStatistics { standardDeviation = 1.0 }
                                  }
                              binResults[2] =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .HistogramResultKt
                                  .binResult {
                                    value = 4
                                    univariateStatistics =
                                      ReportingSetResultKt.ReportingWindowResultKt
                                        .NoisyReportResultValuesKt
                                        .NoisyMetricSetKt
                                        .univariateStatistics { standardDeviation = 1.0 }
                                  }
                              binResults[3] =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .HistogramResultKt
                                  .binResult {
                                    value = 6
                                    univariateStatistics =
                                      ReportingSetResultKt.ReportingWindowResultKt
                                        .NoisyReportResultValuesKt
                                        .NoisyMetricSetKt
                                        .univariateStatistics { standardDeviation = 1.0 }
                                  }
                            }
                        impressionCount =
                          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                            .NoisyMetricSetKt
                            .impressionCountResult {
                              value = 1
                              univariateStatistics =
                                ReportingSetResultKt.ReportingWindowResultKt
                                  .NoisyReportResultValuesKt
                                  .NoisyMetricSetKt
                                  .univariateStatistics { standardDeviation = 1.0 }
                            }
                      }
                }
            }
        }
      )
  }

  @Test
  fun `execute sets both cumulative and noncumulative for report result value`(): Unit =
    runBlocking {
      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          metricCalculationResults.clear()
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    NON_CUMULATIVE_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      job.execute()

      val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
      verifyBlocking(reportResultsMock, times(1)) {
        batchCreateReportingSetResults(requestCaptor.capture())
      }
      assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
      val reportingSetResultEntry: ReportingSetResult =
        requestCaptor.firstValue.requestsList.single().reportingSetResult
      assertThat(reportingSetResultEntry.reportingWindowResultsList).hasSize(1)
      assertThat(reportingSetResultEntry.reportingWindowResultsList[0])
        .isEqualTo(
          ReportingSetResultKt.reportingWindowEntry {
            key =
              ReportingSetResultKt.reportingWindow {
                nonCumulativeStart = date {
                  year = 2025
                  month = 1
                  day = 6
                }
                end = date {
                  year = 2025
                  month = 1
                  day = 13
                }
              }
            value =
              ReportingSetResultKt.reportingWindowResult {
                unprocessedReportResultValues =
                  ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                    cumulativeResults =
                      ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                        .noisyMetricSet {
                          reach =
                            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                              .NoisyMetricSetKt
                              .reachResult { value = 1 }
                        }
                    nonCumulativeResults =
                      ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                        .noisyMetricSet {
                          reach =
                            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                              .NoisyMetricSetKt
                              .reachResult { value = 1 }
                        }
                  }
              }
          }
        )
    }

  @Test
  fun `execute sets only end date when cumulative only`(): Unit = runBlocking {
    val report =
      REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults.clear()
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec =
              MetricCalculationSpecKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                )
                .toName()

            reportingSet =
              ReportingSetKey(
                  CMMS_MEASUREMENT_CONSUMER_ID,
                  COMPOSITE_REPORTING_SET.externalReportingSetId,
                )
                .toName()

            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                timeInterval = interval {
                  startTime = timestamp { seconds = 1736150400 }
                  endTime = timestamp { seconds = 1736755200 }
                }
                metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
              }
          }
      }

    whenever(reportsMock.getReport(any())).thenReturn(report)

    job.execute()

    val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
    verifyBlocking(reportResultsMock, times(1)) {
      batchCreateReportingSetResults(requestCaptor.capture())
    }
    assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
    val reportingSetResultEntry = requestCaptor.firstValue.requestsList.single().reportingSetResult
    assertThat(reportingSetResultEntry.reportingWindowResultsList).hasSize(1)
    assertThat(reportingSetResultEntry.reportingWindowResultsList[0])
      .isEqualTo(
        ReportingSetResultKt.reportingWindowEntry {
          key =
            ReportingSetResultKt.reportingWindow {
              end = date {
                year = 2025
                month = 1
                day = 13
              }
            }
          value =
            ReportingSetResultKt.reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  cumulativeResults =
                    ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                      .noisyMetricSet {
                        reach =
                          ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                            .NoisyMetricSetKt
                            .reachResult { value = 1 }
                      }
                }
            }
        }
      )
  }

  @Test
  fun `execute creates diff reporting window results for diff time intervals`(): Unit =
    runBlocking {
      val report =
        REPORT.copy {
          state = Report.State.SUCCEEDED
          metricCalculationResults.clear()
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec =
                MetricCalculationSpecKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId,
                  )
                  .toName()

              reportingSet =
                ReportingSetKey(
                    CMMS_MEASUREMENT_CONSUMER_ID,
                    COMPOSITE_REPORTING_SET.externalReportingSetId,
                  )
                  .toName()

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736150400 }
                    endTime = timestamp { seconds = 1736755200 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }

              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                  metricSpec = metricSpec { reach = MetricSpecKt.reachParams {} }
                  timeInterval = interval {
                    startTime = timestamp { seconds = 1736755200 }
                    endTime = timestamp { seconds = 1737360000 }
                  }
                  metricResult = metricResult { reach = MetricResultKt.reachResult { value = 1L } }
                }
            }
        }

      whenever(reportsMock.getReport(any())).thenReturn(report)

      job.execute()

      val requestCaptor = argumentCaptor<BatchCreateReportingSetResultsRequest>()
      verifyBlocking(reportResultsMock, times(1)) {
        batchCreateReportingSetResults(requestCaptor.capture())
      }
      assertThat(requestCaptor.firstValue.requestsCount).isEqualTo(1)
      assertThat(
          requestCaptor.firstValue.requestsList
            .single()
            .reportingSetResult
            .reportingWindowResultsList
        )
        .containsExactly(
          ReportingSetResultKt.reportingWindowEntry {
            key =
              ReportingSetResultKt.reportingWindow {
                end = date {
                  year = 2025
                  month = 1
                  day = 13
                }
              }
            value =
              ReportingSetResultKt.reportingWindowResult {
                unprocessedReportResultValues =
                  ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                    cumulativeResults =
                      ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                        .noisyMetricSet {
                          reach =
                            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                              .NoisyMetricSetKt
                              .reachResult { value = 1 }
                        }
                  }
              }
          },
          ReportingSetResultKt.reportingWindowEntry {
            key =
              ReportingSetResultKt.reportingWindow {
                end = date {
                  year = 2025
                  month = 1
                  day = 20
                }
              }
            value =
              ReportingSetResultKt.reportingWindowResult {
                unprocessedReportResultValues =
                  ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                    cumulativeResults =
                      ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                        .noisyMetricSet {
                          reach =
                            ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
                              .NoisyMetricSetKt
                              .reachResult { value = 1 }
                        }
                  }
              }
          },
        )
    }

  @Test
  fun `execute only gets report when report for basic report is RUNNING`(): Unit = runBlocking {
    whenever(reportsMock.getReport(any())).thenReturn(REPORT.copy { state = Report.State.RUNNING })
    job.execute()

    verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::listBasicReports)
      .isEqualTo(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              state = BasicReport.State.REPORT_CREATED
            }
          pageSize = BATCH_SIZE
        }
      )

    verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getReportRequest {
          name =
            ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, INTERNAL_BASIC_REPORT.externalReportId).toName()
        }
      )

    verify(basicReportsMock, times(0)).failBasicReport(any())
  }

  @Test
  fun `execute sets basic report to FAILED when report for basic report is FAILED`(): Unit =
    runBlocking {
      whenever(reportsMock.getReport(any())).thenReturn(REPORT.copy { state = Report.State.FAILED })

      job.execute()

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::listBasicReports)
        .isEqualTo(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = BasicReport.State.REPORT_CREATED
              }
            pageSize = BATCH_SIZE
          }
        )

      verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getReportRequest {
            name =
              ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, INTERNAL_BASIC_REPORT.externalReportId)
                .toName()
          }
        )

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::failBasicReport)
        .isEqualTo(
          failBasicReportRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalBasicReportId = INTERNAL_BASIC_REPORT.externalBasicReportId
          }
        )
    }

  @Test
  fun `execute gets report for basic report when attempt fails for a previous basic report`():
    Unit = runBlocking {
    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(
        listBasicReportsResponse {
          basicReports += INTERNAL_BASIC_REPORT
          basicReports += INTERNAL_BASIC_REPORT
        }
      )
    whenever(reportsMock.getReport(any()))
      .thenThrow(Status.UNKNOWN.asRuntimeException())
      .thenReturn(REPORT)

    job.execute()

    verify(reportsMock, times(2)).getReport(any())
  }

  @Test
  fun `execute processes basic reports for 2 different MCs`(): Unit = runBlocking {
    val measurementConsumerConfigs = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc-user"
      }
      configs[MEASUREMENT_CONSUMER_NAME_2] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc2-user"
      }
    }

    job =
      BasicReportsReportsJob(
        measurementConsumerConfigs,
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        ReportsCoroutineStub(grpcTestServerRule.channel),
        MetricCalculationSpecsCoroutineStub(grpcTestServerRule.channel),
        ReportResultsCoroutineStub(grpcTestServerRule.channel),
        TEST_EVENT_DESCRIPTOR,
      )

    job.execute()

    val listBasicReportsCaptor: KArgumentCaptor<ListBasicReportsRequest> = argumentCaptor()
    verifyBlocking(basicReportsMock, times(2)) {
      listBasicReports(listBasicReportsCaptor.capture())
    }
    assertThat(listBasicReportsCaptor.allValues)
      .containsExactly(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              state = BasicReport.State.REPORT_CREATED
            }
          pageSize = BATCH_SIZE
        },
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID_2
              state = BasicReport.State.REPORT_CREATED
            }
          pageSize = BATCH_SIZE
        },
      )
  }

  companion object {
    private val TEST_EVENT_DESCRIPTOR = EventMessageDescriptor(TestEvent.getDescriptor())

    /**
     * Descriptors of repeated fields in [ReportingSetResult] that are treated as unordered lists.
     */
    private val REPORTING_SET_RESULT_UNORDERED_LISTS = buildList {
      val reportingSetResultDescriptor = ReportingSetResult.getDescriptor()
      add(
        reportingSetResultDescriptor.findFieldByNumber(
          ReportingSetResult.REPORTING_WINDOW_RESULTS_FIELD_NUMBER
        )
      )
    }

    private const val BATCH_SIZE = 10

    private const val EXTERNAL_REPORT_RESULT_ID = 1234L
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID}"
    private const val CMMS_MEASUREMENT_CONSUMER_ID_2 = "A1234"
    private const val MEASUREMENT_CONSUMER_NAME_2 =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID_2}"

    private const val COMPOSITE_REPORTING_SET_ID = "c124"
    private const val COMPOSITE_REPORTING_SET_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/reportingSets/${COMPOSITE_REPORTING_SET_ID}"

    private const val METRIC_CALCULATION_SPEC_ID = "m123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/metricCalculationSpecs/${METRIC_CALCULATION_SPEC_ID}"

    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc-user"
      }
    }

    private val CAMPAIGN_GROUP = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "1234"
      externalCampaignGroupId = "1234"
      displayName = "displayName"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1234"
            }

          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1235"
            }
        }
    }

    private val INTERNAL_BASIC_REPORT = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportId = "1234"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
      details = basicReportDetails {
        effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
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
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 1
            day = 6
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2025
            month = 1
            day = 2
          }
        }
        resultGroupSpecs += resultGroupSpec { dimensionSpec = dimensionSpec {} }
      }
    }

    private val REPORT = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = COMPOSITE_REPORTING_SET_NAME
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += METRIC_CALCULATION_SPEC_NAME
            }
        }
      state = Report.State.SUCCEEDED
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 1
            day = 6
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2025
            month = 1
            day = 2
          }
        }
      createTime = timestamp { seconds = 50 }
    }

    private val PRIMITIVE_REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "p1234"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1234"
            }
        }
    }

    private val PRIMITIVE_REPORTING_SET_2 = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "p12342"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1234"
              cmmsEventGroupId = "1235"
            }
        }
    }

    private val COMPOSITE_REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "c1234"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId

      composite =
        ReportingSetKt.setExpression {
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = PRIMITIVE_REPORTING_SET.externalReportingSetId
            }
          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = PRIMITIVE_REPORTING_SET_2.externalReportingSetId
            }
        }
    }

    private val NON_CUMULATIVE_METRIC_CALCULATION_SPEC = metricCalculationSpec {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalMetricCalculationSpecId = "noncumulative"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
      details =
        MetricCalculationSpecKt.details {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              weekly =
                MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                  dayOfWeek = DayOfWeek.MONDAY
                }
            }
          trailingWindow =
            MetricCalculationSpecKt.trailingWindow {
              count = 1
              increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
            }
        }
    }

    private val CUMULATIVE_WEEKLY_METRIC_CALCULATION_SPEC = metricCalculationSpec {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalMetricCalculationSpecId = "cumulative-weekly"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
      details =
        MetricCalculationSpecKt.details {
          metricFrequencySpec =
            MetricCalculationSpecKt.metricFrequencySpec {
              weekly =
                MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                  dayOfWeek = DayOfWeek.MONDAY
                }
            }
        }
    }

    private val TOTAL_METRIC_CALCULATION_SPEC = metricCalculationSpec {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalMetricCalculationSpecId = "total"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
    }

    private val POPULATION_METRIC_CALCULATION_SPEC = metricCalculationSpec {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalMetricCalculationSpecId = "population"
      externalCampaignGroupId = CAMPAIGN_GROUP.externalReportingSetId
    }
  }
}
