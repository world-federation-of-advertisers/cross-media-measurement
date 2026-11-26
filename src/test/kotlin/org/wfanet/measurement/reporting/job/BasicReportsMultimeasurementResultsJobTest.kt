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

package org.wfanet.measurement.reporting.job

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Empty
import com.google.protobuf.util.Durations
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.doReturn
import org.mockito.kotlin.never
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.GetMeasurementRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt.QueryResultKt
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.MultimeasurementResultKt.queryResult
import org.wfanet.measurement.api.v2alpha.MeasurementKt.ResultKt.multimeasurementResult
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.populationAttributeValue
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.BeginReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.DimensionSpecKt
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.MetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingUnitKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMappingKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportResultAssemblyRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.listReportComputationsResponse
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportComputation
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingSetKey
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.internal.reporting.v2.reportingUnit
import org.wfanet.measurement.internal.reporting.v2.resultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec

@RunWith(JUnit4::class)
class BasicReportsMultimeasurementResultsJobTest {
  private val basicReportsMock: BasicReportsCoroutineImplBase = mockService {
    onBlocking { listBasicReports(any()) } doAnswer
      {
        val request = it.arguments.first() as ListBasicReportsRequest
        if (request.filter.state == BasicReport.State.MEASUREMENTS_CREATED) {
          listBasicReportsResponse { basicReports += BASIC_REPORT }
        } else {
          ListBasicReportsResponse.getDefaultInstance()
        }
      }
    onBlocking { failBasicReport(any()) } doReturn BasicReport.getDefaultInstance()
  }

  private val reportingSetsMock: ReportingSetsCoroutineImplBase = mockService {
    on { streamReportingSets(any()) } doReturn
      flowOf(CAMPAIGN_GROUP, COMPONENT_1, COMPONENT_2, UNION_REPORTING_SET)
  }

  private val reportResultsMock: ReportResultsCoroutineImplBase = mockService {
    onBlocking { discardReportResult(any()) } doReturn Empty.getDefaultInstance()
    onBlocking { beginReportResultAssembly(any()) } doAnswer
      {
        val request = it.arguments.first() as BeginReportResultAssemblyRequest
        request.reportResult.copy {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          externalReportResultId = EXTERNAL_REPORT_RESULT_ID
        }
      }
  }

  private val measurementsMock: MeasurementsCoroutineImplBase = mockService()

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(basicReportsMock)
    addService(reportingSetsMock)
    addService(reportResultsMock)
    addService(measurementsMock)
  }

  private lateinit var job: BasicReportsMultimeasurementResultsJob

  @Before
  fun initJob() {
    job =
      BasicReportsMultimeasurementResultsJob(
        MEASUREMENT_CONSUMER_CONFIGS,
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        ReportResultsCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        mapOf(IMPRESSION_QUALIFICATION_FILTER_ID to EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID),
      )
  }

  private suspend fun stubReportComputations(vararg reportComputations: ReportComputation) {
    whenever(basicReportsMock.listReportComputations(any()))
      .thenReturn(
        listReportComputationsResponse { this.reportComputations += reportComputations.toList() }
      )
  }

  private suspend fun stubMeasurements(vararg measurements: Measurement) {
    val measurementsByName: Map<String, Measurement> = measurements.associateBy { it.name }
    whenever(measurementsMock.getMeasurement(any())).thenAnswer {
      val request = it.arguments.first() as GetMeasurementRequest
      measurementsByName.getValue(request.name)
    }
  }

  @Test
  fun `execute writes reporting unit cell under the union ReportingSet`() = runBlocking {
    stubReportComputations(
      REPORT_COMPUTATION.copy {
        mappings += resultGroupQueryMapping {
          queryId = REPORTING_UNIT_QUERY_ID
          resultGroupSpecIndex = 0
          windowStart = REPORT_START_DATE
          windowEnd = REPORT_END_DATE
          cumulative = true
          reportingUnit = Empty.getDefaultInstance()
        }
      }
    )
    stubMeasurements(
      succeededMeasurement(
        queryResult {
          queryId = REPORTING_UNIT_QUERY_ID
          maxFrequency = 2
          cells +=
            QueryResultKt.cell {
              reach = 100
              reachVariance = 4.0
              impressions = 300
              impressionVariance = 9.0
              frequencyBins += listOf(60L, 40L)
              frequencyBinVariance += listOf(1.0, 4.0)
              populationSize = POPULATION_SIZE
            }
        }
      )
    )

    job.execute()

    verifyProtoArgument(
        reportResultsMock,
        ReportResultsCoroutineImplBase::batchCreateReportResultAssembly,
      )
      .isEqualTo(
        batchCreateReportResultAssemblyRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportResultId = EXTERNAL_REPORT_RESULT_ID
          externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
          finalBatch = true
          requests += createReportingSetResultRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = EXTERNAL_REPORT_RESULT_ID
            reportingSetResult = reportingSetResult {
              dimension =
                buildDimension(
                  UNION_REPORTING_SET.externalReportingSetId,
                  ReportingSetResultKt.DimensionKt.grouping {},
                  TOTAL_FREQUENCY,
                )
              populationSize = POPULATION_SIZE
              reportingWindowResults +=
                ReportingSetResultKt.reportingWindowEntry {
                  key = ReportingSetResultKt.reportingWindow { end = REPORT_END_DATE }
                  value =
                    ReportingSetResultKt.reportingWindowResult {
                      unprocessedReportResultValues =
                        ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                          cumulativeResults =
                            NoisyReportResultValuesKt.noisyMetricSet {
                              reach =
                                NoisyMetricSetKt.reachResult {
                                  value = 100
                                  univariateStatistics =
                                    NoisyMetricSetKt.univariateStatistics {
                                      standardDeviation = 2.0
                                    }
                                }
                              frequencyHistogram =
                                NoisyMetricSetKt.histogramResult {
                                  binResults[1] =
                                    NoisyMetricSetKt.HistogramResultKt.binResult {
                                      value = 60
                                      univariateStatistics =
                                        NoisyMetricSetKt.univariateStatistics {
                                          standardDeviation = 1.0
                                        }
                                    }
                                  binResults[2] =
                                    NoisyMetricSetKt.HistogramResultKt.binResult {
                                      value = 40
                                      univariateStatistics =
                                        NoisyMetricSetKt.univariateStatistics {
                                          standardDeviation = 2.0
                                        }
                                    }
                                }
                              impressionCount =
                                NoisyMetricSetKt.impressionCountResult {
                                  value = 300
                                  univariateStatistics =
                                    NoisyMetricSetKt.univariateStatistics {
                                      standardDeviation = 3.0
                                    }
                                }
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
  fun `execute writes breakdown cell under its grouping`() = runBlocking {
    stubReportComputations(
      REPORT_COMPUTATION.copy {
        mappings += resultGroupQueryMapping {
          queryId = COMPONENT_QUERY_ID
          resultGroupSpecIndex = 1
          windowStart = REPORT_START_DATE
          windowEnd = REPORT_END_DATE
          cumulative = false
          component =
            ResultGroupQueryMappingKt.component { component = COMPONENT_1.externalReportingSetId }
        }
      }
    )
    stubMeasurements(
      succeededMeasurement(
        queryResult {
          queryId = COMPONENT_QUERY_ID
          groupingPaths += GENDER_PATH
          maxFrequency = 1
          cells +=
            QueryResultKt.cell {
              attributeValues += populationAttributeValue { enumValue = "FEMALE" }
              reach = 50
              impressions = 70
              frequencyBins += 50L
            }
        }
      )
    )

    job.execute()

    val request: BatchCreateReportResultAssemblyRequest =
      verifyAndCapture(
        reportResultsMock,
        ReportResultsCoroutineImplBase::batchCreateReportResultAssembly,
      )
    val reportingSetResult: ReportingSetResult = request.requestsList.single().reportingSetResult
    assertThat(reportingSetResult.dimension)
      .isEqualTo(
        buildDimension(
          COMPONENT_1.externalReportingSetId,
          ReportingSetResultKt.DimensionKt.grouping {
            valueByPath[GENDER_PATH] = EventTemplateFieldKt.fieldValue { enumValue = "FEMALE" }
          },
          WEEKLY_FREQUENCY,
        )
      )
    val windowEntry = reportingSetResult.reportingWindowResultsList.single()
    assertThat(windowEntry.key)
      .isEqualTo(
        ReportingSetResultKt.reportingWindow {
          nonCumulativeStart = REPORT_START_DATE
          end = REPORT_END_DATE
        }
      )
    assertThat(windowEntry.value.unprocessedReportResultValues.nonCumulativeResults.reach)
      .isEqualTo(NoisyMetricSetKt.reachResult { value = 50 })
  }

  @Test
  fun `execute writes no row for a suppressed cell`() = runBlocking {
    stubReportComputations(
      REPORT_COMPUTATION.copy {
        mappings += resultGroupQueryMapping {
          queryId = COMPONENT_QUERY_ID
          resultGroupSpecIndex = 1
          windowStart = REPORT_START_DATE
          windowEnd = REPORT_END_DATE
          cumulative = false
          component =
            ResultGroupQueryMappingKt.component { component = COMPONENT_1.externalReportingSetId }
        }
      }
    )
    stubMeasurements(
      succeededMeasurement(
        queryResult {
          queryId = COMPONENT_QUERY_ID
          groupingPaths += GENDER_PATH
          maxFrequency = 1
          cells +=
            QueryResultKt.cell {
              attributeValues += populationAttributeValue { enumValue = "FEMALE" }
              reach = 50
              frequencyBins += 50L
            }
          cells +=
            QueryResultKt.cell {
              attributeValues += populationAttributeValue { enumValue = "MALE" }
              frequencyBins += 0L
              suppressed = true
            }
        }
      )
    )

    job.execute()

    val request: BatchCreateReportResultAssemblyRequest =
      verifyAndCapture(
        reportResultsMock,
        ReportResultsCoroutineImplBase::batchCreateReportResultAssembly,
      )
    assertThat(request.requestsList.single().reportingSetResult.dimension.grouping)
      .isEqualTo(
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath[GENDER_PATH] = EventTemplateFieldKt.fieldValue { enumValue = "FEMALE" }
        }
      )
  }

  @Test
  fun `execute fails BasicReport when a Measurement failed`() = runBlocking {
    stubReportComputations(REPORT_COMPUTATION, REPORT_COMPUTATION_2)
    stubMeasurements(
      succeededMeasurement(),
      measurement {
        name = MEASUREMENT_NAME_2
        state = Measurement.State.FAILED
      },
    )

    job.execute()

    verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::failBasicReport)
      .isEqualTo(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
        }
      )
    verifyBlocking(reportResultsMock, never()) { beginReportResultAssembly(any()) }
  }

  @Test
  fun `execute skips BasicReport when a Measurement is still computing`() = runBlocking {
    stubReportComputations(REPORT_COMPUTATION, REPORT_COMPUTATION_2)
    stubMeasurements(
      succeededMeasurement(),
      measurement {
        name = MEASUREMENT_NAME_2
        state = Measurement.State.COMPUTING
      },
    )

    job.execute()

    verifyBlocking(basicReportsMock, never()) { failBasicReport(any()) }
    verifyBlocking(reportResultsMock, never()) { beginReportResultAssembly(any()) }
  }

  companion object {
    private const val POPULATION_SIZE = 5000L
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "mc"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID"
    private const val EXTERNAL_BASIC_REPORT_ID = "basic-report"
    private const val EXTERNAL_CAMPAIGN_GROUP_ID = "campaign-group"
    private const val EXTERNAL_REPORT_RESULT_ID = 1234L
    private const val IMPRESSION_QUALIFICATION_FILTER_ID = 1L
    private const val EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID = "ami"
    private const val MEASUREMENT_NAME = "$MEASUREMENT_CONSUMER_NAME/measurements/measurement-1"
    private const val MEASUREMENT_NAME_2 = "$MEASUREMENT_CONSUMER_NAME/measurements/measurement-2"
    private const val REPORTING_UNIT_QUERY_ID = "0/c/2026-01-01/2026-01-08/ru"
    private const val COMPONENT_QUERY_ID = "1/n/2026-01-01/2026-01-08/c0"
    private const val GENDER_PATH = "person.gender"

    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig { apiKey = "api-key" }
    }

    private val REPORT_START_DATE = date {
      year = 2026
      month = 1
      day = 1
    }
    private val REPORT_END_DATE = date {
      year = 2026
      month = 1
      day = 8
    }

    private val TOTAL_FREQUENCY = metricFrequencySpec { total = true }
    private val WEEKLY_FREQUENCY = metricFrequencySpec { weekly = DayOfWeek.THURSDAY }

    private val COMPONENT_1: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "component-1"
      externalCampaignGroupId = EXTERNAL_CAMPAIGN_GROUP_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "data-provider-1"
              cmmsEventGroupId = "event-group-1"
            }
        }
    }
    private val COMPONENT_2: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "component-2"
      externalCampaignGroupId = EXTERNAL_CAMPAIGN_GROUP_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "data-provider-2"
              cmmsEventGroupId = "event-group-2"
            }
        }
    }
    private val CAMPAIGN_GROUP: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = EXTERNAL_CAMPAIGN_GROUP_ID
      externalCampaignGroupId = EXTERNAL_CAMPAIGN_GROUP_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys += COMPONENT_1.primitive.eventGroupKeysList
          eventGroupKeys += COMPONENT_2.primitive.eventGroupKeysList
        }
    }
    /** Union of [COMPONENT_1] and [COMPONENT_2], in the form `GetBasicReport` looks it up by. */
    private val UNION_REPORTING_SET: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "union"
      externalCampaignGroupId = EXTERNAL_CAMPAIGN_GROUP_ID
      composite =
        ReportingSetKt.setExpression {
          operation = ReportingSet.SetExpression.Operation.UNION
          lhs =
            ReportingSetKt.SetExpressionKt.operand {
              externalReportingSetId = COMPONENT_2.externalReportingSetId
            }
          rhs =
            ReportingSetKt.SetExpressionKt.operand {
              expression =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = COMPONENT_1.externalReportingSetId
                    }
                }
            }
        }
    }

    private val REPORTING_UNIT = reportingUnit {
      reportingSetKeys =
        ReportingUnitKt.reportingSetKeys {
          for (component in listOf(COMPONENT_1, COMPONENT_2)) {
            reportingSetKeys += reportingSetKey {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportingSetId = component.externalReportingSetId
            }
          }
        }
    }

    private val BASIC_REPORT = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
      externalCampaignGroupId = EXTERNAL_CAMPAIGN_GROUP_ID
      state = BasicReport.State.MEASUREMENTS_CREATED
      details = basicReportDetails {
        reportingInterval = reportingInterval {
          effectiveReportStart = dateTime {
            year = 2026
            month = 1
            day = 1
            utcOffset = Durations.fromHours(0)
          }
          reportEnd = REPORT_END_DATE
        }
        resultGroupSpecs += resultGroupSpec {
          reportingUnit = REPORTING_UNIT
          metricFrequency = TOTAL_FREQUENCY
          dimensionSpec = dimensionSpec {}
        }
        resultGroupSpecs += resultGroupSpec {
          reportingUnit = REPORTING_UNIT
          metricFrequency = WEEKLY_FREQUENCY
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += GENDER_PATH }
          }
        }
      }
    }

    private val REPORT_COMPUTATION = reportComputation {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
      impressionQualificationFilterId = IMPRESSION_QUALIFICATION_FILTER_ID
      cmmsMeasurementId = "measurement-1"
    }
    private val REPORT_COMPUTATION_2 = reportComputation {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = EXTERNAL_BASIC_REPORT_ID
      impressionQualificationFilterId = -1L
      cmmsMeasurementId = "measurement-2"
    }

    private fun succeededMeasurement(
      vararg queryResults: Measurement.Result.MultimeasurementResult.QueryResult
    ): Measurement = measurement {
      name = MEASUREMENT_NAME
      state = Measurement.State.SUCCEEDED
      results +=
        MeasurementKt.resultOutput {
          result =
            MeasurementKt.result {
              multimeasurementResult = multimeasurementResult {
                this.queryResults += queryResults.toList()
              }
            }
        }
    }

    private fun buildDimension(
      externalReportingSetId: String,
      grouping: ReportingSetResult.Dimension.Grouping,
      metricFrequency: MetricFrequencySpec,
    ): ReportingSetResult.Dimension =
      ReportingSetResultKt.dimension {
        this.externalReportingSetId = externalReportingSetId
        vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
        externalImpressionQualificationFilterId = EXTERNAL_IMPRESSION_QUALIFICATION_FILTER_ID
        metricFrequencySpec = metricFrequency
        this.grouping = grouping
      }
  }
}
