/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import com.google.protobuf.util.Durations
import com.google.type.Interval
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Duration
import java.time.Instant
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.eq
import org.mockito.kotlin.times
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.withDataProviderPrincipal
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequestKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt as InternalMetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest as internalGetReportRequest
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec as internalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.streamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.timeIntervals as Intervals
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleInfoServerInterceptor.Companion.withReportScheduleInfoAndMeasurementConsumerPrincipal
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.BatchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportsPageTokenKt
import org.wfanet.measurement.reporting.v2alpha.ListReportsRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachResult
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.watchDurationResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchGetMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsResponse
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeIntervals

private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

private const val METRIC_ID_PREFIX = "a"

// Authentication key
private const val API_AUTHENTICATION_KEY = "nR5QPN7ptx"

// Metric Specs
private const val NUMBER_VID_BUCKETS = 300
private const val REACH_ONLY_VID_SAMPLING_WIDTH = 3.0f / NUMBER_VID_BUCKETS
private const val REACH_ONLY_VID_SAMPLING_START = 0.0f
private const val REACH_ONLY_REACH_EPSILON = 0.0041

private const val REACH_FREQUENCY_VID_SAMPLING_WIDTH = 5.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_VID_SAMPLING_START = 48.0f / NUMBER_VID_BUCKETS
private const val REACH_FREQUENCY_REACH_EPSILON = 0.0033
private const val REACH_FREQUENCY_FREQUENCY_EPSILON = 0.115
private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY = 10

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_EPSILON = 0.0011
private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_EPSILON = 0.001
private val MAXIMUM_WATCH_DURATION_PER_USER = Durations.fromSeconds(4000)

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

private const val BATCH_CREATE_METRICS_LIMIT = 1000
private const val BATCH_GET_METRICS_LIMIT = 100

@RunWith(JUnit4::class)
class ReportsServiceTest {

  private data class InternalReports(
    val requestingReport: InternalReport,
    val initialReport: InternalReport,
    val pendingReport: InternalReport,
  )

  private val internalReportsMock: ReportsCoroutineImplBase = mockService {
    onBlocking {
        createReport(
          eq(
            internalCreateReportRequest {
              report = INTERNAL_REACH_REPORTS.requestingReport
              externalReportId = "report-id"
            }
          )
        )
      }
      .thenReturn(INTERNAL_REACH_REPORTS.initialReport)
    onBlocking {
        getReport(
          eq(
            internalGetReportRequest {
              cmmsMeasurementConsumerId =
                INTERNAL_REACH_REPORTS.initialReport.cmmsMeasurementConsumerId
              externalReportId = INTERNAL_REACH_REPORTS.initialReport.externalReportId
            }
          )
        )
      }
      .thenReturn(INTERNAL_REACH_REPORTS.pendingReport)
    onBlocking { streamReports(any()) }
      .thenReturn(
        flowOf(INTERNAL_REACH_REPORTS.pendingReport, INTERNAL_WATCH_DURATION_REPORTS.pendingReport)
      )
  }

  private val metricsMock: MetricsCoroutineImplBase = mockService {
    onBlocking { batchCreateMetrics(any()) }
      .thenReturn(batchCreateMetricsResponse { metrics += RUNNING_REACH_METRIC })

    onBlocking { batchGetMetrics(any()) }
      .thenAnswer {
        val request = it.arguments[0] as BatchGetMetricsRequest
        val metricsMap =
          mapOf(
            RUNNING_REACH_METRIC.name to RUNNING_REACH_METRIC,
            RUNNING_WATCH_DURATION_METRIC.name to RUNNING_WATCH_DURATION_METRIC,
          )
        batchGetMetricsResponse {
          metrics += request.namesList.map { metricName -> metricsMap.getValue(metricName) }
        }
      }
  }

  private val internalMetricCalculationSpecsMock: MetricCalculationSpecsCoroutineImplBase =
    mockService {
      onBlocking { batchGetMetricCalculationSpecs(any()) }
        .thenAnswer {
          val request = it.arguments[0] as BatchGetMetricCalculationSpecsRequest
          val metricCalculationSpecsMap =
            mapOf(
              INTERNAL_REACH_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId to
                INTERNAL_REACH_METRIC_CALCULATION_SPEC,
              INTERNAL_WATCH_DURATION_METRIC_CALCULATION_SPEC.externalMetricCalculationSpecId to
                INTERNAL_WATCH_DURATION_METRIC_CALCULATION_SPEC,
            )
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs +=
              request.externalMetricCalculationSpecIdsList.map { id ->
                metricCalculationSpecsMap.getValue(id)
              }
          }
        }
    }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportsMock)
    addService(metricsMock)
    addService(internalMetricCalculationSpecsMock)
  }

  private lateinit var service: ReportsService

  @Before
  fun initService() {
    service =
      ReportsService(
        InternalReportsCoroutineStub(grpcTestServerRule.channel),
        InternalMetricCalculationSpecsCoroutineStub(grpcTestServerRule.channel),
        MetricsCoroutineStub(grpcTestServerRule.channel),
        METRIC_SPEC_CONFIG,
      )
  }

  @Test
  fun `createReport returns report with one metric created`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .isEqualTo(
        batchCreateMetricsRequest {
          parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
          requests += createMetricRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            metric = REQUESTING_REACH_METRIC
            requestId = ExternalId(REACH_METRIC_ID_BASE_LONG).apiId.value
            metricId = "$METRIC_ID_PREFIX$requestId"
          }
        }
      )

    assertThat(result).isEqualTo(PENDING_REACH_REPORT)
  }

  @Test
  fun `createReport returns report with one metric created when report schedule name set`() {
    val externalReportScheduleId = "external-report-schedule-id"
    val nextReportCreationTime = timestamp { seconds = 1000 }
    runBlocking {
      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = INTERNAL_REACH_REPORTS.requestingReport
                externalReportId = "report-id"
                reportScheduleInfo =
                  CreateReportRequestKt.reportScheduleInfo {
                    this.externalReportScheduleId = externalReportScheduleId
                    this.nextReportCreationTime = nextReportCreationTime
                  }
              }
            )
          )
        )
        .thenReturn(
          INTERNAL_REACH_REPORTS.initialReport.copy {
            this.externalReportScheduleId = externalReportScheduleId
          }
        )

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId =
                  INTERNAL_REACH_REPORTS.initialReport.cmmsMeasurementConsumerId
                externalReportId = INTERNAL_REACH_REPORTS.initialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(
          INTERNAL_REACH_REPORTS.pendingReport.copy {
            this.externalReportScheduleId = externalReportScheduleId
          }
        )
    }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }

    val reportScheduleName =
      ReportScheduleKey(
          INTERNAL_REACH_REPORTS.initialReport.cmmsMeasurementConsumerId,
          externalReportScheduleId,
        )
        .toName()

    val result =
      withReportScheduleInfoAndMeasurementConsumerPrincipal(
        ReportScheduleInfoServerInterceptor.ReportScheduleInfo(
          reportScheduleName,
          nextReportCreationTime,
        ),
        MEASUREMENT_CONSUMER_KEYS.first().toName(),
        CONFIG,
      ) {
        runBlocking { service.createReport(request) }
      }

    verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .isEqualTo(
        batchCreateMetricsRequest {
          parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
          requests += createMetricRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            metric = REQUESTING_REACH_METRIC
            requestId = ExternalId(REACH_METRIC_ID_BASE_LONG).apiId.value
            metricId = "$METRIC_ID_PREFIX$requestId"
          }
        }
      )

    assertThat(result).isEqualTo(PENDING_REACH_REPORT.copy { reportSchedule = reportScheduleName })
  }

  @Test
  fun `createReport returns report when metric already succeeded`() = runBlocking {
    whenever(
        metricsMock.batchCreateMetrics(
          eq(
            batchCreateMetricsRequest {
              parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
              requests += createMetricRequest {
                parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                metric = REQUESTING_REACH_METRIC
                requestId = ExternalId(REACH_METRIC_ID_BASE_LONG).apiId.value
                metricId = "$METRIC_ID_PREFIX$requestId"
              }
            }
          )
        )
      )
      .thenReturn(batchCreateMetricsResponse { metrics += SUCCEEDED_REACH_METRIC })

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    assertThat(result).isEqualTo(SUCCEEDED_REACH_REPORT)
  }

  @Test
  fun `createReport returns report with two metrics when there are two time intervals`() =
    runBlocking {
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val timeIntervalsList =
        listOf(
          interval {
            startTime = START_TIME
            endTime = END_TIME
          },
          interval {
            startTime = END_TIME
            endTime = END_INSTANT.plus(Duration.ofDays(1)).toProtoTime()
          },
        )

      val intervals =
        listOf(
          interval {
            startTime = START_TIME
            endTime = END_TIME
          },
          interval {
            startTime = END_TIME
            endTime = END_INSTANT.plus(Duration.ofDays(1)).toProtoTime()
          },
        )

      val initialReportingMetrics: List<InternalReport.ReportingMetric> =
        intervals.map { timeInterval ->
          buildInitialReportingMetric(timeInterval, INTERNAL_REACH_METRIC_SPEC, listOf())
        }

      val (internalRequestingReport, internalInitialReport, internalPendingReport) =
        buildInternalReports(
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
          timeIntervals = intervals,
          reportingSetId = targetReportingSet.resourceId,
          reportingMetrics = initialReportingMetrics,
          metricCalculationSpecId = REACH_METRIC_CALCULATION_SPEC_ID,
        )

      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = internalRequestingReport
                externalReportId = "report-id"
              }
            )
          )
        )
        .thenReturn(internalInitialReport)

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
                externalReportId = internalInitialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(internalPendingReport)

      val requestingMetrics: List<Metric> =
        timeIntervalsList.map { timeInterval ->
          metric {
            reportingSet = targetReportingSet.name
            this.timeInterval = timeInterval
            metricSpec = REACH_METRIC_SPEC
          }
        }

      whenever(metricsMock.batchCreateMetrics(any()))
        .thenReturn(
          batchCreateMetricsResponse {
            metrics +=
              requestingMetrics.mapIndexed { index, metric ->
                metric.copy {
                  name =
                    MetricKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        tags.putAll(REPORT_TAGS)
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs += REACH_METRIC_CALCULATION_SPEC_NAME
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeIntervalsList }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
        reportId = "report-id"
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
                  metricId = "$METRIC_ID_PREFIX${this.requestId}"
                }
              }
          }
        )

      assertThat(result)
        .isEqualTo(
          requestingReport.copy {
            name = internalInitialReport.resourceName
            state = Report.State.RUNNING
            createTime = internalInitialReport.createTime
          }
        )
    }

  @Test
  fun `createReport returns report with two metrics when multiple filters`() = runBlocking {
    val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()

    val predicates1 = listOf("gender == MALE", "gender == FEMALE")
    val predicates2 = listOf("age == 18_34", "age == 55_PLUS")

    val groupingsCartesianProduct: List<List<String>> =
      predicates1.flatMap { filter1 -> predicates2.map { filter2 -> listOf(filter1, filter2) } }
    val filter = "device == MOBILE"

    val internalMetricCalculationSpec = internalMetricCalculationSpec {
      externalMetricCalculationSpecId = "1234"
      details =
        InternalMetricCalculationSpecKt.details {
          displayName = DISPLAY_NAME
          metricSpecs += INTERNAL_REACH_METRIC_SPEC
          groupings += InternalMetricCalculationSpecKt.grouping { predicates += predicates1 }
          groupings += InternalMetricCalculationSpecKt.grouping { predicates += predicates2 }
          this.filter = filter
          cumulative = false
        }
    }
    whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
      .thenReturn(
        batchGetMetricCalculationSpecsResponse {
          metricCalculationSpecs += internalMetricCalculationSpec
        }
      )

    val timeInterval = interval {
      startTime = START_TIME
      endTime = END_TIME
    }

    val interval = interval {
      startTime = START_TIME
      endTime = END_TIME
    }

    val initialReportingMetrics: List<InternalReport.ReportingMetric> =
      groupingsCartesianProduct.map { groupingPredicates ->
        buildInitialReportingMetric(interval, INTERNAL_REACH_METRIC_SPEC, groupingPredicates)
      }

    val (internalRequestingReport, internalInitialReport, internalPendingReport) =
      buildInternalReports(
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
        timeIntervals = listOf(interval),
        reportingSetId = targetReportingSet.resourceId,
        reportingMetrics = initialReportingMetrics,
        metricCalculationSpecId = internalMetricCalculationSpec.externalMetricCalculationSpecId,
      )

    whenever(
        internalReportsMock.createReport(
          eq(
            internalCreateReportRequest {
              report = internalRequestingReport
              externalReportId = "report-id"
            }
          )
        )
      )
      .thenReturn(internalInitialReport)

    whenever(
        internalReportsMock.getReport(
          eq(
            internalGetReportRequest {
              cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
              externalReportId = internalInitialReport.externalReportId
            }
          )
        )
      )
      .thenReturn(internalPendingReport)

    val requestingMetrics: List<Metric> =
      groupingsCartesianProduct.map { groupingPredicates ->
        metric {
          reportingSet = targetReportingSet.name
          this.timeInterval = timeInterval
          metricSpec = REACH_METRIC_SPEC
          this.filters += groupingPredicates
          this.filters += filter
        }
      }

    whenever(metricsMock.batchCreateMetrics(any()))
      .thenReturn(
        batchCreateMetricsResponse {
          metrics +=
            requestingMetrics.mapIndexed { index, metric ->
              metric.copy {
                name =
                  MetricKey(
                      MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                      ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                    )
                    .toName()
                state = Metric.State.RUNNING
                createTime = Instant.now().toProtoTime()
              }
            }
        }
      )

    val requestingReport = report {
      tags.putAll(REPORT_TAGS)
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = targetReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                MetricCalculationSpecKey(
                    MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                    internalMetricCalculationSpec.externalMetricCalculationSpecId,
                  )
                  .toName()
            }
        }
      timeIntervals = timeIntervals { timeIntervals += timeInterval }
    }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report = requestingReport
      reportId = "report-id"
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
      .isEqualTo(
        batchCreateMetricsRequest {
          parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
          requests +=
            requestingMetrics.mapIndexed { requestId, metric ->
              createMetricRequest {
                parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                this.metric = metric
                this.requestId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
                metricId = "$METRIC_ID_PREFIX${this.requestId}"
              }
            }
        }
      )

    assertThat(result)
      .isEqualTo(
        requestingReport.copy {
          name = internalPendingReport.resourceName
          state = Report.State.RUNNING
          createTime = internalPendingReport.createTime
        }
      )
  }

  @Test
  fun `createReport returns report with two metrics when there are two metricSpecs`() =
    runBlocking {
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val metricSpecs = listOf(REACH_METRIC_SPEC, REACH_AND_FREQUENCY_METRIC_SPEC)
      val internalMetricSpecs =
        listOf(INTERNAL_REACH_METRIC_SPEC, INTERNAL_REACH_AND_FREQUENCY_METRIC_SPEC)
      val timeInterval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }
      val interval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }

      val internalMetricCalculationSpec = internalMetricCalculationSpec {
        externalMetricCalculationSpecId = "1234"
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = DISPLAY_NAME
            this.metricSpecs += INTERNAL_REACH_METRIC_SPEC
            this.metricSpecs += INTERNAL_REACH_AND_FREQUENCY_METRIC_SPEC
            cumulative = false
          }
      }
      whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
        .thenReturn(
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs += internalMetricCalculationSpec
          }
        )

      val initialReportingMetrics: List<InternalReport.ReportingMetric> =
        internalMetricSpecs.map { metricSpec ->
          buildInitialReportingMetric(interval, metricSpec, listOf())
        }

      val (internalRequestingReport, internalInitialReport, internalPendingReport) =
        buildInternalReports(
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
          timeIntervals = listOf(interval),
          reportingSetId = targetReportingSet.resourceId,
          reportingMetrics = initialReportingMetrics,
          metricCalculationSpecId = internalMetricCalculationSpec.externalMetricCalculationSpecId,
        )

      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = internalRequestingReport
                externalReportId = "report-id"
              }
            )
          )
        )
        .thenReturn(internalInitialReport)

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
                externalReportId = internalInitialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(internalPendingReport)

      val requestingMetrics: List<Metric> =
        metricSpecs.map { metricSpec ->
          metric {
            reportingSet = targetReportingSet.name
            this.timeInterval = timeInterval
            this.metricSpec = metricSpec
          }
        }

      whenever(metricsMock.batchCreateMetrics(any()))
        .thenReturn(
          batchCreateMetricsResponse {
            metrics +=
              requestingMetrics.mapIndexed { index, metric ->
                metric.copy {
                  name =
                    MetricKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        tags.putAll(REPORT_TAGS)
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  MetricCalculationSpecKey(
                      MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                      internalMetricCalculationSpec.externalMetricCalculationSpecId,
                    )
                    .toName()
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeInterval }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
        reportId = "report-id"
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
                  metricId = "$METRIC_ID_PREFIX${this.requestId}"
                }
              }
          }
        )

      assertThat(result)
        .isEqualTo(
          requestingReport.copy {
            name = internalPendingReport.resourceName
            state = Report.State.RUNNING
            createTime = internalPendingReport.createTime
          }
        )
    }

  @Test
  fun `createReport returns report when multiple timeIntervals, groupings, and metricSpecs`() =
    runBlocking {
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val filter = "device == MOBILE"

      // Metric specs
      val metricSpecs = listOf(REACH_METRIC_SPEC, REACH_AND_FREQUENCY_METRIC_SPEC)
      val internalMetricSpecs =
        listOf(INTERNAL_REACH_METRIC_SPEC, INTERNAL_REACH_AND_FREQUENCY_METRIC_SPEC)

      // Time intervals
      val timeIntervalsList =
        listOf(
          interval {
            startTime = START_TIME
            endTime = END_TIME
          },
          interval {
            startTime = END_TIME
            endTime = END_INSTANT.plus(Duration.ofDays(1)).toProtoTime()
          },
        )
      val intervals =
        listOf(
          interval {
            startTime = START_TIME
            endTime = END_TIME
          },
          interval {
            startTime = END_TIME
            endTime = END_INSTANT.plus(Duration.ofDays(1)).toProtoTime()
          },
        )

      // Groupings
      val predicates1 = listOf("gender == MALE", "gender == FEMALE")
      val predicates2 = listOf("age == 18_34", "age == 55_PLUS")

      val internalMetricCalculationSpec = internalMetricCalculationSpec {
        externalMetricCalculationSpecId = "1234"
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = DISPLAY_NAME
            this.metricSpecs +=
              listOf(INTERNAL_REACH_METRIC_SPEC, INTERNAL_REACH_AND_FREQUENCY_METRIC_SPEC)
            groupings += InternalMetricCalculationSpecKt.grouping { predicates += predicates1 }
            groupings += InternalMetricCalculationSpecKt.grouping { predicates += predicates2 }
            this.filter = filter
            cumulative = false
          }
      }
      whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
        .thenReturn(
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs += internalMetricCalculationSpec
          }
        )

      val groupingsCartesianProduct: List<List<String>> =
        predicates1.flatMap { filter1 -> predicates2.map { filter2 -> listOf(filter1, filter2) } }

      // Metric configs for internal and public
      data class MetricConfig(
        val reportingSet: String,
        val metricSpec: MetricSpec,
        val timeInterval: Interval,
        val filters: List<String>,
      )
      val metricConfigs =
        timeIntervalsList.flatMap { timeInterval ->
          metricSpecs.flatMap { metricSpec ->
            groupingsCartesianProduct.map { predicateGroup ->
              MetricConfig(targetReportingSet.name, metricSpec, timeInterval, predicateGroup)
            }
          }
        }

      data class ReportingMetricConfig(
        val reportingSetId: String,
        val metricSpec: InternalMetricSpec,
        val timeInterval: Interval,
        val filters: List<String>,
      )
      val reportingMetricConfigs =
        intervals.flatMap { timeInterval ->
          internalMetricSpecs.flatMap { metricSpec ->
            groupingsCartesianProduct.map { predicateGroup ->
              ReportingMetricConfig(
                targetReportingSet.resourceId,
                metricSpec,
                timeInterval,
                predicateGroup,
              )
            }
          }
        }

      val initialReportingMetrics: List<InternalReport.ReportingMetric> =
        reportingMetricConfigs.map { reportingMetricConfig ->
          buildInitialReportingMetric(
            reportingMetricConfig.timeInterval,
            reportingMetricConfig.metricSpec,
            reportingMetricConfig.filters,
          )
        }

      val (internalRequestingReport, internalInitialReport, internalPendingReport) =
        buildInternalReports(
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
          timeIntervals = intervals,
          reportingSetId = targetReportingSet.resourceId,
          reportingMetrics = initialReportingMetrics,
          metricCalculationSpecId = internalMetricCalculationSpec.externalMetricCalculationSpecId,
        )

      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = internalRequestingReport
                externalReportId = "report-id"
              }
            )
          )
        )
        .thenReturn(internalInitialReport)

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
                externalReportId = internalInitialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(internalPendingReport)

      val requestingMetrics: List<Metric> =
        metricConfigs.map { metricConfig ->
          metric {
            reportingSet = metricConfig.reportingSet
            timeInterval = metricConfig.timeInterval
            metricSpec = metricConfig.metricSpec
            filters += metricConfig.filters
            filters += filter
          }
        }

      whenever(metricsMock.batchCreateMetrics(any()))
        .thenReturn(
          batchCreateMetricsResponse {
            metrics +=
              requestingMetrics.mapIndexed { index, metric ->
                metric.copy {
                  name =
                    MetricKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        tags.putAll(REPORT_TAGS)
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  MetricCalculationSpecKey(
                      MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                      internalMetricCalculationSpec.externalMetricCalculationSpecId,
                    )
                    .toName()
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeIntervalsList }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
        reportId = "report-id"
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
                  metricId = "$METRIC_ID_PREFIX${this.requestId}"
                }
              }
          }
        )

      assertThat(result)
        .isEqualTo(
          requestingReport.copy {
            name = internalPendingReport.resourceName
            state = Report.State.RUNNING
            createTime = internalPendingReport.createTime
          }
        )
    }

  @Test
  fun `createReport returns report with 2 metrics generated when there are 2 reporting sets`() =
    runBlocking {
      val targetReportingSets = PRIMITIVE_REPORTING_SETS
      val timeInterval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }
      val interval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }

      val internalMetricCalculationSpec = internalMetricCalculationSpec {
        externalMetricCalculationSpecId = "1234"
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = DISPLAY_NAME
            metricSpecs += INTERNAL_REACH_METRIC_SPEC
            cumulative = false
          }
      }
      whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
        .thenReturn(
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs += internalMetricCalculationSpec
          }
        )

      val reportingSetToCreateMetricRequestMap: Map<ReportingSet, InternalReport.ReportingMetric> =
        targetReportingSets.associateWith {
          buildInitialReportingMetric(interval, INTERNAL_REACH_METRIC_SPEC, listOf())
        }

      val internalRequestingReport = internalReport {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
        timeIntervals = Intervals { timeIntervals += interval }
        details = InternalReportKt.details { tags.putAll(REPORT_TAGS) }
        reportingSetToCreateMetricRequestMap.forEach { (reportingSet, reportingMetric) ->
          val initialReportingMetrics = listOf(reportingMetric)
          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              reportingSetId = reportingSet.resourceId,
              reportingMetrics = initialReportingMetrics,
              metricCalculationSpecId =
                internalMetricCalculationSpec.externalMetricCalculationSpecId,
            )
          )
        }
      }

      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = "report-id"
          createTime = Instant.now().toProtoTime()
          reportingMetricEntries.clear()

          var requestId = 0
          reportingSetToCreateMetricRequestMap.map { (reportingSet, createMetricRequest) ->
            val updatedReportingMetrics =
              listOf(createMetricRequest.copy { this.createMetricRequestId = requestId.toString() })
            requestId++
            reportingMetricEntries.putAll(
              buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
                reportingSetId = reportingSet.resourceId,
                reportingMetrics = updatedReportingMetrics,
                metricCalculationSpecId =
                  internalMetricCalculationSpec.externalMetricCalculationSpecId,
              )
            )
          }
        }

      val internalPendingReport =
        internalInitialReport.copy {
          reportingMetricEntries.clear()

          var requestId = 0
          reportingSetToCreateMetricRequestMap.map { (reportingSet, createMetricRequest) ->
            val updatedReportingMetrics =
              listOf(
                createMetricRequest.copy {
                  this.createMetricRequestId = requestId.toString()
                  externalMetricId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
                }
              )
            requestId++
            reportingMetricEntries.putAll(
              buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
                reportingSetId = reportingSet.resourceId,
                reportingMetrics = updatedReportingMetrics,
                metricCalculationSpecId =
                  internalMetricCalculationSpec.externalMetricCalculationSpecId,
              )
            )
          }
        }

      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = internalRequestingReport
                externalReportId = "report-id"
              }
            )
          )
        )
        .thenReturn(internalInitialReport)

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
                externalReportId = internalInitialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(internalPendingReport)

      val requestingMetrics: List<Metric> =
        targetReportingSets.map { reportingSet ->
          metric {
            this.reportingSet = reportingSet.name
            this.timeInterval = timeInterval
            metricSpec = REACH_METRIC_SPEC
          }
        }

      whenever(metricsMock.batchCreateMetrics(any()))
        .thenReturn(
          batchCreateMetricsResponse {
            metrics +=
              requestingMetrics.mapIndexed { index, metric ->
                metric.copy {
                  name =
                    MetricKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        tags.putAll(REPORT_TAGS)
        reportingMetricEntries +=
          targetReportingSets.map { reportingSet ->
            ReportKt.reportingMetricEntry {
              key = reportingSet.name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    MetricCalculationSpecKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        internalMetricCalculationSpec.externalMetricCalculationSpecId,
                      )
                      .toName()
                }
            }
          }
        timeIntervals = timeIntervals { timeIntervals += timeInterval }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
        reportId = "report-id"
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = requestId.toString()
                  metricId = "$METRIC_ID_PREFIX${this.requestId}"
                }
              }
          }
        )

      assertThat(result)
        .isEqualTo(
          requestingReport.copy {
            name = internalPendingReport.resourceName
            state = Report.State.RUNNING
            createTime = internalPendingReport.createTime
          }
        )
    }

  @Test
  fun `createReport returns report with 2 metrics generated when 2 MetricCalculationSpec`() =
    runBlocking {
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val timeInterval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }
      val interval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }

      val internalMetricCalculationSpec = internalMetricCalculationSpec {
        externalMetricCalculationSpecId = "1234"
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = DISPLAY_NAME
            metricSpecs += INTERNAL_REACH_METRIC_SPEC
            cumulative = false
          }
      }
      whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
        .thenReturn(
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs += internalMetricCalculationSpec
          }
        )

      val internalReportingMetric =
        InternalReportKt.reportingMetric {
          details =
            InternalReportKt.ReportingMetricKt.details {
              metricSpec = INTERNAL_REACH_METRIC_SPEC
              this.timeInterval = interval
            }
        }

      val metricCalculationSpecReportingMetrics =
        InternalReportKt.metricCalculationSpecReportingMetrics {
          externalMetricCalculationSpecId =
            internalMetricCalculationSpec.externalMetricCalculationSpecId
          reportingMetrics += internalReportingMetric
        }
      val internalRequestingReport = internalReport {
        details = InternalReportKt.details { tags.putAll(REPORT_TAGS) }
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
        timeIntervals = Intervals { timeIntervals += interval }
        reportingMetricEntries.putAll(
          mapOf(
            targetReportingSet.resourceId to
              InternalReportKt.reportingMetricCalculationSpec {
                this.metricCalculationSpecReportingMetrics += metricCalculationSpecReportingMetrics
                this.metricCalculationSpecReportingMetrics += metricCalculationSpecReportingMetrics
              }
          )
        )
      }

      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = "report-id"
          createTime = Instant.now().toProtoTime()

          val updatedMetricCalculationSpecReportingMetrics =
            (0..1).map { requestId ->
              metricCalculationSpecReportingMetrics.copy {
                val updatedReportingMetrics =
                  reportingMetrics.map { reportingMetric ->
                    reportingMetric.copy { this.createMetricRequestId = requestId.toString() }
                  }
                this.reportingMetrics.clear()
                this.reportingMetrics += updatedReportingMetrics
              }
            }
          reportingMetricEntries.putAll(
            mapOf(
              targetReportingSet.resourceId to
                InternalReportKt.reportingMetricCalculationSpec {
                  this.metricCalculationSpecReportingMetrics +=
                    updatedMetricCalculationSpecReportingMetrics
                }
            )
          )
        }

      val internalPendingReport =
        internalInitialReport.copy {
          val updatedMetricCalculationSpecReportingMetrics =
            (0..1).map { requestId ->
              metricCalculationSpecReportingMetrics.copy {
                val updatedReportingMetrics =
                  reportingMetrics.map { reportingMetric ->
                    reportingMetric.copy {
                      this.createMetricRequestId = requestId.toString()
                      externalMetricId =
                        ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
                    }
                  }
                this.reportingMetrics.clear()
                this.reportingMetrics += updatedReportingMetrics
              }
            }
          reportingMetricEntries.putAll(
            mapOf(
              targetReportingSet.resourceId to
                InternalReportKt.reportingMetricCalculationSpec {
                  this.metricCalculationSpecReportingMetrics +=
                    updatedMetricCalculationSpecReportingMetrics
                }
            )
          )
        }

      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = internalRequestingReport
                externalReportId = "report-id"
              }
            )
          )
        )
        .thenReturn(internalInitialReport)

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
                externalReportId = internalInitialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(internalPendingReport)

      val requestingMetrics: List<Metric> =
        (0..1).map {
          metric {
            reportingSet = targetReportingSet.name
            this.timeInterval = timeInterval
            metricSpec = REACH_METRIC_SPEC
          }
        }

      whenever(metricsMock.batchCreateMetrics(any()))
        .thenReturn(
          batchCreateMetricsResponse {
            metrics +=
              requestingMetrics.mapIndexed { index, metric ->
                metric.copy {
                  name =
                    MetricKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        tags.putAll(REPORT_TAGS)
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                val metricCalculationSpecName =
                  MetricCalculationSpecKey(
                      MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                      internalMetricCalculationSpec.externalMetricCalculationSpecId,
                    )
                    .toName()
                metricCalculationSpecs += metricCalculationSpecName
                metricCalculationSpecs += metricCalculationSpecName
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeInterval }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
        reportId = "report-id"
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = requestId.toString()
                  metricId = "$METRIC_ID_PREFIX${this.requestId}"
                }
              }
          }
        )

      assertThat(result)
        .isEqualTo(
          requestingReport.copy {
            name = internalPendingReport.resourceName
            state = Report.State.RUNNING
            createTime = internalPendingReport.createTime
          }
        )
    }

  @Test
  fun `createReport returns report with MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS plus 1 metrics`() =
    runBlocking {
      val internalMetricCalculationSpec = internalMetricCalculationSpec {
        externalMetricCalculationSpecId = "1234"
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = DISPLAY_NAME
            metricSpecs += INTERNAL_REACH_METRIC_SPEC
            cumulative = true
          }
      }
      whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
        .thenReturn(
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs += internalMetricCalculationSpec
          }
        )

      val startSec = 10L
      val incrementSec = 1L
      val intervalCount = BATCH_CREATE_METRICS_LIMIT + 1

      val periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = startSec }
        increment = duration { seconds = incrementSec }
        this.intervalCount = intervalCount
      }
      val endTimesList: List<Long> =
        (startSec + incrementSec until startSec + incrementSec + intervalCount).toList()
      val intervals: List<Interval> =
        endTimesList.map { end ->
          interval {
            startTime = timestamp { seconds = startSec }
            endTime = timestamp { seconds = end }
          }
        }

      val reportingMetrics =
        intervals.map { timeInterval ->
          buildInitialReportingMetric(timeInterval, INTERNAL_REACH_METRIC_SPEC, listOf())
        }

      val internalRequestingReport = internalReport {
        details = InternalReportKt.details { tags.putAll(REPORT_TAGS) }
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId

        this.periodicTimeInterval = internalPeriodicTimeInterval {
          startTime = timestamp { seconds = startSec }
          increment = duration { seconds = incrementSec }
          this.intervalCount = intervalCount
        }

        reportingMetricEntries.putAll(
          buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
            reportingSetId = PRIMITIVE_REPORTING_SETS.first().resourceId,
            reportingMetrics = reportingMetrics,
            metricCalculationSpecId = internalMetricCalculationSpec.externalMetricCalculationSpecId,
          )
        )
      }

      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = "report-id"
          createTime = Instant.now().toProtoTime()

          val updatedReportingMetrics =
            reportingMetrics.mapIndexed { requestId, request ->
              request.copy { this.createMetricRequestId = requestId.toString() }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              reportingSetId = PRIMITIVE_REPORTING_SETS.first().resourceId,
              reportingMetrics = updatedReportingMetrics,
              metricCalculationSpecId =
                internalMetricCalculationSpec.externalMetricCalculationSpecId,
            )
          )
        }

      val internalPendingReport =
        internalInitialReport.copy {
          val updatedReportingMetrics =
            reportingMetrics.mapIndexed { requestId, request ->
              request.copy {
                this.createMetricRequestId = requestId.toString()
                externalMetricId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
              }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              reportingSetId = PRIMITIVE_REPORTING_SETS.first().resourceId,
              reportingMetrics = updatedReportingMetrics,
              metricCalculationSpecId =
                internalMetricCalculationSpec.externalMetricCalculationSpecId,
            )
          )
        }

      whenever(
          internalReportsMock.createReport(
            eq(
              internalCreateReportRequest {
                report = internalRequestingReport
                externalReportId = "report-id"
              }
            )
          )
        )
        .thenReturn(internalInitialReport)

      whenever(
          internalReportsMock.getReport(
            eq(
              internalGetReportRequest {
                cmmsMeasurementConsumerId = internalInitialReport.cmmsMeasurementConsumerId
                externalReportId = internalInitialReport.externalReportId
              }
            )
          )
        )
        .thenReturn(internalPendingReport)

      whenever(metricsMock.batchCreateMetrics(any()))
        .thenReturn(
          batchCreateMetricsResponse {
            metrics +=
              endTimesList.mapIndexed { index, end ->
                metric {
                  name =
                    MetricKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                      )
                      .toName()
                  reportingSet = PRIMITIVE_REPORTING_SETS.first().name
                  timeInterval = interval {
                    startTime = timestamp { seconds = startSec }
                    endTime = timestamp { seconds = end }
                  }
                  metricSpec = REACH_METRIC_SPEC
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        reportId = "report-id"
        report = report {
          tags.putAll(REPORT_TAGS)
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = PRIMITIVE_REPORTING_SETS.first().name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    MetricCalculationSpecKey(
                        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                        internalMetricCalculationSpec.externalMetricCalculationSpecId,
                      )
                      .toName()
                }
            }
          this.periodicTimeInterval = periodicTimeInterval
        }
      }

      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

      val batchCreateMetricsCaptor: KArgumentCaptor<BatchCreateMetricsRequest> = argumentCaptor()
      verifyBlocking(metricsMock, times(2)) {
        batchCreateMetrics(batchCreateMetricsCaptor.capture())
      }
    }

  @Test
  fun `createReport throws UNAUTHENTICATED when no principal is found`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.createReport(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createReport throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.last().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `createReport throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when parent is unspecified`() {
    val request = createReportRequest {
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when report is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when resource ID is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearTime()
          clearCreateTime()
          clearState()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when resource ID starts with number`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportId = "1s"
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearTime()
          clearCreateTime()
          clearState()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when resource ID is too long`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportId = "s".repeat(100)
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearTime()
          clearCreateTime()
          clearState()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when resource ID contains invalid char`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportId = "contain_invalid_char"
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearTime()
          clearCreateTime()
          clearState()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when time in Report is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearTime()
          clearCreateTime()
          clearState()
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when metricCalculationSpec name is invalid`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = PRIMITIVE_REPORTING_SETS.first().name
              value =
                ReportKt.reportingMetricCalculationSpec { metricCalculationSpecs += "badname" }
            }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("MetricCalculationSpec")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeIntervals is set and cumulative is true`() =
    runBlocking {
      val internalMetricCalculationSpec = internalMetricCalculationSpec {
        externalMetricCalculationSpecId = "1234"
        details =
          InternalMetricCalculationSpecKt.details {
            displayName = DISPLAY_NAME
            metricSpecs += INTERNAL_REACH_METRIC_SPEC
            cumulative = true
          }
      }
      whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
        .thenReturn(
          batchGetMetricCalculationSpecsResponse {
            metricCalculationSpecs += internalMetricCalculationSpec
          }
        )

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        reportId = "report-id"
        report =
          PENDING_REACH_REPORT.copy {
            clearName()
            clearCreateTime()
            clearState()
            timeIntervals = timeIntervals {
              timeIntervals += interval {
                startTime = START_TIME
                endTime = END_TIME
              }
            }
            reportingMetricEntries.clear()
            reportingMetricEntries +=
              ReportKt.reportingMetricEntry {
                key = PRIMITIVE_REPORTING_SETS.first().name
                value =
                  ReportKt.reportingMetricCalculationSpec {
                    metricCalculationSpecs +=
                      MetricCalculationSpecKey(
                          MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                          internalMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                  }
              }
          }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
            runBlocking { service.createReport(request) }
          }
        }
      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeIntervals timeIntervalsList is empty`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          timeIntervals = timeIntervals {}
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeInterval startTime is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += interval { endTime = timestamp { seconds = 5 } }
          }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeInterval endTime is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += interval { startTime = timestamp { seconds = 5 } }
          }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when TimeInterval endTime is before startTime`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportId = "report-id"
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp {
                seconds = 5
                nanos = 5
              }
              endTime = timestamp {
                seconds = 5
                nanos = 1
              }
            }
          }
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when PeriodicTimeInterval startTime is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          periodicTimeInterval = periodicTimeInterval {
            increment = duration { seconds = 5 }
            intervalCount = 3
          }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when PeriodicTimeInterval increment is unspecified`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          periodicTimeInterval = periodicTimeInterval {
            startTime = timestamp {
              seconds = 5
              nanos = 5
            }
            intervalCount = 3
          }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when PeriodicTimeInterval intervalCount is 0`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          periodicTimeInterval = periodicTimeInterval {
            startTime = timestamp {
              seconds = 5
              nanos = 5
            }
            increment = duration { seconds = 5 }
          }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when reportingMetricEntries is empty`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries.clear()
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when provided reporting set name is invalid`() {
    val invalidReportingSetName = "invalid"

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries += ReportKt.reportingMetricEntry { key = invalidReportingSetName }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains(invalidReportingSetName)
  }

  @Test
  fun `createReport throws PERMISSION_DENIED when any reporting set is not accessible to caller`() {
    val reportingSetNameForOtherMC =
      ReportingSetKey(
          MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId,
          ExternalId(120L).apiId.value,
        )
        .toName()
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry { key = reportingSetNameForOtherMC }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception.message).contains(reportingSetNameForOtherMC)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when reportingMetricCalculationSpec is not set`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry { key = PRIMITIVE_REPORTING_SETS.first().name }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when metricCalculationSpecs is empty`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = PRIMITIVE_REPORTING_SETS.first().name
              value = ReportKt.reportingMetricCalculationSpec {}
            }
        }
      reportId = "report-id"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws NOT_FOUND when report schedule not found`() = runBlocking {
    val externalReportScheduleId = "external-report-schedule-id"
    val nextReportCreationTime = timestamp { seconds = 1000 }

    whenever(
        internalReportsMock.createReport(
          eq(
            internalCreateReportRequest {
              report = INTERNAL_REACH_REPORTS.requestingReport
              externalReportId = "report-id"
              reportScheduleInfo =
                CreateReportRequestKt.reportScheduleInfo {
                  this.externalReportScheduleId = externalReportScheduleId
                  this.nextReportCreationTime = nextReportCreationTime
                }
            }
          )
        )
      )
      .thenThrow(
        Status.NOT_FOUND.withDescription("external_report_schedule_id").asRuntimeException()
      )

    val measurementConsumerKey = MEASUREMENT_CONSUMER_KEYS.first()
    val request = createReportRequest {
      parent = measurementConsumerKey.toName()
      reportId = "report-id"
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
    }

    val reportScheduleName =
      ReportScheduleKey(measurementConsumerKey.measurementConsumerId, externalReportScheduleId)
        .toName()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withReportScheduleInfoAndMeasurementConsumerPrincipal(
          ReportScheduleInfoServerInterceptor.ReportScheduleInfo(
            reportScheduleName,
            nextReportCreationTime,
          ),
          measurementConsumerKey.toName(),
          CONFIG,
        ) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("ReportSchedule")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when report schedule name is invalid`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      reportId = "report-id"
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
        }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withReportScheduleInfoAndMeasurementConsumerPrincipal(
          ReportScheduleInfoServerInterceptor.ReportScheduleInfo(
            "name123",
            timestamp { seconds = 1000 },
          ),
          MEASUREMENT_CONSUMER_KEYS.first().toName(),
          CONFIG,
        ) {
          runBlocking { service.createReport(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("reportScheduleName")
  }

  @Test
  fun `getReport returns the report with SUCCEEDED when all metrics are SUCCEEDED`() = runBlocking {
    whenever(
        metricsMock.batchGetMetrics(
          eq(
            batchGetMetricsRequest {
              parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
              names += SUCCEEDED_REACH_METRIC.name
            }
          )
        )
      )
      .thenReturn(batchGetMetricsResponse { metrics += SUCCEEDED_REACH_METRIC })

    val request = getReportRequest { name = PENDING_REACH_REPORT.name }

    val report =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.getReport(request) }
      }

    assertThat(report).isEqualTo(SUCCEEDED_REACH_REPORT)
  }

  @Test
  fun `getReport returns the report with FAILED when any metric FAILED`() = runBlocking {
    val failedReachMetric = RUNNING_REACH_METRIC.copy { state = Metric.State.FAILED }

    whenever(
        metricsMock.batchGetMetrics(
          eq(
            batchGetMetricsRequest {
              parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
              names += failedReachMetric.name
            }
          )
        )
      )
      .thenReturn(batchGetMetricsResponse { metrics += failedReachMetric })

    val request = getReportRequest { name = PENDING_REACH_REPORT.name }

    val report =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.getReport(request) }
      }

    assertThat(report)
      .isEqualTo(
        PENDING_REACH_REPORT.copy {
          state = Report.State.FAILED
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec = REACH_METRIC_CALCULATION_SPEC_NAME
              displayName = INTERNAL_REACH_METRIC_CALCULATION_SPEC.details.displayName
              reportingSet = SUCCEEDED_REACH_METRIC.reportingSet
              cumulative = INTERNAL_REACH_METRIC_CALCULATION_SPEC.details.cumulative
              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  metric = failedReachMetric.name
                  metricSpec = failedReachMetric.metricSpec
                  timeInterval = failedReachMetric.timeInterval
                  state = Metric.State.FAILED
                }
            }
        }
      )
  }

  @Test
  fun `getReport returns the report with RUNNING when metric is pending`(): Unit = runBlocking {
    val request = getReportRequest { name = PENDING_REACH_REPORT.name }

    val report =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.getReport(request) }
      }

    assertThat(report).isEqualTo(PENDING_REACH_REPORT)
  }

  @Test
  fun `getReport returns the report with RUNNING when there are more than max batch size metrics`():
    Unit = runBlocking {
    val startSec = 10L
    val incrementSec = 1L
    val intervalCount = BATCH_GET_METRICS_LIMIT + 1

    val endTimesList: List<Long> =
      (startSec + incrementSec until startSec + incrementSec + intervalCount).toList()
    val intervals: List<Interval> =
      endTimesList.map { end ->
        interval {
          startTime = timestamp { seconds = startSec }
          endTime = timestamp { seconds = end }
        }
      }

    val reportingMetrics =
      intervals.map { timeInterval ->
        buildInitialReportingMetric(timeInterval, INTERNAL_REACH_METRIC_SPEC, listOf())
      }

    val internalMetricCalculationSpec = internalMetricCalculationSpec {
      externalMetricCalculationSpecId = "1234"
      details =
        InternalMetricCalculationSpecKt.details {
          displayName = DISPLAY_NAME
          metricSpecs += INTERNAL_REACH_METRIC_SPEC
          cumulative = true
        }
    }
    whenever(internalMetricCalculationSpecsMock.batchGetMetricCalculationSpecs(any()))
      .thenReturn(
        batchGetMetricCalculationSpecsResponse {
          metricCalculationSpecs += internalMetricCalculationSpec
        }
      )

    val internalPendingReport = internalReport {
      details = InternalReportKt.details { tags.putAll(REPORT_TAGS) }
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId

      this.periodicTimeInterval = internalPeriodicTimeInterval {
        startTime = timestamp { seconds = startSec }
        increment = duration { seconds = incrementSec }
        this.intervalCount = intervalCount
      }

      externalReportId = "report-id"
      createTime = Instant.now().toProtoTime()

      val updatedReportingMetrics =
        reportingMetrics.mapIndexed { requestId, request ->
          request.copy {
            this.createMetricRequestId = requestId.toString()
            externalMetricId = ExternalId(REACH_METRIC_ID_BASE_LONG + requestId).apiId.value
          }
        }

      reportingMetricEntries.putAll(
        buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
          reportingSetId = PRIMITIVE_REPORTING_SETS.first().resourceId,
          reportingMetrics = updatedReportingMetrics,
          metricCalculationSpecId = internalMetricCalculationSpec.externalMetricCalculationSpecId,
        )
      )
    }

    whenever(
        internalReportsMock.getReport(
          eq(
            internalGetReportRequest {
              cmmsMeasurementConsumerId = internalPendingReport.cmmsMeasurementConsumerId
              externalReportId = internalPendingReport.externalReportId
            }
          )
        )
      )
      .thenReturn(internalPendingReport)

    whenever(metricsMock.batchGetMetrics(any()))
      .thenReturn(
        batchGetMetricsResponse {
          metrics +=
            endTimesList.mapIndexed { index, end ->
              metric {
                name =
                  MetricKey(
                      MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                      ExternalId(REACH_METRIC_ID_BASE_LONG + index).apiId.value,
                    )
                    .toName()
                reportingSet = PRIMITIVE_REPORTING_SETS.first().name
                timeInterval = interval {
                  startTime = timestamp { seconds = startSec }
                  endTime = timestamp { seconds = end }
                }
                metricSpec = REACH_METRIC_SPEC
                state = Metric.State.RUNNING
                createTime = Instant.now().toProtoTime()
              }
            }
        }
      )

    val request = getReportRequest { name = internalPendingReport.resourceName }

    withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
      runBlocking { service.getReport(request) }
    }

    val batchGetMetricsCaptor: KArgumentCaptor<BatchGetMetricsRequest> = argumentCaptor()
    verifyBlocking(metricsMock, times(2)) { batchGetMetrics(batchGetMetricsCaptor.capture()) }
  }

  @Test
  fun `getReport throws INVALID_ARGUMENT when Report name is invalid`() {
    val request = getReportRequest { name = "INVALID_REPORT_NAME" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `getReport throws PERMISSION_DENIED when Report name is not accessible`() {
    val inaccessibleReportName =
      ReportKey(MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId, "report-id").toName()
    val request = getReportRequest { name = inaccessibleReportName }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getReport throws PERMISSION_DENIED when MeasurementConsumer's identity does not match`() {
    val request = getReportRequest { name = PENDING_REACH_REPORT.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.last().toName(), CONFIG) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `getReport throws UNAUTHENTICATED when the caller is not a MeasurementConsumer`() {
    val request = getReportRequest { name = PENDING_REACH_REPORT.name }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.getReport(request) }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReports returns without a next page token when there is no previous page token`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports += PENDING_REACH_REPORT
      reports += PENDING_WATCH_DURATION_REPORT
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns with a next page token when there is no previous page token`() {
    val pageSize = 1
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      this.pageSize = pageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports.add(PENDING_REACH_REPORT)

      nextPageToken =
        listReportsPageToken {
            this.pageSize = pageSize
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            lastReport =
              ListReportsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
                createTime = PENDING_REACH_REPORT.createTime
                externalReportId = PENDING_REACH_REPORT.resourceId
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = pageSize + 1
          this.filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns without a next page token when there is a previous page token`() =
    runBlocking {
      whenever(internalReportsMock.streamReports(any()))
        .thenReturn(flowOf(INTERNAL_WATCH_DURATION_REPORTS.pendingReport))

      val pageSize = 1
      val request = listReportsRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        this.pageSize = pageSize
        pageToken =
          listReportsPageToken {
              this.pageSize = pageSize
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              lastReport =
                ListReportsPageTokenKt.previousPageEnd {
                  cmmsMeasurementConsumerId =
                    MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
                  createTime = PENDING_REACH_REPORT.createTime
                  externalReportId = PENDING_REACH_REPORT.resourceId
                }
            }
            .toByteString()
            .base64UrlEncode()
      }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse { reports.add(PENDING_WATCH_DURATION_REPORT) }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = pageSize + 1
            this.filter =
              StreamReportsRequestKt.filter {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
                after =
                  StreamReportsRequestKt.afterFilter {
                    createTime = PENDING_REACH_REPORT.createTime
                    externalReportId = PENDING_REACH_REPORT.resourceId
                  }
              }
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listReports with page size replaced with a valid value and no previous page token`() {
    val invalidPageSize = MAX_PAGE_SIZE * 2
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = invalidPageSize
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports += PENDING_REACH_REPORT
      reports += PENDING_WATCH_DURATION_REPORT
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = MAX_PAGE_SIZE + 1
          this.filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports with invalid page size replaced with the one in previous page token`() =
    runBlocking {
      whenever(internalReportsMock.streamReports(any()))
        .thenReturn(flowOf(INTERNAL_WATCH_DURATION_REPORTS.pendingReport))

      val invalidPageSize = MAX_PAGE_SIZE * 2
      val previousPageSize = 1

      val request = listReportsRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        pageSize = invalidPageSize
        pageToken =
          listReportsPageToken {
              pageSize = previousPageSize
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              lastReport =
                ListReportsPageTokenKt.previousPageEnd {
                  cmmsMeasurementConsumerId =
                    MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
                  createTime = PENDING_REACH_REPORT.createTime
                  externalReportId = PENDING_REACH_REPORT.resourceId
                }
            }
            .toByteString()
            .base64UrlEncode()
      }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse { reports += PENDING_WATCH_DURATION_REPORT }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = previousPageSize + 1
            this.filter =
              StreamReportsRequestKt.filter {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
                after =
                  StreamReportsRequestKt.afterFilter {
                    createTime = PENDING_REACH_REPORT.createTime
                    externalReportId = PENDING_REACH_REPORT.resourceId
                  }
              }
          }
        )

      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listReports with page size replacing the one in previous page token`() = runBlocking {
    whenever(internalReportsMock.streamReports(any()))
      .thenReturn(flowOf(INTERNAL_WATCH_DURATION_REPORTS.pendingReport))

    val newPageSize = 10
    val previousPageSize = 1
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = newPageSize
      pageToken =
        listReportsPageToken {
            pageSize = previousPageSize
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            lastReport =
              ListReportsPageTokenKt.previousPageEnd {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
                createTime = PENDING_REACH_REPORT.createTime
                externalReportId = PENDING_REACH_REPORT.resourceId
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse { reports += PENDING_WATCH_DURATION_REPORT }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = newPageSize + 1
          this.filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              after =
                StreamReportsRequestKt.afterFilter {
                  createTime = PENDING_REACH_REPORT.createTime
                  externalReportId = PENDING_REACH_REPORT.resourceId
                }
            }
        }
      )

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports returns reports with SUCCEEDED states when metrics are SUCCEEDED`() =
    runBlocking {
      whenever(metricsMock.batchGetMetrics(any())).thenAnswer {
        val request = it.arguments[0] as BatchGetMetricsRequest
        val metricsMap =
          mapOf(
            SUCCEEDED_REACH_METRIC.name to SUCCEEDED_REACH_METRIC,
            SUCCEEDED_WATCH_DURATION_METRIC.name to SUCCEEDED_WATCH_DURATION_METRIC,
          )
        batchGetMetricsResponse {
          metrics += request.namesList.map { metricName -> metricsMap.getValue(metricName) }
        }
      }

      val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }

      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }

      val expected = listReportsResponse {
        reports += SUCCEEDED_REACH_REPORT
        reports +=
          PENDING_WATCH_DURATION_REPORT.copy {
            state = Report.State.SUCCEEDED
            metricCalculationResults +=
              ReportKt.metricCalculationResult {
                metricCalculationSpec = WATCH_DURATION_METRIC_CALCULATION_SPEC_NAME
                displayName = DISPLAY_NAME
                reportingSet = SUCCEEDED_WATCH_DURATION_METRIC.reportingSet
                cumulative = false
                resultAttributes +=
                  ReportKt.MetricCalculationResultKt.resultAttribute {
                    metric = SUCCEEDED_WATCH_DURATION_METRIC.name
                    metricSpec = SUCCEEDED_WATCH_DURATION_METRIC.metricSpec
                    timeInterval = SUCCEEDED_WATCH_DURATION_METRIC.timeInterval
                    state = SUCCEEDED_WATCH_DURATION_METRIC.state
                    metricResult = SUCCEEDED_WATCH_DURATION_METRIC.result
                  }
              }
          }
      }

      verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
        .isEqualTo(
          streamReportsRequest {
            limit = DEFAULT_PAGE_SIZE + 1
            this.filter =
              StreamReportsRequestKt.filter {
                cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
              }
          }
        )
      assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }

  @Test
  fun `listReports returns reports with FAILED states when metrics are FAILED`() = runBlocking {
    val failedReachMetric = RUNNING_REACH_METRIC.copy { state = Metric.State.FAILED }
    val failedWatchDurationMetric =
      RUNNING_WATCH_DURATION_METRIC.copy { state = Metric.State.FAILED }

    whenever(metricsMock.batchGetMetrics(any())).thenAnswer {
      val request = it.arguments[0] as BatchGetMetricsRequest
      val metricsMap =
        mapOf(
          failedReachMetric.name to failedReachMetric,
          failedWatchDurationMetric.name to failedWatchDurationMetric,
        )
      batchGetMetricsResponse {
        metrics += request.namesList.map { metricName -> metricsMap.getValue(metricName) }
      }
    }

    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse {
      reports +=
        PENDING_REACH_REPORT.copy {
          state = Report.State.FAILED
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec = REACH_METRIC_CALCULATION_SPEC_NAME
              displayName = INTERNAL_REACH_METRIC_CALCULATION_SPEC.details.displayName
              reportingSet = SUCCEEDED_REACH_METRIC.reportingSet
              cumulative = INTERNAL_REACH_METRIC_CALCULATION_SPEC.details.cumulative
              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  metric = failedReachMetric.name
                  metricSpec = failedReachMetric.metricSpec
                  timeInterval = failedReachMetric.timeInterval
                  state = Metric.State.FAILED
                }
            }
        }
      reports +=
        PENDING_WATCH_DURATION_REPORT.copy {
          state = Report.State.FAILED
          metricCalculationResults +=
            ReportKt.metricCalculationResult {
              metricCalculationSpec = WATCH_DURATION_METRIC_CALCULATION_SPEC_NAME
              displayName = INTERNAL_WATCH_DURATION_METRIC_CALCULATION_SPEC.details.displayName
              reportingSet = SUCCEEDED_WATCH_DURATION_METRIC.reportingSet
              cumulative = INTERNAL_WATCH_DURATION_METRIC_CALCULATION_SPEC.details.cumulative
              resultAttributes +=
                ReportKt.MetricCalculationResultKt.resultAttribute {
                  metric = failedWatchDurationMetric.name
                  metricSpec = failedWatchDurationMetric.metricSpec
                  timeInterval = failedWatchDurationMetric.timeInterval
                  state = Metric.State.FAILED
                }
            }
        }
    }

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = DEFAULT_PAGE_SIZE + 1
          this.filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )
    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)
  }

  @Test
  fun `listReports with a filter returns filtered results`() = runBlocking {
    val pageSize = 2
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      this.pageSize = pageSize
      filter = "name != '${PENDING_WATCH_DURATION_REPORT.name}'"
    }

    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.listReports(request) }
      }

    val expected = listReportsResponse { reports.add(PENDING_REACH_REPORT) }

    assertThat(result).ignoringRepeatedFieldOrder().isEqualTo(expected)

    verifyProtoArgument(internalReportsMock, ReportsCoroutineImplBase::streamReports)
      .isEqualTo(
        streamReportsRequest {
          limit = pageSize + 1
          this.filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
            }
        }
      )
  }

  @Test
  fun `listReports throws UNAUTHENTICATED when no principal is found`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }
    val exception =
      assertFailsWith<StatusRuntimeException> { runBlocking { service.listReports(request) } }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReports throws PERMISSION_DENIED when MeasurementConsumer caller doesn't match`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.last().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
  }

  @Test
  fun `listReports throws UNAUTHENTICATED when the caller is not MeasurementConsumer`() {
    val request = listReportsRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withDataProviderPrincipal(DataProviderKey(ExternalId(550L).apiId.value).toName()) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.UNAUTHENTICATED)
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when page size is less than 0`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageSize = -1
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when parent is unspecified`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(ListReportsRequest.getDefaultInstance()) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when mc id doesn't match one in page token`() {
    val cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.last().measurementConsumerId
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      pageToken =
        listReportsPageToken {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            lastReport =
              ListReportsPageTokenKt.previousPageEnd {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                externalReportId = "220L"
              }
          }
          .toByteString()
          .base64UrlEncode()
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `listReports throws INVALID_ARGUMENT when filter is invalid`() {
    val request = listReportsRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      filter = "name <<< '${PENDING_WATCH_DURATION_REPORT.name}'"
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.listReports(request) }
        }
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("not a valid CEL expression")
  }

  companion object {
    private fun buildInitialReportingMetric(
      timeInterval: Interval,
      metricSpec: InternalMetricSpec,
      groupingPredicates: List<String>,
    ): InternalReport.ReportingMetric {
      return InternalReportKt.reportingMetric {
        details =
          InternalReportKt.ReportingMetricKt.details {
            this.metricSpec = metricSpec
            this.timeInterval = timeInterval
            this.groupingPredicates += groupingPredicates
          }
      }
    }

    private fun buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
      reportingSetId: String,
      reportingMetrics: List<InternalReport.ReportingMetric>,
      metricCalculationSpecId: String,
    ): Map<String, InternalReport.ReportingMetricCalculationSpec> {
      return mapOf(
        reportingSetId to
          InternalReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecReportingMetrics +=
              InternalReportKt.metricCalculationSpecReportingMetrics {
                externalMetricCalculationSpecId = metricCalculationSpecId
                this.reportingMetrics += reportingMetrics
              }
          }
      )
    }

    private fun buildInternalReports(
      cmmsMeasurementConsumerId: String,
      timeIntervals: List<Interval>,
      reportingSetId: String,
      reportingMetrics: List<InternalReport.ReportingMetric>,
      metricCalculationSpecId: String,
      reportIdBase: String = "",
      metricIdBaseLong: Long = REACH_METRIC_ID_BASE_LONG,
    ): InternalReports {
      // Internal reports of reach
      val internalRequestingReport = internalReport {
        details = InternalReportKt.details { tags.putAll(REPORT_TAGS) }
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        this.timeIntervals = Intervals { this.timeIntervals += timeIntervals }

        reportingMetricEntries.putAll(
          buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
            reportingSetId = reportingSetId,
            reportingMetrics = reportingMetrics,
            metricCalculationSpecId = metricCalculationSpecId,
          )
        )
      }
      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = reportIdBase + "report-id"
          createTime = Instant.now().toProtoTime()

          val initialReportingMetrics =
            reportingMetrics.mapIndexed { requestId, request ->
              request.copy {
                createMetricRequestId = ExternalId(metricIdBaseLong + requestId).apiId.value
              }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              reportingSetId = reportingSetId,
              reportingMetrics = initialReportingMetrics,
              metricCalculationSpecId = metricCalculationSpecId,
            )
          )
        }
      val internalPendingReport =
        internalInitialReport.copy {
          val pendingReportingMetrics =
            reportingMetrics.mapIndexed { requestId, request ->
              request.copy {
                createMetricRequestId = ExternalId(metricIdBaseLong + requestId).apiId.value
                externalMetricId = createMetricRequestId
              }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              reportingSetId = reportingSetId,
              reportingMetrics = pendingReportingMetrics,
              metricCalculationSpecId = metricCalculationSpecId,
            )
          )
        }
      return InternalReports(internalRequestingReport, internalInitialReport, internalPendingReport)
    }

    // Measurement consumers
    private val MEASUREMENT_CONSUMER_KEYS: List<MeasurementConsumerKey> =
      (1L..2L).map { MeasurementConsumerKey(ExternalId(it + 110L).apiId.value) }

    private val CONFIG = measurementConsumerConfig { apiKey = API_AUTHENTICATION_KEY }

    // Reporting sets
    private val PRIMITIVE_REPORTING_SETS: List<ReportingSet> =
      (0..1).map { index ->
        reportingSet {
          name =
            ReportingSetKey(
                MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
                ExternalId(index + 110L).apiId.value,
              )
              .toName()
          filter = "AGE>18"
          displayName = "reporting-set-$index-display-name"
          primitive = ReportingSetKt.primitive { cmmsEventGroups += "event-group-$index" }
        }
      }

    // Time intervals
    private val START_INSTANT = Instant.now()
    private val END_INSTANT = START_INSTANT.plus(Duration.ofDays(1))

    private val START_TIME = START_INSTANT.toProtoTime()
    private val END_TIME = END_INSTANT.toProtoTime()

    // Metric Specs
    private val METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      reachVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }

      reachAndFrequencyParams =
        MetricSpecConfigKt.reachAndFrequencyParams {
          reachPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
        }
      reachAndFrequencyVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = IMPRESSION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequencyPerUser = IMPRESSION_MAXIMUM_FREQUENCY_PER_USER
        }
      impressionCountVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = IMPRESSION_VID_SAMPLING_START
          width = IMPRESSION_VID_SAMPLING_WIDTH
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          privacyParams =
            MetricSpecConfigKt.differentialPrivacyParams {
              epsilon = WATCH_DURATION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
      watchDurationVidSamplingInterval =
        MetricSpecConfigKt.vidSamplingInterval {
          start = WATCH_DURATION_VID_SAMPLING_START
          width = WATCH_DURATION_VID_SAMPLING_WIDTH
        }
    }

    private val REACH_METRIC_SPEC: MetricSpec = metricSpec {
      reach =
        MetricSpecKt.reachParams {
          privacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }
    }
    private val INTERNAL_REACH_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      reach =
        InternalMetricSpecKt.reachParams {
          privacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_ONLY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
        }
      vidSamplingInterval =
        InternalMetricSpecKt.vidSamplingInterval {
          start = REACH_ONLY_VID_SAMPLING_START
          width = REACH_ONLY_VID_SAMPLING_WIDTH
        }
    }
    private val REACH_AND_FREQUENCY_METRIC_SPEC: MetricSpec = metricSpec {
      reachAndFrequency =
        MetricSpecKt.reachAndFrequencyParams {
          reachPrivacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
        }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }
    }
    private val INTERNAL_REACH_AND_FREQUENCY_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      reachAndFrequency =
        InternalMetricSpecKt.reachAndFrequencyParams {
          reachPrivacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_REACH_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          frequencyPrivacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              epsilon = REACH_FREQUENCY_FREQUENCY_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumFrequency = REACH_FREQUENCY_MAXIMUM_FREQUENCY
        }
      vidSamplingInterval =
        InternalMetricSpecKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }
    }

    private val WATCH_DURATION_METRIC_SPEC: MetricSpec = metricSpec {
      watchDuration =
        MetricSpecKt.watchDurationParams {
          privacyParams =
            MetricSpecKt.differentialPrivacyParams {
              epsilon = WATCH_DURATION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = WATCH_DURATION_VID_SAMPLING_START
          width = WATCH_DURATION_VID_SAMPLING_WIDTH
        }
    }
    private val INTERNAL_WATCH_DURATION_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      watchDuration =
        InternalMetricSpecKt.watchDurationParams {
          privacyParams =
            InternalMetricSpecKt.differentialPrivacyParams {
              epsilon = WATCH_DURATION_EPSILON
              delta = DIFFERENTIAL_PRIVACY_DELTA
            }
          maximumWatchDurationPerUser = MAXIMUM_WATCH_DURATION_PER_USER
        }
      vidSamplingInterval =
        InternalMetricSpecKt.vidSamplingInterval {
          start = WATCH_DURATION_VID_SAMPLING_START
          width = WATCH_DURATION_VID_SAMPLING_WIDTH
        }
    }

    // Metrics
    private const val REACH_METRIC_ID_BASE_LONG: Long = 220L
    private const val WATCH_DURATION_METRIC_ID_BASE_LONG: Long = 320L
    private val REQUESTING_REACH_METRIC = metric {
      reportingSet = PRIMITIVE_REPORTING_SETS.first().name
      timeInterval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }
      metricSpec = REACH_METRIC_SPEC
    }

    private val RUNNING_REACH_METRIC =
      REQUESTING_REACH_METRIC.copy {
        name =
          MetricKey(
              MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
              ExternalId(REACH_METRIC_ID_BASE_LONG).apiId.value,
            )
            .toName()
        state = Metric.State.RUNNING
        createTime = Instant.now().toProtoTime()
      }

    private val SUCCEEDED_REACH_METRIC =
      RUNNING_REACH_METRIC.copy {
        state = Metric.State.SUCCEEDED
        result = metricResult { reach = reachResult { value = 123 } }
      }

    private val RUNNING_WATCH_DURATION_METRIC = metric {
      name =
        MetricKey(
            MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
            ExternalId(WATCH_DURATION_METRIC_ID_BASE_LONG).apiId.value,
          )
          .toName()
      reportingSet = PRIMITIVE_REPORTING_SETS.first().name
      timeInterval = interval {
        startTime = START_TIME
        endTime = END_TIME
      }
      metricSpec = WATCH_DURATION_METRIC_SPEC
      state = Metric.State.RUNNING
      createTime = Instant.now().toProtoTime()
    }

    private val SUCCEEDED_WATCH_DURATION_METRIC =
      RUNNING_WATCH_DURATION_METRIC.copy {
        state = Metric.State.SUCCEEDED
        result = metricResult { watchDuration = watchDurationResult { value = 123.0 } }
      }

    // Reports
    private const val DISPLAY_NAME = "DISPLAY_NAME"
    private val REPORT_TAGS = mapOf("tag1" to "tag_value1", "tag2" to "tag_value2")
    // Internal reports
    private val INITIAL_REACH_REPORTING_METRIC =
      buildInitialReportingMetric(
        interval {
          startTime = START_TIME
          endTime = END_TIME
        },
        INTERNAL_REACH_METRIC_SPEC,
        listOf(),
      )

    private const val REACH_METRIC_CALCULATION_SPEC_ID = "R1234"
    private val REACH_METRIC_CALCULATION_SPEC_NAME =
      "${MEASUREMENT_CONSUMER_KEYS.first().toName()}/metricCalculationSpecs/$REACH_METRIC_CALCULATION_SPEC_ID"
    private val INTERNAL_REACH_METRIC_CALCULATION_SPEC = internalMetricCalculationSpec {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
      externalMetricCalculationSpecId = REACH_METRIC_CALCULATION_SPEC_ID
      details =
        InternalMetricCalculationSpecKt.details {
          displayName = DISPLAY_NAME
          metricSpecs += INTERNAL_REACH_METRIC_SPEC
          cumulative = false
        }
    }

    private val INTERNAL_REACH_REPORTS =
      buildInternalReports(
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
        timeIntervals =
          listOf(
            interval {
              startTime = START_TIME
              endTime = END_TIME
            }
          ),
        reportingSetId = PRIMITIVE_REPORTING_SETS.first().resourceId,
        reportingMetrics = listOf(INITIAL_REACH_REPORTING_METRIC),
        metricCalculationSpecId = REACH_METRIC_CALCULATION_SPEC_ID,
        reportIdBase = "reach-",
      )

    private val INITIAL_WATCH_DURATION_REPORTING_METRIC =
      buildInitialReportingMetric(
        interval {
          startTime = START_TIME
          endTime = END_TIME
        },
        INTERNAL_WATCH_DURATION_METRIC_SPEC,
        listOf(),
      )

    private const val WATCH_DURATION_METRIC_CALCULATION_SPEC_ID = "W1234"
    private val WATCH_DURATION_METRIC_CALCULATION_SPEC_NAME =
      "${MEASUREMENT_CONSUMER_KEYS.first().toName()}/metricCalculationSpecs/$WATCH_DURATION_METRIC_CALCULATION_SPEC_ID"
    private val INTERNAL_WATCH_DURATION_METRIC_CALCULATION_SPEC = internalMetricCalculationSpec {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
      externalMetricCalculationSpecId = WATCH_DURATION_METRIC_CALCULATION_SPEC_ID
      details =
        InternalMetricCalculationSpecKt.details {
          displayName = DISPLAY_NAME
          metricSpecs += INTERNAL_WATCH_DURATION_METRIC_SPEC
          cumulative = false
        }
    }

    private val INTERNAL_WATCH_DURATION_REPORTS =
      buildInternalReports(
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
        timeIntervals =
          listOf(
            interval {
              startTime = START_TIME
              endTime = END_TIME
            }
          ),
        reportingSetId = PRIMITIVE_REPORTING_SETS.first().resourceId,
        reportingMetrics = listOf(INITIAL_WATCH_DURATION_REPORTING_METRIC),
        metricCalculationSpecId = WATCH_DURATION_METRIC_CALCULATION_SPEC_ID,
        reportIdBase = "duration-",
        metricIdBaseLong = WATCH_DURATION_METRIC_ID_BASE_LONG,
      )

    // Public reports
    private val PENDING_REACH_REPORT: Report = report {
      name = INTERNAL_REACH_REPORTS.pendingReport.resourceName
      tags.putAll(REPORT_TAGS)
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = PRIMITIVE_REPORTING_SETS.first().name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += REACH_METRIC_CALCULATION_SPEC_NAME
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = START_TIME
          endTime = END_TIME
        }
      }
      state = Report.State.RUNNING
      createTime = INTERNAL_REACH_REPORTS.pendingReport.createTime
    }

    private val SUCCEEDED_REACH_REPORT =
      PENDING_REACH_REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            metricCalculationSpec = REACH_METRIC_CALCULATION_SPEC_NAME
            displayName = DISPLAY_NAME
            reportingSet = SUCCEEDED_REACH_METRIC.reportingSet
            cumulative = false
            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                metric = SUCCEEDED_REACH_METRIC.name
                metricSpec = SUCCEEDED_REACH_METRIC.metricSpec
                timeInterval = SUCCEEDED_REACH_METRIC.timeInterval
                state = SUCCEEDED_REACH_METRIC.state
                metricResult = SUCCEEDED_REACH_METRIC.result
              }
          }
      }

    private val PENDING_WATCH_DURATION_REPORT: Report = report {
      name = INTERNAL_WATCH_DURATION_REPORTS.pendingReport.resourceName
      tags.putAll(REPORT_TAGS)
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = PRIMITIVE_REPORTING_SETS.first().name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += WATCH_DURATION_METRIC_CALCULATION_SPEC_NAME
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = START_TIME
          endTime = END_TIME
        }
      }
      state = Report.State.RUNNING
      createTime = INTERNAL_WATCH_DURATION_REPORTS.pendingReport.createTime
    }
  }
}

private val ReportingSet.resourceKey: ReportingSetKey
  get() = ReportingSetKey.fromName(name)!!
private val ReportingSet.resourceId: String
  get() = resourceKey.reportingSetId

private val InternalReport.resourceKey: ReportKey
  get() = ReportKey(cmmsMeasurementConsumerId, externalReportId)
private val InternalReport.resourceName: String
  get() = resourceKey.toName()

private val Report.resourceKey: ReportKey
  get() = ReportKey.fromName(name)!!
private val Report.resourceId: String
  get() = resourceKey.reportId
