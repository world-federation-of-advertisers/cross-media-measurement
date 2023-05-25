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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Duration
import java.time.Instant
import kotlin.test.assertFailsWith
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
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.MetricSpec as InternalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt as InternalMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.Report as InternalReport
import org.wfanet.measurement.internal.reporting.v2.ReportKt as InternalReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.TimeInterval as InternalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportRequest as internalCreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest as internalGetReportRequest
import org.wfanet.measurement.internal.reporting.v2.metricSpec as internalMetricSpec
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval as internalPeriodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.report as internalReport
import org.wfanet.measurement.internal.reporting.v2.timeInterval as internalTimeInterval
import org.wfanet.measurement.internal.reporting.v2.timeIntervals as internalTimeIntervals
import org.wfanet.measurement.reporting.v2alpha.BatchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricResultKt.reachResult
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.batchCreateMetricsResponse
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricResult
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.periodicTimeInterval
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.timeInterval
import org.wfanet.measurement.reporting.v2alpha.timeIntervals

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
private const val REACH_FREQUENCY_MAXIMUM_FREQUENCY_PER_USER = 10

private const val IMPRESSION_VID_SAMPLING_WIDTH = 62.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_VID_SAMPLING_START = 143.0f / NUMBER_VID_BUCKETS
private const val IMPRESSION_EPSILON = 0.0011
private const val IMPRESSION_MAXIMUM_FREQUENCY_PER_USER = 60

private const val WATCH_DURATION_VID_SAMPLING_WIDTH = 95.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_VID_SAMPLING_START = 205.0f / NUMBER_VID_BUCKETS
private const val WATCH_DURATION_EPSILON = 0.001
private const val MAXIMUM_WATCH_DURATION_PER_USER = 4000

private const val DIFFERENTIAL_PRIVACY_DELTA = 1e-12

private const val MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS = 1000

@RunWith(JUnit4::class)
class ReportsServiceTest {

  private data class InternalReports(
    val requestingReport: InternalReport,
    val initialReport: InternalReport,
    val pendingReport: InternalReport,
  )

  private val internalReportsMock: ReportsCoroutineImplBase = mockService {
    onBlocking {
        createReport(eq(internalCreateReportRequest { report = INTERNAL_REPORTS.requestingReport }))
      }
      .thenReturn(INTERNAL_REPORTS.initialReport)
    onBlocking {
        getReport(
          eq(
            internalGetReportRequest {
              cmmsMeasurementConsumerId = INTERNAL_REPORTS.initialReport.cmmsMeasurementConsumerId
              externalReportId = INTERNAL_REPORTS.initialReport.externalReportId
            }
          )
        )
      }
      .thenReturn(INTERNAL_REPORTS.pendingReport)
  }

  private val metricsMock: MetricsGrpcKt.MetricsCoroutineImplBase = mockService {
    onBlocking { batchCreateMetrics(any()) }
      .thenReturn(batchCreateMetricsResponse { metrics += RUNNING_REACH_METRIC })
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(internalReportsMock)
    addService(metricsMock)
  }

  private lateinit var service: ReportsService

  @Before
  fun initService() {
    service =
      ReportsService(
        InternalReportsCoroutineStub(grpcTestServerRule.channel),
        MetricsGrpcKt.MetricsCoroutineStub(grpcTestServerRule.channel),
        METRIC_SPEC_CONFIG
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
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
      .isEqualTo(
        batchCreateMetricsRequest {
          parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
          requests += createMetricRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            metric = REQUESTING_REACH_METRIC
            requestId = "0"
          }
        }
      )

    assertThat(result).isEqualTo(PENDING_REACH_REPORT)
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
                requestId = "0"
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
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    val expected =
      PENDING_REACH_REPORT.copy {
        state = Report.State.SUCCEEDED
        metricCalculationResults +=
          ReportKt.metricCalculationResult {
            displayName = DISPLAY_NAME
            reportingSet = SUCCEEDED_REACH_METRIC.reportingSet
            cumulative = false
            resultAttributes +=
              ReportKt.MetricCalculationResultKt.resultAttribute {
                timeInterval = SUCCEEDED_REACH_METRIC.timeInterval
                metricResult = SUCCEEDED_REACH_METRIC.result
              }
          }
      }

    assertThat(result).isEqualTo(expected)
  }

  @Test
  fun `createReport returns report with two metrics when there are two time intervals`() =
    runBlocking {
      val displayName = DISPLAY_NAME
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val timeIntervalsList =
        listOf(
          timeInterval {
            startTime = START_TIME
            endTime = END_TIME
          },
          timeInterval {
            startTime = END_TIME
            endTime = END_INSTANT.plus(Duration.ofDays(1)).toProtoTime()
          }
        )

      val internalTimeIntervals =
        listOf(
          internalTimeInterval {
            startTime = START_TIME
            endTime = END_TIME
          },
          internalTimeInterval {
            startTime = END_TIME
            endTime = END_INSTANT.plus(Duration.ofDays(1)).toProtoTime()
          }
        )

      val initialReportingMetrics: List<InternalReport.ReportingMetric> =
        internalTimeIntervals.map { timeInterval ->
          buildInitialReportingMetric(
            targetReportingSet.externalId,
            timeInterval,
            INTERNAL_REACH_METRIC_SPEC,
            listOf()
          )
        }

      val (internalRequestingReport, internalInitialReport, internalPendingReport) =
        buildInternalReports(
          MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
          internalTimeIntervals,
          targetReportingSet.externalId,
          initialReportingMetrics,
          listOf(),
        )

      whenever(
          internalReportsMock.createReport(
            eq(internalCreateReportRequest { report = internalRequestingReport })
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
                        ExternalId(EXTERNAL_METRIC_ID_BASE + index).apiId.value
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  ReportKt.metricCalculationSpec {
                    this.displayName = displayName
                    metricSpecs += REACH_METRIC_SPEC
                    cumulative = false
                  }
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeIntervalsList }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = requestId.toString()
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
  fun `createReport returns report with two metrics when there are two groupings`() = runBlocking {
    val displayName = DISPLAY_NAME
    val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()

    val predicates1 = listOf("gender == MALE", "gender == FEMALE")
    val predicates2 = listOf("age == 18_34", "age == 55_PLUS")
    val internalGroupings =
      listOf(
        InternalReportKt.MetricCalculationSpecKt.grouping { predicates += predicates1 },
        InternalReportKt.MetricCalculationSpecKt.grouping { predicates += predicates2 }
      )
    val groupingsCartesianProduct: List<List<String>> =
      predicates1.flatMap { filter1 -> predicates2.map { filter2 -> listOf(filter1, filter2) } }

    val timeInterval = timeInterval {
      startTime = START_TIME
      endTime = END_TIME
    }

    val internalTimeInterval = internalTimeInterval {
      startTime = START_TIME
      endTime = END_TIME
    }

    val initialReportingMetrics: List<InternalReport.ReportingMetric> =
      groupingsCartesianProduct.map { filters ->
        buildInitialReportingMetric(
          targetReportingSet.externalId,
          internalTimeInterval,
          INTERNAL_REACH_METRIC_SPEC,
          filters
        )
      }

    val (internalRequestingReport, internalInitialReport, internalPendingReport) =
      buildInternalReports(
        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
        listOf(internalTimeInterval),
        targetReportingSet.externalId,
        initialReportingMetrics,
        internalGroupings,
      )

    whenever(
        internalReportsMock.createReport(
          eq(internalCreateReportRequest { report = internalRequestingReport })
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
      groupingsCartesianProduct.map { filters ->
        metric {
          reportingSet = targetReportingSet.name
          this.timeInterval = timeInterval
          metricSpec = REACH_METRIC_SPEC
          this.filters += filters
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
                      ExternalId(EXTERNAL_METRIC_ID_BASE + index).apiId.value
                    )
                    .toName()
                state = Metric.State.RUNNING
                createTime = Instant.now().toProtoTime()
              }
            }
        }
      )

    val requestingReport = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = targetReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  this.displayName = displayName
                  metricSpecs += REACH_METRIC_SPEC
                  this.groupings +=
                    listOf(
                      ReportKt.grouping { predicates += predicates1 },
                      ReportKt.grouping { predicates += predicates2 }
                    )
                  cumulative = false
                }
            }
        }
      timeIntervals = timeIntervals { timeIntervals += timeInterval }
    }

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report = requestingReport
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
      .isEqualTo(
        batchCreateMetricsRequest {
          parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
          requests +=
            requestingMetrics.mapIndexed { requestId, metric ->
              createMetricRequest {
                parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                this.metric = metric
                this.requestId = requestId.toString()
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
      val displayName = DISPLAY_NAME
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val metricSpecs = listOf(REACH_METRIC_SPEC, FREQUENCY_HISTOGRAM_METRIC_SPEC)
      val internalMetricSpecs =
        listOf(INTERNAL_REACH_METRIC_SPEC, INTERNAL_FREQUENCY_HISTOGRAM_METRIC_SPEC)
      val timeInterval = timeInterval {
        startTime = START_TIME
        endTime = END_TIME
      }
      val internalTimeInterval = internalTimeInterval {
        startTime = START_TIME
        endTime = END_TIME
      }

      val initialReportingMetrics: List<InternalReport.ReportingMetric> =
        internalMetricSpecs.map { metricSpec ->
          buildInitialReportingMetric(
            targetReportingSet.externalId,
            internalTimeInterval,
            metricSpec,
            listOf()
          )
        }

      val (internalRequestingReport, internalInitialReport, internalPendingReport) =
        buildInternalReports(
          MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
          listOf(internalTimeInterval),
          targetReportingSet.externalId,
          initialReportingMetrics,
          listOf(),
        )

      whenever(
          internalReportsMock.createReport(
            eq(internalCreateReportRequest { report = internalRequestingReport })
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
                        ExternalId(EXTERNAL_METRIC_ID_BASE + index).apiId.value
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  ReportKt.metricCalculationSpec {
                    this.displayName = displayName
                    this.metricSpecs += metricSpecs
                    cumulative = false
                  }
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeInterval }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = requestId.toString()
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
      val displayName = DISPLAY_NAME
      val targetReportingSets = PRIMITIVE_REPORTING_SETS
      val timeInterval = timeInterval {
        startTime = START_TIME
        endTime = END_TIME
      }
      val internalTimeInterval = internalTimeInterval {
        startTime = START_TIME
        endTime = END_TIME
      }

      val reportingSetToCreateMetricRequestMap: Map<ReportingSet, InternalReport.ReportingMetric> =
        targetReportingSets.associateWith { reportingSet ->
          buildInitialReportingMetric(
            reportingSet.externalId,
            internalTimeInterval,
            INTERNAL_REACH_METRIC_SPEC,
            listOf()
          )
        }

      val internalRequestingReport = internalReport {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
        timeIntervals = internalTimeIntervals { timeIntervals += internalTimeInterval }
        reportingSetToCreateMetricRequestMap.forEach { (reportingSet, reportingMetric) ->
          val initialReportingMetrics = listOf(reportingMetric)
          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              reportingSet.externalId,
              initialReportingMetrics,
              reportingSet.name + displayName,
              listOf(),
              false
            )
          )
        }
      }

      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = 330L
          createTime = Instant.now().toProtoTime()
          reportingMetricEntries.clear()

          var requestId = 0
          reportingSetToCreateMetricRequestMap.map { (reportingSet, createMetricRequest) ->
            val updatedReportingCreateMetricRequests =
              listOf(createMetricRequest.copy { this.requestId = requestId.toString() })
            requestId++
            reportingMetricEntries.putAll(
              buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
                reportingSet.externalId,
                updatedReportingCreateMetricRequests,
                reportingSet.name + displayName,
                listOf(),
                false
              )
            )
          }
        }

      val internalPendingReport =
        internalInitialReport.copy {
          reportingMetricEntries.clear()

          var requestId = 0
          reportingSetToCreateMetricRequestMap.map { (reportingSet, createMetricRequest) ->
            val updatedReportingCreateMetricRequests =
              listOf(
                createMetricRequest.copy {
                  this.requestId = requestId.toString()
                  externalMetricId = EXTERNAL_METRIC_ID_BASE + requestId
                }
              )
            requestId++
            reportingMetricEntries.putAll(
              buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
                reportingSet.externalId,
                updatedReportingCreateMetricRequests,
                reportingSet.name + displayName,
                listOf(),
                false
              )
            )
          }
        }

      whenever(
          internalReportsMock.createReport(
            eq(internalCreateReportRequest { report = internalRequestingReport })
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
                        ExternalId(EXTERNAL_METRIC_ID_BASE + index).apiId.value
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        reportingMetricEntries +=
          targetReportingSets.map { reportingSet ->
            ReportKt.reportingMetricEntry {
              key = reportingSet.name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      this.displayName = reportingSet.name + displayName
                      metricSpecs += REACH_METRIC_SPEC
                      cumulative = false
                    }
                }
            }
          }
        timeIntervals = timeIntervals { timeIntervals += timeInterval }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = requestId.toString()
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
      val displayName = DISPLAY_NAME
      val targetReportingSet = PRIMITIVE_REPORTING_SETS.first()
      val timeInterval = timeInterval {
        startTime = START_TIME
        endTime = END_TIME
      }
      val internalTimeInterval = internalTimeInterval {
        startTime = START_TIME
        endTime = END_TIME
      }

      val internalMetricCalculationSpec =
        InternalReportKt.metricCalculationSpec {
          this.reportingMetrics +=
            InternalReportKt.reportingMetric {
              details =
                InternalReportKt.ReportingMetricKt.details {
                  externalReportingSetId = targetReportingSet.externalId
                  metricSpec = INTERNAL_REACH_METRIC_SPEC
                  this.timeInterval = internalTimeInterval
                }
            }
          details =
            InternalReportKt.MetricCalculationSpecKt.details {
              this.displayName = displayName
              metricSpecs += INTERNAL_REACH_METRIC_SPEC
              cumulative = false
            }
        }

      val internalRequestingReport = internalReport {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId
        timeIntervals = internalTimeIntervals { timeIntervals += internalTimeInterval }
        reportingMetricEntries.putAll(
          mapOf(
            targetReportingSet.externalId to
              InternalReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs += internalMetricCalculationSpec
                metricCalculationSpecs += internalMetricCalculationSpec
              }
          )
        )
      }

      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = 330L
          createTime = Instant.now().toProtoTime()

          val updatedMetricCalculationSpecs =
            (0..1).map { requestId ->
              internalMetricCalculationSpec.copy {
                val updatedReportingMetrics =
                  reportingMetrics.map { reportingMetric ->
                    reportingMetric.copy { this.requestId = requestId.toString() }
                  }
                this.reportingMetrics.clear()
                this.reportingMetrics += updatedReportingMetrics
              }
            }
          reportingMetricEntries.putAll(
            mapOf(
              targetReportingSet.externalId to
                InternalReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs += updatedMetricCalculationSpecs
                }
            )
          )
        }

      val internalPendingReport =
        internalInitialReport.copy {
          val updatedMetricCalculationSpecs =
            (0..1).map { requestId ->
              internalMetricCalculationSpec.copy {
                val updatedReportingMetrics =
                  reportingMetrics.map { reportingMetric ->
                    reportingMetric.copy {
                      this.requestId = requestId.toString()
                      externalMetricId = EXTERNAL_METRIC_ID_BASE + requestId
                    }
                  }
                this.reportingMetrics.clear()
                this.reportingMetrics += updatedReportingMetrics
              }
            }
          reportingMetricEntries.putAll(
            mapOf(
              targetReportingSet.externalId to
                InternalReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs += updatedMetricCalculationSpecs
                }
            )
          )
        }

      whenever(
          internalReportsMock.createReport(
            eq(internalCreateReportRequest { report = internalRequestingReport })
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
                        ExternalId(EXTERNAL_METRIC_ID_BASE + index).apiId.value
                      )
                      .toName()
                  state = Metric.State.RUNNING
                  createTime = Instant.now().toProtoTime()
                }
              }
          }
        )

      val requestingReport = report {
        reportingMetricEntries +=
          ReportKt.reportingMetricEntry {
            key = targetReportingSet.name
            value =
              ReportKt.reportingMetricCalculationSpec {
                val metricCalculationSpec =
                  ReportKt.metricCalculationSpec {
                    this.displayName = displayName
                    this.metricSpecs += REACH_METRIC_SPEC
                    cumulative = false
                  }
                metricCalculationSpecs += metricCalculationSpec
                metricCalculationSpecs += metricCalculationSpec
              }
          }
        timeIntervals = timeIntervals { timeIntervals += timeInterval }
      }

      val request = createReportRequest {
        parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
        report = requestingReport
      }
      val result =
        withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
          runBlocking { service.createReport(request) }
        }

      verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
        .isEqualTo(
          batchCreateMetricsRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            requests +=
              requestingMetrics.mapIndexed { requestId, metric ->
                createMetricRequest {
                  parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
                  this.metric = metric
                  this.requestId = requestId.toString()
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
      val startSec = 10L
      val incrementSec = 1L
      val intervalCount = MAX_BATCH_SIZE_FOR_BATCH_CREATE_METRICS + 1

      val periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = startSec }
        increment = duration { seconds = incrementSec }
        this.intervalCount = intervalCount
      }
      val endTimesList: List<Long> =
        (startSec + incrementSec until startSec + incrementSec + intervalCount).toList()
      val internalTimeIntervals: List<InternalTimeInterval> =
        endTimesList.map { end ->
          internalTimeInterval {
            startTime = timestamp { seconds = startSec }
            endTime = timestamp { seconds = end }
          }
        }

      val reportingCreateMetricRequests =
        internalTimeIntervals.map { timeInterval ->
          buildInitialReportingMetric(
            PRIMITIVE_REPORTING_SETS.first().externalId,
            timeInterval,
            INTERNAL_REACH_METRIC_SPEC,
            listOf()
          )
        }

      val internalRequestingReport = internalReport {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId

        this.periodicTimeInterval = internalPeriodicTimeInterval {
          startTime = timestamp { seconds = startSec }
          increment = duration { seconds = incrementSec }
          this.intervalCount = intervalCount
        }

        reportingMetricEntries.putAll(
          buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
            PRIMITIVE_REPORTING_SETS.first().externalId,
            reportingCreateMetricRequests,
            DISPLAY_NAME,
            listOf(),
            true
          )
        )
      }

      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = 330L
          createTime = Instant.now().toProtoTime()

          val updatedReportingCreateMetricRequests =
            reportingCreateMetricRequests.mapIndexed { requestId, request ->
              request.copy { this.requestId = requestId.toString() }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              PRIMITIVE_REPORTING_SETS.first().externalId,
              updatedReportingCreateMetricRequests,
              DISPLAY_NAME,
              listOf(),
              true
            )
          )
        }

      val internalPendingReport =
        internalInitialReport.copy {
          val updatedReportingCreateMetricRequests =
            reportingCreateMetricRequests.mapIndexed { requestId, request ->
              request.copy {
                this.requestId = requestId.toString()
                externalMetricId = EXTERNAL_METRIC_ID_BASE + requestId
              }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              PRIMITIVE_REPORTING_SETS.first().externalId,
              updatedReportingCreateMetricRequests,
              DISPLAY_NAME,
              listOf(),
              true
            )
          )
        }

      whenever(
          internalReportsMock.createReport(
            eq(internalCreateReportRequest { report = internalRequestingReport })
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
                        ExternalId(EXTERNAL_METRIC_ID_BASE + index).apiId.value
                      )
                      .toName()
                  reportingSet = PRIMITIVE_REPORTING_SETS.first().name
                  timeInterval = timeInterval {
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
        report = report {
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = PRIMITIVE_REPORTING_SETS.first().name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      displayName = DISPLAY_NAME
                      metricSpecs += REACH_METRIC_SPEC
                      cumulative = true
                    }
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
  fun `createReport returns report when metric spec values are not specified`() = runBlocking {
    val metricSpecWithoutVidSamplingInterval = REACH_METRIC_SPEC.copy { clearVidSamplingInterval() }
    val requestId = "0"

    val (internalRequestingReport, internalInitialReport, internalPendingReport) = INTERNAL_REPORTS

    whenever(
        internalReportsMock.createReport(
          eq(internalCreateReportRequest { report = internalRequestingReport })
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

    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries.clear()
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = PRIMITIVE_REPORTING_SETS.first().name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      displayName = DISPLAY_NAME
                      metricSpecs += metricSpecWithoutVidSamplingInterval
                      cumulative = false
                    }
                }
            }
        }
    }
    val result =
      withMeasurementConsumerPrincipal(MEASUREMENT_CONSUMER_KEYS.first().toName(), CONFIG) {
        runBlocking { service.createReport(request) }
      }

    verifyProtoArgument(metricsMock, MetricsGrpcKt.MetricsCoroutineImplBase::batchCreateMetrics)
      .isEqualTo(
        batchCreateMetricsRequest {
          parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
          requests += createMetricRequest {
            parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
            metric = REQUESTING_REACH_METRIC.copy { metricSpec = REACH_METRIC_SPEC }
            this.requestId = requestId
          }
        }
      )

    assertThat(result).isEqualTo(PENDING_REACH_REPORT)
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
  fun `createReport throws PERMISSION_DENIED when report doesn't belong to caller`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.last().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
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
    val request = createReportRequest { parent = MEASUREMENT_CONSUMER_KEYS.first().toName() }

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
  fun `createReport throws INVALID_ARGUMENT when TimeIntervals is set and cumulative is true`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          reportingMetricEntries.clear()
          reportingMetricEntries +=
            ReportKt.reportingMetricEntry {
              key = PRIMITIVE_REPORTING_SETS.first().name
              value =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      displayName = DISPLAY_NAME
                      metricSpecs += REACH_METRIC_SPEC
                      cumulative = true
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
            timeIntervals += timeInterval { endTime = timestamp { seconds = 5 } }
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
            timeIntervals += timeInterval { startTime = timestamp { seconds = 5 } }
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
  fun `createReport throws INVALID_ARGUMENT when TimeInterval endTime is before startTime`() {
    val request = createReportRequest {
      parent = MEASUREMENT_CONSUMER_KEYS.first().toName()
      report =
        PENDING_REACH_REPORT.copy {
          clearName()
          clearCreateTime()
          clearState()
          clearTime()
          timeIntervals = timeIntervals {
            timeIntervals += timeInterval {
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
          ExternalId(120L).apiId.value
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
  fun `createReport throws INVALID_ARGUMENT when metricCalculationSpec has no display name`() {
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
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      metricSpecs += REACH_METRIC_SPEC
                      cumulative = false
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
  fun `createReport throws INVALID_ARGUMENT when metricCalculationSpec has no metric spec`() {
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
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      displayName = DISPLAY_NAME
                      cumulative = false
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
  fun `createReport throws INVALID_ARGUMENT when grouping has empty predicates`() {
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
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      displayName = DISPLAY_NAME
                      metricSpecs += REACH_METRIC_SPEC
                      groupings += ReportKt.grouping {}
                      cumulative = false
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
  fun `createReport throws INVALID_ARGUMENT when duplicate predicates in groupings`() {
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
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      displayName = DISPLAY_NAME
                      metricSpecs += REACH_METRIC_SPEC
                      groupings +=
                        ReportKt.grouping {
                          predicates += "Gender == Male"
                          predicates += "Gender == Female"
                        }
                      groupings += ReportKt.grouping { predicates += "Gender == Male" }
                      cumulative = false
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

  companion object {
    private fun buildInitialReportingMetric(
      externalReportingSetId: Long,
      timeInterval: InternalTimeInterval,
      metricSpec: InternalMetricSpec,
      filters: List<String>,
    ): InternalReport.ReportingMetric {
      return InternalReportKt.reportingMetric {
        details =
          InternalReportKt.ReportingMetricKt.details {
            this.externalReportingSetId = externalReportingSetId
            this.metricSpec = metricSpec
            this.timeInterval = timeInterval
            this.filters += filters
          }
      }
    }

    private fun buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
      externalReportingSetId: Long,
      reportingMetrics: List<InternalReport.ReportingMetric>,
      displayName: String,
      groupings: List<InternalReport.MetricCalculationSpec.Grouping>,
      cumulative: Boolean,
    ): Map<Long, InternalReport.ReportingMetricCalculationSpec> {
      return mapOf(
        externalReportingSetId to
          InternalReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecs +=
              InternalReportKt.metricCalculationSpec {
                this.reportingMetrics += reportingMetrics
                details =
                  InternalReportKt.MetricCalculationSpecKt.details {
                    this.displayName = displayName
                    metricSpecs += reportingMetrics.map { it.details.metricSpec }.distinct()
                    this.groupings += groupings
                    this.cumulative = cumulative
                  }
              }
          }
      )
    }

    private fun buildInternalReports(
      cmmsMeasurementConsumerId: String,
      timeIntervals: List<InternalTimeInterval>,
      externalReportingSetId: Long,
      reportingMetrics: List<InternalReport.ReportingMetric>,
      groupings: List<InternalReport.MetricCalculationSpec.Grouping>,
    ): InternalReports {
      // Internal reports of reach
      val internalRequestingReport = internalReport {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        this.timeIntervals = internalTimeIntervals { this.timeIntervals += timeIntervals }

        reportingMetricEntries.putAll(
          buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
            externalReportingSetId,
            reportingMetrics,
            DISPLAY_NAME,
            groupings,
            false
          )
        )
      }
      val internalInitialReport =
        internalRequestingReport.copy {
          externalReportId = 330L
          createTime = Instant.now().toProtoTime()

          val reportingCreateMetricRequests =
            reportingMetrics.mapIndexed { requestId, request ->
              request.copy { this.requestId = requestId.toString() }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              externalReportingSetId,
              reportingCreateMetricRequests,
              DISPLAY_NAME,
              groupings,
              false
            )
          )
        }
      val internalPendingReport =
        internalInitialReport.copy {
          val reportingCreateMetricRequests =
            reportingMetrics.mapIndexed { requestId, request ->
              request.copy {
                this.requestId = requestId.toString()
                externalMetricId = EXTERNAL_METRIC_ID_BASE + requestId
              }
            }

          reportingMetricEntries.putAll(
            buildInternalReportingMetricEntryWithOneMetricCalculationSpec(
              externalReportingSetId,
              reportingCreateMetricRequests,
              DISPLAY_NAME,
              groupings,
              false
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
                ExternalId(index + 110L).apiId.value
              )
              .toName()
          filter = "AGE>18"
          displayName = "reporting-set-$index-display-name"
          primitive = ReportingSetKt.primitive { eventGroups += "event-group-$index" }
        }
      }

    // Time intervals
    private val START_INSTANT = Instant.now()
    private val END_INSTANT = START_INSTANT.plus(Duration.ofDays(1))

    private val START_TIME = START_INSTANT.toProtoTime()
    private val TIME_INTERVAL_INCREMENT = Duration.ofDays(1).toProtoDuration()
    private const val INTERVAL_COUNT = 2
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

      frequencyHistogramParams =
        MetricSpecConfigKt.frequencyHistogramParams {
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
          maximumFrequencyPerUser = REACH_FREQUENCY_MAXIMUM_FREQUENCY_PER_USER
        }
      frequencyHistogramVidSamplingInterval =
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
    private val FREQUENCY_HISTOGRAM_METRIC_SPEC: MetricSpec = metricSpec {
      frequencyHistogram =
        MetricSpecKt.frequencyHistogramParams {
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
          maximumFrequencyPerUser = REACH_FREQUENCY_MAXIMUM_FREQUENCY_PER_USER
        }
      vidSamplingInterval =
        MetricSpecKt.vidSamplingInterval {
          start = REACH_FREQUENCY_VID_SAMPLING_START
          width = REACH_FREQUENCY_VID_SAMPLING_WIDTH
        }
    }
    private val INTERNAL_FREQUENCY_HISTOGRAM_METRIC_SPEC: InternalMetricSpec = internalMetricSpec {
      frequencyHistogram =
        InternalMetricSpecKt.frequencyHistogramParams {
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
          maximumFrequencyPerUser = REACH_FREQUENCY_MAXIMUM_FREQUENCY_PER_USER
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

    // Metrics
    private const val EXTERNAL_METRIC_ID_BASE = 220L
    private val REQUESTING_REACH_METRIC = metric {
      reportingSet = PRIMITIVE_REPORTING_SETS.first().name
      timeInterval = timeInterval {
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
              ExternalId(EXTERNAL_METRIC_ID_BASE).apiId.value
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

    // Reports
    private const val DISPLAY_NAME = "DISPLAY_NAME"
    // Internal reports
    private val INITIAL_REPORTING_CREATE_METRIC_REQUEST =
      buildInitialReportingMetric(
        PRIMITIVE_REPORTING_SETS.first().externalId,
        internalTimeInterval {
          startTime = START_TIME
          endTime = END_TIME
        },
        INTERNAL_REACH_METRIC_SPEC,
        listOf()
      )

    private val INTERNAL_REPORTS =
      buildInternalReports(
        MEASUREMENT_CONSUMER_KEYS.first().measurementConsumerId,
        listOf(
          internalTimeInterval {
            startTime = START_TIME
            endTime = END_TIME
          }
        ),
        PRIMITIVE_REPORTING_SETS.first().externalId,
        listOf(INITIAL_REPORTING_CREATE_METRIC_REQUEST),
        listOf(),
      )

    // Public reports
    private val PENDING_REACH_REPORT: Report = report {
      name = INTERNAL_REPORTS.pendingReport.resourceName
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = PRIMITIVE_REPORTING_SETS.first().name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs +=
                ReportKt.metricCalculationSpec {
                  displayName = DISPLAY_NAME
                  metricSpecs += REACH_METRIC_SPEC
                  cumulative = false
                }
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += timeInterval {
          startTime = START_TIME
          endTime = END_TIME
        }
      }
      state = Report.State.RUNNING
      createTime = INTERNAL_REPORTS.pendingReport.createTime
    }
  }
}

private val ReportingSet.resourceKey: ReportingSetKey
  get() = ReportingSetKey.fromName(name)!!
private val ReportingSet.apiId: String
  get() = resourceKey.reportingSetId
private val ReportingSet.externalId: Long
  get() = apiIdToExternalId(apiId)

private val Metric.resourceKey: MetricKey
  get() = MetricKey.fromName(name)!!
private val Metric.apiId: String
  get() = resourceKey.metricId
private val Metric.externalId: Long
  get() = apiIdToExternalId(apiId)

private val InternalReport.resourceKey: ReportKey
  get() = ReportKey(cmmsMeasurementConsumerId, ExternalId(externalReportId).apiId.value)
private val InternalReport.resourceName: String
  get() = resourceKey.toName()

private val Report.resourceKey: ReportKey
  get() = ReportKey.fromName(name)!!
private val Report.apiId: String
  get() = resourceKey.reportId
private val Report.externalId: Long
  get() = apiIdToExternalId(apiId)
