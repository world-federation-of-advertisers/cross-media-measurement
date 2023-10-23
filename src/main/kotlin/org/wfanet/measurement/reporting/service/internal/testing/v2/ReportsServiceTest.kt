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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.duration
import com.google.protobuf.timestamp
import com.google.type.interval
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.createReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.streamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.timeIntervals

@RunWith(JUnit4::class)
abstract class ReportsServiceTest<T : ReportsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportsService: T,
    val metricsService: MetricsCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var metricsService: MetricsCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportsService
    metricsService = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
    metricCalculationSpecsService = services.metricCalculationSpecsService
  }

  @Test
  fun `createReport succeeds when timeIntervals set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(reportingSetsService, metricCalculationSpecsService)

    val report =
      baseReport.copy {
        timeIntervals = timeIntervals {
          timeIntervals += interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          timeIntervals += interval {
            startTime = timestamp { seconds = 300 }
            endTime = timestamp { seconds = 400 }
          }
        }
      }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = "external-report-id"
        }
      )

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
    assertThat(createdReport.hasCreateTime()).isTrue()
    for (reportingMetricCalculationSpec in createdReport.reportingMetricEntriesMap.values) {
      for (metricCalculationSpecReportingMetrics in
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport succeeds when periodicTimeInterval set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(reportingSetsService, metricCalculationSpecsService)

    val report =
      baseReport.copy {
        periodicTimeInterval = periodicTimeInterval {
          startTime = timestamp { seconds = 100 }
          increment = duration { seconds = 50 }
          intervalCount = 3
        }
      }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = "external-report-id"
        }
      )

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
    assertThat(createdReport.hasCreateTime()).isTrue()
    for (reportingMetricCalculationSpec in createdReport.reportingMetricEntriesMap.values) {
      for (metricCalculationSpecReportingMetrics in
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport succeeds when multiple reporting metric entries set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "external-reporting-set-id"
      )
    val createdReportingSet2 =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "external-reporting-set-id-2"
      )

    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecReportingMetrics +=
          ReportKt.metricCalculationSpecReportingMetrics {
            externalMetricCalculationSpecId =
              createdMetricCalculationSpec.externalMetricCalculationSpecId
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate1", "predicate2")
                  }
              }
          }
      }

    val reportingMetricCalculationSpec2 =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecReportingMetrics +=
          ReportKt.metricCalculationSpecReportingMetrics {
            externalMetricCalculationSpecId =
              createdMetricCalculationSpec.externalMetricCalculationSpecId
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate3", "predicate4")
                  }
              }
          }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        reportingMetricCalculationSpec
      reportingMetricEntries[createdReportingSet2.externalReportingSetId] =
        reportingMetricCalculationSpec2
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
        timeIntervals += interval {
          startTime = timestamp { seconds = 300 }
          endTime = timestamp { seconds = 400 }
        }
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = "external-report-id"
        }
      )

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
    assertThat(createdReport.hasCreateTime()).isTrue()
    for (entry in createdReport.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport succeeds when multiple reporting metrics in one entry`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "external-reporting-set-id"
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecReportingMetrics +=
          ReportKt.metricCalculationSpecReportingMetrics {
            externalMetricCalculationSpecId =
              createdMetricCalculationSpec.externalMetricCalculationSpecId
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate1", "predicate2")
                  }
              }
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate2", "predicate2")
                  }
              }
          }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        reportingMetricCalculationSpec
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = "external-report-id"
        }
      )

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
    assertThat(createdReport.hasCreateTime()).isTrue()
    for (entry in createdReport.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport returns the same report when request id used`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(reportingSetsService, metricCalculationSpecsService)

    val requestId = "1234"
    val request = createReportRequest {
      this.requestId = requestId
      report = baseReport
      this.externalReportId = "external-report-id"
    }

    val createdReport = service.createReport(request)
    assertThat(createdReport.externalReportId).isNotEqualTo(0)
    assertThat(createdReport.hasCreateTime()).isTrue()
    for (reportingMetricCalculationSpec in createdReport.reportingMetricEntriesMap.values) {
      for (metricCalculationSpecReportingMetrics in
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
        }
      }
    }

    val sameCreatedReport = service.createReport(request)
    assertThat(createdReport).isEqualTo(sameCreatedReport)
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when request missing external ID`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report =
              createReportForRequest(reportingSetsService, metricCalculationSpecsService)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws NOT_FOUND when ReportingSet not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries["1234"] =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecReportingMetrics +=
            ReportKt.metricCalculationSpecReportingMetrics {
              externalMetricCalculationSpecId =
                createdMetricCalculationSpec.externalMetricCalculationSpecId
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      groupingPredicates += listOf("predicate1", "predicate2")
                    }
                }
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      groupingPredicates += listOf("predicate2", "predicate2")
                    }
                }
            }
        }
      periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = 100 }
        increment = duration { seconds = 50 }
        intervalCount = 3
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = report
            this.externalReportId = "external-report-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `createReport throws NOT_FOUND when not all ReportingSets found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecReportingMetrics +=
          ReportKt.metricCalculationSpecReportingMetrics {
            externalMetricCalculationSpecId =
              createdMetricCalculationSpec.externalMetricCalculationSpecId
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate1", "predicate2")
                  }
              }
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate2", "predicate2")
                  }
              }
          }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        reportingMetricCalculationSpec
      reportingMetricEntries["1234"] = reportingMetricCalculationSpec
      periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = 100 }
        increment = duration { seconds = 50 }
        intervalCount = 3
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = report
            this.externalReportId = "external-report-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `createReport throws NOT_FOUND when MetricCalculationSpec not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecReportingMetrics +=
            ReportKt.metricCalculationSpecReportingMetrics {
              externalMetricCalculationSpecId = "1234"
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      groupingPredicates += listOf("predicate1", "predicate2")
                    }
                }
            }
        }
      periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = 100 }
        increment = duration { seconds = 50 }
        intervalCount = 3
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = report
            this.externalReportId = "external-report-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Metric Calculation Spec")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when time not set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(reportingSetsService, metricCalculationSpecsService)

    val report = baseReport.copy { clearTime() }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = report
            externalReportId = "external-report-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("missing time")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when no reporting metric entries`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = 100 }
        increment = duration { seconds = 50 }
        intervalCount = 3
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = report
            externalReportId = "external-report-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("missing reporting metric entries")
  }

  @Test
  fun `createReport throws FAILED_PRECONDITION when MC not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(reportingSetsService, metricCalculationSpecsService)

    val report = baseReport.copy { cmmsMeasurementConsumerId += "2" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = report
            externalReportId = "external-report-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  @Test
  fun `getReport returns report with timeIntervals set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = false,
        createdReportingSet,
        createdMetricCalculationSpec
      )

    val retrievedReport =
      service.getReport(
        getReportRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportId = createdReport.externalReportId
        }
      )

    assertThat(createdReport).ignoringRepeatedFieldOrder().isEqualTo(retrievedReport)
  }

  @Test
  fun `getReport returns report with periodicTimeInterval set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = true,
        createdReportingSet,
        createdMetricCalculationSpec
      )

    val retrievedReport =
      service.getReport(
        getReportRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportId = createdReport.externalReportId
        }
      )

    assertThat(createdReport).ignoringRepeatedFieldOrder().isEqualTo(retrievedReport)
  }

  @Test
  fun `getReport returns report when report has two reporting metric calculation specs`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdMetricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

      val createdReportingSet =
        createReportingSet(
          CMMS_MEASUREMENT_CONSUMER_ID,
          reportingSetsService,
          "external-reporting-set-id"
        )
      val createdReportingSet2 =
        createReportingSet(
          CMMS_MEASUREMENT_CONSUMER_ID,
          reportingSetsService,
          "external-reporting-set-id2"
        )

      val reportingMetricCalculationSpec =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecReportingMetrics +=
            ReportKt.metricCalculationSpecReportingMetrics {
              externalMetricCalculationSpecId =
                createdMetricCalculationSpec.externalMetricCalculationSpecId
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      groupingPredicates += listOf("predicate1", "predicate2")
                    }
                }
            }
        }

      val reportingMetricCalculationSpec2 =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecReportingMetrics +=
            ReportKt.metricCalculationSpecReportingMetrics {
              externalMetricCalculationSpecId =
                createdMetricCalculationSpec.externalMetricCalculationSpecId
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      groupingPredicates += listOf("predicate3", "predicate4")
                    }
                }
            }
        }

      val report = report {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingMetricEntries[createdReportingSet.externalReportingSetId] =
          reportingMetricCalculationSpec
        reportingMetricEntries[createdReportingSet2.externalReportingSetId] =
          reportingMetricCalculationSpec2
        timeIntervals = timeIntervals {
          timeIntervals += interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          timeIntervals += interval {
            startTime = timestamp { seconds = 300 }
            endTime = timestamp { seconds = 400 }
          }
        }
        details = ReportKt.details { tags.putAll(REPORT_TAGS) }
      }

      val createdReport =
        service.createReport(
          createReportRequest {
            this.report = report
            externalReportId = "external-report-id"
          }
        )

      val retrievedReport =
        service.getReport(
          getReportRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportId = createdReport.externalReportId
          }
        )

      assertThat(createdReport).ignoringRepeatedFieldOrder().isEqualTo(retrievedReport)
    }

  @Test
  fun `getReport returns report after report metrics have been created`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecReportingMetrics +=
          ReportKt.metricCalculationSpecReportingMetrics {
            externalMetricCalculationSpecId =
              createdMetricCalculationSpec.externalMetricCalculationSpecId
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = metricSpec {
                      reach =
                        MetricSpecKt.reachParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                        }
                      vidSamplingInterval =
                        MetricSpecKt.vidSamplingInterval {
                          start = 0.1f
                          width = 0.5f
                        }
                    }
                    timeInterval = interval {
                      startTime = timestamp { seconds = 100 }
                      endTime = timestamp { seconds = 200 }
                    }
                    groupingPredicates += listOf("predicate1", "predicate2")
                  }
              }
          }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        reportingMetricCalculationSpec
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
      }
      details = ReportKt.details { tags.putAll(REPORT_TAGS) }
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id"
        }
      )

    for (entry in createdReport.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          val request = createMetricRequest {
            requestId = reportingMetric.createMetricRequestId
            externalMetricId = "externalMetricId"
            metric = metric {
              cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
              externalReportingSetId = createdReportingSet.externalReportingSetId
              timeInterval = reportingMetric.details.timeInterval
              metricSpec = reportingMetric.details.metricSpec
              weightedMeasurements +=
                MetricKt.weightedMeasurement {
                  weight = 2
                  measurement = measurement {
                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                    timeInterval = interval {
                      startTime = timestamp { seconds = 10 }
                      endTime = timestamp { seconds = 100 }
                    }
                    primitiveReportingSetBases +=
                      ReportingSetKt.primitiveReportingSetBasis {
                        externalReportingSetId = createdReportingSet.externalReportingSetId
                        filters += "filter1"
                        filters += "filter2"
                      }
                  }
                }
              details =
                MetricKt.details {
                  filters +=
                    reportingMetric.details.groupingPredicatesList +
                      createdMetricCalculationSpec.details.filter
                }
            }
          }
          metricsService.createMetric(request)
        }
      }
    }

    val retrievedReport =
      service.getReport(
        getReportRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportId = createdReport.externalReportId
        }
      )

    for (entry in retrievedReport.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.externalMetricId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `getReport throw NOT_FOUND when report not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = false,
        createdReportingSet,
        createdMetricCalculationSpec
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getReport(
          getReportRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportId = createdReport.externalReportId + 1
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `streamReports returns reports when limit not set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = false,
        createdReportingSet,
        createdMetricCalculationSpec,
        "external-report-id"
      )
    val createdReport2 =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = true,
        createdReportingSet,
        createdMetricCalculationSpec,
        "external-report-id2"
      )

    val retrievedReports =
      service.streamReports(
        streamReportsRequest {
          filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )

    assertThat(retrievedReports.toList())
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdReport2, createdReport)
      .inOrder()
  }

  @Test
  fun `streamReports returns reports only belonging to specified mc`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val otherMcId = CMMS_MEASUREMENT_CONSUMER_ID + 2
    createMeasurementConsumer(otherMcId, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "external-reporting-set-id",
        "data-provider-id",
        "event-group-id"
      )
    val createdReportingSet2 =
      createReportingSet(
        otherMcId,
        reportingSetsService,
        "external-reporting-set-id",
        "data-provider-id",
        "event-group-id2"
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdMetricCalculationSpec2 =
      createMetricCalculationSpec(otherMcId, metricCalculationSpecsService)

    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = false,
        createdReportingSet,
        createdMetricCalculationSpec,
        "external-report-id",
      )

    createReport(
      otherMcId,
      service,
      usePeriodicTimeInterval = true,
      createdReportingSet2,
      createdMetricCalculationSpec2,
      "external-report-id2"
    )

    val retrievedReports =
      service.streamReports(
        streamReportsRequest {
          filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )

    assertThat(retrievedReports.toList())
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdReport)
      .inOrder()
  }

  @Test
  fun `streamReports returns reports created after create filter when both fields in after set`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
      val createdMetricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
      val createdReport =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          usePeriodicTimeInterval = false,
          createdReportingSet,
          createdMetricCalculationSpec,
          "external-report-id"
        )
      val createdReport2 =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          usePeriodicTimeInterval = false,
          createdReportingSet,
          createdMetricCalculationSpec,
          "external-report-id2"
        )

      val retrievedReports =
        service.streamReports(
          streamReportsRequest {
            filter =
              StreamReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                after =
                  StreamReportsRequestKt.afterFilter {
                    createTime = createdReport2.createTime
                    externalReportId = createdReport2.externalReportId
                  }
              }
          }
        )

      assertThat(retrievedReports.toList())
        .ignoringRepeatedFieldOrder()
        .containsExactly(createdReport)
        .inOrder()
    }

  @Test
  fun `streamReports returns report with same create_time as filter when only create_time set`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
      val createdMetricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
      val createdReport =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          usePeriodicTimeInterval = false,
          createdReportingSet,
          createdMetricCalculationSpec,
          "external-report-id"
        )
      val createdReport2 =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          usePeriodicTimeInterval = false,
          createdReportingSet,
          createdMetricCalculationSpec,
          "external-report-id2"
        )

      val retrievedReports =
        service.streamReports(
          streamReportsRequest {
            filter =
              StreamReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                after =
                  StreamReportsRequestKt.afterFilter { createTime = createdReport2.createTime }
              }
          }
        )

      assertThat(retrievedReports.toList())
        .ignoringRepeatedFieldOrder()
        .containsExactly(createdReport2, createdReport)
        .inOrder()
    }

  @Test
  fun `streamReports returns number of reports based on limit`(): Unit = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    createReport(
      CMMS_MEASUREMENT_CONSUMER_ID,
      service,
      usePeriodicTimeInterval = false,
      createdReportingSet,
      createdMetricCalculationSpec,
      "external-report-id"
    )
    val createdReport2 =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        usePeriodicTimeInterval = false,
        createdReportingSet,
        createdMetricCalculationSpec,
        "external-report-id2"
      )

    val retrievedReports =
      service.streamReports(
        streamReportsRequest {
          filter =
            StreamReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
          limit = 1
        }
      )

    assertThat(retrievedReports.toList())
      .ignoringRepeatedFieldOrder()
      .containsExactly(createdReport2)
  }

  @Test
  fun `streamReports throw INVALID_ARGUMENT when filter missing mc id`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    createReport(
      CMMS_MEASUREMENT_CONSUMER_ID,
      service,
      usePeriodicTimeInterval = false,
      createdReportingSet,
      createdMetricCalculationSpec
    )

    val exception =
      assertFailsWith<StatusRuntimeException> { service.streamReports(streamReportsRequest {}) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_id")
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
    private val REPORT_TAGS = mapOf("tag1" to "tag_value1", "tag2" to "tag_value2")

    private suspend fun createReportForRequest(
      reportingSetsService: ReportingSetsCoroutineImplBase,
      metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
    ): Report {
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
      val createdMetricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

      return report {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingMetricEntries[createdReportingSet.externalReportingSetId] =
          ReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecReportingMetrics +=
              ReportKt.metricCalculationSpecReportingMetrics {
                externalMetricCalculationSpecId =
                  createdMetricCalculationSpec.externalMetricCalculationSpecId
                reportingMetrics +=
                  ReportKt.reportingMetric {
                    details =
                      ReportKt.ReportingMetricKt.details {
                        metricSpec = metricSpec {
                          reach =
                            MetricSpecKt.reachParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start = 0.1f
                              width = 0.5f
                            }
                        }
                        timeInterval = interval {
                          startTime = timestamp { seconds = 100 }
                          endTime = timestamp { seconds = 200 }
                        }
                        groupingPredicates += listOf("predicate1", "predicate2")
                      }
                  }
              }
          }
        timeIntervals = timeIntervals {
          timeIntervals += interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          timeIntervals += interval {
            startTime = timestamp { seconds = 300 }
            endTime = timestamp { seconds = 400 }
          }
        }
        details = ReportKt.details { tags.putAll(REPORT_TAGS) }
      }
    }

    private suspend fun createReport(
      cmmsMeasurementConsumerId: String,
      reportsService: ReportsCoroutineImplBase,
      usePeriodicTimeInterval: Boolean,
      reportingSet: ReportingSet,
      metricCalculationSpec: MetricCalculationSpec,
      externalReportId: String = "external-report-id",
    ): Report {
      val reportingMetricCalculationSpec =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecReportingMetrics +=
            ReportKt.metricCalculationSpecReportingMetrics {
              externalMetricCalculationSpecId =
                metricCalculationSpec.externalMetricCalculationSpecId
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricSpec {
                        reach =
                          MetricSpecKt.reachParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                          }
                        vidSamplingInterval =
                          MetricSpecKt.vidSamplingInterval {
                            start = 0.1f
                            width = 0.5f
                          }
                      }
                      timeInterval = interval {
                        startTime = timestamp { seconds = 100 }
                        endTime = timestamp { seconds = 200 }
                      }
                      groupingPredicates += listOf("predicate1", "predicate2")
                    }
                }
            }
        }

      val report = report {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        reportingMetricEntries[reportingSet.externalReportingSetId] = reportingMetricCalculationSpec
        if (usePeriodicTimeInterval) {
          periodicTimeInterval = periodicTimeInterval {
            startTime = timestamp { seconds = 100 }
            increment = duration { seconds = 50 }
            intervalCount = 3
          }
        } else {
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
            timeIntervals += interval {
              startTime = timestamp { seconds = 200 }
              endTime = timestamp { seconds = 300 }
            }
          }
        }
        details = ReportKt.details { tags.putAll(REPORT_TAGS) }
      }

      return reportsService.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = externalReportId
        }
      )
    }
  }
}
