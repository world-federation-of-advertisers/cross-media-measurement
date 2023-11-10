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
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
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
    val reportSchedulesService: ReportSchedulesCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var metricsService: MetricsCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var reportSchedulesService: ReportSchedulesCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportsService
    metricsService = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
    reportSchedulesService = services.reportSchedulesService
  }

  @Test
  fun `createReport succeeds when timeIntervals set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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

    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.values.forEach { reportingMetricCalculationSpec ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport succeeds when periodicTimeInterval set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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

    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.values.forEach { reportingMetricCalculationSpec ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
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
        "externalReportingSetId2"
      )

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          ReportKt.metricCalculationSpec {
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    externalReportingSetId = createdReportingSet.externalReportingSetId
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
            details =
              ReportKt.MetricCalculationSpecKt.details {
                displayName = "display"
                metricSpecs += metricSpec {
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
                groupings += ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                filter = "filter"
                cumulative = false
              }
          }
      }

    val reportingMetricCalculationSpec2 =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          ReportKt.metricCalculationSpec {
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    externalReportingSetId = createdReportingSet2.externalReportingSetId
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
            details =
              ReportKt.MetricCalculationSpecKt.details {
                displayName = "display"
                metricSpecs += metricSpec {
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
                groupings += ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                filter = "filter"
                cumulative = false
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

    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.entries.forEach { entry ->
      entry.value.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
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

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          ReportKt.metricCalculationSpec {
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    externalReportingSetId = createdReportingSet.externalReportingSetId
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
                    externalReportingSetId = createdReportingSet.externalReportingSetId
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
            details =
              ReportKt.MetricCalculationSpecKt.details {
                displayName = "display"
                metricSpecs += metricSpec {
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
                groupings += ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                filter = "filter"
                cumulative = false
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

    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.entries.forEach { entry ->
      entry.value.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport returns the same report when request id used`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val report = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val requestId = "1234"
    val request = createReportRequest {
      this.requestId = requestId
      this.report = report
      this.externalReportId = "external-report-id"
    }

    val createdReport = service.createReport(request)
    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.values.forEach { reportingMetricCalculationSpec ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
        }
      }
    }

    val sameCreatedReport = service.createReport(request)
    assertThat(createdReport).isEqualTo(sameCreatedReport)
  }

  @Test
  fun `createReport succeeds when externalReportScheduleId set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule"
      )
    val reportSchedule =
      createReportSchedule(CMMS_MEASUREMENT_CONSUMER_ID, reportingSet, reportSchedulesService)
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val createdReport =
      service.createReport(
        createReportRequest {
          report = baseReport
          externalReportId = "external-report-id"
          externalReportScheduleId = reportSchedule.externalReportScheduleId
        }
      )

    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.externalReportScheduleId)
      .isEqualTo(reportSchedule.externalReportScheduleId)
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.values.forEach { reportingMetricCalculationSpec ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when request missing external ID`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            this.report = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws NOT_FOUND when ReportingSet not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries["1234"] =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            ReportKt.metricCalculationSpec {
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      externalReportingSetId = "1234"
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
                    }
                }
              details =
                ReportKt.MetricCalculationSpecKt.details {
                  displayName = "display"
                  metricSpecs += metricSpec {
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
                  groupings +=
                    ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                  cumulative = false
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

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          ReportKt.metricCalculationSpec {
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    externalReportingSetId = createdReportingSet.externalReportingSetId
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
                  }
              }
            details =
              ReportKt.MetricCalculationSpecKt.details {
                displayName = "display"
                metricSpecs += metricSpec {
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
                groupings += ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                cumulative = false
              }
          }
      }

    val reportingMetricCalculationSpec2 =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          ReportKt.metricCalculationSpec {
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    externalReportingSetId = "1234"
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
                  }
              }
            details =
              ReportKt.MetricCalculationSpecKt.details {
                displayName = "display"
                metricSpecs += metricSpec {
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
                groupings += ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                cumulative = false
              }
          }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        reportingMetricCalculationSpec
      reportingMetricEntries["1234"] = reportingMetricCalculationSpec2
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

  fun `createReport throws INVALID_ARGUMENT when metric in entry doesn't match`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val reportingMetricCalculationSpec =
      ReportKt.reportingMetricCalculationSpec {
        metricCalculationSpecs +=
          ReportKt.metricCalculationSpec {
            reportingMetrics +=
              ReportKt.reportingMetric {
                details =
                  ReportKt.ReportingMetricKt.details {
                    externalReportingSetId = "1234"
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
                  }
              }
            details =
              ReportKt.MetricCalculationSpecKt.details {
                displayName = "display"
                metricSpecs += metricSpec {
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
                groupings += ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                cumulative = false
              }
          }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[createdReportingSet.externalReportingSetId] =
        reportingMetricCalculationSpec
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("external reporting set id")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when time not set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val report = baseReport.copy { cmmsMeasurementConsumerId = cmmsMeasurementConsumerId + "2" }

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
  fun `createReport throws NOT_FOUND when report schedule not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            report = baseReport
            externalReportId = "external-report-id"
            externalReportScheduleId = "external-report-schedule-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Report Schedule")
  }

  @Test
  fun `getReport returns report with timeIntervals set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = false
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
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = true
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
  fun `getReport returns report with external_report_schedule_id set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule"
      )
    val reportSchedule =
      createReportSchedule(CMMS_MEASUREMENT_CONSUMER_ID, reportingSet, reportSchedulesService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        true,
        reportSchedule
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
          "externalReportingSetId2"
        )

      val reportingMetricCalculationSpec =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            ReportKt.metricCalculationSpec {
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      externalReportingSetId = createdReportingSet.externalReportingSetId
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
              details =
                ReportKt.MetricCalculationSpecKt.details {
                  displayName = "display"
                  metricSpecs += metricSpec {
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
                  groupings +=
                    ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                  filter = "filter1"
                  cumulative = false
                }
            }
        }

      val reportingMetricCalculationSpec2 =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            ReportKt.metricCalculationSpec {
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      externalReportingSetId = createdReportingSet2.externalReportingSetId
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
              details =
                ReportKt.MetricCalculationSpecKt.details {
                  displayName = "display"
                  metricSpecs += metricSpec {
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
                  groupings +=
                    ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                  filter = "filter2"
                  cumulative = false
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
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = true
      )

    createdReport.reportingMetricEntriesMap.values.forEach { reportingMetricCalculationSpec ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          val request = createMetricRequest {
            requestId = it.createMetricRequestId
            externalMetricId = "externalMetricId"
            metric = metric {
              cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
              externalReportingSetId = it.details.externalReportingSetId
              timeInterval = it.details.timeInterval
              metricSpec = it.details.metricSpec
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
                        externalReportingSetId = it.details.externalReportingSetId
                        filters += "filter1"
                        filters += "filter2"
                      }
                  }
                }
              details =
                MetricKt.details {
                  filters +=
                    it.details.groupingPredicatesList + metricCalculationSpec.details.filter
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

    retrievedReport.reportingMetricEntriesMap.values.forEach { reportingMetricCalculationSpec ->
      reportingMetricCalculationSpec.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.externalMetricId).isNotEmpty()
        }
      }
    }
  }

  @Test
  fun `getReport throw NOT_FOUND when report not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = false
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
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = false,
        null,
        "external-report-id"
      )
    val createdReport2 =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = true,
        null,
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
  fun `streamReports returns reports created after create filter when both fields in after set`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReport =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          reportingSetsService,
          usePeriodicTimeInterval = false,
          null,
          "external-report-id"
        )
      val createdReport2 =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          reportingSetsService,
          usePeriodicTimeInterval = false,
          null,
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
      val createdReport =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          reportingSetsService,
          usePeriodicTimeInterval = false,
          null,
          "external-report-id"
        )
      val createdReport2 =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          reportingSetsService,
          usePeriodicTimeInterval = false,
          null,
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
    createReport(
      CMMS_MEASUREMENT_CONSUMER_ID,
      service,
      reportingSetsService,
      usePeriodicTimeInterval = false,
      null,
      "external-report-id"
    )
    val createdReport2 =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        reportingSetsService,
        usePeriodicTimeInterval = false,
        null,
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
    createReport(
      CMMS_MEASUREMENT_CONSUMER_ID,
      service,
      reportingSetsService,
      usePeriodicTimeInterval = false
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
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsCoroutineImplBase,
    ): Report {
      val createdReportingSet = createReportingSet(cmmsMeasurementConsumerId, reportingSetsService)

      return report {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        reportingMetricEntries[createdReportingSet.externalReportingSetId] =
          ReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecs +=
              ReportKt.metricCalculationSpec {
                reportingMetrics +=
                  ReportKt.reportingMetric {
                    details =
                      ReportKt.ReportingMetricKt.details {
                        externalReportingSetId = createdReportingSet.externalReportingSetId
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
                details =
                  ReportKt.MetricCalculationSpecKt.details {
                    displayName = "display"
                    metricSpecs += metricSpec {
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
                    groupings +=
                      ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                    filter = "filter"
                    cumulative = false
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
      reportingSetsService: ReportingSetsCoroutineImplBase,
      usePeriodicTimeInterval: Boolean,
      reportSchedule: ReportSchedule? = null,
      externalReportId: String = "external-report-id"
    ): Report {
      val createdReportingSet =
        createReportingSet(
          cmmsMeasurementConsumerId,
          reportingSetsService,
          externalReportId + "_external-reporting-set-id"
        )

      val reportingMetricCalculationSpec =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            ReportKt.metricCalculationSpec {
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      externalReportingSetId = createdReportingSet.externalReportingSetId
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
              details =
                ReportKt.MetricCalculationSpecKt.details {
                  displayName = "display"
                  metricSpecs += metricSpec {
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
                  groupings +=
                    ReportKt.MetricCalculationSpecKt.grouping { predicates += "age > 10" }
                  filter = "filter"
                  cumulative = false
                }
            }
        }

      val report = report {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        reportingMetricEntries[createdReportingSet.externalReportingSetId] =
          reportingMetricCalculationSpec
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
          if (reportSchedule != null) {
            externalReportScheduleId = reportSchedule.externalReportScheduleId
          }
        }
      )
    }
  }
}
