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
import com.google.protobuf.timestamp
import com.google.protobuf.util.Timestamps
import com.google.rpc.errorInfo
import com.google.type.Interval
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
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
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequestKt
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequestKt
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MeasurementKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.TimeIntervals
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createMetricRequest
import org.wfanet.measurement.internal.reporting.v2.createReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.invalidateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.measurement
import org.wfanet.measurement.internal.reporting.v2.metric
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.streamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.timeIntervals
import org.wfanet.measurement.reporting.service.internal.Errors

private const val MAX_BATCH_SIZE = 1000

@RunWith(JUnit4::class)
abstract class ReportsServiceTest<T : ReportsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportsService: T,
    val metricsService: MetricsCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
    val reportSchedulesService: ReportSchedulesCoroutineImplBase,
    val measurementsService: MeasurementsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var metricsService: MetricsCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase
  private lateinit var reportSchedulesService: ReportSchedulesCoroutineImplBase
  private lateinit var measurementsService: MeasurementsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(
    idGenerator: IdGenerator,
    disableMetricsReuse: Boolean = false,
  ): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportsService
    metricsService = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
    metricCalculationSpecsService = services.metricCalculationSpecsService
    reportSchedulesService = services.reportSchedulesService
    measurementsService = services.measurementsService
  }

  @Test
  fun `createReport with details set succeeds`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

    val report =
      baseReport.copy {
        details =
          ReportKt.details {
            tags.putAll(REPORT_TAGS)
            reportingInterval =
              ReportKt.DetailsKt.reportingInterval {
                reportStart = dateTime {
                  year = 2024
                  month = 1
                  day = 1
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2024
                  month = 2
                  day = 1
                }
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

    assertThat(report)
      .ignoringFields(
        Report.EXTERNAL_REPORT_ID_FIELD_NUMBER,
        Report.CREATE_TIME_FIELD_NUMBER,
        Report.REPORTING_METRIC_ENTRIES_FIELD_NUMBER,
      )
      .isEqualTo(createdReport)
    assertThat(createdReport.externalReportId).isNotEmpty()
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
        "external-reporting-set-id",
      )
    val createdReportingSet2 =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "external-reporting-set-id-2",
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
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
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = "external-report-id"
        }
      )

    assertThat(report)
      .ignoringFields(
        Report.EXTERNAL_REPORT_ID_FIELD_NUMBER,
        Report.CREATE_TIME_FIELD_NUMBER,
        Report.REPORTING_METRIC_ENTRIES_FIELD_NUMBER,
      )
      .isEqualTo(createdReport)
    assertThat(createdReport.externalReportId).isNotEmpty()
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
        "external-reporting-set-id",
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
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

    assertThat(report)
      .ignoringFields(
        Report.EXTERNAL_REPORT_ID_FIELD_NUMBER,
        Report.CREATE_TIME_FIELD_NUMBER,
        Report.REPORTING_METRIC_ENTRIES_FIELD_NUMBER,
      )
      .isEqualTo(createdReport)
    assertThat(createdReport.externalReportId).isNotEmpty()
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
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

    val requestId = "1234"
    val request = createReportRequest {
      this.requestId = requestId
      report = baseReport
      this.externalReportId = "external-report-id"
    }

    val createdReport = service.createReport(request)
    assertThat(createdReport.externalReportId).isNotEmpty()
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
  fun `createReport succeeds when report schedule info set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        createdReportingSet,
        createdMetricCalculationSpec,
        reportSchedulesService,
      )
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

    val nextReportCreationTime = timestamp { seconds = 1000 }
    val createdReport =
      service.createReport(
        createReportRequest {
          report = baseReport
          externalReportId = "external-report-id"
          reportScheduleInfo =
            CreateReportRequestKt.reportScheduleInfo {
              externalReportScheduleId = createdReportSchedule.externalReportScheduleId
              this.nextReportCreationTime = nextReportCreationTime
            }
        }
      )

    assertThat(baseReport)
      .ignoringFields(
        Report.EXTERNAL_REPORT_ID_FIELD_NUMBER,
        Report.CREATE_TIME_FIELD_NUMBER,
        Report.REPORTING_METRIC_ENTRIES_FIELD_NUMBER,
        Report.EXTERNAL_REPORT_SCHEDULE_ID_FIELD_NUMBER,
      )
      .isEqualTo(createdReport)
    assertThat(createdReport.externalReportId).isNotEmpty()
    assertThat(createdReport.externalReportScheduleId)
      .isEqualTo(createdReportSchedule.externalReportScheduleId)
    assertThat(createdReport.hasCreateTime()).isTrue()
    for (reportingMetricCalculationSpec in createdReport.reportingMetricEntriesMap.values) {
      for (metricCalculationSpecReportingMetrics in
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
        }
      }
    }

    val updatedReportSchedule =
      reportSchedulesService.getReportSchedule(
        getReportScheduleRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = createdReportSchedule.externalReportScheduleId
        }
      )
    assertThat(
        Timestamps.compare(updatedReportSchedule.updateTime, updatedReportSchedule.createTime)
      )
      .isGreaterThan(0)
    assertThat(updatedReportSchedule.nextReportCreationTime).isEqualTo(nextReportCreationTime)
  }

  @Test
  fun `createReport reuses existing metrics from the other report`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdMetricCalculationSpecsByExternalId =
      (0 until 2).associate {
        val externalMetricCalculationSpecId = "external-metric-calculation-spec-id$it"
        externalMetricCalculationSpecId to
          createMetricCalculationSpec(
            CMMS_MEASUREMENT_CONSUMER_ID,
            metricCalculationSpecsService,
            externalMetricCalculationSpecId,
          )
      }
    val createdReportingSetsByExternalId =
      (0 until 2).associate {
        val externalReportingSetId = "external-reporting-set-id$it"
        externalReportingSetId to
          createReportingSet(
            CMMS_MEASUREMENT_CONSUMER_ID,
            reportingSetsService,
            externalReportingSetId,
            "data-provider-id${it % 3}",
            "event-group-id$it",
          )
      }

    val groupingPredicatesList = List(1) { listOf("predicate1", "predicate2") }

    val intervals: List<Interval> =
      listOf(
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        },
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        },
        interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        },
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 300 }
        },
      )

    val reportingMetrics =
      groupingPredicatesList.flatMap { groupingPredicates ->
        intervals.map { interval ->
          ReportKt.reportingMetric {
            details =
              ReportKt.ReportingMetricKt.details {
                metricSpec = metricSpec {
                  reach =
                    MetricSpecKt.reachParams {
                      multipleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start = 0.1f
                              width = 0.5f
                            }
                        }
                    }
                }
                this.timeInterval = interval
                this.groupingPredicates += groupingPredicates
              }
          }
        }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries.putAll(
        createdReportingSetsByExternalId.keys.associateWith {
          ReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecReportingMetrics +=
              createdMetricCalculationSpecsByExternalId.keys.map { externalMetricCalSpecId ->
                ReportKt.metricCalculationSpecReportingMetrics {
                  externalMetricCalculationSpecId = externalMetricCalSpecId
                  this.reportingMetrics += reportingMetrics
                }
              }
          }
        }
      )
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals { this.timeIntervals += intervals }
        }
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id"
        }
      )

    var metricIndex = 0
    val createMetricsRequests: List<CreateMetricRequest> =
      createdReport.reportingMetricEntriesMap.entries.flatMap { entry ->
        val reportingSet = createdReportingSetsByExternalId.getValue(entry.key)

        entry.value.metricCalculationSpecReportingMetricsList.flatMap {
          metricCalculationSpecReportingMetrics ->
          val metricCalculationSpecFilter =
            createdMetricCalculationSpecsByExternalId
              .getValue(metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId)
              .details
              .filter
          metricCalculationSpecReportingMetrics.reportingMetricsList.map { reportingMetric ->
            val externalMetricId = "externalMetricId$metricIndex"
            metricIndex++
            buildCreateMetricRequest(
              createdReport.cmmsMeasurementConsumerId,
              externalMetricId,
              reportingSet,
              reportingMetric,
              metricCalculationSpecFilter,
            )
          }
        }
      }

    metricsService.batchCreateMetrics(
      batchCreateMetricsRequest {
        cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
        requests += createMetricsRequests
      }
    )

    val retrievedReport =
      service.getReport(
        getReportRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportId = createdReport.externalReportId
        }
      )

    val createdReportReusingMetrics =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id2"
        }
      )

    for (entry in createdReportReusingMetrics.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
          assertThat(reportingMetric.externalMetricId).isNotEmpty()
        }
      }
    }

    // If metric reuse did happen during the second report creation, the second report should have
    // the same key-value pairs in `reportingMetricEntriesMap` as what the first report has.
    // Note that metric reuse mechanism won't produce the same order of `ReportingMetric`
    // internally. This won't impact any immutable fields in public resources.
    for (entry in createdReportReusingMetrics.reportingMetricEntriesMap.entries) {
      val expected = retrievedReport.reportingMetricEntriesMap.getValue(entry.key)
      assertThat(entry.value).ignoringRepeatedFieldOrder().isEqualTo(expected)
    }
  }

  @Test
  fun `createReport doesn't reuse existing FAILED metrics from the other report`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdMetricCalculationSpecsByExternalId =
      (0 until 2).associate {
        val externalMetricCalculationSpecId = "external-metric-calculation-spec-id$it"
        externalMetricCalculationSpecId to
          createMetricCalculationSpec(
            CMMS_MEASUREMENT_CONSUMER_ID,
            metricCalculationSpecsService,
            externalMetricCalculationSpecId,
          )
      }
    val createdReportingSetsByExternalId =
      (0 until 2).associate {
        val externalReportingSetId = "external-reporting-set-id$it"
        externalReportingSetId to
          createReportingSet(
            CMMS_MEASUREMENT_CONSUMER_ID,
            reportingSetsService,
            externalReportingSetId,
            "data-provider-id${it % 3}",
            "event-group-id$it",
          )
      }

    val groupingPredicatesList = List(1) { listOf("predicate1", "predicate2") }

    val intervals: List<Interval> =
      listOf(
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        },
        interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        },
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 300 }
        },
      )

    val reportingMetrics =
      groupingPredicatesList.flatMap { groupingPredicates ->
        intervals.map { interval ->
          ReportKt.reportingMetric {
            details =
              ReportKt.ReportingMetricKt.details {
                metricSpec = metricSpec {
                  reach =
                    MetricSpecKt.reachParams {
                      multipleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start = 0.1f
                              width = 0.5f
                            }
                        }
                    }
                }
                this.timeInterval = interval
                this.groupingPredicates += groupingPredicates
              }
          }
        }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries.putAll(
        createdReportingSetsByExternalId.keys.associateWith {
          ReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecReportingMetrics +=
              createdMetricCalculationSpecsByExternalId.keys.map { externalMetricCalSpecId ->
                ReportKt.metricCalculationSpecReportingMetrics {
                  externalMetricCalculationSpecId = externalMetricCalSpecId
                  this.reportingMetrics += reportingMetrics
                }
              }
          }
        }
      )
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals { this.timeIntervals += intervals }
        }
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id"
        }
      )

    var metricIndex = 0
    val createMetricsRequests: List<CreateMetricRequest> =
      createdReport.reportingMetricEntriesMap.entries.flatMap { entry ->
        val reportingSet = createdReportingSetsByExternalId.getValue(entry.key)

        entry.value.metricCalculationSpecReportingMetricsList.flatMap {
          metricCalculationSpecReportingMetrics ->
          val metricCalculationSpecFilter =
            createdMetricCalculationSpecsByExternalId
              .getValue(metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId)
              .details
              .filter
          metricCalculationSpecReportingMetrics.reportingMetricsList.map { reportingMetric ->
            val externalMetricId = "externalMetricId$metricIndex"
            metricIndex++
            buildCreateMetricRequest(
              createdReport.cmmsMeasurementConsumerId,
              externalMetricId,
              reportingSet,
              reportingMetric,
              metricCalculationSpecFilter,
            )
          }
        }
      }

    val metrics =
      metricsService
        .batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
            requests += createMetricsRequests
          }
        )
        .metricsList

    var i = 0
    val batchSetCmmsMeasurementIdsRequest = batchSetCmmsMeasurementIdsRequest {
      cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
      for (metric in metrics) {
        for (weightedMeasurement in metric.weightedMeasurementsList) {
          measurementIds +=
            BatchSetCmmsMeasurementIdsRequestKt.measurementIds {
              cmmsCreateMeasurementRequestId =
                weightedMeasurement.measurement.cmmsCreateMeasurementRequestId
              cmmsMeasurementId = "${i++}"
            }
        }
      }
    }
    measurementsService.batchSetCmmsMeasurementIds(batchSetCmmsMeasurementIdsRequest)

    i = 0
    val batchSetMeasurementFailuresRequest = batchSetMeasurementFailuresRequest {
      cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
      for (metric in metrics) {
        for (weightedMeasurement in metric.weightedMeasurementsList) {
          measurementFailures +=
            BatchSetMeasurementFailuresRequestKt.measurementFailure {
              cmmsMeasurementId = "${i++}"
              failure = MeasurementKt.failure { message = "failed" }
            }
        }
      }
    }
    measurementsService.batchSetMeasurementFailures(batchSetMeasurementFailuresRequest)

    val report2 =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id2"
        }
      )

    for (entry in report2.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.externalMetricId).isEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport doesn't reuse existing INVALID metrics from the other report`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdMetricCalculationSpecsByExternalId =
      (0 until 2).associate {
        val externalMetricCalculationSpecId = "external-metric-calculation-spec-id$it"
        externalMetricCalculationSpecId to
          createMetricCalculationSpec(
            CMMS_MEASUREMENT_CONSUMER_ID,
            metricCalculationSpecsService,
            externalMetricCalculationSpecId,
          )
      }
    val createdReportingSetsByExternalId =
      (0 until 2).associate {
        val externalReportingSetId = "external-reporting-set-id$it"
        externalReportingSetId to
          createReportingSet(
            CMMS_MEASUREMENT_CONSUMER_ID,
            reportingSetsService,
            externalReportingSetId,
            "data-provider-id${it % 3}",
            "event-group-id$it",
          )
      }

    val groupingPredicatesList = List(1) { listOf("predicate1", "predicate2") }

    val intervals: List<Interval> =
      listOf(
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        },
        interval {
          startTime = timestamp { seconds = 200 }
          endTime = timestamp { seconds = 300 }
        },
        interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 300 }
        },
      )

    val reportingMetrics =
      groupingPredicatesList.flatMap { groupingPredicates ->
        intervals.map { interval ->
          ReportKt.reportingMetric {
            details =
              ReportKt.ReportingMetricKt.details {
                metricSpec = metricSpec {
                  reach =
                    MetricSpecKt.reachParams {
                      multipleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon = 1.0
                              delta = 2.0
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start = 0.1f
                              width = 0.5f
                            }
                        }
                    }
                }
                this.timeInterval = interval
                this.groupingPredicates += groupingPredicates
              }
          }
        }
      }

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries.putAll(
        createdReportingSetsByExternalId.keys.associateWith {
          ReportKt.reportingMetricCalculationSpec {
            metricCalculationSpecReportingMetrics +=
              createdMetricCalculationSpecsByExternalId.keys.map { externalMetricCalSpecId ->
                ReportKt.metricCalculationSpecReportingMetrics {
                  externalMetricCalculationSpecId = externalMetricCalSpecId
                  this.reportingMetrics += reportingMetrics
                }
              }
          }
        }
      )
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals { this.timeIntervals += intervals }
        }
    }

    val createdReport =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id"
        }
      )

    var metricIndex = 0
    val createMetricsRequests: List<CreateMetricRequest> =
      createdReport.reportingMetricEntriesMap.entries.flatMap { entry ->
        val reportingSet = createdReportingSetsByExternalId.getValue(entry.key)

        entry.value.metricCalculationSpecReportingMetricsList.flatMap {
          metricCalculationSpecReportingMetrics ->
          val metricCalculationSpecFilter =
            createdMetricCalculationSpecsByExternalId
              .getValue(metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId)
              .details
              .filter
          metricCalculationSpecReportingMetrics.reportingMetricsList.map { reportingMetric ->
            val externalMetricId = "externalMetricId$metricIndex"
            metricIndex++
            buildCreateMetricRequest(
              createdReport.cmmsMeasurementConsumerId,
              externalMetricId,
              reportingSet,
              reportingMetric,
              metricCalculationSpecFilter,
            )
          }
        }
      }

    val metrics =
      metricsService
        .batchCreateMetrics(
          batchCreateMetricsRequest {
            cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
            requests += createMetricsRequests
          }
        )
        .metricsList

    for (metric in metrics) {
      metricsService.invalidateMetric(
        invalidateMetricRequest {
          cmmsMeasurementConsumerId = metric.cmmsMeasurementConsumerId
          externalMetricId = metric.externalMetricId
        }
      )
    }

    val report2 =
      service.createReport(
        createReportRequest {
          this.report = report
          externalReportId = "external-report-id2"
        }
      )

    for (entry in report2.reportingMetricEntriesMap.entries) {
      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
          assertThat(reportingMetric.externalMetricId).isEmpty()
        }
      }
    }
  }

  @Test
  fun `createReport doesn't reuses existing metrics from the other report when flag true`() =
    runBlocking {
      val services = newServices(idGenerator, disableMetricsReuse = true)
      service = services.reportsService
      metricsService = services.metricsService
      reportingSetsService = services.reportingSetsService
      measurementConsumersService = services.measurementConsumersService
      metricCalculationSpecsService = services.metricCalculationSpecsService
      reportSchedulesService = services.reportSchedulesService
      measurementsService = services.measurementsService

      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdMetricCalculationSpecsByExternalId =
        (0 until 2).associate {
          val externalMetricCalculationSpecId = "external-metric-calculation-spec-id$it"
          externalMetricCalculationSpecId to
            createMetricCalculationSpec(
              CMMS_MEASUREMENT_CONSUMER_ID,
              metricCalculationSpecsService,
              externalMetricCalculationSpecId,
            )
        }
      val createdReportingSetsByExternalId =
        (0 until 2).associate {
          val externalReportingSetId = "external-reporting-set-id$it"
          externalReportingSetId to
            createReportingSet(
              CMMS_MEASUREMENT_CONSUMER_ID,
              reportingSetsService,
              externalReportingSetId,
              "data-provider-id${it % 3}",
              "event-group-id$it",
            )
        }

      val groupingPredicatesList = List(1) { listOf("predicate1", "predicate2") }

      val intervals: List<Interval> =
        listOf(
          interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          },
          interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          },
          interval {
            startTime = timestamp { seconds = 200 }
            endTime = timestamp { seconds = 300 }
          },
          interval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 300 }
          },
        )

      val reportingMetrics =
        groupingPredicatesList.flatMap { groupingPredicates ->
          intervals.map { interval ->
            ReportKt.reportingMetric {
              details =
                ReportKt.ReportingMetricKt.details {
                  metricSpec = metricSpec {
                    reach =
                      MetricSpecKt.reachParams {
                        multipleDataProviderParams =
                          MetricSpecKt.samplingAndPrivacyParams {
                            privacyParams =
                              MetricSpecKt.differentialPrivacyParams {
                                epsilon = 1.0
                                delta = 2.0
                              }
                            vidSamplingInterval =
                              MetricSpecKt.vidSamplingInterval {
                                start = 0.1f
                                width = 0.5f
                              }
                          }
                      }
                  }
                  this.timeInterval = interval
                  this.groupingPredicates += groupingPredicates
                }
            }
          }
        }

      val report = report {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingMetricEntries.putAll(
          createdReportingSetsByExternalId.keys.associateWith {
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecReportingMetrics +=
                createdMetricCalculationSpecsByExternalId.keys.map { externalMetricCalSpecId ->
                  ReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId = externalMetricCalSpecId
                    this.reportingMetrics += reportingMetrics
                  }
                }
            }
          }
        )
        details =
          ReportKt.details {
            tags.putAll(REPORT_TAGS)
            timeIntervals = timeIntervals { this.timeIntervals += intervals }
          }
      }

      val createdReport =
        service.createReport(
          createReportRequest {
            this.report = report
            externalReportId = "external-report-id"
          }
        )

      var metricIndex = 0
      val createMetricsRequests: List<CreateMetricRequest> =
        createdReport.reportingMetricEntriesMap.entries.flatMap { entry ->
          val reportingSet = createdReportingSetsByExternalId.getValue(entry.key)

          entry.value.metricCalculationSpecReportingMetricsList.flatMap {
            metricCalculationSpecReportingMetrics ->
            val metricCalculationSpecFilter =
              createdMetricCalculationSpecsByExternalId
                .getValue(metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId)
                .details
                .filter
            metricCalculationSpecReportingMetrics.reportingMetricsList.map { reportingMetric ->
              val externalMetricId = "externalMetricId$metricIndex"
              metricIndex++
              buildCreateMetricRequest(
                createdReport.cmmsMeasurementConsumerId,
                externalMetricId,
                reportingSet,
                reportingMetric,
                metricCalculationSpecFilter,
              )
            }
          }
        }

      metricsService.batchCreateMetrics(
        batchCreateMetricsRequest {
          cmmsMeasurementConsumerId = createdReport.cmmsMeasurementConsumerId
          requests += createMetricsRequests
        }
      )

      val secondReport =
        service.createReport(
          createReportRequest {
            this.report = report
            externalReportId = "external-report-id2"
          }
        )

      for (entry in secondReport.reportingMetricEntriesMap.entries) {
        for (metricCalculationSpecReportingMetrics in
          entry.value.metricCalculationSpecReportingMetricsList) {
          for (reportingMetric in metricCalculationSpecReportingMetrics.reportingMetricsList) {
            assertThat(reportingMetric.createMetricRequestId).isNotEmpty()
            assertThat(reportingMetric.externalMetricId).isEmpty()
          }
        }
      }
    }

  @Test
  fun `createReport throws INVALID_ARGUMENT when request missing external ID`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(createReportRequest { report = baseReport })
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
                            multipleDataProviderParams =
                              MetricSpecKt.samplingAndPrivacyParams {
                                privacyParams =
                                  MetricSpecKt.differentialPrivacyParams {
                                    epsilon = 1.0
                                    delta = 2.0
                                  }
                                vidSamplingInterval =
                                  MetricSpecKt.vidSamplingInterval {
                                    start = 0.1f
                                    width = 0.5f
                                  }
                              }
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
                            multipleDataProviderParams =
                              MetricSpecKt.samplingAndPrivacyParams {
                                privacyParams =
                                  MetricSpecKt.differentialPrivacyParams {
                                    epsilon = 1.0
                                    delta = 2.0
                                  }
                                vidSamplingInterval =
                                  MetricSpecKt.vidSamplingInterval {
                                    start = 0.1f
                                    width = 0.5f
                                  }
                              }
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
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
          }
        }
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
          }
        }
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
                            multipleDataProviderParams =
                              MetricSpecKt.samplingAndPrivacyParams {
                                privacyParams =
                                  MetricSpecKt.differentialPrivacyParams {
                                    epsilon = 1.0
                                    delta = 2.0
                                  }
                                vidSamplingInterval =
                                  MetricSpecKt.vidSamplingInterval {
                                    start = 0.1f
                                    width = 0.5f
                                  }
                              }
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
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
          }
        }
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
  fun `createReport throws INVALID_ARGUMENT when time intervals and reporting interval not set`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val createdReportingSet =
        createReportingSet(
          CMMS_MEASUREMENT_CONSUMER_ID,
          reportingSetsService,
          "reporting-set-for-report-schedule",
        )
      val createdMetricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
      val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

      val report =
        baseReport.copy {
          details =
            baseReport.details.copy {
              clearReportingInterval()
              clearTimeIntervals()
            }
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
      assertThat(exception.message).contains("missing time_intervals")
    }

  @Test
  fun `createReport throws INVALID_ARGUMENT when time intervals empty`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

    val report =
      baseReport.copy {
        details =
          baseReport.details.copy {
            clearReportingInterval()
            timeIntervals = TimeIntervals.getDefaultInstance()
          }
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
    assertThat(exception.message).contains("missing time_intervals")
  }

  @Test
  fun `createReport throws INVALID_ARGUMENT when no reporting metric entries`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          timeIntervals = timeIntervals {
            timeIntervals += interval {
              startTime = timestamp { seconds = 100 }
              endTime = timestamp { seconds = 200 }
            }
          }
        }
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
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

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
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            baseReport.cmmsMeasurementConsumerId + "2"
        }
      )
  }

  @Test
  fun `createReport throws NOT_FOUND when report schedule not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val baseReport = createReportForRequest(createdReportingSet, createdMetricCalculationSpec)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(
          createReportRequest {
            report = baseReport
            externalReportId = "external-report-id"
            reportScheduleInfo =
              CreateReportRequestKt.reportScheduleInfo {
                externalReportScheduleId = "external-report-schedule-id"
                nextReportCreationTime = timestamp { seconds = 1000 }
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Report Schedule")
  }

  @Test
  fun `getReport returns report with external_report_schedule_id set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val createdReportingSet =
      createReportingSet(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSetsService,
        "reporting-set-for-report-schedule",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdReportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        createdReportingSet,
        createdMetricCalculationSpec,
        reportSchedulesService,
      )
    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        createdReportingSet,
        createdMetricCalculationSpec,
        createdReportSchedule,
      )

    val retrievedReport =
      service.getReport(
        getReportRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportId = createdReport.externalReportId
        }
      )

    assertThat(createdReport).ignoringRepeatedFieldOrder().isEqualTo(retrievedReport)
    assertThat(retrievedReport.externalReportScheduleId)
      .isEqualTo(createdReportSchedule.externalReportScheduleId)
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
          "external-reporting-set-id",
        )
      val createdReportingSet2 =
        createReportingSet(
          CMMS_MEASUREMENT_CONSUMER_ID,
          reportingSetsService,
          "external-reporting-set-id2",
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
                            multipleDataProviderParams =
                              MetricSpecKt.samplingAndPrivacyParams {
                                privacyParams =
                                  MetricSpecKt.differentialPrivacyParams {
                                    epsilon = 1.0
                                    delta = 2.0
                                  }
                                vidSamplingInterval =
                                  MetricSpecKt.vidSamplingInterval {
                                    start = 0.1f
                                    width = 0.5f
                                  }
                              }
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
                            multipleDataProviderParams =
                              MetricSpecKt.samplingAndPrivacyParams {
                                privacyParams =
                                  MetricSpecKt.differentialPrivacyParams {
                                    epsilon = 1.0
                                    delta = 2.0
                                  }
                                vidSamplingInterval =
                                  MetricSpecKt.vidSamplingInterval {
                                    start = 0.1f
                                    width = 0.5f
                                  }
                              }
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
        details =
          ReportKt.details {
            tags.putAll(REPORT_TAGS)
            reportingInterval =
              ReportKt.DetailsKt.reportingInterval {
                reportStart = dateTime {
                  year = 2024
                  month = 1
                  day = 1
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2024
                  month = 2
                  day = 1
                }
              }
          }
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
                          multipleDataProviderParams =
                            MetricSpecKt.samplingAndPrivacyParams {
                              privacyParams =
                                MetricSpecKt.differentialPrivacyParams {
                                  epsilon = 1.0
                                  delta = 2.0
                                }
                              vidSamplingInterval =
                                MetricSpecKt.vidSamplingInterval {
                                  start = 0.1f
                                  width = 0.5f
                                }
                            }
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
      details =
        ReportKt.details {
          tags.putAll(REPORT_TAGS)
          reportingInterval =
            ReportKt.DetailsKt.reportingInterval {
              reportStart = dateTime {
                year = 2024
                month = 1
                day = 1
                timeZone = timeZone { id = "America/Los_Angeles" }
              }
              reportEnd = date {
                year = 2024
                month = 2
                day = 1
              }
            }
        }
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
          val request =
            buildCreateMetricRequest(
              createdReport.cmmsMeasurementConsumerId,
              "externalMetricId",
              createdReportingSet,
              reportingMetric,
              createdMetricCalculationSpec.details.filter,
            )
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
        createdReportingSet,
        createdMetricCalculationSpec,
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
        createdReportingSet,
        createdMetricCalculationSpec,
        null,
        "external-report-id",
      )
    val createdReport2 =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        createdReportingSet,
        createdMetricCalculationSpec,
        null,
        "external-report-id2",
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
        "event-group-id",
      )
    val createdReportingSet2 =
      createReportingSet(
        otherMcId,
        reportingSetsService,
        "external-reporting-set-id",
        "data-provider-id",
        "event-group-id2",
      )
    val createdMetricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val createdMetricCalculationSpec2 =
      createMetricCalculationSpec(otherMcId, metricCalculationSpecsService)

    val createdReport =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        createdReportingSet,
        createdMetricCalculationSpec,
        null,
        "external-report-id",
      )

    createReport(
      otherMcId,
      service,
      createdReportingSet2,
      createdMetricCalculationSpec2,
      null,
      "external-report-id2",
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
          createdReportingSet,
          createdMetricCalculationSpec,
          null,
          "external-report-id",
        )
      val createdReport2 =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          createdReportingSet,
          createdMetricCalculationSpec,
          null,
          "external-report-id2",
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
          createdReportingSet,
          createdMetricCalculationSpec,
          null,
          "external-report-id",
        )
      val createdReport2 =
        createReport(
          CMMS_MEASUREMENT_CONSUMER_ID,
          service,
          createdReportingSet,
          createdMetricCalculationSpec,
          null,
          "external-report-id2",
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
      createdReportingSet,
      createdMetricCalculationSpec,
      null,
      "external-report-id",
    )
    val createdReport2 =
      createReport(
        CMMS_MEASUREMENT_CONSUMER_ID,
        service,
        createdReportingSet,
        createdMetricCalculationSpec,
        null,
        "external-report-id2",
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
      createdReportingSet,
      createdMetricCalculationSpec,
    )

    val exception =
      assertFailsWith<StatusRuntimeException> { service.streamReports(streamReportsRequest {}) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_id")
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
    private val REPORT_TAGS = mapOf("tag1" to "tag_value1", "tag2" to "tag_value2")

    private fun createReportForRequest(
      reportingSet: ReportingSet,
      metricCalculationSpec: MetricCalculationSpec,
    ): Report {
      return report {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingMetricEntries[reportingSet.externalReportingSetId] =
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
                              multipleDataProviderParams =
                                MetricSpecKt.samplingAndPrivacyParams {
                                  privacyParams =
                                    MetricSpecKt.differentialPrivacyParams {
                                      epsilon = 1.0
                                      delta = 2.0
                                    }
                                  vidSamplingInterval =
                                    MetricSpecKt.vidSamplingInterval {
                                      start = 0.1f
                                      width = 0.5f
                                    }
                                }
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
        details =
          ReportKt.details {
            tags.putAll(REPORT_TAGS)
            reportingInterval =
              ReportKt.DetailsKt.reportingInterval {
                reportStart = dateTime {
                  year = 2024
                  month = 1
                  day = 1
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2024
                  month = 2
                  day = 1
                }
              }
          }
      }
    }

    private suspend fun createReport(
      cmmsMeasurementConsumerId: String,
      reportsService: ReportsCoroutineImplBase,
      reportingSet: ReportingSet,
      metricCalculationSpec: MetricCalculationSpec,
      reportSchedule: ReportSchedule? = null,
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
                            multipleDataProviderParams =
                              MetricSpecKt.samplingAndPrivacyParams {
                                privacyParams =
                                  MetricSpecKt.differentialPrivacyParams {
                                    epsilon = 1.0
                                    delta = 2.0
                                  }
                                vidSamplingInterval =
                                  MetricSpecKt.vidSamplingInterval {
                                    start = 0.1f
                                    width = 0.5f
                                  }
                              }
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
        details =
          ReportKt.details {
            tags.putAll(REPORT_TAGS)
            reportingInterval =
              ReportKt.DetailsKt.reportingInterval {
                reportStart = dateTime {
                  year = 2024
                  month = 1
                  day = 1
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2024
                  month = 2
                  day = 1
                }
              }
          }
      }

      return reportsService.createReport(
        createReportRequest {
          this.report = report
          this.externalReportId = externalReportId
          if (reportSchedule != null) {
            reportScheduleInfo =
              CreateReportRequestKt.reportScheduleInfo {
                externalReportScheduleId = reportSchedule.externalReportScheduleId
                nextReportCreationTime = timestamp { seconds = 1000 }
              }
          }
        }
      )
    }

    private fun buildCreateMetricRequest(
      cmmsMeasurementConsumerId: String,
      externalMetricId: String,
      createdReportingSet: ReportingSet,
      reportingMetric: Report.ReportingMetric,
      metricCalculationSpecFilter: String,
    ): CreateMetricRequest {
      return createMetricRequest {
        requestId = reportingMetric.createMetricRequestId
        this.externalMetricId = externalMetricId
        metric = metric {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          externalReportingSetId = createdReportingSet.externalReportingSetId
          timeInterval = reportingMetric.details.timeInterval
          metricSpec = reportingMetric.details.metricSpec
          weightedMeasurements +=
            MetricKt.weightedMeasurement {
              weight = 1
              measurement = measurement {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                timeInterval = reportingMetric.details.timeInterval
                primitiveReportingSetBases +=
                  ReportingSetKt.primitiveReportingSetBasis {
                    externalReportingSetId = createdReportingSet.externalReportingSetId
                    filters += createdReportingSet.filter
                    filters += reportingMetric.details.groupingPredicatesList
                    filters += metricCalculationSpecFilter
                  }
              }
            }
          details =
            MetricKt.details {
              filters +=
                reportingMetric.details.groupingPredicatesList + metricCalculationSpecFilter
            }
        }
      }
    }
  }
}
