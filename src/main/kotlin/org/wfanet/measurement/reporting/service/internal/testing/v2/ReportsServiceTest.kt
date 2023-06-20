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
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Ignore
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.periodicTimeInterval
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.timeInterval
import org.wfanet.measurement.internal.reporting.v2.timeIntervals

@RunWith(JUnit4::class)
abstract class ReportsServiceTest<T : ReportsGrpcKt.ReportsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportsService: T,
    val metricsService: MetricsGrpcKt.MetricsCoroutineImplBase,
    val reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase,
    val measurementConsumersService:
      MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var metricsService: MetricsGrpcKt.MetricsCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
  private lateinit var measurementConsumersService:
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportsService
    metricsService = services.metricsService
    reportingSetsService = services.reportingSetsService
    measurementConsumersService = services.measurementConsumersService
  }

  @Test
  fun `createReport succeeds when timeIntervals set`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val baseReport = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val report =
      baseReport.copy {
        timeIntervals = timeIntervals {
          timeIntervals += timeInterval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          timeIntervals += timeInterval {
            startTime = timestamp { seconds = 300 }
            endTime = timestamp { seconds = 400 }
          }
        }
      }

    val createdReport = service.createReport(createReportRequest { this.report = report })

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
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

    val createdReport = service.createReport(createReportRequest { this.report = report })

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
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
    val createdReportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val createdReportingSet2 =
      createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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
                    timeInterval = timeInterval {
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
                    timeInterval = timeInterval {
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
      reportingMetricEntries[createdReportingSet2.externalReportingSetId] =
        reportingMetricCalculationSpec2
      timeIntervals = timeIntervals {
        timeIntervals += timeInterval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
        timeIntervals += timeInterval {
          startTime = timestamp { seconds = 300 }
          endTime = timestamp { seconds = 400 }
        }
      }
    }

    val createdReport = service.createReport(createReportRequest { this.report = report })

    assertThat(createdReport.externalReportId).isNotEqualTo(0)
    assertThat(createdReport.hasCreateTime()).isTrue()
    createdReport.reportingMetricEntriesMap.entries.forEach { entry ->
      entry.value.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          assertThat(it.createMetricRequestId).isNotEmpty()
        }
      }
    }
  }

  /** TODO(@tristanvuong2021): Test will not pass until read implemented. */
  @Ignore
  @Test
  fun `createReport returns the same report when request id used`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val report = createReportForRequest(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val requestId = "1234"
    val request = createReportRequest {
      this.requestId = requestId
      this.report = report
    }

    val createdReport = service.createReport(request)
    assertThat(createdReport.externalReportId).isNotEqualTo(0)
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
  fun `createReport throws NOT_FOUND when ReportingSet not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val report = report {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportingMetricEntries[1234] =
        ReportKt.reportingMetricCalculationSpec {
          metricCalculationSpecs +=
            ReportKt.metricCalculationSpec {
              reportingMetrics +=
                ReportKt.reportingMetric {
                  details =
                    ReportKt.ReportingMetricKt.details {
                      externalReportingSetId = 1234
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
                      timeInterval = timeInterval {
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
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(createReportRequest { this.report = report })
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
                    timeInterval = timeInterval {
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
                    externalReportingSetId = 1234
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
                    timeInterval = timeInterval {
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
      reportingMetricEntries[1234] = reportingMetricCalculationSpec2
      periodicTimeInterval = periodicTimeInterval {
        startTime = timestamp { seconds = 100 }
        increment = duration { seconds = 50 }
        intervalCount = 3
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(createReportRequest { this.report = report })
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
                    externalReportingSetId = 1234
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
                    timeInterval = timeInterval {
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
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(createReportRequest { this.report = report })
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
        service.createReport(createReportRequest { this.report = report })
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
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReport(createReportRequest { this.report = report })
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
        service.createReport(createReportRequest { this.report = report })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

    private suspend fun createReportForRequest(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase,
    ): Report {
      val createdReportingSet =
        createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

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
                        timeInterval = timeInterval {
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
        timeIntervals = timeIntervals {
          timeIntervals += timeInterval {
            startTime = timestamp { seconds = 100 }
            endTime = timestamp { seconds = 200 }
          }
          timeIntervals += timeInterval {
            startTime = timestamp { seconds = 300 }
            endTime = timestamp { seconds = 400 }
          }
        }
      }
    }

    private suspend fun createReportingSet(
      cmmsMeasurementConsumerId: String,
      reportingSetsService: ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
    ): ReportingSet {
      val reportingSet = reportingSet {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        primitive =
          ReportingSetKt.primitive {
            eventGroupKeys +=
              ReportingSetKt.PrimitiveKt.eventGroupKey {
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                cmmsDataProviderId = "1235"
                cmmsEventGroupId = cmmsMeasurementConsumerId + "123"
              }
          }
      }
      return reportingSetsService.createReportingSet(reportingSet)
    }

    private suspend fun createMeasurementConsumer(
      cmmsMeasurementConsumerId: String,
      measurementConsumersService: MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase,
    ) {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
      )
    }
  }
}
