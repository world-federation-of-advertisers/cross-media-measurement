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
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.reportSchedule

@RunWith(JUnit4::class)
abstract class ReportSchedulesServiceTest<T : ReportSchedulesCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportSchedulesService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportSchedulesService
    measurementConsumersService = services.measurementConsumersService
    reportingSetsService = services.reportingSetsService
  }

  @Test
  fun `createReportSchedule succeeds`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet)

    val request = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(request)

    assertThat(createdReportSchedule.externalReportScheduleId)
      .isEqualTo(request.externalReportScheduleId)
    assertThat(createdReportSchedule.hasCreateTime()).isTrue()
    assertThat(createdReportSchedule.hasUpdateTime()).isTrue()
    assertThat(createdReportSchedule.createTime).isEqualTo(createdReportSchedule.updateTime)
    assertThat(createdReportSchedule.state).isEqualTo(ReportSchedule.State.ACTIVE)
  }

  @Test
  fun `createReportSchedule returns the same report schedule when request id used`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet)

    val request = createReportScheduleRequest {
      this.requestId = "1234"
      this.reportSchedule = reportSchedule
      this.externalReportScheduleId = "external-report-id"
    }

    val createdReportSchedule = service.createReportSchedule(request)
    assertThat(createdReportSchedule.externalReportScheduleId)
      .isEqualTo(request.externalReportScheduleId)
    assertThat(createdReportSchedule.hasCreateTime()).isTrue()
    assertThat(createdReportSchedule.hasUpdateTime()).isTrue()
    assertThat(createdReportSchedule.createTime).isEqualTo(createdReportSchedule.updateTime)
    assertThat(createdReportSchedule.state).isEqualTo(ReportSchedule.State.ACTIVE)

    val sameCreatedReportSchedule = service.createReportSchedule(request)
    assertThat(createdReportSchedule).isEqualTo(sameCreatedReportSchedule)
  }

  @Test
  fun `createReportSchedule throws ALREADY_EXISTS when same external ID used 2x`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet)

    val request = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      this.externalReportScheduleId = "external-report-id"
    }

    val createdReportSchedule = service.createReportSchedule(request)
    assertThat(createdReportSchedule.externalReportScheduleId)
      .isEqualTo(request.externalReportScheduleId)
    assertThat(createdReportSchedule.hasCreateTime()).isTrue()
    assertThat(createdReportSchedule.hasUpdateTime()).isTrue()
    assertThat(createdReportSchedule.createTime).isEqualTo(createdReportSchedule.updateTime)
    assertThat(createdReportSchedule.state).isEqualTo(ReportSchedule.State.ACTIVE)

    val exception =
      assertFailsWith<StatusRuntimeException> { service.createReportSchedule(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when missing external ID`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportSchedule(
          createReportScheduleRequest { this.reportSchedule = reportSchedule }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReportSchedule throws NOT_FOUND when ReportingSet not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val reportSchedule = reportSchedule {
      this.cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      state = ReportSchedule.State.ACTIVE
      details =
        ReportScheduleKt.details {
          displayName = "display"
          description = "description"
          reportTemplate = report {
            reportingMetricEntries["1234"] =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  ReportKt.metricCalculationSpec {
                    details =
                      ReportKt.MetricCalculationSpecKt.details {
                        this.displayName = "displayName"
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
                          ReportKt.MetricCalculationSpecKt.grouping {
                            predicates += "gender.value == MALE"
                          }
                        cumulative = false
                      }
                  }
              }
          }
          eventStart = dateTime {
            year = 2023
            month = 10
            day = 1
            hours = 6
            timeZone = timeZone { id = "America/New_York" }
          }
          eventEnd = date {
            year = 2024
            month = 12
            day = 1
          }
          frequency = ReportScheduleKt.frequency {
            daily = ReportSchedule.Frequency.Daily.getDefaultInstance()
          }
          reportWindow =
            ReportScheduleKt.reportWindow {
              trailingWindow =
                ReportScheduleKt.ReportWindowKt.trailingWindow {
                  count = 1
                  increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                }
            }
        }
      nextReportCreationTime = timestamp { seconds = 200 }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportSchedule(
          createReportScheduleRequest {
            this.reportSchedule = reportSchedule
            this.externalReportScheduleId = "external-report-schedule-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `createReportSchedule throws NOT_FOUND when not all ReportingSets found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)

    val reportSchedule = reportSchedule {
      this.cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      state = ReportSchedule.State.ACTIVE
      details =
        ReportScheduleKt.details {
          displayName = "display"
          description = "description"
          reportTemplate = report {
            reportingMetricEntries[reportingSet.externalReportingSetId] =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  ReportKt.metricCalculationSpec {
                    details =
                      ReportKt.MetricCalculationSpecKt.details {
                        this.displayName = "displayName"
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
                          ReportKt.MetricCalculationSpecKt.grouping {
                            predicates += "gender.value == MALE"
                          }
                        cumulative = false
                      }
                  }
              }

            reportingMetricEntries[reportingSet.externalReportingSetId + "1"] =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecs +=
                  ReportKt.metricCalculationSpec {
                    details =
                      ReportKt.MetricCalculationSpecKt.details {
                        this.displayName = "displayName"
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
                          ReportKt.MetricCalculationSpecKt.grouping {
                            predicates += "gender.value == MALE"
                          }
                        cumulative = false
                      }
                  }
              }
          }
          eventStart = dateTime {
            year = 2023
            month = 10
            day = 1
            hours = 6
            timeZone = timeZone { id = "America/New_York" }
          }
          eventEnd = date {
            year = 2024
            month = 12
            day = 1
          }
          frequency = ReportScheduleKt.frequency {
            daily = ReportSchedule.Frequency.Daily.getDefaultInstance()
          }
          reportWindow =
            ReportScheduleKt.reportWindow {
              trailingWindow =
                ReportScheduleKt.ReportWindowKt.trailingWindow {
                  count = 1
                  increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                }
            }
        }
      nextReportCreationTime = timestamp { seconds = 200 }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportSchedule(
          createReportScheduleRequest {
            this.reportSchedule = reportSchedule
            this.externalReportScheduleId = "external-report-schedule-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("Reporting Set")
  }

  @Test
  fun `createReportSchedule throws INVALID_ARGUMENT when no reporting metric entries`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

      val reportSchedule = reportSchedule {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        state = ReportSchedule.State.ACTIVE
        details =
          ReportScheduleKt.details {
            displayName = "display"
            description = "description"
            reportTemplate = report {}
            eventStart = dateTime {
              year = 2023
              month = 10
              day = 1
              hours = 6
              timeZone = timeZone { id = "America/New_York" }
            }
            eventEnd = date {
              year = 2024
              month = 12
              day = 1
            }
            frequency = ReportScheduleKt.frequency {
              daily = ReportSchedule.Frequency.Daily.getDefaultInstance()
            }
            reportWindow =
              ReportScheduleKt.reportWindow {
                trailingWindow =
                  ReportScheduleKt.ReportWindowKt.trailingWindow {
                    count = 1
                    increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                  }
              }
          }
        nextReportCreationTime = timestamp { seconds = 200 }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createReportSchedule(
            createReportScheduleRequest {
              this.reportSchedule = reportSchedule
              externalReportScheduleId = "external-report-schedule-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("missing reporting metric entries")
    }

  @Test
  fun `createReportSchedule throws FAILED_PRECONDITION when MC not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val reportSchedule =
      createReportScheduleForRequest(reportingSet).copy { cmmsMeasurementConsumerId += "123" }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.createReportSchedule(
          createReportScheduleRequest {
            this.reportSchedule = reportSchedule
            externalReportScheduleId = "external-report-schedule-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("Measurement Consumer")
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

    private fun createReportScheduleForRequest(
      reportingSet: ReportingSet,
    ): ReportSchedule {
      return reportSchedule {
        this.cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        details =
          ReportScheduleKt.details {
            displayName = "display"
            description = "description"
            reportTemplate = report {
              reportingMetricEntries[reportingSet.externalReportingSetId] =
                ReportKt.reportingMetricCalculationSpec {
                  metricCalculationSpecs +=
                    ReportKt.metricCalculationSpec {
                      details =
                        ReportKt.MetricCalculationSpecKt.details {
                          this.displayName = "displayName"
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
                            ReportKt.MetricCalculationSpecKt.grouping {
                              predicates += "gender.value == MALE"
                            }
                          cumulative = false
                        }
                    }
                }
            }
            eventStart = dateTime {
              year = 2023
              month = 10
              day = 1
              hours = 6
              timeZone = timeZone { id = "America/New_York" }
            }
            eventEnd = date {
              year = 2024
              month = 12
              day = 1
            }
            frequency =
              ReportScheduleKt.frequency {
                daily = ReportSchedule.Frequency.Daily.getDefaultInstance()
              }
            reportWindow =
              ReportScheduleKt.reportWindow {
                trailingWindow =
                  ReportScheduleKt.ReportWindowKt.trailingWindow {
                    count = 1
                    increment = ReportSchedule.ReportWindow.TrailingWindow.Increment.DAY
                  }
              }
          }
        nextReportCreationTime = timestamp { seconds = 200 }
      }
    }
  }
}
