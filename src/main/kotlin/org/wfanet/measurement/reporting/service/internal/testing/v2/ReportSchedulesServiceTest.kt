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
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.report
import org.wfanet.measurement.internal.reporting.v2.reportSchedule
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.stopReportScheduleRequest
import org.wfanet.measurement.reporting.service.internal.Errors

@RunWith(JUnit4::class)
abstract class ReportSchedulesServiceTest<T : ReportSchedulesCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportSchedulesService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
    val reportScheduleIterationsService: ReportScheduleIterationsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase
  private lateinit var reportScheduleIterationsService: ReportScheduleIterationsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportSchedulesService
    measurementConsumersService = services.measurementConsumersService
    reportingSetsService = services.reportingSetsService
    metricCalculationSpecsService = services.metricCalculationSpecsService
    reportScheduleIterationsService = services.reportScheduleIterationsService
  }

  @Test
  fun `createReportSchedule succeeds`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

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
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

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
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

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
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

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
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

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
                metricCalculationSpecReportingMetrics +=
                  ReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId =
                      metricCalculationSpec.externalMetricCalculationSpecId
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
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)

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
                metricCalculationSpecReportingMetrics +=
                  ReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId =
                      metricCalculationSpec.externalMetricCalculationSpecId
                  }
              }

            reportingMetricEntries[reportingSet.externalReportingSetId + "1"] =
              ReportKt.reportingMetricCalculationSpec {
                metricCalculationSpecReportingMetrics +=
                  ReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId =
                      metricCalculationSpec.externalMetricCalculationSpecId
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
  fun `createReportSchedule throws NOT_FOUND when MetricCalculationSpec found`() = runBlocking {
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
                metricCalculationSpecReportingMetrics +=
                  ReportKt.metricCalculationSpecReportingMetrics {
                    externalMetricCalculationSpecId = "1234"
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
    assertThat(exception.message).contains("Metric Calculation Spec")
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
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportScheduleForRequest(reportingSet, metricCalculationSpec).copy {
        cmmsMeasurementConsumerId += "123"
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

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.MEASUREMENT_CONSUMER_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            reportSchedule.cmmsMeasurementConsumerId
        }
      )
  }

  @Test
  fun `getReportSchedule succeeds`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)
    val createRequest = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(createRequest)

    val retrievedReportSchedule =
      service.getReportSchedule(
        getReportScheduleRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = createdReportSchedule.externalReportScheduleId
        }
      )

    assertThat(retrievedReportSchedule.externalReportScheduleId)
      .isEqualTo(createRequest.externalReportScheduleId)
    assertThat(retrievedReportSchedule.createTime.seconds).isGreaterThan(0)
    assertThat(retrievedReportSchedule.updateTime.seconds).isGreaterThan(0)
    assertThat(retrievedReportSchedule.createTime).isEqualTo(retrievedReportSchedule.updateTime)
    assertThat(retrievedReportSchedule.state).isEqualTo(ReportSchedule.State.ACTIVE)
    assertThat(retrievedReportSchedule)
      .ignoringFields(
        ReportSchedule.CREATE_TIME_FIELD_NUMBER,
        ReportSchedule.UPDATE_TIME_FIELD_NUMBER,
        ReportSchedule.EXTERNAL_REPORT_SCHEDULE_ID_FIELD_NUMBER,
        ReportSchedule.STATE_FIELD_NUMBER,
      )
      .isEqualTo(reportSchedule)
  }

  @Test
  fun `getReportSchedule succeeds after report schedules iteration is created`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)
    val createRequest = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(createRequest)

    val createdReportScheduleIteration =
      reportScheduleIterationsService.createReportScheduleIteration(
        reportScheduleIteration {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = createdReportSchedule.externalReportScheduleId
          createReportRequestId = "123"
          reportEventTime = timestamp { seconds = 100 }
        }
      )

    val retrievedReportSchedule =
      service.getReportSchedule(
        getReportScheduleRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = createdReportSchedule.externalReportScheduleId
        }
      )

    assertThat(retrievedReportSchedule.externalReportScheduleId)
      .isEqualTo(createRequest.externalReportScheduleId)
    assertThat(retrievedReportSchedule.createTime.seconds).isGreaterThan(0)
    assertThat(retrievedReportSchedule.updateTime.seconds).isGreaterThan(0)
    assertThat(retrievedReportSchedule.createTime).isEqualTo(retrievedReportSchedule.updateTime)
    assertThat(retrievedReportSchedule.state).isEqualTo(ReportSchedule.State.ACTIVE)
    assertThat(retrievedReportSchedule)
      .ignoringFields(
        ReportSchedule.CREATE_TIME_FIELD_NUMBER,
        ReportSchedule.UPDATE_TIME_FIELD_NUMBER,
        ReportSchedule.EXTERNAL_REPORT_SCHEDULE_ID_FIELD_NUMBER,
        ReportSchedule.STATE_FIELD_NUMBER,
      )
      .isEqualTo(reportSchedule.copy { latestIteration = createdReportScheduleIteration })
  }

  @Test
  fun `getReportSchedule throws NOT_FOUND when report schedule not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getReportSchedule(
          getReportScheduleRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = "external-report-schedule-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `getReportSchedule throws INVALID_ARGUMENT when cmms mc id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getReportSchedule(
          getReportScheduleRequest { externalReportScheduleId = "external-report-schedule-id" }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_i")
  }

  @Test
  fun `getReportSchedule throws INVALID_ARGUMENT when schedule id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getReportSchedule(
          getReportScheduleRequest { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("external_report_schedule_id")
  }

  @Test
  fun `listReportSchedules lists 2 schedules in asc order by external id`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

    val request = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(request)
    service.createReportSchedule(
      request.copy { externalReportScheduleId = "external-report-schedule-id-2" }
    )

    val retrievedReportSchedules =
      service
        .listReportSchedules(
          listReportSchedulesRequest {
            filter =
              ListReportSchedulesRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .reportSchedulesList

    assertThat(retrievedReportSchedules).hasSize(2)
    assertThat(retrievedReportSchedules[0].externalReportScheduleId)
      .isLessThan(retrievedReportSchedules[1].externalReportScheduleId)
    assertThat(retrievedReportSchedules[0].externalReportScheduleId)
      .isEqualTo(createdReportSchedule.externalReportScheduleId)
  }

  @Test
  fun `listReportSchedules lists 1 schedule when limit is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

    val request = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(request)
    service.createReportSchedule(
      request.copy { externalReportScheduleId = "external-report-schedule-id-2" }
    )

    val retrievedReportSchedules =
      service
        .listReportSchedules(
          listReportSchedulesRequest {
            filter =
              ListReportSchedulesRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            limit = 1
          }
        )
        .reportSchedulesList

    assertThat(retrievedReportSchedules).hasSize(1)
    assertThat(retrievedReportSchedules[0].externalReportScheduleId)
      .isEqualTo(createdReportSchedule.externalReportScheduleId)
  }

  @Test
  fun `listReportSchedules lists 1 schedule when state is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

    val request = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(request)
    service.stopReportSchedule(
      stopReportScheduleRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = createdReportSchedule.externalReportScheduleId
      }
    )
    val createdReportSchedule2 =
      service.createReportSchedule(
        request.copy { externalReportScheduleId = "external-report-schedule-id-2" }
      )

    val retrievedReportSchedules =
      service
        .listReportSchedules(
          listReportSchedulesRequest {
            filter =
              ListReportSchedulesRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = ReportSchedule.State.ACTIVE
              }
            limit = 1
          }
        )
        .reportSchedulesList

    assertThat(retrievedReportSchedules).hasSize(1)
    assertThat(retrievedReportSchedules[0].externalReportScheduleId)
      .isEqualTo(createdReportSchedule2.externalReportScheduleId)
  }

  @Test
  fun `listReportSchedules lists 1 schedule when after id is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)

    val request = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(request)
    val createdReportSchedule2 =
      service.createReportSchedule(
        request.copy { externalReportScheduleId = "external-report-schedule-id-2" }
      )

    val retrievedReportSchedules =
      service
        .listReportSchedules(
          listReportSchedulesRequest {
            filter =
              ListReportSchedulesRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleIdAfter = createdReportSchedule.externalReportScheduleId
              }
          }
        )
        .reportSchedulesList

    assertThat(retrievedReportSchedules).hasSize(1)
    assertThat(retrievedReportSchedules[0].externalReportScheduleId)
      .isEqualTo(createdReportSchedule2.externalReportScheduleId)
  }

  @Test
  fun `listReportSchedules throws INVALID_ARGUMENT when cmms mc id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listReportSchedules(listReportSchedulesRequest {})
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_id")
  }

  @Test
  fun `stopReportSchedule sets state to STOPPED`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)
    val createRequest = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(createRequest)

    val stopRequest = stopReportScheduleRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = createdReportSchedule.externalReportScheduleId
    }
    val stoppedReportSchedule = service.stopReportSchedule(stopRequest)

    assertThat(stoppedReportSchedule.externalReportScheduleId)
      .isEqualTo(createdReportSchedule.externalReportScheduleId)
    assertThat(stoppedReportSchedule.state).isEqualTo(ReportSchedule.State.STOPPED)
    assertThat(
        Timestamps.compare(stoppedReportSchedule.updateTime, createdReportSchedule.createTime)
      )
      .isGreaterThan(0)

    val retrievedReportSchedule =
      service.getReportSchedule(
        getReportScheduleRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = createdReportSchedule.externalReportScheduleId
        }
      )

    assertThat(retrievedReportSchedule.state).isEqualTo(ReportSchedule.State.STOPPED)
    assertThat(
        Timestamps.compare(retrievedReportSchedule.updateTime, createdReportSchedule.createTime)
      )
      .isGreaterThan(0)
  }

  @Test
  fun `stopReportSchedule throws NOT_FOUND when report schedule not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.stopReportSchedule(
          stopReportScheduleRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = "external-report-schedule-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `stopReportSchedule throws FAILED_PRECONDITION when state not active`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule = createReportScheduleForRequest(reportingSet, metricCalculationSpec)
    val createRequest = createReportScheduleRequest {
      this.reportSchedule = reportSchedule
      externalReportScheduleId = "external-report-schedule-id"
    }
    val createdReportSchedule = service.createReportSchedule(createRequest)

    val stopRequest = stopReportScheduleRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = createdReportSchedule.externalReportScheduleId
    }
    service.stopReportSchedule(stopRequest)

    val exception =
      assertFailsWith<StatusRuntimeException> { service.stopReportSchedule(stopRequest) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.message).contains("not ACTIVE")
  }

  @Test
  fun `stopReportSchedule throws INVALID_ARGUMENT when cmms mc id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.stopReportSchedule(
          stopReportScheduleRequest { externalReportScheduleId = "external-report-schedule-id" }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_i")
  }

  @Test
  fun `stopReportSchedule throws INVALID_ARGUMENT when schedule id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.stopReportSchedule(
          stopReportScheduleRequest { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("external_report_schedule_id")
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

    private fun createReportScheduleForRequest(
      reportingSet: ReportingSet,
      metricCalculationSpec: MetricCalculationSpec,
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
                  metricCalculationSpecReportingMetrics +=
                    ReportKt.metricCalculationSpecReportingMetrics {
                      externalMetricCalculationSpecId =
                        metricCalculationSpec.externalMetricCalculationSpecId
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
