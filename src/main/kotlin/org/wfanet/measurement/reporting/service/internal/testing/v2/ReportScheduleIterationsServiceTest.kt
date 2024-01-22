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
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.getReportScheduleIterationRequest
import org.wfanet.measurement.internal.reporting.v2.listReportScheduleIterationsRequest
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.setReportScheduleIterationStateRequest

@RunWith(JUnit4::class)
abstract class ReportScheduleIterationsServiceTest<T : ReportScheduleIterationsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportScheduleIterationsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase,
    val reportSchedulesService: ReportSchedulesCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var metricCalculationSpecsService: MetricCalculationSpecsCoroutineImplBase
  private lateinit var reportSchedulesService: ReportSchedulesCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportScheduleIterationsService
    measurementConsumersService = services.measurementConsumersService
    reportingSetsService = services.reportingSetsService
    metricCalculationSpecsService = services.metricCalculationSpecsService
    reportSchedulesService = services.reportSchedulesService
  }

  @Test
  fun `createReportScheduleIteration return report schedule iteration`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )
    val reportScheduleIteration = reportScheduleIteration {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportScheduleId = reportSchedule.externalReportScheduleId
      createReportRequestId = "123"
      reportEventTime = timestamp { seconds = 100 }
    }
    val createdReportScheduleIteration =
      service.createReportScheduleIteration(reportScheduleIteration)

    assertThat(createdReportScheduleIteration.externalReportScheduleIterationId).isNotEmpty()
    assertThat(createdReportScheduleIteration.hasCreateTime()).isTrue()
    assertThat(createdReportScheduleIteration.hasUpdateTime()).isTrue()
    assertThat(createdReportScheduleIteration.createTime)
      .isEqualTo(createdReportScheduleIteration.updateTime)
    assertThat(createdReportScheduleIteration.state)
      .isEqualTo(ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY)
    assertThat(createdReportScheduleIteration.numAttempts).isEqualTo(0)
  }

  @Test
  fun `createReportScheduleIteration throws FAILED_PRECONDITION when schedule not found`() =
    runBlocking {
      val reportScheduleIteration = reportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = "123"
        createReportRequestId = "123"
        reportEventTime = timestamp { seconds = 100 }
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.createReportScheduleIteration(reportScheduleIteration)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.message).contains("Report Schedule")
    }

  @Test
  fun `getReportScheduleIteration return report schedule iteration`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )
    val reportScheduleIteration = createReportScheduleIterationForRequest(reportSchedule)

    val createdReportScheduleIteration =
      service.createReportScheduleIteration(reportScheduleIteration)

    val retrievedReportScheduleIteration =
      service.getReportScheduleIteration(
        getReportScheduleIterationRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = createdReportScheduleIteration.externalReportScheduleId
          externalReportScheduleIterationId =
            createdReportScheduleIteration.externalReportScheduleIterationId
        }
      )

    assertThat(retrievedReportScheduleIteration.externalReportScheduleIterationId)
      .isEqualTo(createdReportScheduleIteration.externalReportScheduleIterationId)
    assertThat(retrievedReportScheduleIteration.createTime.seconds).isGreaterThan(0)
    assertThat(retrievedReportScheduleIteration.updateTime.seconds).isGreaterThan(0)
    assertThat(retrievedReportScheduleIteration.createTime)
      .isEqualTo(retrievedReportScheduleIteration.updateTime)
    assertThat(retrievedReportScheduleIteration.state)
      .isEqualTo(ReportScheduleIteration.State.WAITING_FOR_DATA_AVAILABILITY)
    assertThat(retrievedReportScheduleIteration)
      .ignoringFields(
        ReportScheduleIteration.CREATE_TIME_FIELD_NUMBER,
        ReportScheduleIteration.UPDATE_TIME_FIELD_NUMBER,
        ReportScheduleIteration.EXTERNAL_REPORT_SCHEDULE_ITERATION_ID_FIELD_NUMBER,
        ReportScheduleIteration.STATE_FIELD_NUMBER,
      )
      .isEqualTo(reportScheduleIteration)
  }

  @Test
  fun `getReportScheduleIteration throws NOT_FOUND when iteration not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getReportScheduleIteration(
          getReportScheduleIterationRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = reportSchedule.externalReportScheduleId
            externalReportScheduleIterationId = "1234"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `getReportScheduleIteration throws INVALID_ARGUMENT when cmms mc id missing`() = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getReportScheduleIteration(
          getReportScheduleIterationRequest {
            externalReportScheduleId = "external-report-schedule-id"
            externalReportScheduleIterationId = "external-report-schedule-iteration-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.message).contains("cmms_measurement_consumer_i")
  }

  @Test
  fun `getReportScheduleIteration throws INVALID_ARGUMENT when schedule id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getReportScheduleIteration(
            getReportScheduleIterationRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleIterationId = "external-report-schedule-iteration-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("external_report_schedule_id")
    }

  @Test
  fun `getReportScheduleIteration throws INVALID_ARGUMENT when iteration id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getReportScheduleIteration(
            getReportScheduleIterationRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = "external-report-schedule-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("external_report_schedule_iteration_id")
    }

  @Test
  fun `listReportScheduleIterations lists 2 iterations in desc order by report event time`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
      val metricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
      val reportSchedule =
        createReportSchedule(
          CMMS_MEASUREMENT_CONSUMER_ID,
          reportingSet,
          metricCalculationSpec,
          reportSchedulesService,
        )

      val reportScheduleIteration =
        createReportScheduleIterationForRequest(reportSchedule).copy {
          reportEventTime = timestamp { seconds = 200 }
        }
      val createdReportScheduleIteration =
        service.createReportScheduleIteration(reportScheduleIteration)

      val reportScheduleIteration2 =
        createReportScheduleIterationForRequest(reportSchedule).copy {
          reportEventTime = timestamp { seconds = 100 }
        }
      service.createReportScheduleIteration(reportScheduleIteration2)

      val retrievedReportScheduleIterations =
        service
          .listReportScheduleIterations(
            listReportScheduleIterationsRequest {
              filter =
                ListReportScheduleIterationsRequestKt.filter {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  externalReportScheduleId = reportSchedule.externalReportScheduleId
                }
            }
          )
          .reportScheduleIterationsList

      assertThat(retrievedReportScheduleIterations).hasSize(2)
      assertThat(
          Timestamps.compare(
            retrievedReportScheduleIterations[0].reportEventTime,
            retrievedReportScheduleIterations[1].reportEventTime,
          )
        )
        .isGreaterThan(0)
      assertThat(retrievedReportScheduleIterations[0].reportEventTime)
        .isEqualTo(createdReportScheduleIteration.reportEventTime)
    }

  @Test
  fun `listReportScheduleIterations lists 1 iteration when limit is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )

    val reportScheduleIteration =
      createReportScheduleIterationForRequest(reportSchedule).copy {
        reportEventTime = timestamp { seconds = 200 }
      }
    val createdReportScheduleIteration =
      service.createReportScheduleIteration(reportScheduleIteration)

    val reportScheduleIteration2 =
      createReportScheduleIterationForRequest(reportSchedule).copy {
        reportEventTime = timestamp { seconds = 100 }
      }
    service.createReportScheduleIteration(reportScheduleIteration2)

    val retrievedReportScheduleIterations =
      service
        .listReportScheduleIterations(
          listReportScheduleIterationsRequest {
            filter =
              ListReportScheduleIterationsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = reportSchedule.externalReportScheduleId
              }
            limit = 1
          }
        )
        .reportScheduleIterationsList

    assertThat(retrievedReportScheduleIterations).hasSize(1)
    assertThat(retrievedReportScheduleIterations[0].reportEventTime)
      .isEqualTo(createdReportScheduleIteration.reportEventTime)
  }

  @Test
  fun `listReportScheduleIteration lists 1 iteration when after id is specified`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )

    val reportScheduleIteration =
      createReportScheduleIterationForRequest(reportSchedule).copy {
        reportEventTime = timestamp { seconds = 200 }
      }
    val createdReportScheduleIteration =
      service.createReportScheduleIteration(reportScheduleIteration)

    val reportScheduleIteration2 =
      createReportScheduleIterationForRequest(reportSchedule).copy {
        reportEventTime = timestamp { seconds = 100 }
      }
    val createdReportScheduleIteration2 =
      service.createReportScheduleIteration(reportScheduleIteration2)

    val retrievedReportScheduleIterations =
      service
        .listReportScheduleIterations(
          listReportScheduleIterationsRequest {
            filter =
              ListReportScheduleIterationsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportScheduleId = reportSchedule.externalReportScheduleId
                reportEventTimeBefore = createdReportScheduleIteration.reportEventTime
              }
          }
        )
        .reportScheduleIterationsList

    assertThat(retrievedReportScheduleIterations).hasSize(1)
    assertThat(retrievedReportScheduleIterations[0].reportEventTime)
      .isEqualTo(createdReportScheduleIteration2.reportEventTime)
  }

  @Test
  fun `listReportScheduleIterations throws INVALID_ARGUMENT when cmms mc id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.listReportScheduleIterations(listReportScheduleIterationsRequest {})
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("cmms_measurement_consumer_id")
    }

  @Test
  fun `setReportScheduleIterationState sets state to RETRYING_REPORT_CREATION`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )
    val reportScheduleIteration = createReportScheduleIterationForRequest(reportSchedule)
    val createdReportScheduleIteration =
      service.createReportScheduleIteration(reportScheduleIteration)

    val updatedReportScheduleIteration =
      service.setReportScheduleIterationState(
        setReportScheduleIterationStateRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = reportSchedule.externalReportScheduleId
          externalReportScheduleIterationId =
            createdReportScheduleIteration.externalReportScheduleIterationId
          state = ReportScheduleIteration.State.RETRYING_REPORT_CREATION
        }
      )

    assertThat(createdReportScheduleIteration.numAttempts).isEqualTo(0)
    assertThat(updatedReportScheduleIteration.numAttempts).isEqualTo(1)
    assertThat(updatedReportScheduleIteration.state)
      .isEqualTo(ReportScheduleIteration.State.RETRYING_REPORT_CREATION)
    assertThat(
        Timestamps.compare(
          updatedReportScheduleIteration.updateTime,
          updatedReportScheduleIteration.createTime,
        )
      )
      .isGreaterThan(0)

    val retrievedReportScheduleIteration =
      service.getReportScheduleIteration(
        getReportScheduleIterationRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportScheduleId = reportSchedule.externalReportScheduleId
          externalReportScheduleIterationId =
            createdReportScheduleIteration.externalReportScheduleIterationId
        }
      )

    assertThat(retrievedReportScheduleIteration.numAttempts).isEqualTo(1)
    assertThat(retrievedReportScheduleIteration.state)
      .isEqualTo(ReportScheduleIteration.State.RETRYING_REPORT_CREATION)
    assertThat(
        Timestamps.compare(
          retrievedReportScheduleIteration.updateTime,
          retrievedReportScheduleIteration.createTime,
        )
      )
      .isGreaterThan(0)
  }

  @Test
  fun `setReportScheduleIterationState throws NOT_FOUND when iteration not found`() = runBlocking {
    createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
    val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
    val metricCalculationSpec =
      createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
    val reportSchedule =
      createReportSchedule(
        CMMS_MEASUREMENT_CONSUMER_ID,
        reportingSet,
        metricCalculationSpec,
        reportSchedulesService,
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setReportScheduleIterationState(
          setReportScheduleIterationStateRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportScheduleId = reportSchedule.externalReportScheduleId
            externalReportScheduleIterationId = "external-report-schedule-iteration-id"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.message).contains("not found")
  }

  @Test
  fun `setReportScheduleIterationState throws FAILED_PRECONDITION when state is REPORT_CREATED`() =
    runBlocking {
      createMeasurementConsumer(CMMS_MEASUREMENT_CONSUMER_ID, measurementConsumersService)
      val reportingSet = createReportingSet(CMMS_MEASUREMENT_CONSUMER_ID, reportingSetsService)
      val metricCalculationSpec =
        createMetricCalculationSpec(CMMS_MEASUREMENT_CONSUMER_ID, metricCalculationSpecsService)
      val reportSchedule =
        createReportSchedule(
          CMMS_MEASUREMENT_CONSUMER_ID,
          reportingSet,
          metricCalculationSpec,
          reportSchedulesService,
        )
      val reportScheduleIteration = createReportScheduleIterationForRequest(reportSchedule)
      val createdReportScheduleIteration =
        service.createReportScheduleIteration(reportScheduleIteration)

      val setRequest = setReportScheduleIterationStateRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = reportSchedule.externalReportScheduleId
        externalReportScheduleIterationId =
          createdReportScheduleIteration.externalReportScheduleIterationId
        state = ReportScheduleIteration.State.REPORT_CREATED
      }
      service.setReportScheduleIterationState(setRequest)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setReportScheduleIterationState(setRequest)
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.message).contains("REPORT_CREATED")
    }

  @Test
  fun `setReportScheduleIterationState throws INVALID_ARGUMENT when cmms mc id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setReportScheduleIterationState(
            setReportScheduleIterationStateRequest {
              externalReportScheduleId = "external-report-schedule-id"
              externalReportScheduleIterationId = "external-report-schedule-iteration-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("cmms_measurement_consumer_id")
    }

  @Test
  fun `setReportScheduleIterationState throws INVALID_ARGUMENT when schedule id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setReportScheduleIterationState(
            setReportScheduleIterationStateRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleIterationId = "external-report-schedule-iteration-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("external_report_schedule_id")
    }

  @Test
  fun `setReportScheduleIterationState throws INVALID_ARGUMENT when iteration id missing`() =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setReportScheduleIterationState(
            setReportScheduleIterationStateRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportScheduleId = "external-report-schedule-id"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("external_report_schedule_iteration_id")
    }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

    private fun createReportScheduleIterationForRequest(
      reportSchedule: ReportSchedule
    ): ReportScheduleIteration {
      return reportScheduleIteration {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportScheduleId = reportSchedule.externalReportScheduleId
        createReportRequestId = "123"
        reportEventTime = timestamp { seconds = 100 }
      }
    }
  }
}
