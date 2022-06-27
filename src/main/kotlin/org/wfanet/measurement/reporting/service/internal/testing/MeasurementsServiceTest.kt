// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.service.internal.testing

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.duration
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
import org.wfanet.measurement.internal.reporting.CreateReportRequestKt
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.MeasurementKt
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.Metric
import org.wfanet.measurement.internal.reporting.MetricKt
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.ReportKt
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.createReportRequest
import org.wfanet.measurement.internal.reporting.getMeasurementRequest
import org.wfanet.measurement.internal.reporting.getReportRequest
import org.wfanet.measurement.internal.reporting.measurement
import org.wfanet.measurement.internal.reporting.metric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval
import org.wfanet.measurement.internal.reporting.report
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.timeInterval

@RunWith(JUnit4::class)
abstract class MeasurementsServiceTest<T : MeasurementsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val measurementsService: T,
    val reportsService: ReportsCoroutineImplBase
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var reportsService: ReportsCoroutineImplBase

  /** Constructs the service being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.measurementsService
    reportsService = services.reportsService
  }

  @Test
  fun `createMeasurement succeeds`() {
    val measurement = measurement {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
    }

    val createdMeasurement = runBlocking { service.createMeasurement(measurement) }

    assertThat(createdMeasurement).isEqualTo(measurement.copy { state = Measurement.State.PENDING })

    val getRequest = getMeasurementRequest {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
    }
    val retrievedMeasurement = runBlocking { service.getMeasurement(getRequest) }
    assertThat(retrievedMeasurement).isEqualTo(createdMeasurement)
  }

  @Test
  fun `createMeasurement fails when trying to create the same one twice`() {
    val measurement = measurement {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
    }

    val exception = runBlocking {
      service.createMeasurement(measurement)
      assertFailsWith<StatusRuntimeException> { service.createMeasurement(measurement) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `setMeasurementResult stores the result and the succeeded state for a measurement`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = "1234"
          measurementReferenceId = "4321"
        }
      )
    }
    val result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }
    val request = setMeasurementResultRequest {
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
      measurementResult = result
    }

    val updatedMeasurement = runBlocking { service.setMeasurementResult(request) }
    assertThat(updatedMeasurement)
      .isEqualTo(
        measurement {
          measurementConsumerReferenceId = request.measurementConsumerReferenceId
          measurementReferenceId = request.measurementReferenceId
          state = Measurement.State.SUCCEEDED
          this.result = result
        }
      )

    val getRequest = getMeasurementRequest {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
    }
    val retrievedMeasurement = runBlocking { service.getMeasurement(getRequest) }
    assertThat(retrievedMeasurement).isEqualTo(updatedMeasurement)
  }

  @Test
  fun `setMeasurementResult fails when the meaurement doesn't exist`() {
    val request = setMeasurementResultRequest {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
      measurementResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> { service.setMeasurementResult(request) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `setMeasurementResult succeeds in setting the report result and succeeded report state`() {
    val measurementConsumerReferenceId = "1234"
    val measurementReferenceId = "1234"
    val result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              this.measurementConsumerReferenceId = measurementConsumerReferenceId
              this.measurementReferenceId = measurementReferenceId
            }
          report = report {
            this.measurementConsumerReferenceId = measurementConsumerReferenceId
            reportIdempotencyKey = "1235"
            periodicTimeInterval = periodicTimeInterval {
              startTime = timestamp {
                seconds = 100
                nanos = 10
              }
              increment = duration {
                seconds = 10
                nanos = 1
              }
              intervalCount = 2
            }
            val inProgressReport = this
            metrics += metric {
              namedSetOperations +=
                MetricKt.namedSetOperation {
                  displayName = "name4"
                  setOperation = MetricKt.setOperation { type = Metric.SetOperation.Type.UNION }
                  measurementCalculations +=
                    MetricKt.measurementCalculation {
                      timeInterval = timeInterval {
                        startTime =
                          Timestamps.add(
                            inProgressReport.periodicTimeInterval.startTime,
                            inProgressReport.periodicTimeInterval.increment
                          )
                        endTime =
                          Timestamps.add(startTime, inProgressReport.periodicTimeInterval.increment)
                      }
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          this.measurementReferenceId = measurementReferenceId
                          coefficient = 1
                        }
                    }
                }
            }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementResult(
        setMeasurementResultRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          this.measurementReferenceId = measurementReferenceId
          measurementResult = result
          externalReportId = createdReport.externalReportId
          reportResult =
            ReportKt.DetailsKt.result {
              scalarTable = ReportKt.DetailsKt.ResultKt.scalarTable { rowHeaders += "test" }
            }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertThat(retrievedReport.details.result).isNotEqualTo(ReportKt.DetailsKt.result {})
  }

  @Test
  fun `setMeasurementResult throws NOT FOUND when report not found`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = "1234"
          measurementReferenceId = "4321"
        }
      )
    }
    val result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.setMeasurementResult(
          setMeasurementResultRequest {
            measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
            measurementReferenceId = createdMeasurement.measurementReferenceId
            measurementResult = result
            externalReportId = 1234L
            reportResult = ReportKt.DetailsKt.result {}
          }
        )
      }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `setMeasurementFailure stores the failure data and the failed state for a measurement`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = "1234"
          measurementReferenceId = "4321"
        }
      )
    }

    val failure =
      MeasurementKt.failure {
        reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
        message = "Failure"
      }
    val request = setMeasurementFailureRequest {
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
      this.failure = failure
    }

    val updatedMeasurement = runBlocking { service.setMeasurementFailure(request) }
    assertThat(updatedMeasurement)
      .isEqualTo(
        measurement {
          measurementConsumerReferenceId = request.measurementConsumerReferenceId
          measurementReferenceId = request.measurementReferenceId
          state = Measurement.State.FAILED
          this.failure = failure
        }
      )

    val getRequest = getMeasurementRequest {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
    }
    val retrievedMeasurement = runBlocking { service.getMeasurement(getRequest) }
    assertThat(retrievedMeasurement).isEqualTo(updatedMeasurement)
  }

  @Test
  fun `setMeasurementFailure fails when the measurement doesn't exist`() {
    val request = setMeasurementFailureRequest {
      measurementConsumerReferenceId = "1234"
      measurementReferenceId = "4321"
      failure =
        MeasurementKt.failure {
          reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
          message = "Failure"
        }
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> { service.setMeasurementFailure(request) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }
}
