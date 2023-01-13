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
import kotlinx.coroutines.async
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

  /** Build a column header given the metric and set operation name. */
  private fun buildColumnHeader(metricTypeName: String, setOperationName: String): String {
    return "${metricTypeName}_$setOperationName"
  }

  @Test
  fun `createMeasurement succeeds`() {
    val measurement = measurement {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      measurementReferenceId = MEASUREMENT_REFERENCE_ID
    }

    val createdMeasurement = runBlocking { service.createMeasurement(measurement) }

    assertThat(createdMeasurement).isEqualTo(measurement.copy { state = Measurement.State.PENDING })

    val getRequest = getMeasurementRequest {
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
    }
    val retrievedMeasurement = runBlocking { service.getMeasurement(getRequest) }
    assertThat(retrievedMeasurement).isEqualTo(createdMeasurement)
  }

  @Test
  fun `createMeasurement fails when trying to create the same one twice`() {
    val measurement = measurement {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      measurementReferenceId = MEASUREMENT_REFERENCE_ID
    }

    val exception = runBlocking {
      service.createMeasurement(measurement)
      assertFailsWith<StatusRuntimeException> { service.createMeasurement(measurement) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
  }

  @Test
  fun `getMeasurement fails when the measurement doesn't exist`() {
    val request = getMeasurementRequest {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      measurementReferenceId = MEASUREMENT_REFERENCE_ID
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> { service.getMeasurement(request) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `setMeasurementResult stores the result and the succeeded state for a measurement`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
        }
      )
    }
    val result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }
    val request = setMeasurementResultRequest {
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
      this.result = result
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
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
    }
    val retrievedMeasurement = runBlocking { service.getMeasurement(getRequest) }
    assertThat(retrievedMeasurement).isEqualTo(updatedMeasurement)
  }

  @Test
  fun `setMeasurementResult fails when the measurement doesn't exist`() {
    val request = setMeasurementResultRequest {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      measurementReferenceId = MEASUREMENT_REFERENCE_ID
      result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> { service.setMeasurementResult(request) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `setMeasurementResult fails when the meaurement state isn't PENDING`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
        }
      )
    }
    val request = setMeasurementResultRequest {
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
      result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100L } }
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.setMeasurementResult(request)
        service.setMeasurementResult(request)
      }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `setMeasurementResult succeeds in setting the result for report with RF metric`() {
    val metricDetails =
      MetricKt.details {
        frequencyHistogram = MetricKt.frequencyHistogramParams { maximumFrequencyPerUser = 2 }
      }
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric {
              details = metricDetails
              namedSetOperations += NAMED_SET_OPERATION
            }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
          result =
            MeasurementKt.result {
              reach = MeasurementKt.ResultKt.reach { value = 100L }
              frequency =
                MeasurementKt.ResultKt.frequency {
                  relativeFrequencyDistribution[1] = 0.8
                  relativeFrequencyDistribution[2] = 0.2
                }
            }
        }
      )
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
          result =
            MeasurementKt.result {
              reach = MeasurementKt.ResultKt.reach { value = 200L }
              frequency =
                MeasurementKt.ResultKt.frequency {
                  relativeFrequencyDistribution[1] = 0.3
                  relativeFrequencyDistribution[2] = 0.7
                }
            }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertThat(retrievedReport.details.result)
      .usingDoubleTolerance(2.0)
      .isEqualTo(
        ReportKt.DetailsKt.result {
          histogramTables +=
            ReportKt.DetailsKt.ResultKt.histogramTable {
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
                  frequency = 1
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
                  frequency = 2
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
                  frequency = 1
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
                  frequency = 2
                }
              columns +=
                ReportKt.DetailsKt.ResultKt.column {
                  columnHeader =
                    buildColumnHeader(
                      metricDetails.metricTypeCase.name,
                      NAMED_SET_OPERATION.displayName
                    )
                  setOperations += 260.0
                  setOperations += 440.0
                  setOperations += 520.0
                  setOperations += 880.0
                }
            }
        }
      )
  }

  @Test
  fun `setMeasurementResult sets result for RF metric report with padding`() {
    val metricDetails =
      MetricKt.details {
        frequencyHistogram = MetricKt.frequencyHistogramParams { maximumFrequencyPerUser = 3 }
      }
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric {
              details = metricDetails
              namedSetOperations += NAMED_SET_OPERATION
            }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
          result =
            MeasurementKt.result {
              reach = MeasurementKt.ResultKt.reach { value = 100L }
              frequency =
                MeasurementKt.ResultKt.frequency {
                  relativeFrequencyDistribution[1] = 0.8
                  relativeFrequencyDistribution[2] = 0.2
                }
            }
        }
      )
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
          result =
            MeasurementKt.result {
              reach = MeasurementKt.ResultKt.reach { value = 200L }
              frequency =
                MeasurementKt.ResultKt.frequency {
                  relativeFrequencyDistribution[1] = 0.3
                  relativeFrequencyDistribution[2] = 0.7
                }
            }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertThat(retrievedReport.details.result)
      .usingDoubleTolerance(2.0)
      .isEqualTo(
        ReportKt.DetailsKt.result {
          histogramTables +=
            ReportKt.DetailsKt.ResultKt.histogramTable {
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
                  frequency = 1
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
                  frequency = 2
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
                  frequency = 3
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
                  frequency = 1
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
                  frequency = 2
                }
              rows +=
                ReportKt.DetailsKt.ResultKt.HistogramTableKt.row {
                  rowHeader = "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
                  frequency = 3
                }
              columns +=
                ReportKt.DetailsKt.ResultKt.column {
                  columnHeader =
                    buildColumnHeader(
                      metricDetails.metricTypeCase.name,
                      NAMED_SET_OPERATION.displayName
                    )
                  setOperations += 260.0
                  setOperations += 440.0
                  setOperations += 0.0
                  setOperations += 520.0
                  setOperations += 880.0
                  setOperations += 0.0
                }
            }
        }
      )
  }

  @Test
  fun `setMeasurementResult succeeds in setting the result for report with duration metric`() {
    val metricDetails =
      MetricKt.details {
        watchDuration =
          MetricKt.watchDurationParams {
            maximumFrequencyPerUser = 2
            maximumWatchDurationPerUser = 100
          }
      }
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric {
              details = metricDetails
              namedSetOperations += NAMED_SET_OPERATION
            }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
          result =
            MeasurementKt.result {
              watchDuration =
                MeasurementKt.ResultKt.watchDuration {
                  value = duration {
                    seconds = 100
                    nanos = 10
                  }
                }
            }
        }
      )
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
          result =
            MeasurementKt.result {
              watchDuration =
                MeasurementKt.ResultKt.watchDuration {
                  value = duration {
                    seconds = 200
                    nanos = 10
                  }
                }
            }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    assertThat(retrievedReport.details.result)
      .usingDoubleTolerance(2.0)
      .isEqualTo(
        ReportKt.DetailsKt.result {
          scalarTable =
            ReportKt.DetailsKt.ResultKt.scalarTable {
              rowHeaders += "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
              rowHeaders += "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
              columns +=
                ReportKt.DetailsKt.ResultKt.column {
                  columnHeader =
                    buildColumnHeader(
                      metricDetails.metricTypeCase.name,
                      NAMED_SET_OPERATION.displayName
                    )
                  setOperations += 700.0
                  setOperations += 1400.0
                }
            }
        }
      )
  }

  @Test
  fun `setMeasurementResult succeeds in setting the result for report with impression metric`() {
    val metricDetails =
      MetricKt.details {
        impressionCount = MetricKt.impressionCountParams { maximumFrequencyPerUser = 2 }
      }
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric {
              details = metricDetails
              namedSetOperations += NAMED_SET_OPERATION
            }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
          result =
            MeasurementKt.result { impression = MeasurementKt.ResultKt.impression { value = 100 } }
        }
      )
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
          result =
            MeasurementKt.result { impression = MeasurementKt.ResultKt.impression { value = 200 } }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertThat(retrievedReport.details.result)
      .isEqualTo(
        ReportKt.DetailsKt.result {
          scalarTable =
            ReportKt.DetailsKt.ResultKt.scalarTable {
              rowHeaders += "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
              rowHeaders += "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
              columns +=
                ReportKt.DetailsKt.ResultKt.column {
                  columnHeader =
                    buildColumnHeader(
                      metricDetails.metricTypeCase.name,
                      NAMED_SET_OPERATION.displayName
                    )
                  setOperations += 700.0
                  setOperations += 1400.0
                }
            }
        }
      )
  }

  @Test
  fun `setMeasurementResult succeeds in setting the result for report with reach metric`() {
    val metricDetails = MetricKt.details { reach = MetricKt.reachParams {} }
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric {
              details = metricDetails
              namedSetOperations += NAMED_SET_OPERATION
            }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
          result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 100 } }
        }
      )
      service.setMeasurementResult(
        setMeasurementResultRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
          result = MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = 200 } }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
    assertThat(retrievedReport.details.result)
      .isEqualTo(
        ReportKt.DetailsKt.result {
          scalarTable =
            ReportKt.DetailsKt.ResultKt.scalarTable {
              rowHeaders += "1970-01-01T00:01:40.000000010Z-1970-01-01T00:01:50.000000011Z"
              rowHeaders += "1970-01-01T00:01:50.000000011Z-1970-01-01T00:02:00.000000012Z"
              columns +=
                ReportKt.DetailsKt.ResultKt.column {
                  columnHeader =
                    buildColumnHeader(
                      metricDetails.metricTypeCase.name,
                      NAMED_SET_OPERATION.displayName
                    )
                  setOperations += 700.0
                  setOperations += 1400.0
                }
            }
        }
      )
  }

  @Test
  fun `setMeasurementFailure stores the failure data and the failed state for a measurement`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
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
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      measurementReferenceId = MEASUREMENT_REFERENCE_ID
    }
    val retrievedMeasurement = runBlocking { service.getMeasurement(getRequest) }
    assertThat(retrievedMeasurement).isEqualTo(updatedMeasurement)
  }

  @Test
  fun `setMeasurementFailure fails when the measurement doesn't exist`() {
    val request = setMeasurementFailureRequest {
      measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
      measurementReferenceId = MEASUREMENT_REFERENCE_ID
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

  @Test
  fun `setMeasurementFailure fails when the measurement state is not PENDING`() {
    val createdMeasurement = runBlocking {
      service.createMeasurement(
        measurement {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
        }
      )
    }

    val request = setMeasurementFailureRequest {
      measurementConsumerReferenceId = createdMeasurement.measurementConsumerReferenceId
      measurementReferenceId = createdMeasurement.measurementReferenceId
      failure =
        MeasurementKt.failure {
          reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
          message = "Failure"
        }
    }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.setMeasurementFailure(request)
        service.setMeasurementFailure(request)
      }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
  }

  @Test
  fun `setMeasurementFailure succeeds in setting the failed report state`() {
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric { namedSetOperations += NAMED_SET_OPERATION }
          }
        }
      )
    }
    runBlocking {
      service.setMeasurementFailure(
        setMeasurementFailureRequest {
          measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
          measurementReferenceId = MEASUREMENT_REFERENCE_ID
          failure =
            MeasurementKt.failure {
              reason = Measurement.Failure.Reason.COMPUTATION_PARTICIPANT_FAILED
            }
        }
      )
    }
    val retrievedReport = runBlocking {
      reportsService.getReport(
        getReportRequest {
          measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
          externalReportId = createdReport.externalReportId
        }
      )
    }
    assertThat(retrievedReport.state).isEqualTo(Report.State.FAILED)
  }

  @Test
  fun `concurrent setMeasurementResults all succeed`() {
    val createdReport = runBlocking {
      reportsService.createReport(
        createReportRequest {
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID
            }
          measurements +=
            CreateReportRequestKt.measurementKey {
              measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
              measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            }
          report = report {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            reportIdempotencyKey = "1235"
            periodicTimeInterval = PERIODIC_TIME_INTERVAL
            metrics += metric {
              details =
                MetricKt.details {
                  frequencyHistogram =
                    MetricKt.frequencyHistogramParams { maximumFrequencyPerUser = 2 }
                }
              namedSetOperations += NAMED_SET_OPERATION
            }
          }
        }
      )
    }
    runBlocking {
      val result =
        MeasurementKt.result {
          reach = MeasurementKt.ResultKt.reach { value = 100L }
          frequency =
            MeasurementKt.ResultKt.frequency {
              relativeFrequencyDistribution[1] = 0.8
              relativeFrequencyDistribution[2] = 0.2
            }
        }
      val deferredSetMeasurementResult = async {
        service.setMeasurementResult(
          setMeasurementResultRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            measurementReferenceId = MEASUREMENT_REFERENCE_ID
            this.result = result
          }
        )
      }
      val deferredSetMeasurementResult2 = async {
        service.setMeasurementResult(
          setMeasurementResultRequest {
            measurementConsumerReferenceId = MEASUREMENT_CONSUMER_REFERENCE_ID
            measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
            this.result = result
          }
        )
      }
      deferredSetMeasurementResult.await()
      deferredSetMeasurementResult2.await()

      val retrievedReport =
        reportsService.getReport(
          getReportRequest {
            measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
            externalReportId = createdReport.externalReportId
          }
        )
      assertThat(retrievedReport.details.result).isNotEqualTo(Report.Details.getDefaultInstance())
    }
  }

  companion object {
    private const val MEASUREMENT_CONSUMER_REFERENCE_ID = "1234"
    private const val MEASUREMENT_REFERENCE_ID = "1234"
    private const val MEASUREMENT_REFERENCE_ID_2 = "1235"

    private val PERIODIC_TIME_INTERVAL = periodicTimeInterval {
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

    private val NAMED_SET_OPERATION =
      MetricKt.namedSetOperation {
        displayName = "name4"
        setOperation = MetricKt.setOperation { type = Metric.SetOperation.Type.UNION }
        measurementCalculations +=
          MetricKt.measurementCalculation {
            timeInterval = timeInterval {
              startTime =
                Timestamps.add(PERIODIC_TIME_INTERVAL.startTime, PERIODIC_TIME_INTERVAL.increment)
              endTime = Timestamps.add(startTime, PERIODIC_TIME_INTERVAL.increment)
            }
            weightedMeasurements +=
              MetricKt.MeasurementCalculationKt.weightedMeasurement {
                measurementReferenceId = MEASUREMENT_REFERENCE_ID
                coefficient = 2
              }
            weightedMeasurements +=
              MetricKt.MeasurementCalculationKt.weightedMeasurement {
                measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
                coefficient = 6
              }
          }

        measurementCalculations +=
          MetricKt.measurementCalculation {
            timeInterval = timeInterval {
              startTime = PERIODIC_TIME_INTERVAL.startTime
              endTime = Timestamps.add(startTime, PERIODIC_TIME_INTERVAL.increment)
            }
            weightedMeasurements +=
              MetricKt.MeasurementCalculationKt.weightedMeasurement {
                measurementReferenceId = MEASUREMENT_REFERENCE_ID
                coefficient = 1
              }
            weightedMeasurements +=
              MetricKt.MeasurementCalculationKt.weightedMeasurement {
                measurementReferenceId = MEASUREMENT_REFERENCE_ID_2
                coefficient = 3
              }
          }
      }
  }
}
