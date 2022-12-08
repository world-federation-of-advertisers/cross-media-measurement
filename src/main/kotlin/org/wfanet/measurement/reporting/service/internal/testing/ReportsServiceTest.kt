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
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.internal.reporting.CreateReportRequest
import org.wfanet.measurement.internal.reporting.CreateReportRequestKt
import org.wfanet.measurement.internal.reporting.Measurement
import org.wfanet.measurement.internal.reporting.MeasurementKt
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.Metric
import org.wfanet.measurement.internal.reporting.MetricKt
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.ReportKt
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.StreamReportsRequestKt
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.createReportRequest
import org.wfanet.measurement.internal.reporting.getReportByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.getReportRequest
import org.wfanet.measurement.internal.reporting.metric
import org.wfanet.measurement.internal.reporting.periodicTimeInterval
import org.wfanet.measurement.internal.reporting.report
import org.wfanet.measurement.internal.reporting.reportingSet
import org.wfanet.measurement.internal.reporting.setMeasurementFailureRequest
import org.wfanet.measurement.internal.reporting.setMeasurementResultRequest
import org.wfanet.measurement.internal.reporting.streamReportsRequest
import org.wfanet.measurement.internal.reporting.timeInterval
import org.wfanet.measurement.internal.reporting.timeIntervals

@RunWith(JUnit4::class)
abstract class ReportsServiceTest<T : ReportsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

  protected data class Services<T>(
    val reportsService: T,
    val measurementsService: MeasurementsCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T

  private lateinit var measurementsService: MeasurementsCoroutineImplBase

  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase

  /** Constructs the service being tested. */
  protected abstract fun newServices(idGenerator: IdGenerator): Services<T>

  @Before
  fun initService() {
    val services = newServices(idGenerator)
    service = services.reportsService
    measurementsService = services.measurementsService
    reportingSetsService = services.reportingSetsService
  }

  @Test
  fun `createReport with periodic time interval succeeds`() {
    val createdReport = runBlocking {
      service.createReport(createCreateReportRequest("1234", "1234", "1234", "1235"))
    }
    assertThat(createdReport.externalReportId).isNotEqualTo(0L)
    assertThat(createdReport.state).isEqualTo(Report.State.RUNNING)

    val getRequest = getReportRequest {
      measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
      externalReportId = createdReport.externalReportId
    }
    val retrievedReport = runBlocking { service.getReport(getRequest) }
    assertThat(createdReport)
      .ignoringRepeatedFieldOrder()
      .reportingMismatchesOnly()
      .ignoringFields(Report.CREATE_TIME_FIELD_NUMBER, Report.MEASUREMENTS_FIELD_NUMBER)
      .isEqualTo(retrievedReport)
  }

  @Test
  fun `createReport with time intervals list succeeds`() {
    val createReportRequest =
      createCreateReportRequest("1234", "1234", "1234", "1235").copy {
        report =
          report.copy {
            val timeInterval1 = timeInterval {
              startTime = timestamp {
                seconds = 100
                nanos = 10
              }
              endTime = timestamp {
                seconds = 150
                nanos = 10
              }
            }
            val timeInterval2 = timeInterval {
              startTime = timestamp {
                seconds = 200
                nanos = 10
              }
              endTime = timestamp {
                seconds = 250
                nanos = 10
              }
            }
            timeIntervals = timeIntervals {
              timeIntervals += timeInterval1
              timeIntervals += timeInterval2
            }
            metrics.clear()
            metrics += metric {
              details =
                MetricKt.details {
                  reach = MetricKt.reachParams {}
                  cumulative = true
                }
              namedSetOperations +=
                MetricKt.namedSetOperation {
                  displayName = "name5"
                  setOperation =
                    MetricKt.setOperation {
                      lhs = MetricKt.SetOperationKt.operand {}
                      rhs = MetricKt.SetOperationKt.operand {}
                    }
                  measurementCalculations +=
                    MetricKt.measurementCalculation {
                      timeInterval = timeInterval1
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId = "1234"
                          coefficient = 1
                        }
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId = "1235"
                          coefficient = 2
                        }
                    }
                  measurementCalculations +=
                    MetricKt.measurementCalculation {
                      timeInterval = timeInterval2
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId = "1234"
                          coefficient = 3
                        }
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId = "1235"
                          coefficient = 4
                        }
                    }
                }
            }
          }
      }

    val createdReport = runBlocking { service.createReport(createReportRequest) }
    assertThat(createdReport.externalReportId).isNotEqualTo(0L)
    assertThat(createdReport.state).isEqualTo(Report.State.RUNNING)

    val getRequest = getReportRequest {
      measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
      externalReportId = createdReport.externalReportId
    }
    val retrievedReport = runBlocking { service.getReport(getRequest) }
    assertThat(createdReport)
      .ignoringRepeatedFieldOrder()
      .reportingMismatchesOnly()
      .ignoringFields(Report.CREATE_TIME_FIELD_NUMBER, Report.MEASUREMENTS_FIELD_NUMBER)
      .isEqualTo(retrievedReport)
    assertThat(retrievedReport.createTime).isNotEqualTo(timestamp {})
  }

  @Test
  fun `createReport with reporting set as part of set operations succeeds`() {
    val measurementConsumerReferenceId = "1234"
    val reportingSet = reportingSet {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      filter = "filter"
      displayName = "displayName"
    }

    val createdReportingSet = runBlocking { reportingSetsService.createReportingSet(reportingSet) }

    val createReportRequest =
      createCreateReportRequest(measurementConsumerReferenceId, "1234", "1234", "1235").copy {
        report =
          report.copy {
            val periodicTimeInterval = this.periodicTimeInterval
            metrics += metric {
              details = MetricKt.details { cumulative = true }
              namedSetOperations +=
                MetricKt.namedSetOperation {
                  displayName = "name6"
                  setOperation =
                    MetricKt.setOperation {
                      lhs =
                        MetricKt.SetOperationKt.operand {
                          reportingSetId =
                            MetricKt.SetOperationKt.reportingSetKey {
                              this.measurementConsumerReferenceId =
                                createdReportingSet.measurementConsumerReferenceId
                              externalReportingSetId = createdReportingSet.externalReportingSetId
                            }
                        }
                      rhs = MetricKt.SetOperationKt.operand {}
                    }
                  measurementCalculations +=
                    MetricKt.measurementCalculation {
                      timeInterval = timeInterval {
                        startTime =
                          Timestamps.add(
                            periodicTimeInterval.startTime,
                            periodicTimeInterval.increment
                          )
                        endTime = Timestamps.add(startTime, periodicTimeInterval.increment)
                      }
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId = "1234"
                          coefficient = 3
                        }
                    }
                }
            }
          }
      }

    val createdReport = runBlocking { service.createReport(createReportRequest) }
    assertThat(createdReport.externalReportId).isNotEqualTo(0L)
    assertThat(createdReport.state).isEqualTo(Report.State.RUNNING)

    val getRequest = getReportRequest {
      this.measurementConsumerReferenceId = measurementConsumerReferenceId
      externalReportId = createdReport.externalReportId
    }
    val retrievedReport = runBlocking { service.getReport(getRequest) }
    assertThat(createdReport)
      .ignoringRepeatedFieldOrder()
      .reportingMismatchesOnly()
      .ignoringFields(Report.CREATE_TIME_FIELD_NUMBER, Report.MEASUREMENTS_FIELD_NUMBER)
      .isEqualTo(retrievedReport)
  }

  @Test
  fun `createReport throws INVALID ARGUMENT when time interval in calculation not in report`() {
    val createReportRequest =
      createCreateReportRequest("1234", "1234", "1234", "1235").copy {
        report =
          report.copy {
            metrics.clear()
            metrics += metric {
              details =
                MetricKt.details {
                  reach = MetricKt.reachParams {}
                  cumulative = true
                }
              namedSetOperations +=
                MetricKt.namedSetOperation {
                  displayName = "name"
                  measurementCalculations +=
                    MetricKt.measurementCalculation {
                      timeInterval = timeInterval {
                        startTime = timestamp {
                          seconds = 100
                          nanos = 10
                        }
                        endTime = timestamp {
                          seconds = 150
                          nanos = 10
                        }
                      }
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          measurementReferenceId = "1234"
                          coefficient = 1
                        }
                    }
                }
            }
          }
      }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> { service.createReport(createReportRequest) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Test
  fun `createReport throws NOT FOUND when reporting set not found`() {
    val createReportRequest =
      createCreateReportRequest("1234", "1234", "1234", "1235").copy {
        report =
          report.copy {
            metrics += metric {
              namedSetOperations +=
                MetricKt.namedSetOperation {
                  setOperation =
                    MetricKt.setOperation {
                      lhs =
                        MetricKt.SetOperationKt.operand {
                          reportingSetId =
                            MetricKt.SetOperationKt.reportingSetKey {
                              measurementConsumerReferenceId = "1234"
                              externalReportingSetId = 1234L
                            }
                        }
                    }
                }
            }
          }
      }

    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> { service.createReport(createReportRequest) }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getReport succeeds after measurement failure`() = runBlocking {
    val createdReport =
      service.createReport(createCreateReportRequest("1234", "1234", "1234", "1235"))

    measurementsService.setMeasurementFailure(
      setMeasurementFailureRequest {
        measurementConsumerReferenceId = "1234"
        measurementReferenceId = "1234"
        failure =
          MeasurementKt.failure {
            reason = Measurement.Failure.Reason.CERTIFICATE_REVOKED
            message = "message"
          }
      }
    )

    val getRequest = getReportRequest {
      measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
      externalReportId = createdReport.externalReportId
    }
    val retrievedReport = service.getReport(getRequest)
    assertThat(retrievedReport.state).isEqualTo(Report.State.FAILED)
  }

  @Test
  fun `getReport succeeds after measurement with big result makes report succeed`() = runBlocking {
    val createdReport = service.createReport(createCreateReportRequest("1234", "1234", "1234"))

    measurementsService.setMeasurementResult(
      setMeasurementResultRequest {
        measurementConsumerReferenceId = "1234"
        measurementReferenceId = "1234"
        result =
          MeasurementKt.result {
            reach = MeasurementKt.ResultKt.reach { value = 1L }
            frequency =
              MeasurementKt.ResultKt.frequency {
                for (i in 1L..100L) {
                  relativeFrequencyDistribution[i] = i * 1.0
                }
              }
          }
      }
    )

    val getRequest = getReportRequest {
      measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
      externalReportId = createdReport.externalReportId
    }
    val retrievedReport = service.getReport(getRequest)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)
  }

  @Test
  fun `getReport throws NOT FOUND when report not found`() {
    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.getReport(
          getReportRequest {
            measurementConsumerReferenceId = "1234"
            externalReportId = 1234L
          }
        )
      }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `getReportByIdempotencyKey returns report`() {
    val createdReport = runBlocking {
      service.createReport(createCreateReportRequest("1234", "1234", "1234", "1235"))
    }

    val getReportByIdempotencyKeyRequest = getReportByIdempotencyKeyRequest {
      measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
      reportIdempotencyKey = "1234"
    }
    val retrievedReport = runBlocking {
      service.getReportByIdempotencyKey(getReportByIdempotencyKeyRequest)
    }
    assertThat(createdReport)
      .ignoringRepeatedFieldOrder()
      .reportingMismatchesOnly()
      .ignoringFields(Report.CREATE_TIME_FIELD_NUMBER, Report.MEASUREMENTS_FIELD_NUMBER)
      .isEqualTo(retrievedReport)
    assertThat(retrievedReport.measurementsMap).hasSize(2)
  }

  @Test
  fun `getReportByIdempotencyKey throws NOT FOUND when report not found`() {
    val exception = runBlocking {
      assertFailsWith<StatusRuntimeException> {
        service.getReportByIdempotencyKey(
          getReportByIdempotencyKeyRequest {
            measurementConsumerReferenceId = "1234"
            reportIdempotencyKey = "test"
          }
        )
      }
    }
    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
  }

  @Test
  fun `streamReports gets 2 reports when limit is 2`() {
    val measurementConsumerReferenceId = "1234"
    val createdReport = runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId, "report1", "1234", "1235")
      )
    }
    runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId, "report2", "2234", "2235")
      )
    }
    runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId, "report3", "3234", "3235")
      )
    }
    val reports = runBlocking {
      service
        .streamReports(
          streamReportsRequest {
            filter =
              StreamReportsRequestKt.filter {
                this.measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
              }
            limit = 2
          }
        )
        .toList()
    }
    assertThat(reports).hasSize(2)
  }

  @Test
  fun `streamReports skips a report when the after filter is used`() {
    val measurementConsumerReferenceId = "1234"
    val createdReport = runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId, "report1", "1234", "1235")
      )
    }
    val createdReport2 = runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId, "report2", "2234", "2235")
      )
    }
    val minId = createdReport.externalReportId.coerceAtMost(createdReport2.externalReportId)
    val reports = runBlocking {
      service
        .streamReports(
          streamReportsRequest {
            filter =
              StreamReportsRequestKt.filter {
                this.measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
                externalReportIdAfter = minId
              }
          }
        )
        .toList()
    }
    assertThat(reports).hasSize(1)
    assertThat(reports[0].externalReportId).isNotEqualTo(minId)
  }

  @Test
  fun `streamReports doesn't include other measurement consumers when filter is used`() {
    val measurementConsumerReferenceId = "1234"
    val createdReport = runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId, "report1", "1234", "1235")
      )
    }
    val measurementConsumerReferenceId2 = "1235"
    runBlocking {
      service.createReport(
        createCreateReportRequest(measurementConsumerReferenceId2, "report2", "1234", "1235")
      )
    }
    val reports = runBlocking {
      service
        .streamReports(
          streamReportsRequest {
            filter =
              StreamReportsRequestKt.filter {
                this.measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
              }
          }
        )
        .toList()
    }
    assertThat(reports).hasSize(1)
    assertThat(reports)
      .ignoringRepeatedFieldOrder()
      .reportingMismatchesOnly()
      .ignoringFields(Report.CREATE_TIME_FIELD_NUMBER, Report.MEASUREMENTS_FIELD_NUMBER)
      .containsExactly(createdReport)
    assertThat(reports[0].measurementsMap).hasSize(2)
  }

  @Test
  fun `concurrent methods all succeed`() {
    val createdReport = runBlocking {
      service.createReport(createCreateReportRequest("1234", "1234", "1234", "1235"))
    }
    val streamRequest = streamReportsRequest {
      filter =
        StreamReportsRequestKt.filter {
          measurementConsumerReferenceId = createdReport.measurementConsumerReferenceId
        }
    }

    runBlocking {
      val deferredCreatedReport = async {
        service.createReport(createCreateReportRequest("1235", "1235", "1234", "1235"))
      }
      val deferredStreamedReports = async { service.streamReports(streamRequest) }
      val deferredCreatedReport2 = async {
        service.createReport(createCreateReportRequest("1236", "1236", "1234", "1235"))
      }

      assertThat(deferredCreatedReport.await().externalReportId).isNotEqualTo(0L)
      assertThat(deferredStreamedReports.await().toList().size).isGreaterThan(0)
      assertThat(deferredCreatedReport2.await().externalReportId).isNotEqualTo(0L)
    }
  }

  companion object {
    private fun createCreateReportRequest(
      measurementConsumerReferenceId: String,
      reportIdempotencyKey: String,
      vararg measurementReferenceIds: String
    ): CreateReportRequest {
      return createReportRequest {
        report = report {
          val inProgressReport = this
          this.measurementConsumerReferenceId = measurementConsumerReferenceId
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
          this.reportIdempotencyKey = reportIdempotencyKey
          val increment = inProgressReport.periodicTimeInterval.increment
          metrics += metric {
            details =
              MetricKt.details {
                reach = MetricKt.reachParams {}
                cumulative = true
              }
            namedSetOperations +=
              MetricKt.namedSetOperation {
                displayName = "name"
                setOperation =
                  MetricKt.setOperation {
                    lhs = MetricKt.SetOperationKt.operand {}
                    rhs = MetricKt.SetOperationKt.operand {}
                  }
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    timeInterval = timeInterval {
                      startTime = inProgressReport.periodicTimeInterval.startTime
                      endTime = Timestamps.add(startTime, increment)
                    }
                    measurementReferenceIds.forEach { measurementReferenceId ->
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          this.measurementReferenceId = measurementReferenceId
                          coefficient = 1
                        }
                    }
                  }
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    timeInterval = timeInterval {
                      startTime =
                        Timestamps.add(inProgressReport.periodicTimeInterval.startTime, increment)
                      endTime = Timestamps.add(startTime, increment)
                    }
                    measurementReferenceIds.forEach { measurementReferenceId ->
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          this.measurementReferenceId = measurementReferenceId
                          coefficient = 1
                        }
                    }
                  }
              }
            namedSetOperations +=
              MetricKt.namedSetOperation {
                displayName = "name2"
                setOperation =
                  MetricKt.setOperation {
                    type = Metric.SetOperation.Type.UNION
                    lhs =
                      MetricKt.SetOperationKt.operand {
                        operation =
                          MetricKt.setOperation {
                            type = Metric.SetOperation.Type.UNION
                            lhs = MetricKt.SetOperationKt.operand {}
                            rhs = MetricKt.SetOperationKt.operand {}
                          }
                      }
                    rhs = MetricKt.SetOperationKt.operand {}
                  }
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    timeInterval = timeInterval {
                      startTime =
                        Timestamps.add(inProgressReport.periodicTimeInterval.startTime, increment)
                      endTime = Timestamps.add(startTime, increment)
                    }
                    measurementReferenceIds.forEach { measurementReferenceId ->
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          this.measurementReferenceId = measurementReferenceId
                          coefficient = 1
                        }
                    }
                  }
              }
            namedSetOperations +=
              MetricKt.namedSetOperation {
                displayName = "name3"
                setOperation =
                  MetricKt.setOperation {
                    type = Metric.SetOperation.Type.UNION
                    lhs = MetricKt.SetOperationKt.operand {}
                    rhs =
                      MetricKt.SetOperationKt.operand {
                        operation =
                          MetricKt.setOperation {
                            type = Metric.SetOperation.Type.UNION
                            lhs =
                              MetricKt.SetOperationKt.operand {
                                operation =
                                  MetricKt.setOperation {
                                    type = Metric.SetOperation.Type.UNION
                                    lhs = MetricKt.SetOperationKt.operand {}
                                    rhs = MetricKt.SetOperationKt.operand {}
                                  }
                              }
                            rhs = MetricKt.SetOperationKt.operand {}
                          }
                      }
                  }
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    timeInterval = timeInterval {
                      startTime =
                        Timestamps.add(inProgressReport.periodicTimeInterval.startTime, increment)
                      endTime = Timestamps.add(startTime, increment)
                    }
                    measurementReferenceIds.forEach { measurementReferenceId ->
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          this.measurementReferenceId = measurementReferenceId
                          coefficient = 1
                        }
                    }
                  }
              }
          }
          metrics += metric {
            details =
              MetricKt.details {
                reach = MetricKt.reachParams {}
                cumulative = true
              }
            namedSetOperations +=
              MetricKt.namedSetOperation {
                displayName = "name4"
                setOperation =
                  MetricKt.setOperation {
                    type = Metric.SetOperation.Type.UNION
                    lhs =
                      MetricKt.SetOperationKt.operand {
                        operation =
                          MetricKt.setOperation {
                            type = Metric.SetOperation.Type.UNION
                            lhs = MetricKt.SetOperationKt.operand {}
                            rhs = MetricKt.SetOperationKt.operand {}
                          }
                      }
                    rhs = MetricKt.SetOperationKt.operand {}
                  }
                measurementCalculations +=
                  MetricKt.measurementCalculation {
                    timeInterval = timeInterval {
                      startTime =
                        Timestamps.add(inProgressReport.periodicTimeInterval.startTime, increment)
                      endTime = Timestamps.add(startTime, increment)
                    }
                    measurementReferenceIds.forEach { measurementReferenceId ->
                      weightedMeasurements +=
                        MetricKt.MeasurementCalculationKt.weightedMeasurement {
                          this.measurementReferenceId = measurementReferenceId
                          coefficient = 1
                        }
                    }
                  }
              }
          }
          details =
            ReportKt.details {
              eventGroupFilters["/measurementConsumers/1234/dataProviders/1234/eventGroups/1234"] =
                "filter"
            }
        }

        measurementReferenceIds.forEach {
          measurements +=
            CreateReportRequestKt.measurementKey {
              this.measurementConsumerReferenceId = measurementConsumerReferenceId
              measurementReferenceId = it
            }
        }
      }
    }
  }
}
