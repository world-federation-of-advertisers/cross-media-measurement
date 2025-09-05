/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.job

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.Status
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.KArgumentCaptor
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.verifyBlocking
import org.mockito.kotlin.whenever
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.setStateRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.report

@RunWith(JUnit4::class)
class BasicReportsReportsJobTest {
  private val basicReportsMock: BasicReportsCoroutineImplBase = mockService {
    onBlocking { listBasicReports(any()) }
      .thenReturn(listBasicReportsResponse { basicReports += INTERNAL_BASIC_REPORT })
  }
  private val reportsMock: ReportsCoroutineImplBase = mockService {
    onBlocking { getReport(any()) }.thenReturn(REPORT)
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(basicReportsMock)
    addService(reportsMock)
  }

  private lateinit var job: BasicReportsReportsJob

  @Before
  fun initJob() {
    job =
      BasicReportsReportsJob(
        MEASUREMENT_CONSUMER_CONFIGS,
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        ReportsCoroutineStub(grpcTestServerRule.channel),
      )
  }

  @Test
  fun `execute gets report when report for basic report is SUCCEEDED`(): Unit = runBlocking {
    whenever(reportsMock.getReport(any()))
      .thenReturn(REPORT.copy { state = Report.State.SUCCEEDED })

    job.execute()

    verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::listBasicReports)
      .isEqualTo(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              state = BasicReport.State.REPORT_CREATED
            }
          pageSize = BATCH_SIZE
        }
      )

    verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::getReport)
      .isEqualTo(
        getReportRequest {
          name =
            ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, INTERNAL_BASIC_REPORT.externalReportId).toName()
        }
      )
  }

  @Test
  fun `execute sets basic report to FAILED when report for basic report is FAILED`(): Unit =
    runBlocking {
      whenever(reportsMock.getReport(any())).thenReturn(REPORT.copy { state = Report.State.FAILED })

      job.execute()

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::listBasicReports)
        .isEqualTo(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = BasicReport.State.REPORT_CREATED
              }
            pageSize = BATCH_SIZE
          }
        )

      verifyProtoArgument(reportsMock, ReportsCoroutineImplBase::getReport)
        .isEqualTo(
          getReportRequest {
            name =
              ReportKey(CMMS_MEASUREMENT_CONSUMER_ID, INTERNAL_BASIC_REPORT.externalReportId)
                .toName()
          }
        )

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::setState)
        .isEqualTo(
          setStateRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalBasicReportId = INTERNAL_BASIC_REPORT.externalBasicReportId
            state = BasicReport.State.FAILED
          }
        )
    }

  @Test
  fun `execute gets report for basic report when attempt fails for a previous basic report`():
    Unit = runBlocking {
    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(
        listBasicReportsResponse {
          basicReports += INTERNAL_BASIC_REPORT
          basicReports += INTERNAL_BASIC_REPORT
        }
      )
    whenever(reportsMock.getReport(any()))
      .thenThrow(Status.UNKNOWN.asRuntimeException())
      .thenReturn(REPORT)

    job.execute()

    verify(reportsMock, times(2)).getReport(any())
  }

  @Test
  fun `execute processes basic reports for 2 different MCs`(): Unit = runBlocking {
    val measurementConsumerConfigs = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc-user"
      }
      configs[MEASUREMENT_CONSUMER_NAME_2] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc2-user"
      }
    }

    job =
      BasicReportsReportsJob(
        measurementConsumerConfigs,
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        ReportsCoroutineStub(grpcTestServerRule.channel),
      )

    job.execute()

    val listBasicReportsCaptor: KArgumentCaptor<ListBasicReportsRequest> = argumentCaptor()
    verifyBlocking(basicReportsMock, times(2)) {
      listBasicReports(listBasicReportsCaptor.capture())
    }
    assertThat(listBasicReportsCaptor.allValues)
      .containsExactly(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              state = BasicReport.State.REPORT_CREATED
            }
          pageSize = BATCH_SIZE
        },
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID_2
              state = BasicReport.State.REPORT_CREATED
            }
          pageSize = BATCH_SIZE
        },
      )
  }

  companion object {
    private const val BATCH_SIZE = 10

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "A123"
    private const val MEASUREMENT_CONSUMER_NAME =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID}"
    private const val CMMS_MEASUREMENT_CONSUMER_ID_2 = "A1234"
    private const val MEASUREMENT_CONSUMER_NAME_2 =
      "measurementConsumers/${CMMS_MEASUREMENT_CONSUMER_ID_2}"

    private const val COMPOSITE_REPORTING_SET_ID = "c124"
    private const val COMPOSITE_REPORTING_SET_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/reportingSets/${COMPOSITE_REPORTING_SET_ID}"

    private const val METRIC_CALCULATION_SPEC_ID = "m123"
    private const val METRIC_CALCULATION_SPEC_NAME =
      "${MEASUREMENT_CONSUMER_NAME}/metricCalculationSpecs/${METRIC_CALCULATION_SPEC_ID}"

    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
        apiKey = "123"
        offlinePrincipal = "principals/mc-user"
      }
    }

    private val INTERNAL_BASIC_REPORT = basicReport { externalReportId = "a1234" }

    private val REPORT = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = COMPOSITE_REPORTING_SET_NAME
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += METRIC_CALCULATION_SPEC_NAME
            }
        }
      state = Report.State.SUCCEEDED
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2022
            month = 1
            day = 1
            hours = 13
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2022
            month = 1
            day = 2
          }
        }
      createTime = timestamp { seconds = 50 }
    }
  }
}
