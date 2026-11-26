/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Empty
import io.grpc.Status
import java.time.Clock
import java.time.Duration
import java.time.Instant
import java.time.ZoneOffset
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.never
import org.mockito.kotlin.times
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.wfanet.measurement.api.v2alpha.CancelMeasurementRequest
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.createMeasurementRequest
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.testing.verifyAndCapture
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportKt
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMappingKt
import org.wfanet.measurement.internal.reporting.v2.SetReportComputationMeasurementIdRequest
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.listReportComputationsResponse
import org.wfanet.measurement.internal.reporting.v2.reportComputation
import org.wfanet.measurement.internal.reporting.v2.resultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.setMeasurementsCreatedRequest
import org.wfanet.measurement.internal.reporting.v2.setReportComputationMeasurementIdRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey

@RunWith(JUnit4::class)
class BasicReportsMeasurementsJobTest {
  private val basicReportsMock: BasicReportsCoroutineImplBase = mockService {
    onBlocking { listBasicReports(any()) }
      .thenAnswer { invocation ->
        val request: ListBasicReportsRequest = invocation.getArgument(0)
        if (request.filter.state == BasicReport.State.CREATED_MULTIMEASUREMENT) {
          listBasicReportsResponse { basicReports += BASIC_REPORT }
        } else {
          ListBasicReportsResponse.getDefaultInstance()
        }
      }
    onBlocking { listReportComputations(any()) }
      .thenReturn(
        listReportComputationsResponse {
          reportComputations += AMI_REPORT_COMPUTATION
          reportComputations += MRC_REPORT_COMPUTATION
        }
      )
    onBlocking { setReportComputationMeasurementId(any()) }
      .thenAnswer { invocation ->
        val request: SetReportComputationMeasurementIdRequest = invocation.getArgument(0)
        reportComputation { cmmsMeasurementId = request.cmmsMeasurementId }
      }
    onBlocking { setMeasurementsCreated(any()) }
      .thenReturn(BASIC_REPORT.copy { state = BasicReport.State.MEASUREMENTS_CREATED })
    onBlocking { failBasicReport(any()) }
      .thenReturn(BASIC_REPORT.copy { state = BasicReport.State.FAILED })
  }

  private val measurementConsumersMock: MeasurementConsumersCoroutineImplBase = mockService {
    onBlocking { getMeasurementConsumer(any()) }.thenReturn(MEASUREMENT_CONSUMER)
  }

  private var measurementCount = 0
  private val measurementsMock: MeasurementsCoroutineImplBase = mockService {
    onBlocking { createMeasurement(any()) }
      .thenAnswer {
        measurementCount++
        measurement {
          name = MeasurementKey(MEASUREMENT_CONSUMER_ID, "measurement-$measurementCount").toName()
        }
      }
    onBlocking { cancelMeasurement(any()) }.thenReturn(Measurement.getDefaultInstance())
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(basicReportsMock)
    addService(measurementConsumersMock)
    addService(measurementsMock)
  }

  private lateinit var job: BasicReportsMeasurementsJob

  @Before
  fun initJob() {
    job =
      BasicReportsMeasurementsJob(
        MEASUREMENT_CONSUMER_CONFIGS,
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        MAX_CREATED_BASIC_REPORT_AGE,
        CLOCK,
      )
  }

  private suspend fun captureCreateMeasurementRequests(count: Int): List<CreateMeasurementRequest> {
    val captor = argumentCaptor<CreateMeasurementRequest>()
    verify(measurementsMock, times(count)).createMeasurement(captor.capture())
    return captor.allValues
  }

  @Test
  fun `execute creates the stored multimeasurement of each ReportComputation`(): Unit =
    runBlocking {
      job.execute()

      val requests: List<CreateMeasurementRequest> = captureCreateMeasurementRequests(2)
      assertThat(requests[0])
        .isEqualTo(buildExpectedCreateMeasurementRequest(AMI_REPORT_COMPUTATION))
      assertThat(requests[1])
        .isEqualTo(buildExpectedCreateMeasurementRequest(MRC_REPORT_COMPUTATION))
    }

  @Test
  fun `execute records Measurement IDs and sets Measurements created`(): Unit = runBlocking {
    job.execute()

    val captor = argumentCaptor<SetReportComputationMeasurementIdRequest>()
    verify(basicReportsMock, times(2)).setReportComputationMeasurementId(captor.capture())
    assertThat(captor.allValues)
      .containsExactly(
        setReportComputationMeasurementIdRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalBasicReportId = BASIC_REPORT_ID
          externalImpressionQualificationFilterId = "ami"
          cmmsMeasurementId = "measurement-1"
        },
        setReportComputationMeasurementIdRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalBasicReportId = BASIC_REPORT_ID
          externalImpressionQualificationFilterId = "mrc"
          cmmsMeasurementId = "measurement-2"
        },
      )
      .inOrder()
    verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::setMeasurementsCreated)
      .isEqualTo(
        setMeasurementsCreatedRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalBasicReportId = BASIC_REPORT_ID
        }
      )
  }

  @Test
  fun `execute skips ReportComputations that already have a Measurement`(): Unit = runBlocking {
    whenever(basicReportsMock.listReportComputations(any()))
      .thenReturn(
        listReportComputationsResponse {
          reportComputations += AMI_REPORT_COMPUTATION.copy { cmmsMeasurementId = "existing" }
          reportComputations += MRC_REPORT_COMPUTATION
        }
      )

    job.execute()

    val request: CreateMeasurementRequest = captureCreateMeasurementRequests(1).single()
    assertThat(request.requestId).isEqualTo(MRC_REPORT_COMPUTATION.cmmsCreateMeasurementRequestId)
  }

  @Test
  fun `execute fails BasicReport and cancels its Measurements when the CMMS rejects one`(): Unit =
    runBlocking {
      whenever(measurementsMock.createMeasurement(any()))
        .thenReturn(
          measurement { name = MeasurementKey(MEASUREMENT_CONSUMER_ID, "measurement-1").toName() }
        )
        .thenThrow(Status.INVALID_ARGUMENT.asRuntimeException())
      whenever(basicReportsMock.listReportComputations(any()))
        .thenReturn(
          listReportComputationsResponse {
            reportComputations += AMI_REPORT_COMPUTATION
            reportComputations += MRC_REPORT_COMPUTATION
          }
        )
        .thenReturn(
          listReportComputationsResponse {
            reportComputations +=
              AMI_REPORT_COMPUTATION.copy { cmmsMeasurementId = "measurement-1" }
            reportComputations += MRC_REPORT_COMPUTATION
          }
        )

      job.execute()

      verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::failBasicReport)
        .isEqualTo(
          failBasicReportRequest {
            cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
            externalBasicReportId = BASIC_REPORT_ID
          }
        )
      val captor = argumentCaptor<CancelMeasurementRequest>()
      verify(measurementsMock).cancelMeasurement(captor.capture())
      assertThat(captor.firstValue.name)
        .isEqualTo(MeasurementKey(MEASUREMENT_CONSUMER_ID, "measurement-1").toName())
      verify(basicReportsMock, never()).setMeasurementsCreated(any())
    }

  @Test
  fun `execute fails a timed-out BasicReport and cancels its Measurements`(): Unit = runBlocking {
    stubTimedOutBasicReport()

    job.execute()

    // The Measurement without a recorded ID is replayed by its request ID, so it is cancelled too.
    verifyProtoArgument(measurementsMock, MeasurementsCoroutineImplBase::createMeasurement)
      .comparingExpectedFieldsOnly()
      .isEqualTo(
        createMeasurementRequest {
          requestId = MRC_REPORT_COMPUTATION.cmmsCreateMeasurementRequestId
        }
      )
    val cancelRequests: List<CancelMeasurementRequest> =
      verifyAndCapture(measurementsMock, MeasurementsCoroutineImplBase::cancelMeasurement, times(2))
    assertThat(cancelRequests.map { it.name })
      .containsExactly(
        MeasurementKey(MEASUREMENT_CONSUMER_ID, "measurement-0").toName(),
        MeasurementKey(MEASUREMENT_CONSUMER_ID, "measurement-1").toName(),
      )
    verify(basicReportsMock, never()).setMeasurementsCreated(any())
    verifyProtoArgument(basicReportsMock, BasicReportsCoroutineImplBase::failBasicReport)
      .isEqualTo(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalBasicReportId = BASIC_REPORT_ID
        }
      )
  }

  @Test
  fun `execute fails BasicReport when its Measurement is already cancelled`(): Unit = runBlocking {
    stubTimedOutBasicReport()
    whenever(measurementsMock.cancelMeasurement(any()))
      .thenThrow(Status.FAILED_PRECONDITION.asRuntimeException())

    job.execute()

    verify(basicReportsMock).failBasicReport(any())
  }

  @Test
  fun `execute fails BasicReport when its Measurement cannot be cancelled`(): Unit = runBlocking {
    stubTimedOutBasicReport()
    whenever(measurementsMock.cancelMeasurement(any()))
      .thenThrow(Status.PERMISSION_DENIED.asRuntimeException())

    job.execute()

    verify(measurementsMock, times(2)).cancelMeasurement(any())
    verify(basicReportsMock).failBasicReport(any())
  }

  @Test
  fun `execute leaves BasicReport for the next run when a cancel is UNAVAILABLE`(): Unit =
    runBlocking {
      stubTimedOutBasicReport()
      whenever(measurementsMock.cancelMeasurement(any()))
        .thenThrow(Status.UNAVAILABLE.asRuntimeException())

      job.execute()

      verify(basicReportsMock, never()).failBasicReport(any())
    }

  /**
   * Stubs a BasicReport past [MAX_CREATED_BASIC_REPORT_AGE] with one Measurement recorded and one
   * not, which the replay creates as `measurement-1`.
   */
  private suspend fun stubTimedOutBasicReport() {
    whenever(basicReportsMock.listBasicReports(any())).thenAnswer { invocation ->
      val request: ListBasicReportsRequest = invocation.getArgument(0)
      if (request.filter.state == BasicReport.State.CREATED_MULTIMEASUREMENT) {
        listBasicReportsResponse {
          basicReports +=
            BASIC_REPORT.copy {
              createTime = CREATE_TIME.minus(MAX_CREATED_BASIC_REPORT_AGE).toProtoTime()
            }
        }
      } else {
        ListBasicReportsResponse.getDefaultInstance()
      }
    }
    val recorded: ReportComputation =
      AMI_REPORT_COMPUTATION.copy { cmmsMeasurementId = "measurement-0" }
    whenever(basicReportsMock.listReportComputations(any()))
      .thenReturn(
        listReportComputationsResponse {
          reportComputations += recorded
          reportComputations += MRC_REPORT_COMPUTATION
        }
      )
      .thenReturn(
        listReportComputationsResponse {
          reportComputations += recorded
          reportComputations += MRC_REPORT_COMPUTATION.copy { cmmsMeasurementId = "measurement-1" }
        }
      )
  }

  @Test
  fun `execute leaves BasicReport for the next run when the CMMS is UNAVAILABLE`(): Unit =
    runBlocking {
      whenever(measurementsMock.createMeasurement(any()))
        .thenThrow(Status.UNAVAILABLE.asRuntimeException())

      job.execute()

      verify(basicReportsMock, never()).failBasicReport(any())
      verify(basicReportsMock, never()).setMeasurementsCreated(any())
    }

  @Test
  fun `execute skips MeasurementConsumers without an API key`(): Unit = runBlocking {
    val jobWithoutApiKey =
      BasicReportsMeasurementsJob(
        measurementConsumerConfigs {
          configs[MEASUREMENT_CONSUMER_NAME] = measurementConsumerConfig {
            signingCertificateName = SIGNING_CERTIFICATE_NAME
          }
        },
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        MAX_CREATED_BASIC_REPORT_AGE,
        CLOCK,
      )

    jobWithoutApiKey.execute()

    verify(basicReportsMock, never()).listBasicReports(any())
  }

  companion object {
    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private val MEASUREMENT_CONSUMER_NAME = MeasurementConsumerKey(MEASUREMENT_CONSUMER_ID).toName()
    private const val SIGNING_CERTIFICATE_NAME = "measurementConsumers/mc/certificates/cert"
    private const val BASIC_REPORT_ID = "basic-report"
    private val BASIC_REPORT_NAME =
      BasicReportKey(MEASUREMENT_CONSUMER_ID, BASIC_REPORT_ID).toName()
    private val MODEL_LINE_NAME = ModelLineKey("provider", "suite", "line").toName()

    private val MEASUREMENT_PUBLIC_KEY: ProtoAny = ProtoAny.pack(Empty.getDefaultInstance())

    private val MEASUREMENT_CONSUMER = measurementConsumer {
      name = MEASUREMENT_CONSUMER_NAME
      publicKey = signedMessage { message = MEASUREMENT_PUBLIC_KEY }
    }

    private val MEASUREMENT_CONSUMER_CONFIG = measurementConsumerConfig {
      apiKey = "api-key"
      signingCertificateName = SIGNING_CERTIFICATE_NAME
    }
    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MEASUREMENT_CONSUMER_NAME] = MEASUREMENT_CONSUMER_CONFIG
    }

    private val CREATE_TIME: Instant = Instant.parse("2026-01-05T00:00:00Z")
    private val MAX_CREATED_BASIC_REPORT_AGE: Duration = Duration.ofHours(1)
    private val CLOCK: Clock = Clock.fixed(CREATE_TIME.plusSeconds(60), ZoneOffset.UTC)

    private val BASIC_REPORT: BasicReport = basicReport {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalBasicReportId = BASIC_REPORT_ID
      state = BasicReport.State.CREATED_MULTIMEASUREMENT
      createTime = CREATE_TIME.toProtoTime()
      modelLineKey =
        BasicReportKt.modelLineKey {
          cmmsModelProviderId = "provider"
          cmmsModelSuiteId = "suite"
          cmmsModelLineId = "line"
        }
    }

    private val MULTIMEASUREMENT: Measurement.Multimeasurement =
      MeasurementKt.multimeasurement {
        dataProviderVectors +=
          MeasurementKt.MultimeasurementKt.dataProviderVectors {
            dataProvider = "dataProviders/dp1"
            vectors +=
              MeasurementKt.MultimeasurementKt.vector {
                index = 0
                requisitionSpec = requisitionSpec {}
              }
          }
      }

    private fun buildReportComputation(externalIqfId: String): ReportComputation =
      reportComputation {
        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
        externalBasicReportId = BASIC_REPORT_ID
        externalImpressionQualificationFilterId = externalIqfId
        cmmsCreateMeasurementRequestId = "request-$externalIqfId"
        multimeasurement = MULTIMEASUREMENT.toByteString()
        mappings += resultGroupQueryMapping {
          queryId = "query"
          reportingUnit = ResultGroupQueryMappingKt.reportingSet { externalReportingSetId = "ru" }
        }
      }

    private val AMI_REPORT_COMPUTATION: ReportComputation = buildReportComputation("ami")
    private val MRC_REPORT_COMPUTATION: ReportComputation = buildReportComputation("mrc")

    private fun buildExpectedCreateMeasurementRequest(
      reportComputation: ReportComputation
    ): CreateMeasurementRequest = createMeasurementRequest {
      parent = MEASUREMENT_CONSUMER_NAME
      requestId = reportComputation.cmmsCreateMeasurementRequestId
      measurement = measurement {
        measurementConsumerCertificate = SIGNING_CERTIFICATE_NAME
        unsignedMeasurementSpec = measurementSpec {
          measurementPublicKey = MEASUREMENT_PUBLIC_KEY
          multi = MeasurementSpec.MultiMeasurementSpec.getDefaultInstance()
          vidSamplingInterval =
            MeasurementSpecKt.vidSamplingInterval {
              start = 0.0f
              width = 1.0f
            }
          modelLine = MODEL_LINE_NAME
          reportingMetadata =
            MeasurementSpecKt.reportingMetadata {
              report = ReportKey(MEASUREMENT_CONSUMER_ID, BASIC_REPORT_ID).toName()
              basicReport = BASIC_REPORT_NAME
            }
        }
        multimeasurement =
          MULTIMEASUREMENT.copy {
            dataProviderVectors[0] =
              dataProviderVectors[0].copy {
                vectors[0] =
                  vectors[0].copy {
                    requisitionSpec =
                      requisitionSpec.copy { measurementPublicKey = MEASUREMENT_PUBLIC_KEY }
                  }
              }
          }
        measurementReferenceId = reportComputation.cmmsCreateMeasurementRequestId
      }
    }
  }
}
