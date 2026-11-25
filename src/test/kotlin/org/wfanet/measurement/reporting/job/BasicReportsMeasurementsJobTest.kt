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
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.Status
import kotlin.random.Random
import kotlinx.coroutines.flow.flowOf
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
import org.wfanet.measurement.api.v2alpha.CreateMeasurementRequest
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKey
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.measurement
import org.wfanet.measurement.api.v2alpha.measurementConsumer
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.config.reporting.copy
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportKt
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportComputationsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingUnitKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.SetReportComputationMeasurementIdRequest
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportComputationsResponse
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.dataProviderKey
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportComputation
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingSetKey
import org.wfanet.measurement.internal.reporting.v2.reportingUnit
import org.wfanet.measurement.internal.reporting.v2.resultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.setMeasurementsCreatedRequest
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.MultimeasurementCompiler

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
    onBlocking { batchCreateReportComputations(any()) }
      .thenAnswer { invocation ->
        val request: BatchCreateReportComputationsRequest = invocation.getArgument(0)
        batchCreateReportComputationsResponse {
          reportComputations += request.reportComputationsList
        }
      }
    onBlocking { setReportComputationMeasurementId(any()) }
      .thenAnswer { invocation ->
        val request: SetReportComputationMeasurementIdRequest = invocation.getArgument(0)
        reportComputation {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          externalBasicReportId = request.externalBasicReportId
          impressionQualificationFilterId = request.impressionQualificationFilterId
          cmmsMeasurementId = request.cmmsMeasurementId
        }
      }
    onBlocking { setMeasurementsCreated(any()) }
      .thenReturn(BASIC_REPORT.copy { state = BasicReport.State.MEASUREMENTS_CREATED })
    onBlocking { failBasicReport(any()) }
      .thenReturn(BASIC_REPORT.copy { state = BasicReport.State.FAILED })
  }

  private val reportingSetsMock: ReportingSetsCoroutineImplBase = mockService {
    onBlocking { batchGetReportingSets(any()) }
      .thenReturn(batchGetReportingSetsResponse { reportingSets += CAMPAIGN_GROUP })
    on { streamReportingSets(any()) }
      .thenReturn(flowOf(CAMPAIGN_GROUP, PRIMITIVE_REPORTING_SET_1, PRIMITIVE_REPORTING_SET_2))
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
  }

  @get:Rule
  val grpcTestServerRule = GrpcTestServerRule {
    addService(basicReportsMock)
    addService(reportingSetsMock)
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
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        MultimeasurementCompiler(
          TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
          emitCelNullGuardsForNestedMembers = false,
        ),
        IMPRESSION_QUALIFICATION_FILTER_IDS,
        Random(1),
      )
  }

  private suspend fun captureCreateMeasurementRequests(count: Int): List<CreateMeasurementRequest> {
    val captor = argumentCaptor<CreateMeasurementRequest>()
    verify(measurementsMock, times(count)).createMeasurement(captor.capture())
    return captor.allValues
  }

  private suspend fun captureReportComputations(): List<ReportComputation> {
    val captor = argumentCaptor<BatchCreateReportComputationsRequest>()
    verify(basicReportsMock).batchCreateReportComputations(captor.capture())
    return captor.firstValue.reportComputationsList
  }

  /** Stubs `batchCreateReportComputations` to return each computation updated by [transform]. */
  private suspend fun stubStoredReportComputations(
    transform: (ReportComputation) -> ReportComputation
  ) {
    whenever(basicReportsMock.batchCreateReportComputations(any())).thenAnswer { invocation ->
      val request: BatchCreateReportComputationsRequest = invocation.getArgument(0)
      batchCreateReportComputationsResponse {
        reportComputations += request.reportComputationsList.map(transform)
      }
    }
  }

  @Test
  fun `execute creates one ReportComputation per impression qualification filter`(): Unit =
    runBlocking {
      job.execute()

      val reportComputations: List<ReportComputation> = captureReportComputations()
      assertThat(reportComputations.map { it.impressionQualificationFilterId })
        .containsExactly(AMI_IQF_ID, MRC_IQF_ID)
        .inOrder()
      assertThat(reportComputations.map { it.cmmsCreateMeasurementRequestId }.toSet()).hasSize(2)
      assertThat(reportComputations.map { it.cmmsMeasurementId }).containsExactly("", "")
    }

  @Test
  fun `execute creates a multimeasurement per ReportComputation`(): Unit = runBlocking {
    job.execute()

    val reportComputations: List<ReportComputation> = captureReportComputations()
    val requests: List<CreateMeasurementRequest> = captureCreateMeasurementRequests(2)
    assertThat(requests.map { it.requestId })
      .containsExactlyElementsIn(reportComputations.map { it.cmmsCreateMeasurementRequestId })
      .inOrder()
    for (request in requests) {
      assertThat(request.parent).isEqualTo(MEASUREMENT_CONSUMER_NAME)
      assertThat(request.measurement.measurementConsumerCertificate)
        .isEqualTo(SIGNING_CERTIFICATE_NAME)
      assertThat(request.measurement.measurementReferenceId).isEqualTo(request.requestId)
      assertThat(request.measurement.hasMeasurementSpec()).isFalse()
      assertThat(request.measurement.unsignedMeasurementSpec)
        .isEqualTo(
          measurementSpec {
            measurementPublicKey = MEASUREMENT_PUBLIC_KEY
            multi = MeasurementSpec.MultiMeasurementSpec.getDefaultInstance()
            vidSamplingInterval =
              MeasurementSpecKt.vidSamplingInterval {
                start = 0.0f
                width = 1.0f
              }
            modelLine = MODEL_LINE_NAME
            reportingMetadata =
              MeasurementSpecKt.reportingMetadata { basicReport = BASIC_REPORT_NAME }
          }
        )
      assertThat(request.measurement.multimeasurement.queriesList).isNotEmpty()
    }
  }

  @Test
  fun `execute resolves DataProvider components to their primitive ReportingSets`(): Unit =
    runBlocking {
      job.execute()

      val request: CreateMeasurementRequest = captureCreateMeasurementRequests(2).first()
      val dataProviderVectors = request.measurement.multimeasurement.dataProviderVectorsList
      assertThat(dataProviderVectors.map { it.dataProvider })
        .containsExactly(DATA_PROVIDER_NAME_1, DATA_PROVIDER_NAME_2)
      assertThat(
          dataProviderVectors.flatMap { entry ->
            entry.vectorsList.flatMap { vector ->
              vector.requisitionSpec.events.eventGroupsList.map { it.key }
            }
          }
        )
        .containsExactly(EVENT_GROUP_NAME_1, EVENT_GROUP_NAME_2)
      val components: List<String> =
        captureReportComputations().first().mappingsList.flatMap {
          listOf(it.component.component, it.componentUnique.component).filter(String::isNotEmpty)
        }
      assertThat(components)
        .containsAtLeast(
          PRIMITIVE_REPORTING_SET_1.externalReportingSetId,
          PRIMITIVE_REPORTING_SET_2.externalReportingSetId,
        )
    }

  @Test
  fun `execute resolves ReportingSet components`(): Unit = runBlocking {
    val basicReport: BasicReport =
      BASIC_REPORT.copy {
        details =
          details.copy {
            resultGroupSpecs.clear()
            resultGroupSpecs +=
              RESULT_GROUP_SPEC.copy {
                reportingUnit = reportingUnit {
                  reportingSetKeys =
                    ReportingUnitKt.reportingSetKeys {
                      reportingSetKeys += reportingSetKey {
                        cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
                        externalReportingSetId = PRIMITIVE_REPORTING_SET_1.externalReportingSetId
                      }
                    }
                }
              }
          }
      }
    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(listBasicReportsResponse { basicReports += basicReport })
    whenever(reportingSetsMock.batchGetReportingSets(any()))
      .thenReturn(batchGetReportingSetsResponse { reportingSets += PRIMITIVE_REPORTING_SET_1 })

    job.execute()

    val request: CreateMeasurementRequest = captureCreateMeasurementRequests(2).first()
    assertThat(request.measurement.multimeasurement.dataProviderVectorsList.map { it.dataProvider })
      .containsExactly(DATA_PROVIDER_NAME_1)
  }

  @Test
  fun `execute sets the MeasurementConsumer key and a nonce in every RequisitionSpec`(): Unit =
    runBlocking {
      job.execute()

      val requisitionSpecs =
        captureCreateMeasurementRequests(2).flatMap { request ->
          request.measurement.multimeasurement.dataProviderVectorsList.flatMap { entry ->
            entry.vectorsList.map { it.requisitionSpec }
          }
        }
      assertThat(requisitionSpecs).isNotEmpty()
      for (requisitionSpec in requisitionSpecs) {
        assertThat(requisitionSpec.measurementPublicKey).isEqualTo(MEASUREMENT_PUBLIC_KEY)
      }
      assertThat(requisitionSpecs.map { it.nonce }.toSet()).hasSize(requisitionSpecs.size)
    }

  @Test
  fun `execute records Measurement IDs and sets Measurements created`(): Unit = runBlocking {
    job.execute()

    val captor = argumentCaptor<SetReportComputationMeasurementIdRequest>()
    verify(basicReportsMock, times(2)).setReportComputationMeasurementId(captor.capture())
    assertThat(captor.allValues.map { it.impressionQualificationFilterId })
      .containsExactly(AMI_IQF_ID, MRC_IQF_ID)
      .inOrder()
    assertThat(captor.allValues.map { it.cmmsMeasurementId })
      .containsExactly("measurement-1", "measurement-2")
      .inOrder()
    verify(basicReportsMock)
      .setMeasurementsCreated(
        setMeasurementsCreatedRequest {
          cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
          externalBasicReportId = BASIC_REPORT_ID
        }
      )
  }

  @Test
  fun `execute uses the stored request ID`(): Unit = runBlocking {
    stubStoredReportComputations {
      it.copy { cmmsCreateMeasurementRequestId = "stored-${it.impressionQualificationFilterId}" }
    }

    job.execute()

    assertThat(captureCreateMeasurementRequests(2).map { it.requestId })
      .containsExactly("stored-$AMI_IQF_ID", "stored-$MRC_IQF_ID")
      .inOrder()
  }

  @Test
  fun `execute skips ReportComputations that already have a Measurement`(): Unit = runBlocking {
    stubStoredReportComputations {
      if (it.impressionQualificationFilterId == AMI_IQF_ID) {
        it.copy { cmmsMeasurementId = "existing" }
      } else {
        it
      }
    }

    job.execute()

    val request: CreateMeasurementRequest = captureCreateMeasurementRequests(1).single()
    assertThat(request.requestId).isNotEmpty()
    val captor = argumentCaptor<SetReportComputationMeasurementIdRequest>()
    verify(basicReportsMock).setReportComputationMeasurementId(captor.capture())
    assertThat(captor.firstValue.impressionQualificationFilterId).isEqualTo(MRC_IQF_ID)
    verify(basicReportsMock).setMeasurementsCreated(any())
  }

  @Test
  fun `execute fails BasicReport when the CMMS returns INVALID_ARGUMENT`(): Unit = runBlocking {
    whenever(measurementsMock.createMeasurement(any()))
      .thenThrow(Status.INVALID_ARGUMENT.asRuntimeException())

    job.execute()

    verify(basicReportsMock).failBasicReport(FAIL_BASIC_REPORT_REQUEST)
    verify(basicReportsMock, never()).setReportComputationMeasurementId(any())
    verify(basicReportsMock, never()).setMeasurementsCreated(any())
  }

  @Test
  fun `execute fails BasicReport when the CMMS returns FAILED_PRECONDITION`(): Unit = runBlocking {
    whenever(measurementsMock.createMeasurement(any()))
      .thenThrow(Status.FAILED_PRECONDITION.asRuntimeException())

    job.execute()

    verify(basicReportsMock).failBasicReport(FAIL_BASIC_REPORT_REQUEST)
    verify(basicReportsMock, never()).setMeasurementsCreated(any())
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
  fun `execute fails BasicReport that cannot be compiled`(): Unit = runBlocking {
    whenever(basicReportsMock.listBasicReports(any()))
      .thenReturn(
        listBasicReportsResponse {
          basicReports +=
            BASIC_REPORT.copy {
              details =
                details.copy {
                  resultGroupSpecs.clear()
                  resultGroupSpecs +=
                    RESULT_GROUP_SPEC.copy {
                      resultGroupMetricSpec = resultGroupMetricSpec {
                        reportingUnit =
                          ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                            stackedIncrementalReach = true
                          }
                      }
                    }
                }
            }
        }
      )

    job.execute()

    verify(basicReportsMock).failBasicReport(FAIL_BASIC_REPORT_REQUEST)
    verify(basicReportsMock, never()).batchCreateReportComputations(any())
    verify(measurementsMock, never()).createMeasurement(any())
  }

  @Test
  fun `execute skips MeasurementConsumers without an API key`(): Unit = runBlocking {
    val job =
      BasicReportsMeasurementsJob(
        measurementConsumerConfigs {
          configs[MEASUREMENT_CONSUMER_NAME] = MEASUREMENT_CONSUMER_CONFIG.copy { clearApiKey() }
        },
        BasicReportsCoroutineStub(grpcTestServerRule.channel),
        ReportingSetsCoroutineStub(grpcTestServerRule.channel),
        MeasurementConsumersCoroutineStub(grpcTestServerRule.channel),
        MeasurementsCoroutineStub(grpcTestServerRule.channel),
        MultimeasurementCompiler(
          TEST_EVENT_DESCRIPTOR.eventTemplateFieldsByPath,
          emitCelNullGuardsForNestedMembers = false,
        ),
        IMPRESSION_QUALIFICATION_FILTER_IDS,
        Random(1),
      )

    job.execute()

    verify(basicReportsMock, never()).listBasicReports(any())
  }

  companion object {
    private val TEST_EVENT_DESCRIPTOR = EventMessageDescriptor(TestEvent.getDescriptor())

    private const val MEASUREMENT_CONSUMER_ID = "mc"
    private val MEASUREMENT_CONSUMER_NAME = MeasurementConsumerKey(MEASUREMENT_CONSUMER_ID).toName()
    private const val SIGNING_CERTIFICATE_NAME = "measurementConsumers/mc/certificates/cert"
    private const val BASIC_REPORT_ID = "basic-report"
    private val BASIC_REPORT_NAME =
      BasicReportKey(MEASUREMENT_CONSUMER_ID, BASIC_REPORT_ID).toName()
    private const val CAMPAIGN_GROUP_ID = "campaign-group"
    private const val DATA_PROVIDER_ID_1 = "dp1"
    private const val DATA_PROVIDER_ID_2 = "dp2"
    private val DATA_PROVIDER_NAME_1 = DataProviderKey(DATA_PROVIDER_ID_1).toName()
    private val DATA_PROVIDER_NAME_2 = DataProviderKey(DATA_PROVIDER_ID_2).toName()
    private val EVENT_GROUP_NAME_1 = CmmsEventGroupKey(DATA_PROVIDER_ID_1, "eg1").toName()
    private val EVENT_GROUP_NAME_2 = CmmsEventGroupKey(DATA_PROVIDER_ID_2, "eg2").toName()
    private val MODEL_LINE_NAME = ModelLineKey("provider", "suite", "line").toName()

    private const val AMI_IQF_ID = 1L
    private const val MRC_IQF_ID = 2L
    private val IMPRESSION_QUALIFICATION_FILTER_IDS =
      mapOf("ami" to AMI_IQF_ID, "mrc" to MRC_IQF_ID)

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

    private val EVENT_GROUP_KEY_1 =
      ReportingSetKt.PrimitiveKt.eventGroupKey {
        cmmsDataProviderId = DATA_PROVIDER_ID_1
        cmmsEventGroupId = "eg1"
      }
    private val EVENT_GROUP_KEY_2 =
      ReportingSetKt.PrimitiveKt.eventGroupKey {
        cmmsDataProviderId = DATA_PROVIDER_ID_2
        cmmsEventGroupId = "eg2"
      }

    private val CAMPAIGN_GROUP: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalReportingSetId = CAMPAIGN_GROUP_ID
      externalCampaignGroupId = CAMPAIGN_GROUP_ID
      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys += EVENT_GROUP_KEY_1
          eventGroupKeys += EVENT_GROUP_KEY_2
        }
    }
    private val PRIMITIVE_REPORTING_SET_1: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "rs-1"
      externalCampaignGroupId = CAMPAIGN_GROUP_ID
      primitive = ReportingSetKt.primitive { eventGroupKeys += EVENT_GROUP_KEY_1 }
    }
    private val PRIMITIVE_REPORTING_SET_2: ReportingSet = reportingSet {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "rs-2"
      externalCampaignGroupId = CAMPAIGN_GROUP_ID
      primitive = ReportingSetKt.primitive { eventGroupKeys += EVENT_GROUP_KEY_2 }
    }

    private val RESULT_GROUP_SPEC: ResultGroupSpec = resultGroupSpec {
      reportingUnit = reportingUnit {
        dataProviderKeys =
          ReportingUnitKt.dataProviderKeys {
            dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_ID_1 }
            dataProviderKeys += dataProviderKey { cmmsDataProviderId = DATA_PROVIDER_ID_2 }
          }
      }
      metricFrequency = metricFrequencySpec { total = true }
      resultGroupMetricSpec = resultGroupMetricSpec {
        reportingUnit =
          ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
            cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
          }
        component =
          ResultGroupMetricSpecKt.componentMetricSetSpec {
            cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
          }
      }
    }

    private val BASIC_REPORT: BasicReport = basicReport {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalBasicReportId = BASIC_REPORT_ID
      externalCampaignGroupId = CAMPAIGN_GROUP_ID
      state = BasicReport.State.CREATED_MULTIMEASUREMENT
      modelLineKey =
        BasicReportKt.modelLineKey {
          cmmsModelProviderId = "provider"
          cmmsModelSuiteId = "suite"
          cmmsModelLineId = "line"
        }
      details = basicReportDetails {
        effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId = "ami"
        }
        effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId = "mrc"
        }
        reportingInterval = reportingInterval {
          reportStartDate = date {
            year = 2026
            month = 1
            day = 5
          }
          effectiveReportStart = dateTime {
            year = 2026
            month = 1
            day = 5
            timeZone = timeZone { id = "UTC" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 19
          }
        }
        resultGroupSpecs += RESULT_GROUP_SPEC
      }
    }

    private val FAIL_BASIC_REPORT_REQUEST = failBasicReportRequest {
      cmmsMeasurementConsumerId = MEASUREMENT_CONSUMER_ID
      externalBasicReportId = BASIC_REPORT_ID
    }
  }
}
