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

package org.wfanet.measurement.reporting.service.internal.testing.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.rpc.errorInfo
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import com.google.type.timeZone
import io.grpc.BindableService
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import io.grpc.testing.GrpcCleanupRule
import java.util.concurrent.atomic.AtomicLong
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.IdGenerator
import org.wfanet.measurement.common.grpc.ProtobufServiceConfig
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig
import org.wfanet.measurement.internal.reporting.v2.AddProcessedResultValuesRequest
import org.wfanet.measurement.internal.reporting.v2.AddProcessedResultValuesRequestKt
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpc
import org.wfanet.measurement.internal.reporting.v2.BatchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpc
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpc
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.noisyMetricSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.reportingWindow
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.reportingWindowEntry
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.reportingWindowResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultView
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpc
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt.MetricSetKt.basicMetricSet
import org.wfanet.measurement.internal.reporting.v2.addProcessedResultValuesRequest
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.createReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.listReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.internal.reporting.v2.setExternalReportIdRequest
import org.wfanet.measurement.reporting.service.internal.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

@RunWith(JUnit4::class)
abstract class ReportResultsServiceTest {
  private val idGenerator =
    object : IdGenerator {
      private val nextId = AtomicLong(1)

      override fun generateId(): Long {
        return nextId.getAndIncrement()
      }
    }

  @get:Rule val grpcCleanup = GrpcCleanupRule()

  private val grpcChannel by lazy {
    val serverName = InProcessServerBuilder.generateName()
    val services = createServices(idGenerator, IMPRESSION_QUALIFICATION_FILTER_MAPPING)
    grpcCleanup.register(
      InProcessServerBuilder.forName(serverName)
        .apply {
          directExecutor()
          for (service in services) {
            addService(service)
          }
        }
        .build()
        .start()
    )
    grpcCleanup.register(
      InProcessChannelBuilder.forName(serverName)
        .apply {
          directExecutor()
          propagateCauseWithStatus(true)
          defaultServiceConfig(ProtobufServiceConfig.DEFAULT.asMap())
        }
        .build()
    )
  }

  private val basicReportsStub by lazy { BasicReportsGrpc.newBlockingV2Stub(grpcChannel) }

  private val reportResultsStub by lazy { ReportResultsGrpc.newBlockingV2Stub(grpcChannel) }

  protected abstract fun createServices(
    idGenerator: IdGenerator,
    impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  ): List<BindableService>

  @Test
  fun `createReportResult creates ReportResult`() {
    ensureMeasurementConsumer()
    val request = CREATE_REPORT_RESULT_REQUEST

    val response: ReportResult = reportResultsStub.createReportResult(request)

    assertThat(response)
      .ignoringFields(ReportResult.EXTERNAL_REPORT_RESULT_ID_FIELD_NUMBER)
      .isEqualTo(
        CREATE_REPORT_RESULT_REQUEST.reportResult.copy {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
        }
      )
    assertThat(response.externalReportResultId).isNotEqualTo(0L)
    assertWithMessage("Should match persisted value")
      .about(ProtoTruth.protos())
      .that(response)
      .isEqualTo(
        reportResultsStub.getReportResult(
          getReportResultRequest {
            cmmsMeasurementConsumerId = response.cmmsMeasurementConsumerId
            externalReportResultId = response.externalReportResultId
          }
        )
      )
  }

  @Test
  fun `batchCreateReportingSetResults creates ReportingSetResults`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val request =
      BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
        reportResult.externalReportResultId
      )

    val response = reportResultsStub.batchCreateReportingSetResults(request)

    assertThat(response)
      .ignoringFieldDescriptors(
        ReportingSetResult.getDescriptor()
          .findFieldByNumber(ReportingSetResult.EXTERNAL_REPORTING_SET_RESULT_ID_FIELD_NUMBER)
      )
      .isEqualTo(
        batchCreateReportingSetResultsResponse {
          reportingSetResults +=
            request.requestsList[0].reportingSetResult.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = request.externalReportResultId
              metricFrequencySpecFingerprint = 5458822473151716274
              groupingDimensionFingerprint = 9076537016683682170
              filterFingerprint = 12115333261944561450UL.toLong()
            }
          reportingSetResults +=
            request.requestsList[1].reportingSetResult.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = request.externalReportResultId
              metricFrequencySpecFingerprint = 5458822473151716274
              groupingDimensionFingerprint = 3506459991786236532
              filterFingerprint = 12115333261944561450UL.toLong()
            }
          reportingSetResults +=
            request.requestsList[2].reportingSetResult.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = request.externalReportResultId
              metricFrequencySpecFingerprint = 11987848417806868115UL.toLong()
              groupingDimensionFingerprint = 9076537016683682170
              filterFingerprint = 15177473705012173011UL.toLong()
            }
        }
      )
    for (reportingSetResult in response.reportingSetResultsList) {
      assertThat(reportingSetResult.externalReportingSetResultId).isNotEqualTo(0L)
    }
    assertWithMessage("Should match persisted value")
      .about(ProtoTruth.protos())
      .that(response.reportingSetResultsList)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELDS)
      .containsExactlyElementsIn(
        reportResultsStub
          .listReportingSetResults(
            listReportingSetResultsRequest {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = request.externalReportResultId
            }
          )
          .reportingSetResultsList
      )
      .inOrder()
  }

  @Test
  fun `batchCreateReportingSetResults updates BasicReport state`() {
    val basicReport: BasicReport = ensureMeasurementConsumer()
    val externalReportId = "report-1"
    basicReportsStub.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
        externalBasicReportId = basicReport.externalBasicReportId
        this.externalReportId = externalReportId
      }
    )
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val request =
      BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
          reportResult.externalReportResultId
        )
        .copy { externalBasicReportId = basicReport.externalBasicReportId }

    reportResultsStub.batchCreateReportingSetResults(request)

    val updatedBasicReport: BasicReport =
      basicReportsStub.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          externalBasicReportId = request.externalBasicReportId
        }
      )
    assertThat(updatedBasicReport.state).isEqualTo(BasicReport.State.UNPROCESSED_RESULTS_READY)
    assertThat(updatedBasicReport.externalReportResultId)
      .isEqualTo(reportResult.externalReportResultId)
  }

  @Test
  fun `batchCreateReportingSetResults throws when event filters are not normalized`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val request =
      BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
          reportResult.externalReportResultId
        )
        .copy {
          requests[1] =
            requests[1].copy {
              reportingSetResult =
                reportingSetResult.copy {
                  dimension =
                    dimension.copy {
                      eventFilters += eventFilter {
                        terms += eventTemplateField {
                          path = "person.gender"
                          value =
                            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
                        }
                        terms += eventTemplateField {
                          path = "person.age_group"
                          value =
                            EventTemplateFieldKt.fieldValue {
                              enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                            }
                        }
                      }
                    }
                }
            }
        }

    val exception =
      assertFailsWith<StatusException> { reportResultsStub.batchCreateReportingSetResults(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "requests[1].reporting_set_result.dimension.event_filters"
        }
      )
  }

  @Test
  fun `batchCreateReportingSetResults throws when non-cumulative result missing for non-cumulative window`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val request =
      BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
          reportResult.externalReportResultId
        )
        .copy {
          requests[0] =
            requests[0].copy {
              reportingSetResult =
                reportingSetResult.copy {
                  reportingWindowResults[0] =
                    reportingWindowResults[0].copy {
                      value =
                        value.copy {
                          unprocessedReportResultValues =
                            unprocessedReportResultValues.copy { clearNonCumulativeResults() }
                        }
                    }
                }
            }
        }

    val exception =
      assertFailsWith<StatusException> { reportResultsStub.batchCreateReportingSetResults(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "requests[0].reporting_set_result.reporting_window_results[0].value." +
              "unprocessed_report_result_values.non_cumulative_results"
        }
      )
  }

  @Test
  fun `addProcessedResultValues adds values to existing ReportingSetResults`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val reportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .batchCreateReportingSetResults(
          BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
            reportResult.externalReportResultId
          )
        )
        .reportingSetResultsList
    val request = buildAddProcessedResultValuesRequest(reportingSetResults)

    reportResultsStub.addProcessedResultValues(request)

    val updatedReportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .listReportingSetResults(
          listReportingSetResultsRequest {
            cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
            externalReportResultId = reportResult.externalReportResultId
            view = ReportingSetResultView.REPORTING_SET_RESULT_VIEW_FULL
          }
        )
        .reportingSetResultsList
    assertThat(updatedReportingSetResults)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELDS)
      .containsExactly(
        reportingSetResults[0].copy {
          reportingWindowResults[0] =
            reportingWindowResults[0].copy {
              value =
                value.copy {
                  processedReportResultValues =
                    request.reportingSetResultsMap
                      .getValue(reportingSetResults[0].externalReportingSetResultId)
                      .reportingWindowResultsList[0]
                      .value
                }
            }
          reportingWindowResults[1] =
            reportingWindowResults[1].copy {
              value =
                value.copy {
                  processedReportResultValues =
                    request.reportingSetResultsMap
                      .getValue(reportingSetResults[0].externalReportingSetResultId)
                      .reportingWindowResultsList[1]
                      .value
                }
            }
        },
        reportingSetResults[1].copy {
          reportingWindowResults[0] =
            reportingWindowResults[0].copy {
              value =
                value.copy {
                  processedReportResultValues =
                    request.reportingSetResultsMap
                      .getValue(reportingSetResults[1].externalReportingSetResultId)
                      .reportingWindowResultsList[0]
                      .value
                }
            }
        },
        reportingSetResults[2],
      )
  }

  @Test
  fun `addProcessedResultValues adds values when nonCumulative window doesn't exist`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val reportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .batchCreateReportingSetResults(
          batchCreateReportingSetResultsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = reportResult.externalReportResultId
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              reportingSetResult = reportingSetResult {
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = "primitive-1"
                    vennDiagramRegionType =
                      ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                    externalImpressionQualificationFilterId =
                      AMI_IQF.externalImpressionQualificationFilterId
                    metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                    grouping =
                      ReportingSetResultKt.DimensionKt.grouping {
                        valueByPath["person.age_group"] =
                          EventTemplateFieldKt.fieldValue {
                            enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                          }
                        valueByPath["person.gender"] =
                          EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
                      }
                    eventFilters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.age_group"
                        value =
                          EventTemplateFieldKt.fieldValue {
                            enumValue = Person.AgeGroup.YEARS_35_TO_54.name
                          }
                      }
                    }
                  }
                populationSize = 1000
                reportingWindowResults += reportingWindowEntry {
                  key = reportingWindow {
                    end = date {
                      year = 2025
                      month = 1
                      day = 20
                    }
                  }
                  value = reportingWindowResult {
                    unprocessedReportResultValues =
                      ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                        cumulativeResults = noisyMetricSet {
                          impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                        }
                      }
                  }
                }
              }
            }
          }
        )
        .reportingSetResultsList

    val request = addProcessedResultValuesRequest {
      cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
      externalReportResultId = reportResult.externalReportResultId
      this.reportingSetResults[reportingSetResults[0].externalReportingSetResultId] =
        AddProcessedResultValuesRequestKt.processedReportingSetResult {
          reportingWindowResults +=
            AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
              key = reportingSetResults[0].reportingWindowResultsList[0].key
              value =
                ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = basicMetricSet {
                    reach = 2
                    impressions = 10
                  }
                }
            }
        }
    }

    reportResultsStub.addProcessedResultValues(request)

    val updatedReportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .listReportingSetResults(
          listReportingSetResultsRequest {
            cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
            externalReportResultId = reportResult.externalReportResultId
            view = ReportingSetResultView.REPORTING_SET_RESULT_VIEW_FULL
          }
        )
        .reportingSetResultsList
    assertThat(updatedReportingSetResults)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELDS)
      .containsExactly(
        reportingSetResults[0].copy {
          reportingWindowResults[0] =
            reportingWindowResults[0].copy {
              value =
                value.copy {
                  processedReportResultValues =
                    request.reportingSetResultsMap
                      .getValue(reportingSetResults[0].externalReportingSetResultId)
                      .reportingWindowResultsList[0]
                      .value
                }
            }
        }
      )
  }

  @Test
  fun `addProcessedResultValues adds values when no grouping nor filter`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val reportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .batchCreateReportingSetResults(
          batchCreateReportingSetResultsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = reportResult.externalReportResultId
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              reportingSetResult = reportingSetResult {
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = "primitive-1"
                    vennDiagramRegionType =
                      ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                    externalImpressionQualificationFilterId =
                      AMI_IQF.externalImpressionQualificationFilterId
                    metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  }
                populationSize = 1000
                reportingWindowResults += reportingWindowEntry {
                  key = reportingWindow {
                    end = date {
                      year = 2025
                      month = 1
                      day = 20
                    }
                  }
                  value = reportingWindowResult {
                    unprocessedReportResultValues =
                      ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                        cumulativeResults = noisyMetricSet {
                          impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                        }
                      }
                  }
                }
              }
            }
          }
        )
        .reportingSetResultsList

    val request = addProcessedResultValuesRequest {
      cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
      externalReportResultId = reportResult.externalReportResultId
      this.reportingSetResults[reportingSetResults[0].externalReportingSetResultId] =
        AddProcessedResultValuesRequestKt.processedReportingSetResult {
          reportingWindowResults +=
            AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
              key = reportingSetResults[0].reportingWindowResultsList[0].key
              value =
                ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = basicMetricSet {
                    reach = 2
                    impressions = 10
                  }
                }
            }
        }
    }

    reportResultsStub.addProcessedResultValues(request)

    val updatedReportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .listReportingSetResults(
          listReportingSetResultsRequest {
            cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
            externalReportResultId = reportResult.externalReportResultId
            view = ReportingSetResultView.REPORTING_SET_RESULT_VIEW_FULL
          }
        )
        .reportingSetResultsList
    assertThat(updatedReportingSetResults)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELDS)
      .containsExactly(
        reportingSetResults[0].copy {
          dimension =
            dimension.copy { grouping = ReportingSetResult.Dimension.Grouping.getDefaultInstance() }
          reportingWindowResults[0] =
            reportingWindowResults[0].copy {
              value =
                value.copy {
                  processedReportResultValues =
                    request.reportingSetResultsMap
                      .getValue(reportingSetResults[0].externalReportingSetResultId)
                      .reportingWindowResultsList[0]
                      .value
                }
            }
        }
      )
  }

  @Test
  fun `addProcessedResultValues throws if reporting window not found`() {
    ensureMeasurementConsumer()
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    val reportingSetResults: List<ReportingSetResult> =
      reportResultsStub
        .batchCreateReportingSetResults(
          BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
            reportResult.externalReportResultId
          )
        )
        .reportingSetResultsList
    val request = addProcessedResultValuesRequest {
      cmmsMeasurementConsumerId = reportResult.cmmsMeasurementConsumerId
      externalReportResultId = reportResult.externalReportResultId
      this.reportingSetResults[reportingSetResults[0].externalReportingSetResultId] =
        AddProcessedResultValuesRequestKt.processedReportingSetResult {
          reportingWindowResults +=
            AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
              key = reportingWindow {
                end = date {
                  year = 2025
                  month = 11
                  day = 15
                }
              }
              value =
                ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = basicMetricSet { impressions = 2 }
                }
            }
        }
    }

    val exception =
      assertFailsWith<StatusException> { reportResultsStub.addProcessedResultValues(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REPORTING_WINDOW_RESULT_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            request.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_REPORT_RESULT_ID.key] =
            request.externalReportResultId.toString()
          metadata[Errors.Metadata.EXTERNAL_REPORTING_SET_RESULT_ID.key] =
            reportingSetResults[0].externalReportingSetResultId.toString()
          metadata[Errors.Metadata.REPORTING_WINDOW_END.key] = "2025-11-15"
        }
      )
  }

  @Test
  fun `addProcessedResultValues updates BasicReport state`() {
    val basicReport: BasicReport = ensureMeasurementConsumer()
    val externalReportId = "report-1"
    basicReportsStub.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
        externalBasicReportId = basicReport.externalBasicReportId
        this.externalReportId = externalReportId
      }
    )
    val reportResult: ReportResult =
      reportResultsStub.createReportResult(CREATE_REPORT_RESULT_REQUEST)
    // Ensure that BasicReport is associated with ReportResult.
    val reportingSetResults =
      reportResultsStub
        .batchCreateReportingSetResults(
          BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST.withExternalReportResultId(
              reportResult.externalReportResultId
            )
            .copy { externalBasicReportId = basicReport.externalBasicReportId }
        )
        .reportingSetResultsList
    val request = buildAddProcessedResultValuesRequest(reportingSetResults)

    reportResultsStub.addProcessedResultValues(request)

    val updatedBasicReport: BasicReport =
      basicReportsStub.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )
    assertThat(updatedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)
  }

  /**
   * Builds a test [AddProcessedResultValuesRequest].
   *
   * @param reportingSetResults Results created from [BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST].
   */
  private fun buildAddProcessedResultValuesRequest(
    reportingSetResults: List<ReportingSetResult>
  ): AddProcessedResultValuesRequest {
    val cmmsMeasurementConsumerId = reportingSetResults.first().cmmsMeasurementConsumerId
    val externalReportResultId = reportingSetResults.first().externalReportResultId
    return addProcessedResultValuesRequest {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalReportResultId = externalReportResultId
      this.reportingSetResults[reportingSetResults[0].externalReportingSetResultId] =
        AddProcessedResultValuesRequestKt.processedReportingSetResult {
          reportingWindowResults +=
            AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
              key = reportingSetResults[0].reportingWindowResultsList[0].key
              value =
                ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  cumulativeResults = basicMetricSet {
                    reach = 2
                    impressions = 10
                  }
                  nonCumulativeResults = basicMetricSet {
                    reach = 1
                    impressions = 5
                  }
                }
            }
          reportingWindowResults +=
            AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
              key = reportingSetResults[0].reportingWindowResultsList[1].key
              value =
                ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = basicMetricSet { impressions = 2 }
                }
            }
        }
      this.reportingSetResults[reportingSetResults[1].externalReportingSetResultId] =
        AddProcessedResultValuesRequestKt.processedReportingSetResult {
          reportingWindowResults +=
            AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
              key = reportingSetResults[1].reportingWindowResultsList[0].key
              value =
                ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                  nonCumulativeResults = basicMetricSet { impressions = 2 }
                }
            }
        }
    }
  }

  /**
   * Ensures that a MeasurementConsumer exists in the DB accessible by the ReportResults service.
   */
  private fun ensureMeasurementConsumer(): BasicReport {
    val reportingSetsStub = ReportingSetsGrpc.newBlockingV2Stub(grpcChannel)
    val measurementConsumersStub = MeasurementConsumersGrpc.newBlockingV2Stub(grpcChannel)

    measurementConsumersStub.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    val cmmsDataProviderId = "edp-1"
    val campaignGroup: ReportingSet =
      reportingSetsStub.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "primitive-1"
          reportingSet = reportingSet {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
            primitive =
              ReportingSetKt.primitive {
                this.eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    this.cmmsDataProviderId = cmmsDataProviderId
                    cmmsEventGroupId = "eg-1"
                  }
              }
          }
        }
      )

    // Creating a BasicReport is what ensures that the MeasurementConsumer exists in the DB, as it
    // uses a different DB than the Measurements service.
    return basicReportsStub.createBasicReport(
      createBasicReportRequest {
        basicReport = basicReport {
          cmmsMeasurementConsumerId = campaignGroup.cmmsMeasurementConsumerId
          externalBasicReportId = "basic-report-1"
          externalCampaignGroupId = campaignGroup.externalCampaignGroupId
          details = basicReportDetails {
            reportingInterval = reportingInterval {
              reportStart = dateTime {
                year = 2025
                month = 1
                day = 6
                timeZone = timeZone { id = "America/Los_Angeles" }
              }
            }
          }
        }
      }
    )
  }

  companion object {
    private val UNORDERED_FIELDS =
      listOf(
        ReportingSetResult.getDescriptor()
          .findFieldByNumber(ReportingSetResult.REPORTING_WINDOW_RESULTS_FIELD_NUMBER)
      )

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "mc-1"
    val AMI_IQF =
      ImpressionQualificationFilterConfigKt.impressionQualificationFilter {
        externalImpressionQualificationFilterId = "ami"
        impressionQualificationFilterId = 1
        filterSpecs +=
          ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
            mediaType =
              ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
                .DISPLAY
            filters +=
              ImpressionQualificationFilterConfigKt.eventFilter {
                terms +=
                  ImpressionQualificationFilterConfigKt.eventTemplateField {
                    path = "banner_ad.viewable"
                    value =
                      ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                        boolValue = false
                      }
                  }
              }
          }
      }
    private val IMPRESSION_QUALIFICATION_FILTER_CONFIG = impressionQualificationFilterConfig {
      impressionQualificationFilters += AMI_IQF
    }
    private val IMPRESSION_QUALIFICATION_FILTER_MAPPING =
      ImpressionQualificationFilterMapping(
        IMPRESSION_QUALIFICATION_FILTER_CONFIG,
        TestEvent.getDescriptor(),
      )

    val CREATE_REPORT_RESULT_REQUEST = createReportResultRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      reportResult = reportResult {
        reportStart = dateTime {
          year = 2025
          month = 1
          day = 6
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
      }
    }
    val BATCH_CREATE_REPORTING_SET_RESULTS_REQUEST = batchCreateReportingSetResultsRequest {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      requests += createReportingSetResultRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingSetResult = reportingSetResult {
          dimension =
            ReportingSetResultKt.dimension {
              externalReportingSetId = "primitive-1"
              vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
              externalImpressionQualificationFilterId =
                AMI_IQF.externalImpressionQualificationFilterId
              metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
              grouping =
                ReportingSetResultKt.DimensionKt.grouping {
                  valueByPath["person.age_group"] =
                    EventTemplateFieldKt.fieldValue {
                      enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                    }
                  valueByPath["person.gender"] =
                    EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
                }
              eventFilters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value =
                    EventTemplateFieldKt.fieldValue {
                      enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                    }
                }
              }
            }
          populationSize = 1000
          reportingWindowResults += reportingWindowEntry {
            key = reportingWindow {
              nonCumulativeStart = date {
                year = 2025
                month = 1
                day = 6
              }
              end = date {
                year = 2025
                month = 1
                day = 13
              }
            }

            value = reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  cumulativeResults = noisyMetricSet {
                    reach = NoisyMetricSetKt.reachResult { value = 1 }
                    impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                  }
                  nonCumulativeResults = noisyMetricSet {
                    reach = NoisyMetricSetKt.reachResult { value = 1 }
                    impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                  }
                }
            }
          }
          reportingWindowResults += reportingWindowEntry {
            key = reportingWindow {
              nonCumulativeStart = date {
                year = 2025
                month = 1
                day = 13
              }
              end = date {
                year = 2025
                month = 1
                day = 20
              }
            }
            value = reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  nonCumulativeResults = noisyMetricSet {
                    impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                  }
                }
            }
          }
        }
      }
      requests += createReportingSetResultRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingSetResult = reportingSetResult {
          dimension =
            ReportingSetResultKt.dimension {
              externalReportingSetId = "primitive-1"
              vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
              custom = true
              metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
              grouping =
                ReportingSetResultKt.DimensionKt.grouping {
                  valueByPath["person.age_group"] =
                    EventTemplateFieldKt.fieldValue {
                      enumValue = Person.AgeGroup.YEARS_35_TO_54.name
                    }
                  valueByPath["person.gender"] =
                    EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
                }
              eventFilters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value =
                    EventTemplateFieldKt.fieldValue {
                      enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                    }
                }
              }
            }
          reportingWindowResults += reportingWindowEntry {
            key = reportingWindow {
              nonCumulativeStart = date {
                year = 2025
                month = 1
                day = 6
              }
              end = date {
                year = 2025
                month = 1
                day = 13
              }
            }
            value = reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  nonCumulativeResults = noisyMetricSet {
                    impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                  }
                }
            }
          }
        }
      }
      requests += createReportingSetResultRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        reportingSetResult = reportingSetResult {
          dimension =
            ReportingSetResultKt.dimension {
              externalReportingSetId = "composite-1"
              vennDiagramRegionType = ReportingSetResult.Dimension.VennDiagramRegionType.UNION
              custom = true
              metricFrequencySpec = metricFrequencySpec { total = true }
              grouping =
                ReportingSetResultKt.DimensionKt.grouping {
                  valueByPath["person.age_group"] =
                    EventTemplateFieldKt.fieldValue {
                      enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                    }
                  valueByPath["person.gender"] =
                    EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
                }
              eventFilters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value =
                    EventTemplateFieldKt.fieldValue {
                      enumValue = Person.AgeGroup.YEARS_35_TO_54.name
                    }
                }
              }
            }
          reportingWindowResults += reportingWindowEntry {
            key = reportingWindow {
              end = date {
                year = 2025
                month = 1
                day = 13
              }
            }
            value = reportingWindowResult {
              unprocessedReportResultValues =
                ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                  cumulativeResults = noisyMetricSet {
                    reach = NoisyMetricSetKt.reachResult { value = 1 }
                  }
                }
            }
          }
        }
      }
    }
  }
}

private fun BatchCreateReportingSetResultsRequest.withExternalReportResultId(
  externalReportResultId: Long
): BatchCreateReportingSetResultsRequest {
  return copy {
    this.externalReportResultId = externalReportResultId
    requests.mapIndexed { index, createRequest ->
      requests[index] = createRequest.copy { this.externalReportResultId = externalReportResultId }
    }
  }
}
