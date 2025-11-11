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
import com.google.protobuf.Descriptors
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
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpc
import org.wfanet.measurement.internal.reporting.v2.CreateReportResultRequestKt
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
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpc
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.createReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.getReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.reporting.service.internal.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.service.internal.Normalization

@RunWith(JUnit4::class)
abstract class ReportResultsServiceTest {
  private val idGenerator = IdGenerator.Default

  @get:Rule private val grpcCleanup = GrpcCleanupRule()

  private val grpcChannel by lazy {
    val serverName = InProcessServerBuilder.generateName()
    val services =
      createServices(
        idGenerator,
        IMPRESSION_QUALIFICATION_FILTER_MAPPING,
        TestEvent.getDescriptor(),
      )
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

  protected abstract fun createServices(
    idGenerator: IdGenerator,
    impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
    eventMessageDescriptor: Descriptors.Descriptor,
  ): List<BindableService>

  @Test
  fun `createReportResult creates ReportResult`() {
    ensureMeasurementConsumer()
    val reportResultsStub = ReportResultsGrpc.newBlockingV2Stub(grpcChannel)
    val request = CREATE_REPORT_RESULT_REQUEST

    val response: ReportResult = reportResultsStub.createReportResult(request)

    assertThat(response)
      .ignoringFields(ReportResult.EXTERNAL_REPORT_RESULT_ID_FIELD_NUMBER)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELDS)
      .isEqualTo(
        request.reportResult.copy {
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
          reportingSetResults +=
            request.createReportingSetResultRequestsList[0].reportingSetResult.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = response.externalReportResultId
              externalReportingSetResultId =
                request.createReportingSetResultRequestsList[0].externalReportingSetResultId
              metricFrequencySpecFingerprint = 5458822473151716274
              groupingDimensionFingerprint = 9076537016683682170
              filterFingerprint = 12115333261944561450UL.toLong()
            }
          reportingSetResults +=
            request.createReportingSetResultRequestsList[1].reportingSetResult.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = response.externalReportResultId
              externalReportingSetResultId =
                request.createReportingSetResultRequestsList[1].externalReportingSetResultId
              metricFrequencySpecFingerprint = 5458822473151716274
              groupingDimensionFingerprint = 3506459991786236532
              filterFingerprint = 12115333261944561450UL.toLong()
            }
          reportingSetResults +=
            request.createReportingSetResultRequestsList[2].reportingSetResult.copy {
              cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId
              externalReportResultId = response.externalReportResultId
              externalReportingSetResultId =
                request.createReportingSetResultRequestsList[2].externalReportingSetResultId
              metricFrequencySpecFingerprint = 11987848417806868115UL.toLong()
              groupingDimensionFingerprint = 9076537016683682170
              filterFingerprint = 15177473705012173011UL.toLong()
            }
        }
      )
    assertWithMessage("Should match persisted value")
      .about(ProtoTruth.protos())
      .that(response)
      .ignoringRepeatedFieldOrderOfFieldDescriptors(UNORDERED_FIELDS)
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
  fun `createReportResult throws when event filters are not normalized`() {
    ensureMeasurementConsumer()
    val reportResultsStub = ReportResultsGrpc.newBlockingV2Stub(grpcChannel)
    val request =
      CREATE_REPORT_RESULT_REQUEST.copy {
        createReportingSetResultRequests[1] =
          createReportingSetResultRequests[1].copy {
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
      assertFailsWith<StatusException> { reportResultsStub.createReportResult(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "create_reporting_set_result_requests[1].reporting_set_result.dimension.event_filters"
        }
      )
  }

  /**
   * Ensures that a MeasurementConsumer exists in the DB accessible by the ReportResults service.
   */
  private fun ensureMeasurementConsumer() {
    val reportingSetsStub = ReportingSetsGrpc.newBlockingV2Stub(grpcChannel)
    val basicReportsStub = BasicReportsGrpc.newBlockingV2Stub(grpcChannel)
    val measurementConsumersStub = MeasurementConsumersGrpc.newBlockingV2Stub(grpcChannel)

    measurementConsumersStub.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    val campaignGroup: ReportingSet =
      reportingSetsStub.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "reporting-set-1"
          reportingSet = reportingSet {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
            primitive =
              ReportingSetKt.primitive {
                this.eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "edp-1"
                    cmmsEventGroupId = "eg-1"
                  }
              }
          }
        }
      )

    // Creating a BasicReport is what ensures that the MeasurementConsumer exists in the DB, as it
    // uses a different DB than the Measurements service.
    basicReportsStub.createBasicReport(
      createBasicReportRequest {
        basicReport = basicReport {
          cmmsMeasurementConsumerId = campaignGroup.cmmsMeasurementConsumerId
          externalBasicReportId = "basic-report-1"
          externalCampaignGroupId = campaignGroup.externalCampaignGroupId
        }
      }
    )
  }

  companion object {
    private val UNORDERED_FIELDS =
      listOf(
        ReportResult.getDescriptor()
          .findFieldByNumber(ReportResult.REPORTING_SET_RESULTS_FIELD_NUMBER),
        ReportingSetResult.getDescriptor()
          .findFieldByNumber(ReportingSetResult.REPORTING_WINDOW_RESULTS_FIELD_NUMBER),
      )

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "mc-1"
    val AMI_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
      filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
    }
    private val IMPRESSION_QUALIFICATION_FILTER_CONFIG = impressionQualificationFilterConfig {
      impressionQualificationFilters += AMI_IQF
    }
    private val IMPRESSION_QUALIFICATION_FILTER_MAPPING =
      ImpressionQualificationFilterMapping(IMPRESSION_QUALIFICATION_FILTER_CONFIG)

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
      createReportingSetResultRequests +=
        CreateReportResultRequestKt.createReportingSetResultRequest {
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
                noisyReportResultValues =
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
                noisyReportResultValues =
                  ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                    nonCumulativeResults = noisyMetricSet {
                      impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                    }
                  }
              }
            }
          }
          externalReportingSetResultId =
            Normalization.computeFingerprint(reportingSetResult.dimension)
        }
      createReportingSetResultRequests +=
        CreateReportResultRequestKt.createReportingSetResultRequest {
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
                noisyReportResultValues =
                  ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                    nonCumulativeResults = noisyMetricSet {
                      impressionCount = NoisyMetricSetKt.impressionCountResult { value = 1 }
                    }
                  }
              }
            }
          }
          externalReportingSetResultId =
            Normalization.computeFingerprint(reportingSetResult.dimension)
        }
      createReportingSetResultRequests +=
        CreateReportResultRequestKt.createReportingSetResultRequest {
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
                noisyReportResultValues =
                  ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                    cumulativeResults = noisyMetricSet {
                      reach = NoisyMetricSetKt.reachResult { value = 1 }
                    }
                  }
              }
            }
          }
          externalReportingSetResultId =
            Normalization.computeFingerprint(reportingSetResult.dimension)
        }
    }
  }
}
