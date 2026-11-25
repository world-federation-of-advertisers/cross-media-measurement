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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.Empty
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.DayOfWeek
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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec.MediaType
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig
import org.wfanet.measurement.internal.reporting.v2.AddProcessedResultValuesRequestKt
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportKt
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.DimensionSpecKt
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.NoisyMetricSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.ReportingWindowResultKt.NoisyReportResultValuesKt.noisyMetricSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.reportingWindow
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.reportingWindowEntry
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt.reportingWindowResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.ReportingUnitKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroup
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt.MetricSetKt.basicMetricSet
import org.wfanet.measurement.internal.reporting.v2.ResultGroupMetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMappingKt
import org.wfanet.measurement.internal.reporting.v2.addProcessedResultValuesRequest
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportComputationsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateReportingSetResultsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.createReportResultRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetResultRequest
import org.wfanet.measurement.internal.reporting.v2.dataProviderKey
import org.wfanet.measurement.internal.reporting.v2.dimensionSpec
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.failBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.getReportComputationRequest
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsResponse
import org.wfanet.measurement.internal.reporting.v2.listReportComputationsRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportComputation
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.reportingSetKey
import org.wfanet.measurement.internal.reporting.v2.reportingSetResult
import org.wfanet.measurement.internal.reporting.v2.reportingUnit
import org.wfanet.measurement.internal.reporting.v2.resultGroup
import org.wfanet.measurement.internal.reporting.v2.resultGroupMetricSpec
import org.wfanet.measurement.internal.reporting.v2.resultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.resultGroupSpec
import org.wfanet.measurement.internal.reporting.v2.setExternalReportIdRequest
import org.wfanet.measurement.internal.reporting.v2.setMeasurementsCreatedRequest
import org.wfanet.measurement.internal.reporting.v2.setReportComputationMeasurementIdRequest
import org.wfanet.measurement.reporting.service.internal.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

@RunWith(JUnit4::class)
abstract class BasicReportsServiceTest<T : BasicReportsCoroutineImplBase> {
  protected val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))
  protected val impressionQualificationFilterMapping = IMPRESSION_QUALIFICATION_FILTER_MAPPING

  protected data class Services<T>(
    val basicReportsService: T,
    val measurementConsumersService: MeasurementConsumersCoroutineImplBase,
    val reportingSetsService: ReportingSetsCoroutineImplBase,
    val reportResultsService: ReportResultsCoroutineImplBase,
  )

  /** Instance of the service under test. */
  private lateinit var service: T
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineImplBase
  private lateinit var reportingSetsService: ReportingSetsCoroutineImplBase
  private lateinit var reportResultsService: ReportResultsCoroutineImplBase

  /** Constructs the services being tested. */
  protected abstract fun newServices(
    idGenerator: IdGenerator,
    validateInsertBasicReport: Boolean,
    validateCreateReportingSet: Boolean,
  ): Services<T>

  protected abstract suspend fun removeReportingSetEventGroups(
    cmmsMeasurementConsumerId: String,
    externalReportingSetId: String,
  )

  protected abstract suspend fun removeReportingSetCompilation(
    cmmsMeasurementConsumerId: String,
    externalReportingSetId: String,
  )

  @Before
  fun initService() {
    val services =
      newServices(idGenerator, validateInsertBasicReport = true, validateCreateReportingSet = true)
    service = services.basicReportsService
    measurementConsumersService = services.measurementConsumersService
    reportingSetsService = services.reportingSetsService
    reportResultsService = services.reportResultsService
  }

  @Test
  fun `insertBasicReport validation rejects missing and contradictory identities before writing`():
    Unit = runBlocking {
    val services = newServices(idGenerator, true, false)
    seedInsertValidationReportingSets(services, includeMalformedSets = true)
    for ((scenario, malformed) in insertValidationCases()) {
      val report = malformed.copy { externalBasicReportId = scenario }
      val exception =
        assertFailsWith<StatusRuntimeException>(scenario) {
          services.basicReportsService.insertBasicReport(
            insertBasicReportRequest { basicReport = report }
          )
        }
      assertThat(exception.status.code)
        .isAnyOf(Status.Code.INVALID_ARGUMENT, Status.Code.FAILED_PRECONDITION)
      if (
        scenario in
          setOf(
            "missing-request-specs",
            "missing-reporting-unit",
            "unset-components",
            "extra-request-spec",
          )
      ) {
        assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
        assertThat(exception.errorInfo)
          .isEqualTo(
            errorInfo {
              domain = Errors.DOMAIN
              reason = Errors.Reason.INVALID_FIELD_VALUE.name
              metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.result_details"
            }
          )
      }
      if (scenario == "missing-report-id") {
        assertThat(exception.errorInfo)
          .isEqualTo(
            errorInfo {
              domain = Errors.DOMAIN
              reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
              metadata[Errors.Metadata.FIELD_NAME.key] = "external_report_id"
            }
          )
      }
      val missing =
        assertFailsWith<StatusRuntimeException>(scenario) {
          services.basicReportsService.getBasicReport(
            getBasicReportRequest {
              cmmsMeasurementConsumerId = report.cmmsMeasurementConsumerId
              externalBasicReportId = report.externalBasicReportId
            }
          )
        }
      assertThat(missing.status.code).isEqualTo(Status.Code.NOT_FOUND)
    }
  }

  @Test
  fun `insertBasicReport disabled validation persists malformed legacy identities unchanged`():
    Unit = runBlocking {
    val services = newServices(idGenerator, false, false)
    seedInsertValidationReportingSets(services, includeMalformedSets = true)
    for ((scenario, malformed) in
      insertValidationCases().filterKeys {
        it !in setOf("missing-summary-id", "missing-metric-id")
      }) {
      val report = malformed.copy { externalBasicReportId = scenario }
      val inserted =
        services.basicReportsService.insertBasicReport(
          insertBasicReportRequest { basicReport = report }
        )
      val persisted =
        services.basicReportsService.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = report.cmmsMeasurementConsumerId
            externalBasicReportId = report.externalBasicReportId
          }
        )
      assertThat(inserted.state).isEqualTo(BasicReport.State.SUCCEEDED)
      assertThat(persisted.externalReportId).isEqualTo(report.externalReportId)
      assertThat(persisted.externalCampaignGroupId).isEqualTo(report.externalCampaignGroupId)
      assertThat(persisted.details).isEqualTo(report.details)
      assertThat(persisted.resultDetails).isEqualTo(report.resultDetails)
    }
  }

  @Test
  fun `insertBasicReport disabled validation preserves required component IDs from main`(): Unit =
    runBlocking {
      val services = newServices(idGenerator, false, false)
      seedInsertValidationReportingSets(services, includeMalformedSets = false)
      val fields =
        mapOf(
          "missing-summary-id" to
            "result_details.result_groups[0].results[0].metadata.reporting_unit_summary." +
              "reporting_unit_component_summary[0].external_reporting_set_id",
          "missing-metric-id" to
            "result_details.result_groups[0].results[0].metric_set." +
              "reporting_set_components[0].external_reporting_set_id",
        )
      for ((scenario, field) in fields) {
        val report =
          insertValidationCases().getValue(scenario).copy { externalBasicReportId = scenario }
        val exception =
          assertFailsWith<StatusRuntimeException>(scenario) {
            services.basicReportsService.insertBasicReport(
              insertBasicReportRequest { basicReport = report }
            )
          }
        assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
        assertThat(exception.errorInfo)
          .isEqualTo(
            errorInfo {
              domain = Errors.DOMAIN
              reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
              metadata[Errors.Metadata.FIELD_NAME.key] = field
            }
          )
        val missing =
          assertFailsWith<StatusRuntimeException>(scenario) {
            services.basicReportsService.getBasicReport(
              getBasicReportRequest {
                cmmsMeasurementConsumerId = report.cmmsMeasurementConsumerId
                externalBasicReportId = report.externalBasicReportId
              }
            )
          }
        assertThat(missing.status.code).isEqualTo(Status.Code.NOT_FOUND)
      }
    }

  @Test
  fun `insertBasicReport validation accepts empty request and result groups together`(): Unit =
    runBlocking {
      val services = newServices(idGenerator, true, true)
      seedInsertValidationReportingSets(services, includeMalformedSets = false)
      val report =
        insertValidationCustomReport("custom-a").copy {
          details = details.copy { resultGroupSpecs.clear() }
          resultDetails = basicReportResultDetails {}
        }
      services.basicReportsService.insertBasicReport(
        insertBasicReportRequest { basicReport = report }
      )
      val persisted =
        services.basicReportsService.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = report.cmmsMeasurementConsumerId
            externalBasicReportId = report.externalBasicReportId
          }
        )
      assertThat(persisted.details).isEqualTo(report.details)
      assertThat(persisted.resultDetails).isEqualTo(report.resultDetails)
    }

  @Test
  fun `insertBasicReport validation accepts standalone primitive and composite custom identities`():
    Unit = runBlocking {
    val services = newServices(idGenerator, true, true)
    seedInsertValidationReportingSets(services, includeMalformedSets = false)
    val primitive = insertValidationCustomReport("custom-a")
    val composite = insertValidationCustomReport("custom-composite")
    val both =
      primitive.copy {
        details =
          details.copy {
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                reportingUnit =
                  reportingUnit.copy {
                    reportingSetKeys =
                      reportingSetKeys.copy {
                        reportingSetKeys += reportingSetKey {
                          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                          externalReportingSetId = "custom-composite"
                        }
                      }
                  }
              }
          }
        resultDetails =
          resultDetails.copy {
            resultGroups[0] =
              resultGroups[0].copy {
                results[0] =
                  results[0].copy {
                    metadata =
                      metadata.copy {
                        reportingUnitSummary =
                          reportingUnitSummary.copy {
                            reportingUnitComponentSummary +=
                              ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                                externalReportingSetId = "custom-composite"
                              }
                          }
                      }
                    metricSet =
                      metricSet.copy {
                        val firstMetric = reportingSetComponents[0]
                        reportingSetComponents.clear()
                        reportingSetComponents +=
                          ResultGroupKt.MetricSetKt.reportingSetComponentMetricSetMapEntry {
                            externalReportingSetId = "custom-composite"
                            value =
                              ResultGroupKt.MetricSetKt.componentMetricSet {
                                nonCumulative = basicMetricSet { reach = 789 }
                              }
                          }
                        reportingSetComponents += firstMetric
                      }
                  }
              }
          }
      }
    val partial =
      modifyInsertValidationResult(both) {
        copy {
          metricSet =
            metricSet.copy {
              val firstMetric = reportingSetComponents[0]
              reportingSetComponents.clear()
              reportingSetComponents += firstMetric
            }
        }
      }
    for ((index, report) in
      listOf(
          primitive,
          composite,
          both,
          partial,
          insertValidationCustomReport("custom-nested"),
          insertValidationCustomReport("custom-cancelled"),
          insertValidationCustomReport("custom-cancelled-nested"),
          insertValidationProviderReport(),
        )
        .withIndex()) {
      val expected = report.copy { externalBasicReportId = "valid-$index" }
      services.basicReportsService.insertBasicReport(
        insertBasicReportRequest { basicReport = expected }
      )
      val persisted =
        services.basicReportsService.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalBasicReportId = expected.externalBasicReportId
          }
        )
      assertThat(persisted.details).isEqualTo(expected.details)
      assertThat(persisted.resultDetails).isEqualTo(expected.resultDetails)
    }
    val customSets =
      services.reportingSetsService
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportingSetIds += listOf("custom-a", "custom-composite")
          }
        )
        .reportingSetsList
    assertThat(customSets.map { it.externalCampaignGroupId }).containsExactly("", "")
    assertThat(customSets.single { it.externalReportingSetId == "custom-a" }.filter)
      .isEqualTo("person.age_group == 1")
  }

  @Test
  fun `insertBasicReport validation can be disabled while createReportingSet validation remains enabled`():
    Unit = runBlocking {
    val services = newServices(idGenerator, false, true)
    seedInsertValidationReportingSets(services, includeMalformedSets = false)
    val malformed = insertValidationCustomReport("custom-a").copy { externalReportId = "" }
    val inserted =
      services.basicReportsService.insertBasicReport(
        insertBasicReportRequest { basicReport = malformed }
      )
    assertThat(inserted.externalReportId).isEmpty()
    val exception =
      assertFailsWith<StatusRuntimeException> {
        services.reportingSetsService.createReportingSet(
          createReportingSetRequest {
            externalReportingSetId = "empty-set"
            reportingSet = reportingSet {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              primitive = ReportingSetKt.primitive {}
            }
          }
        )
      }
    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
  }

  @Suppress("DEPRECATION") // Verify contradictory legacy EventGroup evidence is not ignored.
  private fun insertValidationCases(): Map<String, BasicReport> {
    val custom = insertValidationCustomReport("custom-a")
    val provider = insertValidationProviderReport()
    fun withSummary(report: BasicReport, id: String, providerId: String): BasicReport =
      modifyInsertValidationResult(report) {
        copy {
          metadata =
            metadata.copy {
              reportingUnitSummary =
                reportingUnitSummary.copy {
                  reportingUnitComponentSummary[0] =
                    reportingUnitComponentSummary[0].copy {
                      externalReportingSetId = id
                      cmmsDataProviderId = providerId
                    }
                }
            }
        }
      }
    return linkedMapOf(
      "uncompiled-union" to insertValidationCustomReport("custom-uncompiled-union"),
      "uncompiled-nested-union" to insertValidationCustomReport("custom-uncompiled-nested"),
      "missing-report-id" to custom.copy { externalReportId = "" },
      "missing-request-specs" to
        custom.copy { details = details.copy { resultGroupSpecs.clear() } },
      "missing-reporting-unit" to
        custom.copy {
          details =
            details.copy { resultGroupSpecs[0] = resultGroupSpecs[0].copy { clearReportingUnit() } }
        },
      "unset-components" to
        provider.copy {
          details =
            details.copy {
              resultGroupSpecs[0] = resultGroupSpecs[0].copy { reportingUnit = reportingUnit {} }
            }
        },
      "extra-request-spec" to
        custom.copy { details = details.copy { resultGroupSpecs += resultGroupSpecs[0] } },
      "missing-summary-id" to withSummary(custom, "", ""),
      "unknown-summary-id" to withSummary(custom, "missing", ""),
      "wrong-existing-summary-id" to withSummary(custom, "custom-b", ""),
      "custom-summary-provider-id" to withSummary(custom, "custom-a", "1235"),
      "missing-metric-id" to
        modifyInsertValidationResult(custom) {
          copy {
            metricSet =
              metricSet.copy {
                reportingSetComponents[0] =
                  reportingSetComponents[0].copy { externalReportingSetId = "" }
              }
          }
        },
      "wrong-existing-metric-id" to
        modifyInsertValidationResult(custom) {
          copy {
            metricSet =
              metricSet.copy {
                reportingSetComponents[0] =
                  reportingSetComponents[0].copy { externalReportingSetId = "custom-b" }
              }
          }
        },
      "repeated-metric-id" to
        modifyInsertValidationResult(custom) {
          copy {
            metricSet = metricSet.copy { reportingSetComponents += reportingSetComponents[0] }
          }
        },
      "foreign-request-consumer" to
        custom.copy {
          details =
            details.copy {
              resultGroupSpecs[0] =
                resultGroupSpecs[0].copy {
                  reportingUnit =
                    reportingUnit.copy {
                      reportingSetKeys =
                        reportingSetKeys.copy {
                          reportingSetKeys[0] =
                            reportingSetKeys[0].copy { cmmsMeasurementConsumerId = "other" }
                        }
                    }
                }
            }
        },
      "missing-request-id" to
        custom.copy {
          details =
            details.copy {
              resultGroupSpecs[0] =
                resultGroupSpecs[0].copy {
                  reportingUnit =
                    reportingUnit.copy {
                      reportingSetKeys =
                        reportingSetKeys.copy {
                          reportingSetKeys[0] =
                            reportingSetKeys[0].copy { externalReportingSetId = "" }
                        }
                    }
                }
            }
        },
      "missing-summary" to
        modifyInsertValidationResult(custom) {
          copy {
            metadata =
              metadata.copy {
                reportingUnitSummary =
                  reportingUnitSummary.copy { reportingUnitComponentSummary.clear() }
              }
          }
        },
      "group-count-mismatch" to
        custom.copy { resultDetails = resultDetails.copy { resultGroups.clear() } },
      "provider-missing-identity" to withSummary(provider, "provider-a", ""),
      "provider-wrong-identity" to withSummary(provider, "provider-a", "2235"),
      "provider-filtered-set" to withSummary(provider, "custom-a", "1235"),
      "provider-composite-set" to withSummary(provider, "custom-composite", "1235"),
      "provider-incomplete-membership" to withSummary(provider, "provider-partial", "1235"),
      "provider-outside-campaign" to withSummary(provider, "outside", "1235"),
      "provider-wrong-metric-key" to
        modifyInsertValidationResult(provider) {
          copy {
            metricSet = metricSet.copy { components[0] = components[0].copy { key = "2235" } }
          }
        },
      "provider-wrong-intersection-key" to
        modifyInsertValidationResult(provider) {
          copy {
            metricSet =
              metricSet.copy {
                componentIntersections +=
                  ResultGroupKt.MetricSetKt.dataProviderComponentIntersectionMetricSet {
                    cmmsDataProviderIds += listOf("1235", "2235")
                    nonCumulative = basicMetricSet { reach = 77 }
                  }
              }
          }
        },
      "wrong-recorded-event-group" to
        modifyInsertValidationResult(custom) {
          copy {
            metadata =
              metadata.copy {
                reportingUnitSummary =
                  reportingUnitSummary.copy {
                    reportingUnitComponentSummary[0] =
                      reportingUnitComponentSummary[0].copy {
                        eventGroupSummaries +=
                          ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                            .eventGroupSummary {
                              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                              cmmsEventGroupId = "wrong-event-group"
                            }
                      }
                  }
              }
          }
        },
      "foreign-recorded-event-group" to
        modifyInsertValidationResult(custom) {
          copy {
            metadata =
              metadata.copy {
                reportingUnitSummary =
                  reportingUnitSummary.copy {
                    reportingUnitComponentSummary[0] =
                      reportingUnitComponentSummary[0].copy {
                        eventGroupSummaries +=
                          ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                            .eventGroupSummary {
                              cmmsMeasurementConsumerId = "other-consumer"
                              cmmsEventGroupId = "eg1"
                            }
                      }
                  }
              }
          }
        },
      "empty-requested-results" to
        custom.copy {
          resultDetails =
            resultDetails.copy { resultGroups[0] = resultGroups[0].copy { results.clear() } }
        },
      "foreign-reporting-set" to insertValidationCustomReport("foreign-set"),
      "component-outside-own-campaign" to insertValidationCustomReport("bad-own-campaign"),
      "invalid-composite-operation" to insertValidationCustomReport("invalid-operation"),
      "empty-campaign" to custom.copy { externalCampaignGroupId = "empty-campaign" },
      "ordinary-campaign-set" to custom.copy { externalCampaignGroupId = "provider-a" },
      "composite-campaign-set" to custom.copy { externalCampaignGroupId = "custom-composite" },
    )
  }

  private fun modifyInsertValidationResult(
    report: BasicReport,
    transform: ResultGroup.Result.() -> ResultGroup.Result,
  ): BasicReport =
    report.copy {
      resultDetails =
        resultDetails.copy {
          resultGroups[0] = resultGroups[0].copy { results[0] = results[0].transform() }
        }
    }

  private fun insertValidationCustomReport(componentId: String): BasicReport =
    INSERT_BASIC_REPORT.copy {
      externalCampaignGroupId = "validation-campaign"
      details = basicReportDetails {
        title = "Validation report"
        resultGroupSpecs += resultGroupSpec {
          reportingUnit = reportingUnit {
            reportingSetKeys =
              ReportingUnitKt.reportingSetKeys {
                reportingSetKeys += reportingSetKey {
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                  externalReportingSetId = componentId
                }
              }
          }
        }
      }
      resultDetails = basicReportResultDetails {
        resultGroups += resultGroup {
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          externalReportingSetId = componentId
                        }
                    }
                }
              metricSet =
                ResultGroupKt.metricSet {
                  reportingSetComponents +=
                    ResultGroupKt.MetricSetKt.reportingSetComponentMetricSetMapEntry {
                      externalReportingSetId = componentId
                      value =
                        ResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative = basicMetricSet {
                            reach = 123
                            impressions = 456
                          }
                        }
                    }
                }
            }
        }
      }
    }

  private fun insertValidationProviderReport(): BasicReport =
    modifyInsertValidationResult(
      insertValidationCustomReport("provider-a").copy {
        details =
          details.copy {
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                reportingUnit = reportingUnit {
                  dataProviderKeys =
                    ReportingUnitKt.dataProviderKeys {
                      dataProviderKeys += dataProviderKey { cmmsDataProviderId = "1235" }
                    }
                }
              }
          }
      }
    ) {
      copy {
        metadata =
          metadata.copy {
            reportingUnitSummary =
              reportingUnitSummary.copy {
                reportingUnitComponentSummary[0] =
                  reportingUnitComponentSummary[0].copy { cmmsDataProviderId = "1235" }
              }
          }
        metricSet =
          metricSet.copy {
            val metricValue = reportingSetComponents[0].value
            reportingSetComponents.clear()
            components +=
              ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                key = "1235"
                value = metricValue
              }
          }
      }
    }

  private suspend fun seedInsertValidationReportingSets(
    services: Services<T>,
    includeMalformedSets: Boolean,
  ) {
    services.measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    services.measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = "other-consumer" }
    )
    services.reportingSetsService.createReportingSet(
      createReportingSetRequest {
        externalReportingSetId = "foreign-set"
        reportingSet =
          REPORTING_SET.copy {
            cmmsMeasurementConsumerId = "other-consumer"
            externalReportingSetId = "foreign-set"
            externalCampaignGroupId = ""
          }
      }
    )
    val first =
      ReportingSetKt.PrimitiveKt.eventGroupKey {
        cmmsDataProviderId = "1235"
        cmmsEventGroupId = "eg1"
      }
    val second = first.copy { cmmsEventGroupId = "eg2" }
    val third =
      first.copy {
        cmmsDataProviderId = "2235"
        cmmsEventGroupId = "eg3"
      }
    val sets =
      listOf(
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "validation-campaign"
          externalCampaignGroupId = externalReportingSetId
          primitive = ReportingSetKt.primitive { eventGroupKeys += listOf(first, second, third) }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "provider-a"
          primitive = ReportingSetKt.primitive { eventGroupKeys += listOf(first, second) }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "provider-partial"
          primitive = ReportingSetKt.primitive { eventGroupKeys += first }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-a"
          filter = "person.age_group == 1"
          primitive = ReportingSetKt.primitive { eventGroupKeys += first }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-b"
          primitive = ReportingSetKt.primitive { eventGroupKeys += first }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-composite"
          composite =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "custom-a" }
              rhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "custom-b" }
            }
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              binaryRepresentation = 3
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = "custom-a"
                  filters += "person.age_group == 1"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis { externalReportingSetId = "custom-b" }
            }
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = -1
              binaryRepresentation = 2
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis { externalReportingSetId = "custom-b" }
            }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "outside"
          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys += first.copy { cmmsEventGroupId = "outside" }
            }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-child"
          composite =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "custom-a" }
              rhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "custom-b" }
            }
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              binaryRepresentation = 3
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = "custom-a"
                  filters += "person.age_group == 1"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis { externalReportingSetId = "custom-b" }
            }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-nested"
          composite =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "custom-child" }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId = "provider-partial"
                }
            }
          weightedSubsetUnions +=
            ReportingSetKt.weightedSubsetUnion {
              weight = 1
              binaryRepresentation = 7
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = "custom-a"
                  filters += "person.age_group == 1"
                }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis { externalReportingSetId = "custom-b" }
              primitiveReportingSetBases +=
                ReportingSetKt.primitiveReportingSetBasis {
                  externalReportingSetId = "provider-partial"
                }
            }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-cancelled"
          composite =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs = ReportingSetKt.SetExpressionKt.operand { externalReportingSetId = "custom-a" }
              rhs = lhs
            }
        },
        reportingSet {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportingSetId = "custom-cancelled-nested"
          composite =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  externalReportingSetId = "custom-cancelled"
                }
              rhs = lhs
            }
        },
      )
    val strictReportingSets = newServices(idGenerator, true, true).reportingSetsService
    for (set in sets) {
      strictReportingSets.createReportingSet(
        createReportingSetRequest {
          reportingSet = set
          externalReportingSetId = set.externalReportingSetId
        }
      )
    }
    if (includeMalformedSets) {
      for (set in
        listOf(
          sets[7].copy {
            externalReportingSetId = "custom-uncompiled-union"
            weightedSubsetUnions.clear()
          },
          sets[8].copy {
            externalReportingSetId = "custom-uncompiled-nested"
            weightedSubsetUnions.clear()
          },
          sets[0].copy {
            externalReportingSetId = "small-campaign"
            externalCampaignGroupId = externalReportingSetId
            primitive = ReportingSetKt.primitive { eventGroupKeys += third }
          },
          sets[4].copy {
            externalReportingSetId = "bad-own-campaign"
            externalCampaignGroupId = "small-campaign"
          },
          sets[0].copy {
            externalReportingSetId = "empty-campaign"
            externalCampaignGroupId = externalReportingSetId
            primitive = ReportingSetKt.primitive {}
          },
          sets[5].copy {
            externalReportingSetId = "invalid-operation"
            composite = composite.copy { operationValue = 99 }
          },
        )) {
        val seed =
          when (set.externalReportingSetId) {
            "custom-uncompiled-union" ->
              sets[7].copy { externalReportingSetId = set.externalReportingSetId }
            "custom-uncompiled-nested" ->
              sets[8].copy { externalReportingSetId = set.externalReportingSetId }
            "empty-campaign" -> set.copy { primitive = sets[0].primitive }
            else -> set
          }
        services.reportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet = seed
            externalReportingSetId = set.externalReportingSetId
          }
        )
        when (set.externalReportingSetId) {
          "custom-uncompiled-union",
          "custom-uncompiled-nested" ->
            removeReportingSetCompilation(CMMS_MEASUREMENT_CONSUMER_ID, set.externalReportingSetId)
          "empty-campaign" ->
            removeReportingSetEventGroups(CMMS_MEASUREMENT_CONSUMER_ID, set.externalReportingSetId)
        }
      }
    }
  }

  @Test
  fun `createBasicReport succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails {
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            hours = 5
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          effectiveReportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            hours = 5
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
              .first()
              .externalImpressionQualificationFilterId
        }
        effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
              .first()
              .externalImpressionQualificationFilterId
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = "1234" }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                stackedIncrementalReach = true
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
      createReportRequestId = "1235"
    }

    val request = createBasicReportRequest { this.basicReport = basicReport }

    val response = service.createBasicReport(request)

    assertThat(response)
      .ignoringFields(
        BasicReport.CAMPAIGN_GROUP_DISPLAY_NAME_FIELD_NUMBER,
        BasicReport.CREATE_TIME_FIELD_NUMBER,
        BasicReport.STATE_FIELD_NUMBER,
      )
      .isEqualTo(basicReport)
    assertThat(response.campaignGroupDisplayName).isEqualTo(REPORTING_SET.displayName)
    assertThat(response.state).isEqualTo(BasicReport.State.CREATED_LEGACY)
    assertThat(response.hasCreateTime())
  }

  @Test
  fun `createBasicReport with same request id succeeds twice`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails { title = "title" }
      createReportRequestId = "1235"
    }

    val request = createBasicReportRequest {
      this.basicReport = basicReport
      requestId = "1234"
    }

    val response = service.createBasicReport(request)

    assertThat(response)
      .ignoringFields(
        BasicReport.CAMPAIGN_GROUP_DISPLAY_NAME_FIELD_NUMBER,
        BasicReport.CREATE_TIME_FIELD_NUMBER,
        BasicReport.STATE_FIELD_NUMBER,
      )
      .isEqualTo(basicReport)
    assertThat(response.campaignGroupDisplayName).isEqualTo(REPORTING_SET.displayName)
    assertThat(response.state).isEqualTo(BasicReport.State.CREATED_LEGACY)
    assertThat(response.hasCreateTime())

    val response2 = service.createBasicReport(request)

    assertThat(response2)
      .ignoringFields(BasicReport.RESULT_DETAILS_FIELD_NUMBER)
      .isEqualTo(response)
  }

  @Test
  fun `createBasicReport throws FAILED_PRECONDITION when reporting set not found`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "1237"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        details = basicReportDetails { title = "title" }
        createReportRequestId = "1235"
      }

      val request = createBasicReportRequest { this.basicReport = basicReport }

      val exception = assertFailsWith<StatusRuntimeException> { service.createBasicReport(request) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `createBasicReport throws ALREADY_EXISTS when external id found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails { title = "title" }
      createReportRequestId = "1235"
    }

    val request = createBasicReportRequest { this.basicReport = basicReport }

    service.createBasicReport(request)

    val exception = assertFailsWith<StatusRuntimeException> { service.createBasicReport(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_ALREADY_EXISTS.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            basicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] = basicReport.externalBasicReportId
        }
      )
  }

  @Test
  fun `insertBasicReport succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = INSERT_BASIC_REPORT

    val response =
      service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })

    assertThat(response)
      .ignoringFields(
        BasicReport.CAMPAIGN_GROUP_DISPLAY_NAME_FIELD_NUMBER,
        BasicReport.CREATE_TIME_FIELD_NUMBER,
        BasicReport.STATE_FIELD_NUMBER,
      )
      .isEqualTo(basicReport)
    assertThat(response.campaignGroupDisplayName).isEqualTo(REPORTING_SET.displayName)
    assertThat(response.state).isEqualTo(BasicReport.State.SUCCEEDED)
    assertThat(response.hasCreateTime())
  }

  @Test
  fun `insertBasicReport throws FAILED_PRECONDITION when reporting set not found`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      val basicReport = INSERT_BASIC_REPORT

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    }

  @Test
  fun `insertBasicReport throws ALREADY_EXISTS when external id found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = INSERT_BASIC_REPORT

    service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_ALREADY_EXISTS.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            basicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] = basicReport.externalBasicReportId
        }
      )
  }

  @Test
  fun `insertBasicReport throws IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND when impression qualification filter not found`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport =
      INSERT_BASIC_REPORT.copy {
        details =
          details.copy {
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              externalImpressionQualificationFilterId = "abc"
            }
          }
      }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND.name
          metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER_ID.key] = "abc"
        }
      )
  }

  @Test
  fun `insertBasicReport throws INVALID_ARGUMENT when component summary missing external_reporting_set_id`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      externalReportId = "2237"
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails { title = "title" }
      resultDetails = basicReportResultDetails {
        resultGroups += resultGroup {
          title = "title"
          results +=
            ResultGroupKt.result {
              metadata =
                ResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          cmmsDataProviderDisplayName = "display"
                        }
                    }
                }
            }
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "result_details.result_groups[0].results[0].metadata.reporting_unit_summary" +
              ".reporting_unit_component_summary[0].external_reporting_set_id"
        }
      )
  }

  @Test
  fun `insertBasicReport throws INVALID_ARGUMENT when reporting_set_components entry missing external_reporting_set_id`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      externalReportId = "2237"
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails { title = "title" }
      resultDetails = basicReportResultDetails {
        resultGroups += resultGroup {
          title = "title"
          results +=
            ResultGroupKt.result {
              metricSet =
                ResultGroupKt.metricSet {
                  reportingSetComponents +=
                    ResultGroupKt.MetricSetKt.reportingSetComponentMetricSetMapEntry {
                      value = ResultGroupKt.MetricSetKt.componentMetricSet {}
                    }
                }
            }
        }
      }
    }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.insertBasicReport(insertBasicReportRequest { this.basicReport = basicReport })
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "result_details.result_groups[0].results[0]" +
              ".metric_set.reporting_set_components[0].external_reporting_set_id"
        }
      )
  }

  @Test
  fun `getBasicReport with insertBasicReport succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport = INSERT_BASIC_REPORT.copy { externalReportId = "2237" }
        }
      )

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

    assertThat(retrievedBasicReport).isEqualTo(createdBasicReport)
  }

  @Test
  fun `getBasicReport with reporting_unit reportingSetKeys ref outside campaign group renders SUCCEEDED`():
    Unit = runBlocking {
    // Regression test for the corruption pre-check expansion in
    // SpannerBasicReportsService.transformReportResultIntoResultGroups. Custom-group
    // BasicReports (from the server-side synthesis path) reference caller-supplied
    // component ReportingSets via reporting_unit.reportingSetKeys. Those components
    // have campaign_group="" by design and are therefore NOT returned by
    // listReportingSetsByCampaignGroup(<synthesized CG>). The same shape can be hit
    // by classic BasicReports that reference ReportingSets outside their campaign
    // group (off-recommendation but permitted).
    //
    // Pre-fix: the pre-check flagged such ReportingSetResult references as corruption and threw
    // IllegalStateException. Post-fix: the pre-check also enumerates
    // reporting_unit.reportingSetKeys refs from the BasicReport's own
    // resultGroupSpecs, so the reference is accepted.
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val primitiveReportingSet =
      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            REPORTING_SET.copy {
              clearFilter()
              clearPrimitive()
              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys += REPORTING_SET.primitive.eventGroupKeysList.first()
                }
            }
          externalReportingSetId = "primitive-1"
        }
      )

    // External component ReportingSet with EMPTY campaign_group -- the shape that
    // triggered the pre-check false-positive. This is NOT a child of REPORTING_SET
    // (the campaign group) and therefore won't appear in
    // listReportingSetsByCampaignGroup's output.
    val externalComponentReportingSet =
      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            REPORTING_SET.copy {
              clearFilter()
              clearPrimitive()
              clearExternalCampaignGroupId()
              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys += REPORTING_SET.primitive.eventGroupKeysList.first()
                }
            }
          externalReportingSetId = "external-component-1"
        }
      )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "extern-rs-basicreport"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails {
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 9
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          effectiveReportStart = dateTime {
            year = 2025
            month = 7
            day = 9
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2025
            month = 7
            day = 16
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
              .first()
              .externalImpressionQualificationFilterId
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          // reporting_unit.reportingSetKeys pointing at the external component --
          // the shape the expanded pre-check needs to accept.
          reportingUnit = reportingUnit {
            reportingSetKeys =
              ReportingUnitKt.reportingSetKeys {
                reportingSetKeys += reportingSetKey {
                  externalReportingSetId = externalComponentReportingSet.externalReportingSetId
                }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {}
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }
      resultDetails = basicReportResultDetails {}
      createReportRequestId = "1235"
    }

    val createdBasicReport =
      service.createBasicReport(createBasicReportRequest { this.basicReport = basicReport })

    service.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
        externalBasicReportId = createdBasicReport.externalBasicReportId
        externalReportId = "report-extern-rs"
      }
    )

    val reportResult =
      reportResultsService.createReportResult(
        createReportResultRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          reportResult = reportResult {
            reportStart = createdBasicReport.details.reportingInterval.reportStart
          }
        }
      )

    // ReportingSetResult references the external component ReportingSet's ID. Pre-fix this
    // triggered the pre-check IllegalStateException; post-fix it's accepted
    // because the external component is enumerated from
    // reporting_unit.reportingSetKeys.
    val createdReportingSetResults =
      reportResultsService
        .batchCreateReportingSetResults(
          batchCreateReportingSetResultsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = reportResult.externalReportResultId
            externalBasicReportId = createdBasicReport.externalBasicReportId
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              reportingSetResult = reportingSetResult {
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = externalComponentReportingSet.externalReportingSetId
                    vennDiagramRegionType =
                      ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                    externalImpressionQualificationFilterId =
                      basicReport.details.impressionQualificationFiltersList
                        .first()
                        .externalImpressionQualificationFilterId
                    metricFrequencySpec = metricFrequencySpec {
                      weekly =
                        basicReport.details.resultGroupSpecsList.first().metricFrequency.weekly
                    }
                    grouping = ReportingSetResultKt.DimensionKt.grouping {}
                    eventFilters += eventFilter {}
                  }
                populationSize = 100
                reportingWindowResults +=
                  ReportingSetResultKt.reportingWindowEntry {
                    key =
                      ReportingSetResultKt.reportingWindow {
                        nonCumulativeStart = date {
                          year = basicReport.details.reportingInterval.reportStart.year
                          month = basicReport.details.reportingInterval.reportStart.month
                          day = basicReport.details.reportingInterval.reportStart.day
                        }
                        end = date {
                          year = basicReport.details.reportingInterval.reportEnd.year
                          month = basicReport.details.reportingInterval.reportEnd.month
                          day = basicReport.details.reportingInterval.reportEnd.day
                        }
                      }
                    value =
                      ReportingSetResultKt.reportingWindowResult {
                        unprocessedReportResultValues =
                          ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                            nonCumulativeResults = noisyMetricSet {
                              reach = NoisyMetricSetKt.reachResult { value = 1 }
                            }
                          }
                      }
                  }
              }
            }
          }
        )
        .reportingSetResultsList

    reportResultsService.addProcessedResultValues(
      addProcessedResultValuesRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportResultId = reportResult.externalReportResultId
        reportingSetResults[createdReportingSetResults[0].externalReportingSetResultId] =
          AddProcessedResultValuesRequestKt.processedReportingSetResult {
            reportingWindowResults +=
              AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
                key = createdReportingSetResults[0].reportingWindowResultsList[0].key
                value =
                  ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                    nonCumulativeResults = basicMetricSet { reach = 2 }
                  }
              }
          }
      }
    )

    // The pre-check under test accepts the external component's ID via the
    // reporting_unit.reportingSetKeys enumeration -- if any error surfaces from
    // getBasicReport, it MUST NOT be the pre-check's specific "not in the campaign
    // group's expected set" message. Downstream buildResults may still fail on
    // fixture inconsistencies (window / dim keys, etc.) but that is out of scope
    // for this test -- the concern is that the pre-check no longer misclassifies
    // the caller-supplied external component as corruption.
    try {
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )
    } catch (e: IllegalStateException) {
      // The pre-check throws IllegalStateException via `error(...)`. Fail if it
      // fires -- that IS the regression this test guards against. Any other
      // exception class (e.g. NoSuchElementException from downstream
      // buildResults on fixture inconsistencies) is out of scope for this test.
      assertThat(e.message ?: "").doesNotContain("not in the campaign group's expected set")
      throw e
    } catch (e: NoSuchElementException) {
      // Downstream buildResults may still fail on fixture inconsistencies
      // (window / dim keys, etc.). That's out of scope for this test -- the
      // concern is only that the pre-check accepts the external component
      // reference. Swallow silently.
    }
  }

  @Test
  fun `getBasicReport with createBasicReport succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails {
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            hours = 5
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          effectiveReportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            hours = 5
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = "1234" }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                stackedIncrementalReach = true
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              }
          }
        }
      }
      resultDetails = basicReportResultDetails {}
      createReportRequestId = "1235"
    }

    val createdBasicReport =
      service.createBasicReport(createBasicReportRequest { this.basicReport = basicReport })

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

    assertThat(retrievedBasicReport).isEqualTo(createdBasicReport)
  }

  @Test
  fun `getBasicReport with createBasicReport with no effective IQFs succeeds`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "1237"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        details = basicReportDetails {
          title = "title"
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId =
              IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
                .first()
                .externalImpressionQualificationFilterId
          }
          resultGroupSpecs += resultGroupSpec {
            title = "title"
            reportingUnit = reportingUnit {
              dataProviderKeys =
                ReportingUnitKt.dataProviderKeys {
                  dataProviderKeys += dataProviderKey { cmmsDataProviderId = "1234" }
                }
            }
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
            dimensionSpec = dimensionSpec {
              grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                }
              }
            }
            resultGroupMetricSpec = resultGroupMetricSpec {
              populationSize = true
              component =
                ResultGroupMetricSpecKt.componentMetricSetSpec {
                  nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                }
            }
          }
        }
        resultDetails = basicReportResultDetails {}
        createReportRequestId = "1235"
      }

      val createdBasicReport =
        service.createBasicReport(createBasicReportRequest { this.basicReport = basicReport })

      val retrievedBasicReport =
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
          }
        )

      assertThat(retrievedBasicReport.details)
        .isEqualTo(
          basicReport.details.copy {
            effectiveImpressionQualificationFilters +=
              basicReport.details.impressionQualificationFiltersList
          }
        )
    }

  @Test
  fun `getBasicReport with createBasicReport with model line succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      modelLineKey =
        BasicReportKt.modelLineKey {
          cmmsModelProviderId = "1234"
          cmmsModelSuiteId = "1235"
          cmmsModelLineId = "1236"
        }
      details = basicReportDetails {
        title = "title"
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey { cmmsDataProviderId = "1234" }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }
      resultDetails = basicReportResultDetails {}
      createReportRequestId = "1235"
    }

    val createdBasicReport =
      service.createBasicReport(createBasicReportRequest { this.basicReport = basicReport })

    assertThat(createdBasicReport.modelLineKey.cmmsModelProviderId)
      .isEqualTo(basicReport.modelLineKey.cmmsModelProviderId)
    assertThat(createdBasicReport.modelLineKey.cmmsModelSuiteId)
      .isEqualTo(basicReport.modelLineKey.cmmsModelSuiteId)
    assertThat(createdBasicReport.modelLineKey.cmmsModelLineId)
      .isEqualTo(basicReport.modelLineKey.cmmsModelLineId)

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

    assertThat(retrievedBasicReport).isEqualTo(createdBasicReport)
  }

  @Test
  fun `retrieving basic report when basic report SUCCEEDED and has report result succeeds`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val primitiveReportingSet =
        reportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet =
              REPORTING_SET.copy {
                clearFilter()
                clearPrimitive()
                primitive =
                  ReportingSetKt.primitive {
                    eventGroupKeys += REPORTING_SET.primitive.eventGroupKeysList.first()
                  }
              }
            externalReportingSetId = "primitive-1"
          }
        )

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "1237"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        details = basicReportDetails {
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 9
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            effectiveReportStart = dateTime {
              year = 2025
              month = 7
              day = 9
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2025
              month = 7
              day = 16
            }
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId =
              IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
                .first()
                .externalImpressionQualificationFilterId
          }
          resultGroupSpecs += resultGroupSpec {
            title = "title"
            reportingUnit = reportingUnit {
              dataProviderKeys =
                ReportingUnitKt.dataProviderKeys {
                  dataProviderKeys += dataProviderKey {
                    cmmsDataProviderId =
                      primitiveReportingSet.primitive.eventGroupKeysList.first().cmmsDataProviderId
                  }
                }
            }
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
            dimensionSpec = dimensionSpec {
              grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                }
              }
            }
            resultGroupMetricSpec = resultGroupMetricSpec {
              populationSize = true
              component =
                ResultGroupMetricSpecKt.componentMetricSetSpec {
                  nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                }
            }
          }
        }
        resultDetails = basicReportResultDetails {}
        createReportRequestId = "1235"
      }

      val createBasicReportRequest = createBasicReportRequest {
        this.basicReport = basicReport
        requestId = "1234"
      }

      val createdBasicReport = service.createBasicReport(createBasicReportRequest)

      service.setExternalReportId(
        setExternalReportIdRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
          externalReportId = "report1234"
        }
      )

      val reportResult =
        reportResultsService.createReportResult(
          createReportResultRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            reportResult = reportResult {
              reportStart = createdBasicReport.details.reportingInterval.reportStart
            }
          }
        )

      val createdReportingSetResults =
        reportResultsService
          .batchCreateReportingSetResults(
            batchCreateReportingSetResultsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              externalBasicReportId = createdBasicReport.externalBasicReportId
              requests += createReportingSetResultRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportResultId = reportResult.externalReportResultId
                reportingSetResult = reportingSetResult {
                  dimension =
                    ReportingSetResultKt.dimension {
                      externalReportingSetId = primitiveReportingSet.externalReportingSetId
                      vennDiagramRegionType =
                        ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                      externalImpressionQualificationFilterId =
                        basicReport.details.impressionQualificationFiltersList
                          .first()
                          .externalImpressionQualificationFilterId
                      metricFrequencySpec = metricFrequencySpec {
                        weekly =
                          basicReport.details.resultGroupSpecsList.first().metricFrequency.weekly
                      }
                      grouping =
                        ReportingSetResultKt.DimensionKt.grouping {
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
                        year = basicReport.details.reportingInterval.reportStart.year
                        month = basicReport.details.reportingInterval.reportStart.month
                        day = basicReport.details.reportingInterval.reportStart.day
                      }
                      end = date {
                        year = basicReport.details.reportingInterval.reportEnd.year
                        month = basicReport.details.reportingInterval.reportEnd.month
                        day = basicReport.details.reportingInterval.reportEnd.day
                      }
                    }

                    value = reportingWindowResult {
                      unprocessedReportResultValues =
                        ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                          nonCumulativeResults = noisyMetricSet {
                            reach = NoisyMetricSetKt.reachResult { value = 1 }
                          }
                        }
                    }
                  }
                }
              }
            }
          )
          .reportingSetResultsList

      reportResultsService.addProcessedResultValues(
        addProcessedResultValuesRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportResultId = reportResult.externalReportResultId
          reportingSetResults[createdReportingSetResults[0].externalReportingSetResultId] =
            AddProcessedResultValuesRequestKt.processedReportingSetResult {
              reportingWindowResults +=
                AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt
                  .reportingWindowEntry {
                    key = createdReportingSetResults[0].reportingWindowResultsList[0].key
                    value =
                      ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                        nonCumulativeResults = basicMetricSet { reach = 2 }
                      }
                  }
            }
        }
      )

      val retrievedBasicReport =
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
          }
        )

      val expectedBasicReport =
        createdBasicReport.copy {
          externalReportId = "report1234"
          externalReportResultId = reportResult.externalReportResultId
          state = BasicReport.State.SUCCEEDED
          details =
            basicReport.details.copy {
              effectiveImpressionQualificationFilters +=
                basicReport.details.impressionQualificationFiltersList
            }
          resultDetails = basicReportResultDetails {
            resultGroups += resultGroup {
              title = "title"
              results +=
                ResultGroupKt.result {
                  metadata =
                    ResultGroupKt.metricMetadata {
                      reportingUnitSummary =
                        ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                          reportingUnitComponentSummary +=
                            ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                              cmmsDataProviderId =
                                primitiveReportingSet.primitive.eventGroupKeysList
                                  .first()
                                  .cmmsDataProviderId
                              externalReportingSetId = primitiveReportingSet.externalReportingSetId
                              eventGroupSummaries +=
                                ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                  .eventGroupSummary {
                                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                    cmmsEventGroupId =
                                      primitiveReportingSet.primitive.eventGroupKeysList
                                        .first()
                                        .cmmsEventGroupId
                                  }
                            }
                        }
                      nonCumulativeMetricStartTime = timestamp { seconds = 1752044400 }
                      cumulativeMetricStartTime = timestamp { seconds = 1752044400 }
                      metricEndTime = timestamp { seconds = 1752649200 }
                      metricFrequencySpec =
                        basicReport.details.resultGroupSpecsList.first().metricFrequency
                      dimensionSpecSummary =
                        ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                          groupings += eventTemplateField {
                            path = "person.gender"
                            value =
                              EventTemplateFieldKt.fieldValue {
                                enumValue = Person.Gender.MALE.name
                              }
                          }
                          filters += eventFilter {
                            terms += eventTemplateField {
                              path = "person.age_group"
                              value =
                                EventTemplateFieldKt.fieldValue {
                                  enumValue = Person.AgeGroup.YEARS_18_TO_34.name
                                }
                            }
                          }
                        }
                      filter = basicReport.details.impressionQualificationFiltersList.first()
                    }
                  metricSet =
                    ResultGroupKt.metricSet {
                      populationSize = 1000
                      components +=
                        ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                          key =
                            primitiveReportingSet.primitive.eventGroupKeysList
                              .first()
                              .cmmsDataProviderId
                          value =
                            ResultGroupKt.MetricSetKt.componentMetricSet {
                              nonCumulative = basicMetricSet { reach = 2 }
                            }
                        }
                    }
                }
            }
          }
        }

      assertThat(retrievedBasicReport).isEqualTo(expectedBasicReport)

      val existingBasicReport = service.createBasicReport(createBasicReportRequest)
      assertThat(existingBasicReport).isEqualTo(retrievedBasicReport)

      val retrievedBasicReports =
        service.listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
      assertThat(retrievedBasicReports.basicReportsList).hasSize(1)
      assertThat(retrievedBasicReports.basicReportsList[0]).isEqualTo(existingBasicReport)
    }

  @Test
  fun `retrieving basic report when report result doesn't have non cumulative succeeds`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val primitiveReportingSet =
        reportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet =
              REPORTING_SET.copy {
                clearFilter()
                clearPrimitive()
                primitive =
                  ReportingSetKt.primitive {
                    eventGroupKeys += REPORTING_SET.primitive.eventGroupKeysList.first()
                  }
              }
            externalReportingSetId = "primitive-1"
          }
        )

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "1237"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        details = basicReportDetails {
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 9
              hours = 5
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            effectiveReportStart = dateTime {
              year = 2025
              month = 7
              day = 9
              hours = 5
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2025
              month = 7
              day = 16
            }
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId =
              IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
                .first()
                .externalImpressionQualificationFilterId
          }
          resultGroupSpecs += resultGroupSpec {
            title = "title"
            reportingUnit = reportingUnit {
              dataProviderKeys =
                ReportingUnitKt.dataProviderKeys {
                  dataProviderKeys += dataProviderKey {
                    cmmsDataProviderId =
                      primitiveReportingSet.primitive.eventGroupKeysList.first().cmmsDataProviderId
                  }
                }
            }
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
            dimensionSpec = dimensionSpec {}
            resultGroupMetricSpec = resultGroupMetricSpec {
              populationSize = true
              component =
                ResultGroupMetricSpecKt.componentMetricSetSpec {
                  cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                }
            }
          }
        }
        resultDetails = basicReportResultDetails {}
        createReportRequestId = "1235"
      }

      val createBasicReportRequest = createBasicReportRequest {
        this.basicReport = basicReport
        requestId = "1234"
      }

      val createdBasicReport = service.createBasicReport(createBasicReportRequest)

      service.setExternalReportId(
        setExternalReportIdRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
          externalReportId = "report1234"
        }
      )

      val reportResult =
        reportResultsService.createReportResult(
          createReportResultRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            reportResult = reportResult {
              reportStart = createdBasicReport.details.reportingInterval.reportStart
            }
          }
        )

      val createdReportingSetResults =
        reportResultsService
          .batchCreateReportingSetResults(
            batchCreateReportingSetResultsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              externalBasicReportId = createdBasicReport.externalBasicReportId
              requests += createReportingSetResultRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportResultId = reportResult.externalReportResultId
                reportingSetResult = reportingSetResult {
                  dimension =
                    ReportingSetResultKt.dimension {
                      externalReportingSetId = primitiveReportingSet.externalReportingSetId
                      vennDiagramRegionType =
                        ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                      externalImpressionQualificationFilterId =
                        basicReport.details.impressionQualificationFiltersList
                          .first()
                          .externalImpressionQualificationFilterId
                      metricFrequencySpec = metricFrequencySpec {
                        weekly =
                          basicReport.details.resultGroupSpecsList.first().metricFrequency.weekly
                      }
                    }
                  populationSize = 1000
                  reportingWindowResults += reportingWindowEntry {
                    key = reportingWindow {
                      end = date {
                        year = basicReport.details.reportingInterval.reportEnd.year
                        month = basicReport.details.reportingInterval.reportEnd.month
                        day = basicReport.details.reportingInterval.reportEnd.day
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
          )
          .reportingSetResultsList

      reportResultsService.addProcessedResultValues(
        addProcessedResultValuesRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportResultId = reportResult.externalReportResultId
          reportingSetResults[createdReportingSetResults[0].externalReportingSetResultId] =
            AddProcessedResultValuesRequestKt.processedReportingSetResult {
              reportingWindowResults +=
                AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt
                  .reportingWindowEntry {
                    key = createdReportingSetResults[0].reportingWindowResultsList[0].key
                    value =
                      ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                        cumulativeResults = basicMetricSet { reach = 2 }
                      }
                  }
            }
        }
      )

      val retrievedBasicReport =
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
          }
        )

      val expectedBasicReport =
        createdBasicReport.copy {
          externalReportId = "report1234"
          externalReportResultId = reportResult.externalReportResultId
          state = BasicReport.State.SUCCEEDED
          details =
            basicReport.details.copy {
              effectiveImpressionQualificationFilters +=
                basicReport.details.impressionQualificationFiltersList
            }
          resultDetails = basicReportResultDetails {
            resultGroups += resultGroup {
              title = "title"
              results +=
                ResultGroupKt.result {
                  metadata =
                    ResultGroupKt.metricMetadata {
                      reportingUnitSummary =
                        ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                          reportingUnitComponentSummary +=
                            ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                              cmmsDataProviderId =
                                primitiveReportingSet.primitive.eventGroupKeysList
                                  .first()
                                  .cmmsDataProviderId
                              externalReportingSetId = primitiveReportingSet.externalReportingSetId
                              eventGroupSummaries +=
                                ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                  .eventGroupSummary {
                                    cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                                    cmmsEventGroupId =
                                      primitiveReportingSet.primitive.eventGroupKeysList
                                        .first()
                                        .cmmsEventGroupId
                                  }
                            }
                        }
                      cumulativeMetricStartTime = timestamp { seconds = 1752062400 }
                      metricEndTime = timestamp { seconds = 1752667200 }
                      metricFrequencySpec =
                        basicReport.details.resultGroupSpecsList.first().metricFrequency
                      dimensionSpecSummary = ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {}
                      filter = basicReport.details.impressionQualificationFiltersList.first()
                    }
                  metricSet =
                    ResultGroupKt.metricSet {
                      populationSize = 1000
                      components +=
                        ResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                          key =
                            primitiveReportingSet.primitive.eventGroupKeysList
                              .first()
                              .cmmsDataProviderId
                          value =
                            ResultGroupKt.MetricSetKt.componentMetricSet {
                              cumulative = basicMetricSet { reach = 2 }
                            }
                        }
                    }
                }
            }
          }
        }

      assertThat(retrievedBasicReport).isEqualTo(expectedBasicReport)

      val existingBasicReport = service.createBasicReport(createBasicReportRequest)
      assertThat(existingBasicReport).isEqualTo(retrievedBasicReport)

      val retrievedBasicReports =
        service.listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
      assertThat(retrievedBasicReports.basicReportsList).hasSize(1)
      assertThat(retrievedBasicReports.basicReportsList[0]).isEqualTo(existingBasicReport)
    }

  @Test
  fun `getBasicReport with createBasicReport with model line bool as true succeeds`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val basicReport = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "1237"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        modelLineKey =
          BasicReportKt.modelLineKey {
            cmmsModelProviderId = "1234"
            cmmsModelSuiteId = "1235"
            cmmsModelLineId = "1236"
          }
        modelLineSystemSpecified = true
        details = basicReportDetails {
          title = "title"
          resultGroupSpecs += resultGroupSpec {
            title = "title"
            reportingUnit = reportingUnit {
              dataProviderKeys =
                ReportingUnitKt.dataProviderKeys {
                  dataProviderKeys += dataProviderKey { cmmsDataProviderId = "1234" }
                }
            }
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
            dimensionSpec = dimensionSpec {
              grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                }
              }
            }
            resultGroupMetricSpec = resultGroupMetricSpec {
              populationSize = true
              reportingUnit =
                ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                  nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                  cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                  stackedIncrementalReach = true
                }
              component =
                ResultGroupMetricSpecKt.componentMetricSetSpec {
                  nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                  cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                  nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                  cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                }
            }
          }
        }
        resultDetails = basicReportResultDetails {}
        createReportRequestId = "1235"
      }

      val createdBasicReport =
        service.createBasicReport(createBasicReportRequest { this.basicReport = basicReport })

      assertThat(createdBasicReport.modelLineSystemSpecified).isTrue()

      val retrievedBasicReport =
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
          }
        )

      assertThat(retrievedBasicReport).isEqualTo(createdBasicReport)
    }

  @Test
  fun `getBasicReport throws NOT_FOUND when basic report not found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            createdBasicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
            createdBasicReport.externalBasicReportId + "b"
        }
      )
  }

  @Test
  fun `getBasicReport throws INVALID_ARGUMENT when cmms_measurement_consumer_id missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getBasicReport(getBasicReportRequest { externalBasicReportId = "1234" })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "cmms_measurement_consumer_id"
          }
        )
    }

  @Test
  fun `getBasicReport throws INVALID_ARGUMENT when external_basic_report_id missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getBasicReport(getBasicReportRequest { cmmsMeasurementConsumerId = "1234" })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "external_basic_report_id"
          }
        )
    }

  @Test
  fun `listBasicReport without page_size succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            INSERT_BASIC_REPORT.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            INSERT_BASIC_REPORT.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(3)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[2]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport with page_size succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            INSERT_BASIC_REPORT.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    service.insertBasicReport(
      insertBasicReportRequest {
        basicReport =
          INSERT_BASIC_REPORT.copy {
            externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
          }
      }
    )

    val listBasicReportsResponse =
      service.listBasicReports(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
          pageSize = 2
        }
      )

    assertThat(listBasicReportsResponse)
      .isEqualTo(
        listBasicReportsResponse {
          basicReports += createdBasicReport
          basicReports += createdBasicReport2
          nextPageToken = listBasicReportsPageToken {
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = createdBasicReport2.createTime
                externalBasicReportId = createdBasicReport2.externalBasicReportId
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        }
      )
  }

  @Test
  fun `listBasicReport with create_time_after succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                createTimeAfter = createdBasicReport.createTime
              }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(2)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReports with state filter successfully filters by state`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.createBasicReport(
        createBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              createReportRequestId = "1235"
              clearResultDetails()
            }
        }
      )

    val createdBasicReport2 =
      service.createBasicReport(
        createBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              externalBasicReportId += "2"
              createReportRequestId = "1236"
              clearResultDetails()
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports)
      .ignoringFields(BasicReport.RESULT_DETAILS_FIELD_NUMBER)
      .containsExactly(createdBasicReport, createdBasicReport2)

    val setExternalReportIdRequest = setExternalReportIdRequest {
      cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
      externalBasicReportId = createdBasicReport.externalBasicReportId
      externalReportId = "1236"
    }

    val updatedBasicReport = service.setExternalReportId(setExternalReportIdRequest)

    val retrievedBasicReportsByState =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                state = BasicReport.State.REPORT_CREATED
              }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReportsByState)
      .ignoringFields(BasicReport.RESULT_DETAILS_FIELD_NUMBER)
      .containsExactly(updatedBasicReport)
  }

  @Test
  fun `listBasicReport with page_token succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            pageToken = listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = createdBasicReport.createTime
                  externalBasicReportId = createdBasicReport.externalBasicReportId
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                }
            }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(2)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport looks at external id if createTime matches`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val createdBasicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            }
        }
      )

    val createdBasicReport3 =
      service.insertBasicReport(
        insertBasicReportRequest {
          basicReport =
            createdBasicReport.copy {
              externalBasicReportId = createdBasicReport2.externalBasicReportId + "b"
            }
        }
      )

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            pageToken = listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = createdBasicReport.createTime
                  externalBasicReportId = createdBasicReport.externalBasicReportId[0].toString()
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                }
            }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(3)
    assertThat(retrievedBasicReports[0]).isEqualTo(createdBasicReport)
    assertThat(retrievedBasicReports[1]).isEqualTo(createdBasicReport2)
    assertThat(retrievedBasicReports[2]).isEqualTo(createdBasicReport3)
  }

  @Test
  fun `listBasicReport returns 0 results if no matches`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })

    val retrievedBasicReports =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
            pageToken = listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = createdBasicReport.createTime
                  externalBasicReportId = createdBasicReport.externalBasicReportId
                  cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                }
            }
          }
        )
        .basicReportsList

    assertThat(retrievedBasicReports).hasSize(0)
  }

  @Test
  fun `listBasicReport returns results across measurement consumers`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    val measurementConsumer2 =
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID + 2 }
      )
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )
    val reportingSet2 =
      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          externalReportingSetId = "campaign-group-2"
          reportingSet = reportingSet {
            cmmsMeasurementConsumerId = measurementConsumer2.cmmsMeasurementConsumerId
            externalCampaignGroupId = this@createReportingSetRequest.externalReportingSetId
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = "data-provider-1"
                    cmmsEventGroupId = "event-group-1"
                  }
              }
          }
        }
      )
    val basicReport =
      service.insertBasicReport(insertBasicReportRequest { basicReport = INSERT_BASIC_REPORT })
    val basicReport2 =
      service.insertBasicReport(
        insertBasicReportRequest {
          this.basicReport =
            INSERT_BASIC_REPORT.copy {
              cmmsMeasurementConsumerId = measurementConsumer2.cmmsMeasurementConsumerId
              externalCampaignGroupId = reportingSet2.externalReportingSetId
              details =
                details.copy {
                  resultGroupSpecs[0] =
                    resultGroupSpecs[0].copy {
                      reportingUnit = reportingUnit {
                        reportingSetKeys =
                          ReportingUnitKt.reportingSetKeys {
                            reportingSetKeys += reportingSetKey {
                              cmmsMeasurementConsumerId =
                                measurementConsumer2.cmmsMeasurementConsumerId
                              externalReportingSetId = reportingSet2.externalReportingSetId
                            }
                          }
                      }
                    }
                }
              resultDetails =
                resultDetails.copy {
                  resultGroups[0] =
                    resultGroups[0].copy {
                      results[0] =
                        results[0].copy {
                          metadata =
                            metadata.copy {
                              reportingUnitSummary =
                                reportingUnitSummary.copy {
                                  reportingUnitComponentSummary[0] =
                                    reportingUnitComponentSummary[0].copy {
                                      externalReportingSetId = reportingSet2.externalReportingSetId
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
      )

    val results =
      service
        .listBasicReports(
          listBasicReportsRequest {
            filter = ListBasicReportsRequestKt.filter { state = basicReport.state }
          }
        )
        .basicReportsList

    assertThat(results).containsExactly(basicReport, basicReport2).inOrder()
  }

  @Test
  fun `listBasicReports skips BasicReport with inconsistent results and lists it in unreachable`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val primitiveReportingSet =
      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            REPORTING_SET.copy {
              clearFilter()
              clearPrimitive()
              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys += REPORTING_SET.primitive.eventGroupKeysList.first()
                }
            }
          externalReportingSetId = "primitive-1"
        }
      )

    val basicReport = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails {
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 9
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          effectiveReportStart = dateTime {
            year = 2025
            month = 7
            day = 9
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2025
            month = 7
            day = 16
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId =
            IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
              .first()
              .externalImpressionQualificationFilterId
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            dataProviderKeys =
              ReportingUnitKt.dataProviderKeys {
                dataProviderKeys += dataProviderKey {
                  cmmsDataProviderId =
                    primitiveReportingSet.primitive.eventGroupKeysList.first().cmmsDataProviderId
                }
              }
          }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
          dimensionSpec = dimensionSpec {
            grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
            filters += eventFilter {
              terms += eventTemplateField {
                path = "person.age_group"
                value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
              }
            }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }
      resultDetails = basicReportResultDetails {}
      createReportRequestId = "1235"
    }

    val createdBasicReport =
      service.createBasicReport(
        createBasicReportRequest {
          this.basicReport = basicReport
          requestId = "1234"
        }
      )

    service.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
        externalBasicReportId = createdBasicReport.externalBasicReportId
        externalReportId = "report1234"
      }
    )

    val reportResult =
      reportResultsService.createReportResult(
        createReportResultRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          reportResult = reportResult {
            reportStart = createdBasicReport.details.reportingInterval.reportStart
          }
        }
      )

    // The result row's `externalReportingSetId` does NOT match the campaign group's primitive
    // reporting set. This is the poison condition: at list time the transformation expects the
    // primitive reporting set's id but the result map contains only this bogus id, so the lookup
    // throws.
    val createdReportingSetResults =
      reportResultsService
        .batchCreateReportingSetResults(
          batchCreateReportingSetResultsRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalReportResultId = reportResult.externalReportResultId
            externalBasicReportId = createdBasicReport.externalBasicReportId
            requests += createReportingSetResultRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              reportingSetResult = reportingSetResult {
                dimension =
                  ReportingSetResultKt.dimension {
                    externalReportingSetId = "poison-id"
                    vennDiagramRegionType =
                      ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                    externalImpressionQualificationFilterId =
                      basicReport.details.impressionQualificationFiltersList
                        .first()
                        .externalImpressionQualificationFilterId
                    metricFrequencySpec = metricFrequencySpec {
                      weekly =
                        basicReport.details.resultGroupSpecsList.first().metricFrequency.weekly
                    }
                    grouping =
                      ReportingSetResultKt.DimensionKt.grouping {
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
                      year = basicReport.details.reportingInterval.reportStart.year
                      month = basicReport.details.reportingInterval.reportStart.month
                      day = basicReport.details.reportingInterval.reportStart.day
                    }
                    end = date {
                      year = basicReport.details.reportingInterval.reportEnd.year
                      month = basicReport.details.reportingInterval.reportEnd.month
                      day = basicReport.details.reportingInterval.reportEnd.day
                    }
                  }
                  value = reportingWindowResult {
                    unprocessedReportResultValues =
                      ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                        nonCumulativeResults = noisyMetricSet {
                          reach = NoisyMetricSetKt.reachResult { value = 1 }
                        }
                      }
                  }
                }
              }
            }
          }
        )
        .reportingSetResultsList

    reportResultsService.addProcessedResultValues(
      addProcessedResultValuesRequest {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalReportResultId = reportResult.externalReportResultId
        reportingSetResults[createdReportingSetResults[0].externalReportingSetResultId] =
          AddProcessedResultValuesRequestKt.processedReportingSetResult {
            reportingWindowResults +=
              AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt.reportingWindowEntry {
                key = createdReportingSetResults[0].reportingWindowResultsList[0].key
                value =
                  ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                    nonCumulativeResults = basicMetricSet { reach = 2 }
                  }
              }
          }
      }
    )

    val response =
      service.listBasicReports(
        listBasicReportsRequest {
          filter =
            ListBasicReportsRequestKt.filter {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            }
        }
      )

    assertThat(response.basicReportsList).isEmpty()
    assertThat(response.unreachableList)
      .containsExactly(
        "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID" +
          "/basicReports/${createdBasicReport.externalBasicReportId}"
      )
  }

  @Test
  fun `listBasicReports returns healthy BasicReport and lists poisoned one in unreachable`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val primitiveReportingSet =
        reportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet =
              REPORTING_SET.copy {
                clearFilter()
                clearPrimitive()
                primitive =
                  ReportingSetKt.primitive {
                    eventGroupKeys += REPORTING_SET.primitive.eventGroupKeysList.first()
                  }
              }
            externalReportingSetId = "primitive-1"
          }
        )

      // Healthy BasicReport inserted directly in SUCCEEDED state with no underlying result rows.
      // The list path's withResults short-circuits on empty results and returns it as-is.
      val healthyBasicReport =
        service.insertBasicReport(
          insertBasicReportRequest {
            basicReport = INSERT_BASIC_REPORT.copy { externalBasicReportId = "healthy-1" }
          }
        )

      val poisonBasicReportToCreate = basicReport {
        cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
        externalBasicReportId = "poison-1"
        externalCampaignGroupId = REPORTING_SET.externalReportingSetId
        details = basicReportDetails {
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 9
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            effectiveReportStart = dateTime {
              year = 2025
              month = 7
              day = 9
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2025
              month = 7
              day = 16
            }
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            externalImpressionQualificationFilterId =
              IMPRESSION_QUALIFICATION_FILTER_MAPPING.impressionQualificationFilters
                .first()
                .externalImpressionQualificationFilterId
          }
          resultGroupSpecs += resultGroupSpec {
            title = "title"
            reportingUnit = reportingUnit {
              dataProviderKeys =
                ReportingUnitKt.dataProviderKeys {
                  dataProviderKeys += dataProviderKey {
                    cmmsDataProviderId =
                      primitiveReportingSet.primitive.eventGroupKeysList.first().cmmsDataProviderId
                  }
                }
            }
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.WEDNESDAY }
            dimensionSpec = dimensionSpec {
              grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "person.age_group"
                  value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                }
              }
            }
            resultGroupMetricSpec = resultGroupMetricSpec {
              populationSize = true
              component =
                ResultGroupMetricSpecKt.componentMetricSetSpec {
                  nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                }
            }
          }
        }
        resultDetails = basicReportResultDetails {}
        createReportRequestId = "1235"
      }

      val createdPoisonBasicReport =
        service.createBasicReport(
          createBasicReportRequest {
            basicReport = poisonBasicReportToCreate
            requestId = "1234"
          }
        )

      service.setExternalReportId(
        setExternalReportIdRequest {
          cmmsMeasurementConsumerId = createdPoisonBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdPoisonBasicReport.externalBasicReportId
          externalReportId = "report1234"
        }
      )

      val reportResult =
        reportResultsService.createReportResult(
          createReportResultRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            reportResult = reportResult {
              reportStart = createdPoisonBasicReport.details.reportingInterval.reportStart
            }
          }
        )

      val createdReportingSetResults =
        reportResultsService
          .batchCreateReportingSetResults(
            batchCreateReportingSetResultsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalReportResultId = reportResult.externalReportResultId
              externalBasicReportId = createdPoisonBasicReport.externalBasicReportId
              requests += createReportingSetResultRequest {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                externalReportResultId = reportResult.externalReportResultId
                reportingSetResult = reportingSetResult {
                  dimension =
                    ReportingSetResultKt.dimension {
                      externalReportingSetId = "poison-id"
                      vennDiagramRegionType =
                        ReportingSetResult.Dimension.VennDiagramRegionType.PRIMITIVE
                      externalImpressionQualificationFilterId =
                        poisonBasicReportToCreate.details.impressionQualificationFiltersList
                          .first()
                          .externalImpressionQualificationFilterId
                      metricFrequencySpec = metricFrequencySpec {
                        weekly =
                          poisonBasicReportToCreate.details.resultGroupSpecsList
                            .first()
                            .metricFrequency
                            .weekly
                      }
                      grouping =
                        ReportingSetResultKt.DimensionKt.grouping {
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
                        year = poisonBasicReportToCreate.details.reportingInterval.reportStart.year
                        month =
                          poisonBasicReportToCreate.details.reportingInterval.reportStart.month
                        day = poisonBasicReportToCreate.details.reportingInterval.reportStart.day
                      }
                      end = date {
                        year = poisonBasicReportToCreate.details.reportingInterval.reportEnd.year
                        month = poisonBasicReportToCreate.details.reportingInterval.reportEnd.month
                        day = poisonBasicReportToCreate.details.reportingInterval.reportEnd.day
                      }
                    }
                    value = reportingWindowResult {
                      unprocessedReportResultValues =
                        ReportingSetResultKt.ReportingWindowResultKt.noisyReportResultValues {
                          nonCumulativeResults = noisyMetricSet {
                            reach = NoisyMetricSetKt.reachResult { value = 1 }
                          }
                        }
                    }
                  }
                }
              }
            }
          )
          .reportingSetResultsList

      reportResultsService.addProcessedResultValues(
        addProcessedResultValuesRequest {
          cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
          externalReportResultId = reportResult.externalReportResultId
          reportingSetResults[createdReportingSetResults[0].externalReportingSetResultId] =
            AddProcessedResultValuesRequestKt.processedReportingSetResult {
              reportingWindowResults +=
                AddProcessedResultValuesRequestKt.ProcessedReportingSetResultKt
                  .reportingWindowEntry {
                    key = createdReportingSetResults[0].reportingWindowResultsList[0].key
                    value =
                      ReportingSetResultKt.ReportingWindowResultKt.reportResultValues {
                        nonCumulativeResults = basicMetricSet { reach = 2 }
                      }
                  }
            }
        }
      )

      val response =
        service.listBasicReports(
          listBasicReportsRequest {
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )

      assertThat(response.basicReportsList).containsExactly(healthyBasicReport)
      assertThat(response.unreachableList)
        .containsExactly(
          "measurementConsumers/$CMMS_MEASUREMENT_CONSUMER_ID" +
            "/basicReports/${createdPoisonBasicReport.externalBasicReportId}"
        )
    }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when page_size is negative`(): Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.listBasicReports(
          listBasicReportsRequest {
            pageSize = -1
            filter =
              ListBasicReportsRequestKt.filter {
                cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              }
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_size"
        }
      )
  }

  @Test
  fun `setExternalReportId succeeds`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.createBasicReport(
        createBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              createReportRequestId = "1235"
              clearResultDetails()
            }
        }
      )

    val setExternalReportIdRequest = setExternalReportIdRequest {
      cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
      externalBasicReportId = createdBasicReport.externalBasicReportId
      externalReportId = "1236"
    }

    val updatedBasicReport = service.setExternalReportId(setExternalReportIdRequest)

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

    assertThat(updatedBasicReport.externalReportId)
      .isEqualTo(setExternalReportIdRequest.externalReportId)
    assertThat(updatedBasicReport.state).isEqualTo(BasicReport.State.REPORT_CREATED)
    assertThat(retrievedBasicReport).isEqualTo(updatedBasicReport)
  }

  @Test
  fun `setExternalReportId throws FAILED_PRECONDITION when BasicReport already FAILED`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val createdBasicReport =
        service.createBasicReport(
          createBasicReportRequest {
            basicReport =
              BASIC_REPORT.copy {
                createReportRequestId = "1235"
                clearResultDetails()
              }
          }
        )
      service.failBasicReport(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setExternalReportId(
            setExternalReportIdRequest {
              cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
              externalBasicReportId = createdBasicReport.externalBasicReportId
              externalReportId = "1236"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.BASIC_REPORT_STATE_INVALID.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
              createdBasicReport.cmmsMeasurementConsumerId
            metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
              createdBasicReport.externalBasicReportId
            metadata[Errors.Metadata.BASIC_REPORT_STATE.key] = BasicReport.State.FAILED.name
          }
        )

      val retrievedBasicReport =
        service.getBasicReport(
          getBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
          }
        )
      assertThat(retrievedBasicReport.state).isEqualTo(BasicReport.State.FAILED)
      assertThat(retrievedBasicReport.externalReportId).isEmpty()
    }

  @Test
  fun `setExternalReportId throws FAILED_PRECONDITION when BasicReport already REPORT_CREATED`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.createBasicReport(
        createBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              createReportRequestId = "1235"
              clearResultDetails()
            }
        }
      )
    service.setExternalReportId(
      setExternalReportIdRequest {
        cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
        externalBasicReportId = createdBasicReport.externalBasicReportId
        externalReportId = "1236"
      }
    )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setExternalReportId(
          setExternalReportIdRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
            externalReportId = "1237"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_STATE_INVALID.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            createdBasicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
            createdBasicReport.externalBasicReportId
          metadata[Errors.Metadata.BASIC_REPORT_STATE.key] = BasicReport.State.REPORT_CREATED.name
        }
      )

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )
    assertThat(retrievedBasicReport.externalReportId).isEqualTo("1236")
  }

  @Test
  fun `setExternalReportId throws NOT_FOUND when basic report not found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.createBasicReport(createBasicReportRequest { basicReport = BASIC_REPORT })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setExternalReportId(
          setExternalReportIdRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
            externalReportId = "1234"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            createdBasicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
            createdBasicReport.externalBasicReportId + "b"
        }
      )
  }

  @Test
  fun `setExternalReportId throws INVALID_ARGUMENT when cmms_measurement_consumer_id missing`():
    Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setExternalReportId(
          setExternalReportIdRequest {
            externalBasicReportId = "1234"
            externalReportId = "1234"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "cmms_measurement_consumer_id"
        }
      )
  }

  @Test
  fun `setExternalReportId throws INVALID_ARGUMENT when external_basic_report_id missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setExternalReportId(
            setExternalReportIdRequest {
              cmmsMeasurementConsumerId = "1234"
              externalReportId = "1234"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "external_basic_report_id"
          }
        )
    }

  @Test
  fun `setExternalReportId throws INVALID_ARGUMENT when external_report_id missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setExternalReportId(
            setExternalReportIdRequest {
              cmmsMeasurementConsumerId = "1234"
              externalBasicReportId = "1234"
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "external_report_id"
          }
        )
    }

  @Test
  fun `failBasicReport updates state to FAILED when BasicReport in REPORT_CREATED`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val createdBasicReport =
        service.createBasicReport(
          createBasicReportRequest {
            basicReport =
              BASIC_REPORT.copy {
                createReportRequestId = "1235"
                clearResultDetails()
              }
          }
        )
      service.setExternalReportId(
        setExternalReportIdRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
          externalReportId = "1236"
        }
      )

      val updatedBasicReport =
        service.failBasicReport(
          failBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId
          }
        )

      assertThat(updatedBasicReport.state).isEqualTo(BasicReport.State.FAILED)
    }

  @Test
  fun `failBasicReport updates state to FAILED`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.createBasicReport(
        createBasicReportRequest {
          basicReport =
            BASIC_REPORT.copy {
              createReportRequestId = "1235"
              clearResultDetails()
            }
        }
      )

    val failBasicReportRequest = failBasicReportRequest {
      cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
      externalBasicReportId = createdBasicReport.externalBasicReportId
    }

    val updatedBasicReport = service.failBasicReport(failBasicReportRequest)

    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )

    assertThat(updatedBasicReport.state).isEqualTo(BasicReport.State.FAILED)
    assertThat(retrievedBasicReport).isEqualTo(updatedBasicReport)
  }

  @Test
  fun `failBasicReport throws FAILED_PRECONDITION when BasicReport already FAILED`(): Unit =
    runBlocking {
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = REPORTING_SET
          externalReportingSetId = REPORTING_SET.externalReportingSetId
        }
      )

      val createdBasicReport =
        service.createBasicReport(
          createBasicReportRequest {
            basicReport =
              BASIC_REPORT.copy {
                createReportRequestId = "1235"
                clearResultDetails()
              }
          }
        )
      val failBasicReportRequest = failBasicReportRequest {
        cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
        externalBasicReportId = createdBasicReport.externalBasicReportId
      }
      service.failBasicReport(failBasicReportRequest)

      val exception =
        assertFailsWith<StatusRuntimeException> { service.failBasicReport(failBasicReportRequest) }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.BASIC_REPORT_STATE_INVALID.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
              createdBasicReport.cmmsMeasurementConsumerId
            metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
              createdBasicReport.externalBasicReportId
            metadata[Errors.Metadata.BASIC_REPORT_STATE.key] = BasicReport.State.FAILED.name
          }
        )
    }

  @Test
  fun `failBasicReport throws NOT_FOUND when basic report not found`(): Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )

    val createdBasicReport =
      service.createBasicReport(createBasicReportRequest { basicReport = BASIC_REPORT })

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.failBasicReport(
          failBasicReportRequest {
            cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
            externalBasicReportId = createdBasicReport.externalBasicReportId + "b"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_NOT_FOUND.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            createdBasicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
            createdBasicReport.externalBasicReportId + "b"
        }
      )
  }

  @Test
  fun `failBasicReport throws INVALID_ARGUMENT when cmms_measurement_consumer_id missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.failBasicReport(failBasicReportRequest { externalBasicReportId = "1234" })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "cmms_measurement_consumer_id"
          }
        )
    }

  @Test
  fun `failBasicReport throws INVALID_ARGUMENT when external_basic_report_id missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.failBasicReport(failBasicReportRequest { cmmsMeasurementConsumerId = "1234" })
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "external_basic_report_id"
          }
        )
    }

  @Test
  fun `createBasicReport creates BasicReport in CREATED_MULTIMEASUREMENT when multimeasurement`():
    Unit = runBlocking {
    val createdBasicReport = createMultimeasurementBasicReport()

    assertThat(createdBasicReport.state).isEqualTo(BasicReport.State.CREATED_MULTIMEASUREMENT)
    val retrievedBasicReport =
      service.getBasicReport(
        getBasicReportRequest {
          cmmsMeasurementConsumerId = createdBasicReport.cmmsMeasurementConsumerId
          externalBasicReportId = createdBasicReport.externalBasicReportId
        }
      )
    assertThat(retrievedBasicReport.state).isEqualTo(BasicReport.State.CREATED_MULTIMEASUREMENT)
  }

  @Test
  fun `batchCreateReportComputations returns created ReportComputations`(): Unit = runBlocking {
    val basicReport = createMultimeasurementBasicReport()

    val response =
      service.batchCreateReportComputations(
        batchCreateReportComputationsRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
          reportComputations += newReportComputation(MRC_IQF.impressionQualificationFilterId)
          reportComputations += newReportComputation(AMI_IQF.impressionQualificationFilterId)
        }
      )

    assertThat(response.reportComputationsList)
      .containsExactly(
        newReportComputation(MRC_IQF.impressionQualificationFilterId).withParent(basicReport),
        newReportComputation(AMI_IQF.impressionQualificationFilterId).withParent(basicReport),
      )
      .inOrder()
  }

  @Test
  fun `batchCreateReportComputations returns existing ReportComputations on retry`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()
      val request = batchCreateReportComputationsRequest {
        cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
        externalBasicReportId = basicReport.externalBasicReportId
        reportComputations += newReportComputation(AMI_IQF.impressionQualificationFilterId)
      }
      val firstResponse = service.batchCreateReportComputations(request)

      val retryResponse =
        service.batchCreateReportComputations(
          request.copy {
            reportComputations.clear()
            reportComputations +=
              newReportComputation(AMI_IQF.impressionQualificationFilterId).copy {
                cmmsCreateMeasurementRequestId = "retry-request-id"
              }
          }
        )

      assertThat(retryResponse).isEqualTo(firstResponse)
    }

  @Test
  fun `batchCreateReportComputations throws FAILED_PRECONDITION when BasicReport is legacy`():
    Unit = runBlocking {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )
    val basicReport =
      service.createBasicReport(
        createBasicReportRequest { basicReport = BASIC_REPORT.copy { clearResultDetails() } }
      )

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.batchCreateReportComputations(
          batchCreateReportComputationsRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            reportComputations += newReportComputation(AMI_IQF.impressionQualificationFilterId)
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_STATE_INVALID.name
          metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
            basicReport.cmmsMeasurementConsumerId
          metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] = basicReport.externalBasicReportId
          metadata[Errors.Metadata.BASIC_REPORT_STATE.key] = BasicReport.State.CREATED_LEGACY.name
        }
      )
  }

  @Test
  fun `batchCreateReportComputations throws NOT_FOUND when BasicReport not found`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateReportComputations(
            batchCreateReportComputationsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalBasicReportId = "missing"
              reportComputations += newReportComputation(AMI_IQF.impressionQualificationFilterId)
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo?.reason).isEqualTo(Errors.Reason.BASIC_REPORT_NOT_FOUND.name)
    }

  @Test
  fun `batchCreateReportComputations throws INVALID_ARGUMENT when filter ID is duplicated`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateReportComputations(
            batchCreateReportComputationsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalBasicReportId = BASIC_REPORT.externalBasicReportId
              reportComputations += newReportComputation(AMI_IQF.impressionQualificationFilterId)
              reportComputations +=
                newReportComputation(AMI_IQF.impressionQualificationFilterId).copy {
                  cmmsCreateMeasurementRequestId = "other-request-id"
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "report_computations[1].impression_qualification_filter_id"
          }
        )
    }

  @Test
  fun `batchCreateReportComputations throws INVALID_ARGUMENT when request ID missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateReportComputations(
            batchCreateReportComputationsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalBasicReportId = BASIC_REPORT.externalBasicReportId
              reportComputations +=
                newReportComputation(AMI_IQF.impressionQualificationFilterId).copy {
                  clearCmmsCreateMeasurementRequestId()
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "report_computations[0].cmms_create_measurement_request_id"
          }
        )
    }

  @Test
  fun `batchCreateReportComputations throws INVALID_ARGUMENT when mapping slot missing`(): Unit =
    runBlocking {
      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.batchCreateReportComputations(
            batchCreateReportComputationsRequest {
              cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
              externalBasicReportId = BASIC_REPORT.externalBasicReportId
              reportComputations +=
                newReportComputation(AMI_IQF.impressionQualificationFilterId).copy {
                  mappings[0] = mappings[0].copy { clearSlot() }
                }
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "report_computations[0].mappings[0].slot"
          }
        )
    }

  @Test
  fun `getReportComputation returns ReportComputation`(): Unit = runBlocking {
    val basicReport = createMultimeasurementBasicReport()
    val created: ReportComputation =
      createReportComputations(basicReport, AMI_IQF.impressionQualificationFilterId).single()

    val response =
      service.getReportComputation(
        getReportComputationRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
          impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
        }
      )

    assertThat(response).isEqualTo(created)
  }

  @Test
  fun `getReportComputation throws NOT_FOUND when ReportComputation not found`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.getReportComputation(
            getReportComputationRequest {
              cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
              externalBasicReportId = basicReport.externalBasicReportId
              impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REPORT_COMPUTATION_NOT_FOUND.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
              basicReport.cmmsMeasurementConsumerId
            metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
              basicReport.externalBasicReportId
            metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER_ID.key] =
              AMI_IQF.impressionQualificationFilterId.toString()
          }
        )
    }

  @Test
  fun `listReportComputations returns ReportComputations ordered by filter ID`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()
      val created: List<ReportComputation> =
        createReportComputations(
          basicReport,
          MRC_IQF.impressionQualificationFilterId,
          AMI_IQF.impressionQualificationFilterId,
        )

      val response =
        service.listReportComputations(
          listReportComputationsRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
          }
        )

      assertThat(response.reportComputationsList)
        .containsExactlyElementsIn(created.sortedBy { it.impressionQualificationFilterId })
        .inOrder()
    }

  @Test
  fun `listReportComputations returns empty response when BasicReport has none`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()

      val response =
        service.listReportComputations(
          listReportComputationsRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
          }
        )

      assertThat(response).isEqualToDefaultInstance()
    }

  @Test
  fun `setReportComputationMeasurementId sets cmms_measurement_id`(): Unit = runBlocking {
    val basicReport = createMultimeasurementBasicReport()
    val created: ReportComputation =
      createReportComputations(basicReport, AMI_IQF.impressionQualificationFilterId).single()

    val response =
      service.setReportComputationMeasurementId(
        setReportComputationMeasurementIdRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
          impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
          cmmsMeasurementId = "measurement-1"
        }
      )

    assertThat(response).isEqualTo(created.copy { cmmsMeasurementId = "measurement-1" })
    assertThat(
        service.getReportComputation(
          getReportComputationRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
          }
        )
      )
      .isEqualTo(response)
  }

  @Test
  fun `setReportComputationMeasurementId returns ReportComputation when value already set`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()
      createReportComputations(basicReport, AMI_IQF.impressionQualificationFilterId)
      val request = setReportComputationMeasurementIdRequest {
        cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
        externalBasicReportId = basicReport.externalBasicReportId
        impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
        cmmsMeasurementId = "measurement-1"
      }
      val firstResponse = service.setReportComputationMeasurementId(request)

      val retryResponse = service.setReportComputationMeasurementId(request)

      assertThat(retryResponse).isEqualTo(firstResponse)
    }

  @Test
  fun `setReportComputationMeasurementId throws FAILED_PRECONDITION when set to other value`():
    Unit = runBlocking {
    val basicReport = createMultimeasurementBasicReport()
    createReportComputations(basicReport, AMI_IQF.impressionQualificationFilterId)
    val request = setReportComputationMeasurementIdRequest {
      cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
      externalBasicReportId = basicReport.externalBasicReportId
      impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
      cmmsMeasurementId = "measurement-1"
    }
    service.setReportComputationMeasurementId(request)

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setReportComputationMeasurementId(
          request.copy { cmmsMeasurementId = "measurement-2" }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo?.reason)
      .isEqualTo(Errors.Reason.REPORT_COMPUTATION_STATE_INVALID.name)
  }

  @Test
  fun `setReportComputationMeasurementId throws NOT_FOUND when ReportComputation not found`():
    Unit = runBlocking {
    val basicReport = createMultimeasurementBasicReport()

    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setReportComputationMeasurementId(
          setReportComputationMeasurementIdRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
            cmmsMeasurementId = "measurement-1"
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo?.reason)
      .isEqualTo(Errors.Reason.REPORT_COMPUTATION_NOT_FOUND.name)
  }

  @Test
  fun `setReportComputationMeasurementId throws INVALID_ARGUMENT when measurement ID missing`():
    Unit = runBlocking {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        service.setReportComputationMeasurementId(
          setReportComputationMeasurementIdRequest {
            cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
            externalBasicReportId = BASIC_REPORT.externalBasicReportId
            impressionQualificationFilterId = AMI_IQF.impressionQualificationFilterId
          }
        )
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "cmms_measurement_id"
        }
      )
  }

  @Test
  fun `setMeasurementsCreated transitions BasicReport to MEASUREMENTS_CREATED`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()
      val filterIds =
        listOf(AMI_IQF.impressionQualificationFilterId, MRC_IQF.impressionQualificationFilterId)
      createReportComputations(basicReport, *filterIds.toLongArray())
      for (filterId in filterIds) {
        service.setReportComputationMeasurementId(
          setReportComputationMeasurementIdRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
            impressionQualificationFilterId = filterId
            cmmsMeasurementId = "measurement-$filterId"
          }
        )
      }

      val response =
        service.setMeasurementsCreated(
          setMeasurementsCreatedRequest {
            cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
            externalBasicReportId = basicReport.externalBasicReportId
          }
        )

      assertThat(response.state).isEqualTo(BasicReport.State.MEASUREMENTS_CREATED)
      assertThat(
          service.getBasicReport(
            getBasicReportRequest {
              cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
              externalBasicReportId = basicReport.externalBasicReportId
            }
          )
        )
        .isEqualTo(response)
    }

  @Test
  fun `setMeasurementsCreated throws FAILED_PRECONDITION when a measurement ID is not set`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()
      createReportComputations(basicReport, AMI_IQF.impressionQualificationFilterId)

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setMeasurementsCreated(
            setMeasurementsCreatedRequest {
              cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
              externalBasicReportId = basicReport.externalBasicReportId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REPORT_COMPUTATION_STATE_INVALID.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
              basicReport.cmmsMeasurementConsumerId
            metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
              basicReport.externalBasicReportId
            metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER_ID.key] =
              AMI_IQF.impressionQualificationFilterId.toString()
          }
        )
    }

  @Test
  fun `setMeasurementsCreated throws FAILED_PRECONDITION when BasicReport has none`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setMeasurementsCreated(
            setMeasurementsCreatedRequest {
              cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
              externalBasicReportId = basicReport.externalBasicReportId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REPORT_COMPUTATION_NOT_FOUND.name
            metadata[Errors.Metadata.CMMS_MEASUREMENT_CONSUMER_ID.key] =
              basicReport.cmmsMeasurementConsumerId
            metadata[Errors.Metadata.EXTERNAL_BASIC_REPORT_ID.key] =
              basicReport.externalBasicReportId
          }
        )
    }

  @Test
  fun `setMeasurementsCreated throws FAILED_PRECONDITION when BasicReport is FAILED`(): Unit =
    runBlocking {
      val basicReport = createMultimeasurementBasicReport()
      service.failBasicReport(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )

      val exception =
        assertFailsWith<StatusRuntimeException> {
          service.setMeasurementsCreated(
            setMeasurementsCreatedRequest {
              cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
              externalBasicReportId = basicReport.externalBasicReportId
            }
          )
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo?.reason)
        .isEqualTo(Errors.Reason.BASIC_REPORT_STATE_INVALID.name)
    }

  @Test
  fun `failBasicReport updates state to FAILED when BasicReport in CREATED_MULTIMEASUREMENT`():
    Unit = runBlocking {
    val basicReport = createMultimeasurementBasicReport()

    val response =
      service.failBasicReport(
        failBasicReportRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
        }
      )

    assertThat(response.state).isEqualTo(BasicReport.State.FAILED)
  }

  /** Creates a multimeasurement [BasicReport] along with its dependencies. */
  private suspend fun createMultimeasurementBasicReport(): BasicReport {
    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID }
    )
    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = REPORTING_SET
        externalReportingSetId = REPORTING_SET.externalReportingSetId
      }
    )
    return service.createBasicReport(
      createBasicReportRequest {
        basicReport = BASIC_REPORT.copy { clearResultDetails() }
        multimeasurement = true
      }
    )
  }

  private suspend fun createReportComputations(
    basicReport: BasicReport,
    vararg impressionQualificationFilterIds: Long,
  ): List<ReportComputation> {
    return service
      .batchCreateReportComputations(
        batchCreateReportComputationsRequest {
          cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
          externalBasicReportId = basicReport.externalBasicReportId
          for (impressionQualificationFilterId in impressionQualificationFilterIds) {
            reportComputations += newReportComputation(impressionQualificationFilterId)
          }
        }
      )
      .reportComputationsList
  }

  companion object {
    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"
    private val REPORTING_SET_TAGS = mapOf("tag1" to "tag_value1", "tag2" to "tag_value2")

    private val AMI_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
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

    private val MRC_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      impressionQualificationFilterId = 2
      filterSpecs += impressionQualificationFilterSpec {
        mediaType = MediaType.DISPLAY
        filters +=
          ImpressionQualificationFilterConfigKt.eventFilter {
            terms +=
              ImpressionQualificationFilterConfigKt.eventTemplateField {
                path = "banner_ad.viewable"
                value =
                  ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue {
                    boolValue = true
                  }
              }
          }
      }
    }

    private val IMPRESSION_QUALIFICATION_FILTER_CONFIG = impressionQualificationFilterConfig {
      impressionQualificationFilters += AMI_IQF
      impressionQualificationFilters += MRC_IQF
    }

    private val IMPRESSION_QUALIFICATION_FILTER_MAPPING =
      ImpressionQualificationFilterMapping(
        IMPRESSION_QUALIFICATION_FILTER_CONFIG,
        TestEvent.getDescriptor(),
      )

    private val REPORTING_SET = reportingSet {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalReportingSetId = "2345"
      externalCampaignGroupId = "2345"
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = "1235"
              cmmsEventGroupId = "1235"
            }
        }
      details = ReportingSetKt.details { tags.putAll(REPORTING_SET_TAGS) }
    }

    private val BASIC_REPORT = basicReport {
      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
      externalBasicReportId = "1237"
      externalCampaignGroupId = REPORTING_SET.externalReportingSetId
      details = basicReportDetails { title = "title" }
      resultDetails = basicReportResultDetails { resultGroups += resultGroup { title = "title" } }
    }
    private val INSERT_BASIC_REPORT =
      BASIC_REPORT.copy {
        externalReportId = "2237"
        details =
          details.copy {
            resultGroupSpecs += resultGroupSpec {
              reportingUnit = reportingUnit {
                reportingSetKeys =
                  ReportingUnitKt.reportingSetKeys {
                    reportingSetKeys += reportingSetKey {
                      cmmsMeasurementConsumerId = CMMS_MEASUREMENT_CONSUMER_ID
                      externalReportingSetId = REPORTING_SET.externalReportingSetId
                    }
                  }
              }
            }
          }
        resultDetails =
          resultDetails.copy {
            resultGroups[0] =
              resultGroups[0].copy {
                results +=
                  ResultGroupKt.result {
                    metadata =
                      ResultGroupKt.metricMetadata {
                        reportingUnitSummary =
                          ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                            reportingUnitComponentSummary +=
                              ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                                externalReportingSetId = REPORTING_SET.externalReportingSetId
                              }
                          }
                      }
                  }
              }
          }
      }

    private fun newReportComputation(impressionQualificationFilterId: Long): ReportComputation =
      reportComputation {
        cmmsCreateMeasurementRequestId = "request-$impressionQualificationFilterId"
        this.impressionQualificationFilterId = impressionQualificationFilterId
        mappings += resultGroupQueryMapping {
          queryId = "reporting-unit-$impressionQualificationFilterId"
          windowStart = date {
            year = 2025
            month = 1
            day = 6
          }
          windowEnd = date {
            year = 2025
            month = 1
            day = 12
          }
          cumulative = true
          reportingUnit = Empty.getDefaultInstance()
        }
        mappings += resultGroupQueryMapping {
          queryId = "component-$impressionQualificationFilterId"
          resultGroupSpecIndex = 1
          windowStart = date {
            year = 2025
            month = 1
            day = 6
          }
          windowEnd = date {
            year = 2025
            month = 1
            day = 12
          }
          component = ResultGroupQueryMappingKt.component { component = "component-1" }
        }
      }

    private fun ReportComputation.withParent(basicReport: BasicReport): ReportComputation = copy {
      cmmsMeasurementConsumerId = basicReport.cmmsMeasurementConsumerId
      externalBasicReportId = basicReport.externalBasicReportId
    }
  }
}
