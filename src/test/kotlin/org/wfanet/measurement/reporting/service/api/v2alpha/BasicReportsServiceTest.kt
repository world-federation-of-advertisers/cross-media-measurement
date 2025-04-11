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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.rpc.errorInfo
import com.google.type.DayOfWeek
import com.google.type.date
import com.google.type.dateTime
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.mockito.kotlin.doAnswer
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec as InternalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec as internalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter as internalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval as internalReportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.SpannerBasicReportsService
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequestKt
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.resultGroup

class BasicReportsServiceTest {
  val spannerDatabase =
    SpannerEmulatorDatabaseRule(spannerEmulator, Schemata.REPORTING_CHANGELOG_PATH)

  private val permissionsServiceMock: PermissionsGrpcKt.PermissionsCoroutineImplBase = mockService {
    // Grant all permissions for PRINCIPAL.
    onBlocking { checkPermissions(hasPrincipal(PRINCIPAL.name)) } doAnswer
      { invocation ->
        val request: CheckPermissionsRequest = invocation.getArgument(0)
        checkPermissionsResponse { permissions += request.permissionsList }
      }
  }

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val postgresDatabaseClient = postgresDatabaseProvider.createDatabase()
    val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

    addService(permissionsServiceMock)
    addService(
      SpannerBasicReportsService(
        spannerDatabaseClient,
        postgresDatabaseClient,
        IMPRESSION_QUALIFICATION_FILTER_MAPPING,
      )
    )
    addService(PostgresMeasurementConsumersService(idGenerator, postgresDatabaseClient))
    addService(PostgresReportingSetsService(idGenerator, postgresDatabaseClient))
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  private lateinit var authorization: Authorization
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineStub
  private lateinit var reportingSetsService: ReportingSetsCoroutineStub
  private lateinit var internalBasicReportsService: InternalBasicReportsCoroutineStub

  private lateinit var service: BasicReportsService

  @Before
  fun initService() {
    measurementConsumersService = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
    reportingSetsService = ReportingSetsCoroutineStub(grpcTestServerRule.channel)
    internalBasicReportsService = InternalBasicReportsCoroutineStub(grpcTestServerRule.channel)
    authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))

    service = BasicReportsService(internalBasicReportsService, authorization)
  }

  @Test
  fun `getBasicReport returns basic report when found`() = runBlocking {
    val cmmsMeasurementConsumerId = "1234"
    val cmmsDataProviderId = "1235"
    val reportingSetId = "4322"
    val basicReportId = "4321"
    val impressionQualificationFilterId = "ami"

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
        externalReportingSetId = reportingSetId
      }
    )

    var internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
        impressionQualificationFilters += internalReportingImpressionQualificationFilter {
          externalImpressionQualificationFilterId = impressionQualificationFilterId
        }
        impressionQualificationFilters += internalReportingImpressionQualificationFilter {
          filterSpecs += internalImpressionQualificationFilterSpec {
            mediaType = InternalImpressionQualificationFilterSpec.MediaType.VIDEO

            filters += internalEventFilter {
              terms += internalEventTemplateField {
                path = "common.age_group"
                value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
              }
              terms += internalEventTemplateField {
                path = "common.age_group"
                value = InternalEventTemplateFieldKt.fieldValue { enumValue = "35_TO_54" }
              }
            }

            filters += internalEventFilter {
              terms += internalEventTemplateField {
                path = "common.gender"
                value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
              }
            }
          }

          filterSpecs += internalImpressionQualificationFilterSpec {
            mediaType = InternalImpressionQualificationFilterSpec.MediaType.DISPLAY

            filters += internalEventFilter {
              terms += internalEventTemplateField {
                path = "common.age_group"
                value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
              }
            }
          }

          filterSpecs += internalImpressionQualificationFilterSpec {
            mediaType = InternalImpressionQualificationFilterSpec.MediaType.OTHER

            filters += internalEventFilter {
              terms += internalEventTemplateField {
                path = "common.age_group"
                value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
              }
            }
          }
        }
      }

      resultDetails = basicReportResultDetails {
        resultGroups += internalResultGroup {
          title = "title"
          results +=
            InternalResultGroupKt.result {
              metadata =
                InternalResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          this.cmmsDataProviderId = cmmsDataProviderId
                          cmmsDataProviderDisplayName = "display"
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                                cmmsEventGroupId = "12345"
                              }
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                                cmmsEventGroupId = "123456"
                              }
                        }
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          this.cmmsDataProviderId = cmmsDataProviderId
                          cmmsDataProviderDisplayName = "display"
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                                cmmsEventGroupId = "12345"
                              }
                        }
                    }
                  nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                  cumulativeMetricStartTime = timestamp { seconds = 12 }
                  metricEndTime = timestamp { seconds = 20 }
                  metricFrequencySpec = internalMetricFrequencySpec { weekly = DayOfWeek.MONDAY }
                  dimensionSpecSummary =
                    InternalResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      grouping = internalEventTemplateField {
                        path = "common.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  filter = internalReportingImpressionQualificationFilter {
                    externalImpressionQualificationFilterId = impressionQualificationFilterId
                  }
                }

              metricSet =
                InternalResultGroupKt.metricSet {
                  populationSize = 1000
                  reportingUnit =
                    InternalResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 1
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 2
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      stackedIncrementalReach += 10
                      stackedIncrementalReach += 15
                    }
                  components +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = cmmsDataProviderId
                      value =
                        InternalResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 1
                              percentReach = 0.1f
                              kPlusReach += 1
                              kPlusReach += 2
                              percentKPlusReach += 0.1f
                              percentKPlusReach += 0.2f
                              averageFrequency = 0.1f
                              impressions = 1
                              grps = 0.1f
                            }
                          cumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 2
                              percentReach = 0.2f
                              kPlusReach += 2
                              kPlusReach += 4
                              percentKPlusReach += 0.2f
                              percentKPlusReach += 0.4f
                              averageFrequency = 0.2f
                              impressions = 2
                              grps = 0.2f
                            }
                          uniqueReach = 5
                        }
                    }
                  components +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = cmmsDataProviderId + "b"
                      value =
                        InternalResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 10
                              percentReach = 0.1f
                              kPlusReach += 1
                              kPlusReach += 2
                              percentKPlusReach += 0.1f
                              percentKPlusReach += 0.2f
                              averageFrequency = 0.1f
                              impressions = 1
                              grps = 0.1f
                            }
                          cumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 5
                              percentReach = 0.2f
                              kPlusReach += 2
                              kPlusReach += 4
                              percentKPlusReach += 0.2f
                              percentKPlusReach += 0.4f
                              averageFrequency = 0.2f
                              impressions = 2
                              grps = 0.2f
                            }
                          uniqueReach = 10
                        }
                    }
                  componentIntersections +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentIntersectionMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 15
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 20
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      cmmsDataProviderIds += cmmsDataProviderId
                      cmmsDataProviderIds += cmmsDataProviderId + "1"
                    }
                  componentIntersections +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentIntersectionMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 25
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 30
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      cmmsDataProviderIds += cmmsDataProviderId
                    }
                }
            }

          results +=
            InternalResultGroupKt.result {
              metadata =
                InternalResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          this.cmmsDataProviderId = cmmsDataProviderId
                          cmmsDataProviderDisplayName = "display"
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                                cmmsEventGroupId = "12345"
                              }
                        }
                    }
                  nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                  cumulativeMetricStartTime = timestamp { seconds = 12 }
                  metricEndTime = timestamp { seconds = 20 }
                  metricFrequencySpec = internalMetricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    InternalResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      grouping = internalEventTemplateField {
                        path = "common.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  filter = internalReportingImpressionQualificationFilter {
                    filterSpecs += internalImpressionQualificationFilterSpec {
                      mediaType = InternalImpressionQualificationFilterSpec.MediaType.VIDEO

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  }
                }

              metricSet =
                InternalResultGroupKt.metricSet {
                  populationSize = 1000
                  reportingUnit =
                    InternalResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 1
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 2
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      stackedIncrementalReach += 10
                      stackedIncrementalReach += 15
                    }
                  components +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = cmmsDataProviderId
                      value =
                        InternalResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 1
                              percentReach = 0.1f
                              kPlusReach += 1
                              kPlusReach += 2
                              percentKPlusReach += 0.1f
                              percentKPlusReach += 0.2f
                              averageFrequency = 0.1f
                              impressions = 1
                              grps = 0.1f
                            }
                          cumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 2
                              percentReach = 0.2f
                              kPlusReach += 2
                              kPlusReach += 4
                              percentKPlusReach += 0.2f
                              percentKPlusReach += 0.4f
                              averageFrequency = 0.2f
                              impressions = 2
                              grps = 0.2f
                            }
                          uniqueReach = 5
                        }
                    }
                  componentIntersections +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentIntersectionMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 15
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 20
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      cmmsDataProviderIds += cmmsDataProviderId
                      cmmsDataProviderIds += cmmsDataProviderId + "1"
                    }
                }
            }
        }

        resultGroups += internalResultGroup {
          title = "title2"
          results +=
            InternalResultGroupKt.result {
              metadata =
                InternalResultGroupKt.metricMetadata {
                  reportingUnitSummary =
                    InternalResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                      reportingUnitComponentSummary +=
                        InternalResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                          this.cmmsDataProviderId = cmmsDataProviderId
                          cmmsDataProviderDisplayName = "display"
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                                cmmsEventGroupId = "12345"
                              }
                        }
                    }
                  nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                  cumulativeMetricStartTime = timestamp { seconds = 12 }
                  metricEndTime = timestamp { seconds = 20 }
                  metricFrequencySpec = internalMetricFrequencySpec { total = true }
                  dimensionSpecSummary =
                    InternalResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                      grouping = internalEventTemplateField {
                        path = "common.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  filter = internalReportingImpressionQualificationFilter {
                    filterSpecs += internalImpressionQualificationFilterSpec {
                      mediaType = InternalImpressionQualificationFilterSpec.MediaType.VIDEO

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "common.age_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                        }
                      }
                    }
                  }
                }

              metricSet =
                InternalResultGroupKt.metricSet {
                  populationSize = 1000
                  reportingUnit =
                    InternalResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 1
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 2
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      stackedIncrementalReach += 10
                      stackedIncrementalReach += 15
                    }
                  components +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentMetricSetMapEntry {
                      key = cmmsDataProviderId
                      value =
                        InternalResultGroupKt.MetricSetKt.componentMetricSet {
                          nonCumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 1
                              percentReach = 0.1f
                              kPlusReach += 1
                              kPlusReach += 2
                              percentKPlusReach += 0.1f
                              percentKPlusReach += 0.2f
                              averageFrequency = 0.1f
                              impressions = 1
                              grps = 0.1f
                            }
                          cumulative =
                            InternalResultGroupKt.MetricSetKt.basicMetricSet {
                              reach = 2
                              percentReach = 0.2f
                              kPlusReach += 2
                              kPlusReach += 4
                              percentKPlusReach += 0.2f
                              percentKPlusReach += 0.4f
                              averageFrequency = 0.2f
                              impressions = 2
                              grps = 0.2f
                            }
                          uniqueReach = 5
                        }
                    }
                  componentIntersections +=
                    InternalResultGroupKt.MetricSetKt.dataProviderComponentIntersectionMetricSet {
                      nonCumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 15
                          percentReach = 0.1f
                          kPlusReach += 1
                          kPlusReach += 2
                          percentKPlusReach += 0.1f
                          percentKPlusReach += 0.2f
                          averageFrequency = 0.1f
                          impressions = 1
                          grps = 0.1f
                        }
                      cumulative =
                        InternalResultGroupKt.MetricSetKt.basicMetricSet {
                          reach = 20
                          percentReach = 0.2f
                          kPlusReach += 2
                          kPlusReach += 4
                          percentKPlusReach += 0.2f
                          percentKPlusReach += 0.4f
                          averageFrequency = 0.2f
                          impressions = 2
                          grps = 0.2f
                        }
                      cmmsDataProviderIds += cmmsDataProviderId
                      cmmsDataProviderIds += cmmsDataProviderId + "1"
                    }
                }
            }
        }
      }
    }

    internalBasicReport =
      internalBasicReportsService.insertBasicReport(
        insertBasicReportRequest { basicReport = internalBasicReport }
      )

    val basicReportName =
      BasicReportKey(
          cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
          basicReportId = basicReportId,
        )
        .toName()
    val request = getBasicReportRequest { name = basicReportName }
    val getBasicReportResponse: BasicReport =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.getBasicReport(request) }

    assertThat(getBasicReportResponse)
      .isEqualTo(
        basicReport {
          name = basicReportName
          title = internalBasicReport.details.title
          campaignGroup =
            ReportingSetKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                reportingSetId = reportingSetId,
              )
              .toName()
          campaignGroupDisplayName = REPORTING_SET.displayName
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }

          impressionQualificationFilters += reportingImpressionQualificationFilter {
            impressionQualificationFilter =
              ImpressionQualificationFilterKey(impressionQualificationFilterId).toName()
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.VIDEO

                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "common.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                    }
                    terms += eventTemplateField {
                      path = "common.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "35_TO_54" }
                    }
                  }

                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "common.gender"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                    }
                  }
                }

                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY

                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "common.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                    }
                  }
                }

                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.OTHER

                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "common.age_group"
                      value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                    }
                  }
                }
              }
          }

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
                            component = DataProviderKey(cmmsDataProviderId).toName()
                            displayName = "display"
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary {
                                  eventGroup =
                                    EventGroupKey(
                                        cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                                        cmmsEventGroupId = "12345",
                                      )
                                      .toName()
                                }
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary {
                                  eventGroup =
                                    EventGroupKey(
                                        cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                                        cmmsEventGroupId = "123456",
                                      )
                                      .toName()
                                }
                          }
                        reportingUnitComponentSummary +=
                          ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                            component = DataProviderKey(cmmsDataProviderId).toName()
                            displayName = "display"
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary {
                                  eventGroup =
                                    EventGroupKey(
                                        cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                                        cmmsEventGroupId = "12345",
                                      )
                                      .toName()
                                }
                          }
                      }
                    nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                    cumulativeMetricStartTime = timestamp { seconds = 12 }
                    metricEndTime = timestamp { seconds = 20 }
                    metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                    dimensionSpecSummary =
                      ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                        grouping = eventTemplateField {
                          path = "common.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "common.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                          }
                          terms += eventTemplateField {
                            path = "common.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                          }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "common.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                          }
                        }
                      }
                    filter = reportingImpressionQualificationFilter {
                      impressionQualificationFilter =
                        ImpressionQualificationFilterKey(impressionQualificationFilterId).toName()
                    }
                  }

                metricSet =
                  ResultGroupKt.metricSet {
                    populationSize = 1000
                    reportingUnit =
                      ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 1
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 2
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        stackedIncrementalReach += 10
                        stackedIncrementalReach += 15
                      }
                    components +=
                      ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
                        key = DataProviderKey(cmmsDataProviderId).toName()
                        value =
                          ResultGroupKt.MetricSetKt.componentMetricSet {
                            nonCumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 1
                                percentReach = 0.1f
                                kPlusReach += 1
                                kPlusReach += 2
                                percentKPlusReach += 0.1f
                                percentKPlusReach += 0.2f
                                averageFrequency = 0.1f
                                impressions = 1
                                grps = 0.1f
                              }
                            cumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 2
                                percentReach = 0.2f
                                kPlusReach += 2
                                kPlusReach += 4
                                percentKPlusReach += 0.2f
                                percentKPlusReach += 0.4f
                                averageFrequency = 0.2f
                                impressions = 2
                                grps = 0.2f
                              }
                            uniqueReach = 5
                          }
                      }
                    components +=
                      ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
                        key = DataProviderKey(cmmsDataProviderId + "b").toName()
                        value =
                          ResultGroupKt.MetricSetKt.componentMetricSet {
                            nonCumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 10
                                percentReach = 0.1f
                                kPlusReach += 1
                                kPlusReach += 2
                                percentKPlusReach += 0.1f
                                percentKPlusReach += 0.2f
                                averageFrequency = 0.1f
                                impressions = 1
                                grps = 0.1f
                              }
                            cumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 5
                                percentReach = 0.2f
                                kPlusReach += 2
                                kPlusReach += 4
                                percentKPlusReach += 0.2f
                                percentKPlusReach += 0.4f
                                averageFrequency = 0.2f
                                impressions = 2
                                grps = 0.2f
                              }
                            uniqueReach = 10
                          }
                      }
                    componentIntersections +=
                      ResultGroupKt.MetricSetKt.componentIntersectionMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 15
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 20
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        components += DataProviderKey(cmmsDataProviderId).toName()
                        components += DataProviderKey(cmmsDataProviderId + "1").toName()
                      }
                    componentIntersections +=
                      ResultGroupKt.MetricSetKt.componentIntersectionMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 25
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 30
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        components += DataProviderKey(cmmsDataProviderId).toName()
                      }
                  }
              }

            results +=
              ResultGroupKt.result {
                metadata =
                  ResultGroupKt.metricMetadata {
                    reportingUnitSummary =
                      ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                        reportingUnitComponentSummary +=
                          ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                            component = DataProviderKey(cmmsDataProviderId).toName()
                            displayName = "display"
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary {
                                  eventGroup =
                                    EventGroupKey(
                                        cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                                        cmmsEventGroupId = "12345",
                                      )
                                      .toName()
                                }
                          }
                      }
                    nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                    cumulativeMetricStartTime = timestamp { seconds = 12 }
                    metricEndTime = timestamp { seconds = 20 }
                    metricFrequency = metricFrequencySpec { total = true }
                    dimensionSpecSummary =
                      ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                        grouping = eventTemplateField {
                          path = "common.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "common.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                          }
                        }
                      }
                    filter = reportingImpressionQualificationFilter {
                      custom =
                        ReportingImpressionQualificationFilterKt
                          .customImpressionQualificationFilterSpec {
                            filterSpec += impressionQualificationFilterSpec {
                              mediaType = MediaType.VIDEO

                              filters += eventFilter {
                                terms += eventTemplateField {
                                  path = "common.age_group"
                                  value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                                }
                              }
                            }
                          }
                    }
                  }

                metricSet =
                  ResultGroupKt.metricSet {
                    populationSize = 1000
                    reportingUnit =
                      ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 1
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 2
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        stackedIncrementalReach += 10
                        stackedIncrementalReach += 15
                      }
                    components +=
                      ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
                        key = DataProviderKey(cmmsDataProviderId).toName()
                        value =
                          ResultGroupKt.MetricSetKt.componentMetricSet {
                            nonCumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 1
                                percentReach = 0.1f
                                kPlusReach += 1
                                kPlusReach += 2
                                percentKPlusReach += 0.1f
                                percentKPlusReach += 0.2f
                                averageFrequency = 0.1f
                                impressions = 1
                                grps = 0.1f
                              }
                            cumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 2
                                percentReach = 0.2f
                                kPlusReach += 2
                                kPlusReach += 4
                                percentKPlusReach += 0.2f
                                percentKPlusReach += 0.4f
                                averageFrequency = 0.2f
                                impressions = 2
                                grps = 0.2f
                              }
                            uniqueReach = 5
                          }
                      }
                    componentIntersections +=
                      ResultGroupKt.MetricSetKt.componentIntersectionMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 15
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 20
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        components += DataProviderKey(cmmsDataProviderId).toName()
                        components += DataProviderKey(cmmsDataProviderId + "1").toName()
                      }
                  }
              }
          }

          resultGroups += resultGroup {
            title = "title2"
            results +=
              ResultGroupKt.result {
                metadata =
                  ResultGroupKt.metricMetadata {
                    reportingUnitSummary =
                      ResultGroupKt.MetricMetadataKt.reportingUnitSummary {
                        reportingUnitComponentSummary +=
                          ResultGroupKt.MetricMetadataKt.reportingUnitComponentSummary {
                            component = DataProviderKey(cmmsDataProviderId).toName()
                            displayName = "display"
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary {
                                  eventGroup =
                                    EventGroupKey(
                                        cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                                        cmmsEventGroupId = "12345",
                                      )
                                      .toName()
                                }
                          }
                      }
                    nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                    cumulativeMetricStartTime = timestamp { seconds = 12 }
                    metricEndTime = timestamp { seconds = 20 }
                    metricFrequency = metricFrequencySpec { total = true }
                    dimensionSpecSummary =
                      ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                        grouping = eventTemplateField {
                          path = "common.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "common.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                          }
                        }
                      }
                    filter = reportingImpressionQualificationFilter {
                      custom =
                        ReportingImpressionQualificationFilterKt
                          .customImpressionQualificationFilterSpec {
                            filterSpec += impressionQualificationFilterSpec {
                              mediaType = MediaType.VIDEO

                              filters += eventFilter {
                                terms += eventTemplateField {
                                  path = "common.age_group"
                                  value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                                }
                              }
                            }
                          }
                    }
                  }

                metricSet =
                  ResultGroupKt.metricSet {
                    populationSize = 1000
                    reportingUnit =
                      ResultGroupKt.MetricSetKt.reportingUnitMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 1
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 2
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        stackedIncrementalReach += 10
                        stackedIncrementalReach += 15
                      }
                    components +=
                      ResultGroupKt.MetricSetKt.componentMetricSetMapEntry {
                        key = DataProviderKey(cmmsDataProviderId).toName()
                        value =
                          ResultGroupKt.MetricSetKt.componentMetricSet {
                            nonCumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 1
                                percentReach = 0.1f
                                kPlusReach += 1
                                kPlusReach += 2
                                percentKPlusReach += 0.1f
                                percentKPlusReach += 0.2f
                                averageFrequency = 0.1f
                                impressions = 1
                                grps = 0.1f
                              }
                            cumulative =
                              ResultGroupKt.MetricSetKt.basicMetricSet {
                                reach = 2
                                percentReach = 0.2f
                                kPlusReach += 2
                                kPlusReach += 4
                                percentKPlusReach += 0.2f
                                percentKPlusReach += 0.4f
                                averageFrequency = 0.2f
                                impressions = 2
                                grps = 0.2f
                              }
                            uniqueReach = 5
                          }
                      }
                    componentIntersections +=
                      ResultGroupKt.MetricSetKt.componentIntersectionMetricSet {
                        nonCumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 15
                            percentReach = 0.1f
                            kPlusReach += 1
                            kPlusReach += 2
                            percentKPlusReach += 0.1f
                            percentKPlusReach += 0.2f
                            averageFrequency = 0.1f
                            impressions = 1
                            grps = 0.1f
                          }
                        cumulative =
                          ResultGroupKt.MetricSetKt.basicMetricSet {
                            reach = 20
                            percentReach = 0.2f
                            kPlusReach += 2
                            kPlusReach += 4
                            percentKPlusReach += 0.2f
                            percentKPlusReach += 0.4f
                            averageFrequency = 0.2f
                            impressions = 2
                            grps = 0.2f
                          }
                        components += DataProviderKey(cmmsDataProviderId).toName()
                        components += DataProviderKey(cmmsDataProviderId + "1").toName()
                      }
                  }
              }
          }

          createTime = internalBasicReport.createTime
        }
      )
  }

  @Test
  fun `getBasicReport throws INVALID_ARGUMENT when name is missing`() = runBlocking {
    val request = getBasicReportRequest {}
    val exception = assertFailsWith<StatusRuntimeException> { service.getBasicReport(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getBasicReport throws INVALID_ARGUMENT when name is invalid`() = runBlocking {
    val request = getBasicReportRequest { name = "/basicReports/def" }
    val exception = assertFailsWith<StatusRuntimeException> { service.getBasicReport(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "name"
        }
      )
  }

  @Test
  fun `getBasicReport throws NOT_FOUND when not found`() = runBlocking {
    val request = getBasicReportRequest { name = "measurementConsumers/abc/basicReports/def" }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.getBasicReport(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_NOT_FOUND.name
          metadata[Errors.Metadata.BASIC_REPORT.key] = request.name
        }
      )
  }

  @Test
  fun `getBasicReport throws PERMISSION_DENIED when caller does not have permission`() =
    runBlocking {
      val request = getBasicReportRequest { name = "measurementConsumers/abc/basicReports/def" }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/other-mc-user" }, SCOPES) {
            service.getBasicReport(request)
          }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(exception).hasMessageThat().contains(BasicReportsService.Permission.GET)
    }

  @Test
  fun `listBasicReports returns basic report when page size 1`(): Unit = runBlocking {
    val cmmsMeasurementConsumerId = "1234"
    val reportingSetId = "4322"
    val basicReportId = "4321"

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
        externalReportingSetId = reportingSetId
      }
    )

    var internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
      }
      resultDetails = basicReportResultDetails {}
    }

    internalBasicReport =
      internalBasicReportsService.insertBasicReport(
        insertBasicReportRequest { basicReport = internalBasicReport }
      )

    val listBasicReportsResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listBasicReports(
          listBasicReportsRequest {
            parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
            pageSize = 1
          }
        )
      }

    assertThat(listBasicReportsResponse.basicReportsList)
      .containsExactly(
        basicReport {
          name =
            BasicReportKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                basicReportId = basicReportId,
              )
              .toName()
          campaignGroup =
            ReportingSetKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                reportingSetId = reportingSetId,
              )
              .toName()
          campaignGroupDisplayName = REPORTING_SET.displayName
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          createTime = internalBasicReport.createTime
        }
      )
    assertThat(listBasicReportsResponse.nextPageToken).isEmpty()
  }

  @Test
  fun `listBasicReports returns no basic report with create_time_after when time matches`(): Unit =
    runBlocking {
      val cmmsMeasurementConsumerId = "1234"
      val reportingSetId = "4322"
      val basicReportId = "4321"

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
      )

      reportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
          externalReportingSetId = reportingSetId
        }
      )

      var internalBasicReport = internalBasicReport {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalBasicReportId = basicReportId
        externalCampaignGroupId = reportingSetId
        campaignGroupDisplayName = REPORTING_SET.displayName
        details = basicReportDetails {
          title = "title"
          reportingInterval = internalReportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
        }
        resultDetails = basicReportResultDetails {}
      }

      internalBasicReport =
        internalBasicReportsService.insertBasicReport(
          insertBasicReportRequest { basicReport = internalBasicReport }
        )

      val listBasicReportsResponse =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listBasicReports(
            listBasicReportsRequest {
              parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
              filter =
                ListBasicReportsRequestKt.filter {
                  createTimeAfter = internalBasicReport.createTime
                }
              pageSize = 1
            }
          )
        }

      assertThat(listBasicReportsResponse.basicReportsList).hasSize(0)
      assertThat(listBasicReportsResponse.nextPageToken).isEmpty()
    }

  @Test
  fun `listBasicReports returns default num basic reports when no page size`(): Unit = runBlocking {
    val cmmsMeasurementConsumerId = "1234"
    val reportingSetId = "4322"
    val basicReportId = "4321"

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
      }
      resultDetails = basicReportResultDetails {}
    }

    for (i in 1..DEFAULT_PAGE_SIZE) {
      internalBasicReportsService.insertBasicReport(
        insertBasicReportRequest {
          basicReport = internalBasicReport.copy { externalBasicReportId += "$i" }
        }
      )
    }

    val listBasicReportsResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listBasicReports(
          listBasicReportsRequest {
            parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
          }
        )
      }

    assertThat(listBasicReportsResponse.basicReportsList).hasSize(DEFAULT_PAGE_SIZE)
    assertThat(listBasicReportsResponse.nextPageToken).isEmpty()
  }

  @Test
  fun `listBasicReports returns max num basic reports when pg size too big`(): Unit = runBlocking {
    val cmmsMeasurementConsumerId = "1234"
    val reportingSetId = "4322"
    val basicReportId = "4321"

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
      }
      resultDetails = basicReportResultDetails {}
    }

    for (i in 1..MAX_PAGE_SIZE) {
      internalBasicReportsService.insertBasicReport(
        insertBasicReportRequest {
          basicReport = internalBasicReport.copy { externalBasicReportId += "$i" }
        }
      )
    }

    val listBasicReportsResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listBasicReports(
          listBasicReportsRequest {
            parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
            pageSize = MAX_PAGE_SIZE + 1
          }
        )
      }

    assertThat(listBasicReportsResponse.basicReportsList).hasSize(MAX_PAGE_SIZE)
    assertThat(listBasicReportsResponse.nextPageToken).isEmpty()
  }

  @Test
  fun `listBasicReports returns next page token when more to list`(): Unit = runBlocking {
    val cmmsMeasurementConsumerId = "1234"
    val reportingSetId = "4322"
    val basicReportId = "4321"

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
      }
      resultDetails = basicReportResultDetails {}
    }

    val internalBasicReport1 =
      internalBasicReportsService.insertBasicReport(
        insertBasicReportRequest {
          basicReport = internalBasicReport.copy { externalBasicReportId += "1" }
        }
      )

    internalBasicReportsService.insertBasicReport(
      insertBasicReportRequest {
        basicReport = internalBasicReport.copy { externalBasicReportId += "2" }
      }
    )

    val listBasicReportsResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listBasicReports(
          listBasicReportsRequest {
            parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
            pageSize = 1
          }
        )
      }

    assertThat(listBasicReportsResponse.basicReportsList)
      .containsExactly(
        basicReport {
          name =
            BasicReportKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                basicReportId = internalBasicReport1.externalBasicReportId,
              )
              .toName()
          campaignGroup =
            ReportingSetKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                reportingSetId = reportingSetId,
              )
              .toName()
          campaignGroupDisplayName = REPORTING_SET.displayName
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          createTime = internalBasicReport1.createTime
        }
      )
    assertThat(listBasicReportsResponse.nextPageToken)
      .isEqualTo(
        listBasicReportsPageToken {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = internalBasicReport1.createTime
                externalBasicReportId = internalBasicReport1.externalBasicReportId
              }
          }
          .toByteString()
          .base64UrlEncode()
      )
  }

  @Test
  fun `listBasicReports returns basic report when using page token`(): Unit = runBlocking {
    val cmmsMeasurementConsumerId = "1234"
    val reportingSetId = "4322"
    val basicReportId = "4321"

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
    )

    reportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          REPORTING_SET.copy { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = REPORTING_SET.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
        }
      }
      resultDetails = basicReportResultDetails {}
    }

    internalBasicReportsService.insertBasicReport(
      insertBasicReportRequest {
        basicReport = internalBasicReport.copy { externalBasicReportId += "1" }
      }
    )

    val internalBasicReport2 =
      internalBasicReportsService.insertBasicReport(
        insertBasicReportRequest {
          basicReport = internalBasicReport.copy { externalBasicReportId += "2" }
        }
      )

    val nextPageToken =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service
          .listBasicReports(
            listBasicReportsRequest {
              parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
              pageSize = 1
            }
          )
          .nextPageToken
      }

    val listBasicReportsResponse =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.listBasicReports(
          listBasicReportsRequest {
            parent = MeasurementConsumerKey(cmmsMeasurementConsumerId).toName()
            pageSize = 1
            pageToken = nextPageToken
          }
        )
      }

    assertThat(listBasicReportsResponse.basicReportsList)
      .containsExactly(
        basicReport {
          name =
            BasicReportKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                basicReportId = internalBasicReport2.externalBasicReportId,
              )
              .toName()
          campaignGroup =
            ReportingSetKey(
                cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
                reportingSetId = reportingSetId,
              )
              .toName()
          campaignGroupDisplayName = REPORTING_SET.displayName
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          createTime = internalBasicReport2.createTime
        }
      )
    assertThat(listBasicReportsResponse.nextPageToken).isEmpty()
  }

  @Test
  fun `listBasicReports throws PERMISSION_DENIED when caller does not have permission`() {
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/other-mc-user" }, SCOPES) {
          runBlocking {
            service.listBasicReports(
              listBasicReportsRequest { parent = "measurementConsumers/foo" }
            )
          }
        }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception).hasMessageThat().contains(BasicReportsService.Permission.LIST)
  }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val request = listBasicReportsRequest { pageSize = 5 }
    val exception = assertFailsWith<StatusRuntimeException> { service.listBasicReports(request) }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when parent is invalid`() = runBlocking {
    val request = listBasicReportsRequest { parent = "measurementConsumers" }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.listBasicReports(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when page_size is negative`() = runBlocking {
    val request = listBasicReportsRequest {
      parent = "measurementConsumers/abc"
      pageSize = -1
    }
    val exception = assertFailsWith<StatusRuntimeException> { service.listBasicReports(request) }

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
  fun `listBasicReports throws INVALID_ARGUMENT when page_token is invalid`() = runBlocking {
    val request = listBasicReportsRequest {
      parent = "measurementConsumers/abc"
      pageSize = 5
      pageToken = "abc"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.listBasicReports(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
        }
      )
  }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when request doesn't match page token`() =
    runBlocking {
      val request = listBasicReportsRequest {
        parent = "measurementConsumers/abc"
        pageSize = 5
        filter = ListBasicReportsRequestKt.filter { createTimeAfter = timestamp { seconds = 2 } }
        pageToken =
          listBasicReportsPageToken {
              filter =
                ListBasicReportsPageTokenKt.filter { createTimeAfter = timestamp { seconds = 1 } }
            }
            .toByteString()
            .base64UrlEncode()
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.listBasicReports(request) }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.ARGUMENT_CHANGED_IN_REQUEST_FOR_NEXT_PAGE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "filter.create_time_after"
          }
        )
    }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)

    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25

    private val ALL_PERMISSIONS =
      setOf(BasicReportsService.Permission.GET, BasicReportsService.Permission.LIST)
    private val SCOPES = ALL_PERMISSIONS
    private val PRINCIPAL = principal { name = "principals/mc-user" }
    private val REPORTING_SET = reportingSet {
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
    }

    private val AMI_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.VIDEO
        }
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
        }
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.OTHER
        }
    }

    private val MRC_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      impressionQualificationFilterId = 2
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
          filters +=
            ImpressionQualificationFilterConfigKt.eventFilter {
              terms +=
                ImpressionQualificationFilterConfigKt.eventTemplateField {
                  path = "banner_ad.viewable_fraction_1_second"
                  value = fieldValue { floatValue = 0.5F }
                }
            }
        }
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.VIDEO
          filters +=
            ImpressionQualificationFilterConfigKt.eventFilter {
              terms +=
                ImpressionQualificationFilterConfigKt.eventTemplateField {
                  path = "video.viewable_fraction_1_second"
                  value = fieldValue { floatValue = 1.0F }
                }
            }
        }
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.OTHER
        }
    }

    private val IMPRESSION_QUALIFICATION_FILTER_CONFIG = impressionQualificationFilterConfig {
      impressionQualificationFilters += AMI_IQF
      impressionQualificationFilters += MRC_IQF
    }

    private val IMPRESSION_QUALIFICATION_FILTER_MAPPING =
      ImpressionQualificationFilterMapping(IMPRESSION_QUALIFICATION_FILTER_CONFIG)
  }
}
