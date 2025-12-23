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
import com.google.type.copy
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import io.grpc.Status
import io.grpc.StatusRuntimeException
import java.nio.file.Paths
import java.security.SecureRandom
import java.time.Clock
import java.util.UUID
import kotlin.random.Random
import kotlin.random.asKotlinRandom
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.argumentCaptor
import org.mockito.kotlin.doAnswer
import org.mockito.kotlin.verify
import org.mockito.kotlin.whenever
import org.mockito.kotlin.wheneverBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.testing.Authentication.withPrincipalAndScopes
import org.wfanet.measurement.access.client.v1alpha.testing.PrincipalMatcher.Companion.hasPrincipal
import org.wfanet.measurement.access.v1alpha.CheckPermissionsRequest
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.checkPermissionsResponse
import org.wfanet.measurement.access.v1alpha.copy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.EnumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesRequest
import org.wfanet.measurement.api.v2alpha.enumerateValidModelLinesResponse
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.modelLine
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.db.r2dbc.postgres.testing.PostgresDatabaseProviderRule
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.testing.StatusExceptionSubject.Companion.assertThat
import org.wfanet.measurement.common.grpc.testing.mockService
import org.wfanet.measurement.common.identity.RandomIdGenerator
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.verifyProtoArgument
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig.ImpressionQualificationFilterSpec
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.EventTemplateFieldKt.fieldValue
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfigKt.impressionQualificationFilter
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.config.reporting.impressionQualificationFilterConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorDatabaseRule
import org.wfanet.measurement.gcloud.spanner.testing.SpannerEmulatorRule
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec as InternalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub as InternalImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequestKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricSpecKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt.primitiveReportingSetBasis
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt.weightedSubsetUnion
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequestKt
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilter as internalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricCalculationSpec as internalMetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec as internalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.metricSpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter as internalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval as internalReportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.common.service.ImpressionQualificationFiltersService
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.SpannerBasicReportsService
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMetricCalculationSpecsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.CreateReportRequest
import org.wfanet.measurement.reporting.v2alpha.DimensionSpec
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequestKt
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroup
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

@RunWith(JUnit4::class)
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

  private val reportsServiceMock: ReportsCoroutineImplBase = mockService {
    onBlocking { createReport(any()) }
      .thenReturn(report { name = ReportKey("a1234", "a1234").toName() })
  }

  private val modelLinesServiceMock: ModelLinesCoroutineImplBase = mockService {
    onBlocking { enumerateValidModelLines(any()) }
      .thenReturn(enumerateValidModelLinesResponse { modelLines += ModelLine.getDefaultInstance() })
  }

  val grpcTestServerRule = GrpcTestServerRule {
    val spannerDatabaseClient = spannerDatabase.databaseClient
    val postgresDatabaseClient = postgresDatabaseProvider.createDatabase()
    val idGenerator = RandomIdGenerator(Clock.systemUTC(), Random(1))

    addService(permissionsServiceMock)
    addService(reportsServiceMock)
    addService(
      SpannerBasicReportsService(
        spannerDatabaseClient,
        postgresDatabaseClient,
        IMPRESSION_QUALIFICATION_FILTER_MAPPING,
      )
    )
    addService(PostgresMeasurementConsumersService(idGenerator, postgresDatabaseClient))
    addService(PostgresMetricCalculationSpecsService(idGenerator, postgresDatabaseClient))
    addService(PostgresReportingSetsService(idGenerator, postgresDatabaseClient))
    addService(ImpressionQualificationFiltersService(IMPRESSION_QUALIFICATION_FILTER_MAPPING))
    addService(modelLinesServiceMock)
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  private lateinit var authorization: Authorization
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineStub
  private lateinit var internalImpressionQualificationFiltersService:
    InternalImpressionQualificationFiltersCoroutineStub
  private lateinit var internalMetricCalculationSpecsService:
    InternalMetricCalculationSpecsCoroutineStub
  private lateinit var internalReportingSetsService: InternalReportingSetsCoroutineStub
  private lateinit var internalBasicReportsService: InternalBasicReportsCoroutineStub
  private lateinit var reportsService: ReportsCoroutineStub
  private lateinit var modelLinesService: ModelLinesCoroutineStub

  private lateinit var service: BasicReportsService

  @Before
  fun initService() {
    measurementConsumersService = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
    internalImpressionQualificationFiltersService =
      InternalImpressionQualificationFiltersCoroutineStub(grpcTestServerRule.channel)
    internalMetricCalculationSpecsService =
      InternalMetricCalculationSpecsCoroutineStub(grpcTestServerRule.channel)
    internalReportingSetsService = InternalReportingSetsCoroutineStub(grpcTestServerRule.channel)
    internalBasicReportsService = InternalBasicReportsCoroutineStub(grpcTestServerRule.channel)
    reportsService = ReportsCoroutineStub(grpcTestServerRule.channel)
    modelLinesService = ModelLinesCoroutineStub(grpcTestServerRule.channel)
    authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))

    service =
      BasicReportsService(
        internalBasicReportsService,
        internalImpressionQualificationFiltersService,
        internalReportingSetsService,
        internalMetricCalculationSpecsService,
        reportsService,
        modelLinesService,
        TEST_EVENT_DESCRIPTOR,
        METRIC_SPEC_CONFIG,
        SecureRandom().asKotlinRandom(),
        authorization,
        MEASUREMENT_CONSUMER_CONFIGS,
        emptyList(),
      )
  }

  @Test
  fun `createBasicReport returns basic report`(): Unit = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    val campaignGroup =
      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroupKey.toName()
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2026
          month = 1
          day = 5
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        impressionQualificationFilter =
          ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId).toName()
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              stackedIncrementalReach = false
            }
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
            }
        }
      }
    }

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      this.basicReport = basicReport
      basicReportId = "a1234"
    }

    val response = withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

    assertThat(response)
      .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        basicReport.copy {
          name = BasicReportKey(measurementConsumerKey, request.basicReportId).toName()
          campaignGroupDisplayName = campaignGroup.displayName
          state = BasicReport.State.RUNNING
          effectiveImpressionQualificationFilters += basicReport.impressionQualificationFiltersList
        }
      )
    assertThat(response.createTime.seconds).isAtLeast(1)
  }

  @Test
  fun `createBasicReport creates report and persists its report id`(): Unit = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          externalCampaignGroupId = campaignGroupKey.reportingSetId
          displayName = "displayName"
          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                  cmmsEventGroupId = "1235"
                }
              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                  cmmsEventGroupId = "1235"
                }
            }
        }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroupKey.toName()
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2026
          month = 1
          day = 5
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        impressionQualificationFilter =
          ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId).toName()
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit {
          components += DATA_PROVIDER_KEY.toName()
          components += DATA_PROVIDER_KEY.toName() + "b"
        }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
              cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
            }
        }
      }
    }

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      this.basicReport = basicReport
      basicReportId = "a1234"
    }

    withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

    val createdReportingSets =
      internalReportingSetsService
        .streamReportingSets(
          streamReportingSetsRequest {
            filter =
              StreamReportingSetsRequestKt.filter {
                cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              }
          }
        )
        .toList()
        .filter { it.externalReportingSetId != campaignGroupKey.reportingSetId }

    val primitiveReportingSet1 =
      createdReportingSets.first {
        it.primitive.eventGroupKeysList.first() ==
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
            cmmsEventGroupId = "1235"
          }
      }

    val primitiveReportingSet2 =
      createdReportingSets.first {
        it.primitive.eventGroupKeysList.last() ==
          ReportingSetKt.PrimitiveKt.eventGroupKey {
            cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
            cmmsEventGroupId = "1235"
          }
      }

    val createdMetricCalculationSpecs =
      internalMetricCalculationSpecsService
        .listMetricCalculationSpecs(
          listMetricCalculationSpecsRequest {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            limit = 50
          }
        )
        .metricCalculationSpecsList

    val nonCumulativeMetricCalculationSpec =
      createdMetricCalculationSpecs.first { it.details.hasTrailingWindow() }

    val cumulativeMetricCalculationSpec =
      createdMetricCalculationSpecs.first {
        it.details.hasMetricFrequencySpec() && !it.details.hasTrailingWindow()
      }

    val populationMetricCalculationSpec =
      createdMetricCalculationSpecs.first {
        !(it.details.hasMetricFrequencySpec() || it.details.hasTrailingWindow())
      }

    val createReportRequest =
      argumentCaptor { verify(reportsServiceMock).createReport(capture()) }.firstValue

    assertThat(createReportRequest)
      .ignoringRepeatedFieldOrder()
      .ignoringFields(
        CreateReportRequest.REPORT_ID_FIELD_NUMBER,
        CreateReportRequest.REQUEST_ID_FIELD_NUMBER,
      )
      .isEqualTo(
        createReportRequest {
          parent = request.parent
          report = report {
            reportingMetricEntries +=
              ReportKt.reportingMetricEntry {
                key =
                  ReportingSetKey(
                      measurementConsumerKey,
                      primitiveReportingSet1.externalReportingSetId,
                    )
                    .toName()
                value =
                  ReportKt.reportingMetricCalculationSpec {
                    metricCalculationSpecs +=
                      MetricCalculationSpecKey(
                          measurementConsumerKey,
                          populationMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                    metricCalculationSpecs +=
                      MetricCalculationSpecKey(
                          measurementConsumerKey,
                          nonCumulativeMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                    metricCalculationSpecs +=
                      MetricCalculationSpecKey(
                          measurementConsumerKey,
                          cumulativeMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                  }
              }
            reportingMetricEntries +=
              ReportKt.reportingMetricEntry {
                key =
                  ReportingSetKey(
                      measurementConsumerKey,
                      primitiveReportingSet2.externalReportingSetId,
                    )
                    .toName()
                value =
                  ReportKt.reportingMetricCalculationSpec {
                    metricCalculationSpecs +=
                      MetricCalculationSpecKey(
                          measurementConsumerKey,
                          nonCumulativeMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                    metricCalculationSpecs +=
                      MetricCalculationSpecKey(
                          measurementConsumerKey,
                          cumulativeMetricCalculationSpec.externalMetricCalculationSpecId,
                        )
                        .toName()
                  }
              }
            reportingInterval =
              ReportKt.reportingInterval {
                reportStart = dateTime {
                  year = 2025
                  month = 7
                  day = 3
                  timeZone = timeZone { id = "America/Los_Angeles" }
                }
                reportEnd = date {
                  year = 2026
                  month = 1
                  day = 5
                }
              }
          }
        }
      )

    assertThat(createReportRequest.reportId).isNotEmpty()
    assertThat(createReportRequest.requestId).isNotEmpty()

    val internalBasicReport =
      internalBasicReportsService.getBasicReport(
        internalGetBasicReportRequest {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          externalBasicReportId = request.basicReportId
        }
      )

    assertThat(internalBasicReport.externalReportId).isEqualTo(createReportRequest.reportId)
    assertThat(internalBasicReport.createReportRequestId).isEqualTo(createReportRequest.requestId)
  }

  @Test
  fun `createBasicReport creates only 1 basic report when request id is repeated`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val basicReport = BASIC_REPORT.copy { this.campaignGroup = campaignGroupKey.toName() }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
        requestId = UUID.randomUUID().toString()
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.listBasicReports(
            listBasicReportsRequest { parent = measurementConsumerKey.toName() }
          )
        }

      assertThat(response.basicReportsList).hasSize(1)
    }

  @Test
  fun `createBasicReport creates new primitive reportingsets only when needed`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      val campaignGroup =
        internalReportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet = internalReportingSet {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
              displayName = "displayName"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                      cmmsEventGroupId = "1235"
                    }
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                      cmmsEventGroupId = "1236"
                    }
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                      cmmsEventGroupId = "1235"
                    }
                }
            }
            externalReportingSetId = campaignGroupKey.reportingSetId
          }
        )

      val streamReportingSetsRequest = streamReportingSetsRequest {
        filter =
          StreamReportingSetsRequestKt.filter {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          }
      }

      val existingReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(existingReportingSets).ignoringRepeatedFieldOrder().containsExactly(campaignGroup)

      val basicReport = basicReport {
        this.campaignGroup = campaignGroupKey.toName()
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          custom =
            ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
              filterSpec += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
                  }
                }
              }
            }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                  }
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() + "b" }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                  }
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
              }
          }
        }
      }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

      val updatedReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(updatedReportingSets).hasSize(3)
      assertThat(
          updatedReportingSets.map {
            if (it.externalReportingSetId == campaignGroup.externalReportingSetId) {
              it
            } else {
              it.copy {
                clearExternalReportingSetId()
                weightedSubsetUnions.clear()
              }
            }
          }
        )
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          campaignGroup,
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1236"
                  }
              }
          },
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                    cmmsEventGroupId = "1235"
                  }
              }
          },
        )

      val request2 = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "b1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request2) }

      val identicalReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(identicalReportingSets).hasSize(3)
      assertThat(updatedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(identicalReportingSets)
    }

  @Test
  fun `createBasicReport returns basic report when only custom IQF exists`(): Unit = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    val campaignGroup =
      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroupKey.toName()
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2026
          month = 1
          day = 5
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = DimensionSpec.getDefaultInstance()
        resultGroupMetricSpec = resultGroupMetricSpec {
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
            }
        }
      }
    }

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      this.basicReport = basicReport
      basicReportId = "a1234"
    }

    val response = withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

    assertThat(response)
      .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        basicReport.copy {
          name = BasicReportKey(measurementConsumerKey, request.basicReportId).toName()
          campaignGroupDisplayName = campaignGroup.displayName
          state = BasicReport.State.RUNNING
          effectiveImpressionQualificationFilters += basicReport.impressionQualificationFiltersList
        }
      )
    assertThat(response.createTime.seconds).isAtLeast(1)

    val listMetricCalculationSpecsRequest = listMetricCalculationSpecsRequest {
      cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      filter =
        ListMetricCalculationSpecsRequestKt.filter {
          externalCampaignGroupId = campaignGroupKey.reportingSetId
        }
      limit = 50
    }

    val createdMetricCalculationSpecs =
      internalMetricCalculationSpecsService
        .listMetricCalculationSpecs(listMetricCalculationSpecsRequest)
        .metricCalculationSpecsList

    assertThat(
        createdMetricCalculationSpecs.first().details.filter.isNotEmpty() ||
          createdMetricCalculationSpecs.last().details.filter.isNotEmpty()
      )
      .isTrue()
  }

  @Test
  fun `createBasicReport creates new composite reportingsets only when needed`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      val campaignGroup =
        internalReportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet = internalReportingSet {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
              displayName = "displayName"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                      cmmsEventGroupId = "1235"
                    }
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                      cmmsEventGroupId = "1235"
                    }
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "c"
                      cmmsEventGroupId = "1235"
                    }
                }
            }
            externalReportingSetId = campaignGroupKey.reportingSetId
          }
        )

      val streamReportingSetsRequest = streamReportingSetsRequest {
        filter =
          StreamReportingSetsRequestKt.filter {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          }
      }

      val existingReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(existingReportingSets).ignoringRepeatedFieldOrder().containsExactly(campaignGroup)

      val basicReport = basicReport {
        this.campaignGroup = campaignGroupKey.toName()
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          custom =
            ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
              filterSpec += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
                  }
                }
              }
            }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_KEY.toName()
            components += DATA_PROVIDER_KEY.toName() + "b"
            components += DATA_PROVIDER_KEY.toName() + "c"
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
                stackedIncrementalReach = true
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
                stackedIncrementalReach = true
              }
          }
        }
      }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

      val updatedReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(updatedReportingSets).hasSize(6)

      val componentPrimitiveReportingSets =
        updatedReportingSets
          .filter { it.hasPrimitive() && it.externalReportingSetId != it.externalCampaignGroupId }
          .sortedBy { it.primitive.eventGroupKeysList.first().cmmsDataProviderId }
          .map { it.externalReportingSetId }

      assertThat(
          updatedReportingSets
            .filter { it.hasComposite() }
            .map {
              it.copy {
                clearExternalReportingSetId()
                weightedSubsetUnions.clear()
              }
            }
        )
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            composite =
              ReportingSetKt.setExpression {
                operation = ReportingSet.SetExpression.Operation.UNION
                lhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    externalReportingSetId = componentPrimitiveReportingSets[2]
                  }
                rhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    expression =
                      ReportingSetKt.setExpression {
                        operation = ReportingSet.SetExpression.Operation.UNION
                        lhs =
                          ReportingSetKt.SetExpressionKt.operand {
                            externalReportingSetId = componentPrimitiveReportingSets[1]
                          }
                        rhs =
                          ReportingSetKt.SetExpressionKt.operand {
                            expression =
                              ReportingSetKt.setExpression {
                                operation = ReportingSet.SetExpression.Operation.UNION
                                lhs =
                                  ReportingSetKt.SetExpressionKt.operand {
                                    externalReportingSetId = componentPrimitiveReportingSets[0]
                                  }
                              }
                          }
                      }
                  }
              }
          },
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            composite =
              ReportingSetKt.setExpression {
                operation = ReportingSet.SetExpression.Operation.UNION
                lhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    externalReportingSetId = componentPrimitiveReportingSets[1]
                  }
                rhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    expression =
                      ReportingSetKt.setExpression {
                        operation = ReportingSet.SetExpression.Operation.UNION
                        lhs =
                          ReportingSetKt.SetExpressionKt.operand {
                            externalReportingSetId = componentPrimitiveReportingSets[0]
                          }
                      }
                  }
              }
          },
        )

      val request2 = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "b1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request2) }

      val identicalReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(identicalReportingSets).hasSize(6)
      assertThat(updatedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(identicalReportingSets)
    }

  @Test
  fun `createBasicReport creates reportingsets when filters exists in old ones`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      val campaignGroup =
        internalReportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet = internalReportingSet {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
              displayName = "displayName"

              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                      cmmsEventGroupId = "1235"
                    }
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                      cmmsEventGroupId = "1235"
                    }
                }
            }
            externalReportingSetId = campaignGroupKey.reportingSetId
          }
        )

      val streamReportingSetsRequest = streamReportingSetsRequest {
        filter =
          StreamReportingSetsRequestKt.filter {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          }
      }

      val existingReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(existingReportingSets).ignoringRepeatedFieldOrder().containsExactly(campaignGroup)

      val basicReport = basicReport {
        this.campaignGroup = campaignGroupKey.toName()
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          custom =
            ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
              filterSpec += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
                  }
                }
              }
            }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                  }
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() + "b" }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                  }
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
              }
          }
        }
      }

      val firstCreateBasicReportRequest = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.createBasicReport(firstCreateBasicReportRequest)
      }

      val primitiveReportingSetsOnly =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(primitiveReportingSetsOnly).hasSize(3)
      assertThat(
          primitiveReportingSetsOnly.map {
            if (it.externalReportingSetId == campaignGroup.externalReportingSetId) {
              it
            } else {
              it.copy {
                clearExternalReportingSetId()
                weightedSubsetUnions.clear()
              }
            }
          }
        )
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          campaignGroup,
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          },
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                    cmmsEventGroupId = "1235"
                  }
              }
          },
        )

      val componentPrimitiveReportingSets =
        primitiveReportingSetsOnly
          .filter { it.hasPrimitive() && it.externalReportingSetId != it.externalCampaignGroupId }
          .sortedBy { it.primitive.eventGroupKeysList.first().cmmsDataProviderId }
          .map { it.externalReportingSetId }

      val unusedCompositeReportingSet =
        internalReportingSetsService.createReportingSet(
          createReportingSetRequest {
            externalReportingSetId = "c1234"
            reportingSet = internalReportingSet {
              filter = "filter"
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
              composite =
                ReportingSetKt.setExpression {
                  operation = ReportingSet.SetExpression.Operation.UNION
                  lhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      externalReportingSetId = componentPrimitiveReportingSets[1]
                    }
                  rhs =
                    ReportingSetKt.SetExpressionKt.operand {
                      expression =
                        ReportingSetKt.setExpression {
                          operation = ReportingSet.SetExpression.Operation.UNION
                          lhs =
                            ReportingSetKt.SetExpressionKt.operand {
                              externalReportingSetId = componentPrimitiveReportingSets[0]
                            }
                        }
                    }
                }
              weightedSubsetUnions += weightedSubsetUnion {
                primitiveReportingSetBases += primitiveReportingSetBasis {
                  externalReportingSetId = componentPrimitiveReportingSets[0]
                }
                primitiveReportingSetBases += primitiveReportingSetBasis {
                  externalReportingSetId = componentPrimitiveReportingSets[1]
                }
                weight = 1
                binaryRepresentation = 1
              }
            }
          }
        )

      assertThat(
          internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()
        )
        .hasSize(4)

      val basicReportThatNeedsComposite = basicReport {
        this.campaignGroup = campaignGroupKey.toName()
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          custom =
            ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
              filterSpec += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
                  }
                }
              }
            }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            components += DATA_PROVIDER_KEY.toName()
            components += DATA_PROVIDER_KEY.toName() + "b"
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                cumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    percentReach = true
                    kPlusReach = 5
                    percentKPlusReach = true
                    averageFrequency = true
                    impressions = true
                    grps = true
                  }
                stackedIncrementalReach = true
              }
          }
        }
      }

      val secondCreateBasicReportRequest = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReportThatNeedsComposite
        basicReportId = "b1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.createBasicReport(secondCreateBasicReportRequest)
      }

      val updatedReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(updatedReportingSets).hasSize(5)

      assertThat(
          updatedReportingSets
            .filter { it.hasComposite() }
            .map {
              it.copy {
                clearExternalReportingSetId()
                weightedSubsetUnions.clear()
              }
            }
        )
        .ignoringRepeatedFieldOrder()
        .containsExactly(
          unusedCompositeReportingSet.copy {
            clearExternalReportingSetId()
            weightedSubsetUnions.clear()
          },
          internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            composite =
              ReportingSetKt.setExpression {
                operation = ReportingSet.SetExpression.Operation.UNION
                lhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    externalReportingSetId = componentPrimitiveReportingSets[1]
                  }
                rhs =
                  ReportingSetKt.SetExpressionKt.operand {
                    expression =
                      ReportingSetKt.setExpression {
                        operation = ReportingSet.SetExpression.Operation.UNION
                        lhs =
                          ReportingSetKt.SetExpressionKt.operand {
                            externalReportingSetId = componentPrimitiveReportingSets[0]
                          }
                      }
                  }
              }
          },
        )

      val duplicateRequest = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReportThatNeedsComposite
        basicReportId = "f1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(duplicateRequest) }

      val identicalReportingSets =
        internalReportingSetsService.streamReportingSets(streamReportingSetsRequest).toList()

      assertThat(identicalReportingSets).hasSize(5)
      assertThat(updatedReportingSets)
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(identicalReportingSets)
    }

  @Test
  fun `createBasicReport creates new metric calculation specs only when needed`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      val specifiedModelLine = modelLine {
        name = ModelLineKey("1234", "1234", "1234").toName()
        type = ModelLine.Type.PROD
      }
      whenever(modelLinesServiceMock.enumerateValidModelLines(any()))
        .thenReturn(enumerateValidModelLinesResponse { modelLines += specifiedModelLine })

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"

            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId + "b"
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val listMetricCalculationSpecsRequest = listMetricCalculationSpecsRequest {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        filter =
          ListMetricCalculationSpecsRequestKt.filter {
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        limit = 50
      }

      val existingMetricCalculationSpecs =
        internalMetricCalculationSpecsService
          .listMetricCalculationSpecs(listMetricCalculationSpecsRequest)
          .metricCalculationSpecsList

      assertThat(existingMetricCalculationSpecs).hasSize(0)

      val basicReport = basicReport {
        this.campaignGroup = campaignGroupKey.toName()
        title = "title"
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        modelLine = specifiedModelLine.name
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(MRC_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = false
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
          }
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = false
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulative =
                  ResultGroupMetricSpecKt.basicMetricSetSpec {
                    reach = true
                    kPlusReach = 5
                    averageFrequency = true
                    impressions = true
                  }
              }
          }
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() + "b" }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
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
                cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

      val updatedMetricCalculationSpecs =
        internalMetricCalculationSpecsService
          .listMetricCalculationSpecs(listMetricCalculationSpecsRequest)
          .metricCalculationSpecsList

      assertThat(updatedMetricCalculationSpecs).hasSize(6)
      assertThat(updatedMetricCalculationSpecs)
        .ignoringRepeatedFieldOrder()
        .ignoringFields(MetricCalculationSpec.EXTERNAL_METRIC_CALCULATION_SPEC_ID_FIELD_NUMBER)
        .containsExactly(
          internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            cmmsModelLine = specifiedModelLine.name
            details =
              MetricCalculationSpecKt.details {
                groupings +=
                  MetricCalculationSpecKt.grouping {
                    predicates += "person.social_grade_group == 1"
                    predicates += "person.social_grade_group == 2"
                  }
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricFrequencySpec =
                  MetricCalculationSpecKt.metricFrequencySpec {
                    weekly =
                      MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                        dayOfWeek = DayOfWeek.MONDAY
                      }
                  }
                metricSpecs += metricSpec {
                  reach =
                    MetricSpecKt.reachParams {
                      multipleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .privacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                      singleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .privacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                    }
                }
              }
          },
          internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            cmmsModelLine = specifiedModelLine.name
            details =
              MetricCalculationSpecKt.details {
                groupings +=
                  MetricCalculationSpecKt.grouping {
                    predicates += "person.social_grade_group == 1"
                    predicates += "person.social_grade_group == 2"
                  }
                metricSpecs += metricSpec {
                  populationCount = MetricSpecKt.populationCountParams {}
                }
              }
          },
          internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            cmmsModelLine = specifiedModelLine.name
            details =
              MetricCalculationSpecKt.details {
                groupings +=
                  MetricCalculationSpecKt.grouping {
                    predicates += "person.social_grade_group == 1"
                    predicates += "person.social_grade_group == 2"
                  }
                filter = "((has(banner_ad.viewable) && banner_ad.viewable == true))"
                metricFrequencySpec =
                  MetricCalculationSpecKt.metricFrequencySpec {
                    weekly =
                      MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                        dayOfWeek = DayOfWeek.MONDAY
                      }
                  }
                trailingWindow =
                  MetricCalculationSpecKt.trailingWindow {
                    count = 1
                    increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                  }
                metricSpecs += metricSpec {
                  reachAndFrequency =
                    MetricSpecKt.reachAndFrequencyParams {
                      multipleDataProviderParams =
                        MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                          reachPrivacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams
                                  .multipleDataProviderParams
                                  .reachPrivacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams
                                  .multipleDataProviderParams
                                  .reachPrivacyParams
                                  .delta
                            }
                          frequencyPrivacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams
                                  .multipleDataProviderParams
                                  .frequencyPrivacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams
                                  .multipleDataProviderParams
                                  .frequencyPrivacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams
                                  .multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams
                                  .multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                      singleDataProviderParams =
                        MetricSpecKt.reachAndFrequencySamplingAndPrivacyParams {
                          reachPrivacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                                  .reachPrivacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                                  .reachPrivacyParams
                                  .delta
                            }
                          frequencyPrivacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                                  .frequencyPrivacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                                  .frequencyPrivacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachAndFrequencyParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                      maximumFrequency = METRIC_SPEC_CONFIG.reachAndFrequencyParams.maximumFrequency
                    }
                }
                metricSpecs += metricSpec {
                  impressionCount =
                    MetricSpecKt.impressionCountParams {
                      params =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.impressionCountParams.params.privacyParams.delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.impressionCountParams.params.vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                      maximumFrequencyPerUser =
                        METRIC_SPEC_CONFIG.impressionCountParams.maximumFrequencyPerUser
                    }
                }
              }
          },
          internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            cmmsModelLine = specifiedModelLine.name
            details =
              MetricCalculationSpecKt.details {
                filter =
                  "((has(banner_ad.viewable) && banner_ad.viewable == true)) && (person.age_group == 1)"
                metricFrequencySpec =
                  MetricCalculationSpecKt.metricFrequencySpec {
                    weekly =
                      MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                        dayOfWeek = DayOfWeek.MONDAY
                      }
                  }
                metricSpecs += metricSpec {
                  reach =
                    MetricSpecKt.reachParams {
                      multipleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .privacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                      singleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .privacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                    }
                }
              }
          },
          internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            cmmsModelLine = specifiedModelLine.name
            details =
              MetricCalculationSpecKt.details {
                filter = "(person.age_group == 1)"
                metricSpecs += metricSpec {
                  populationCount = MetricSpecKt.populationCountParams {}
                }
              }
          },
          internalMetricCalculationSpec {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            cmmsModelLine = specifiedModelLine.name
            details =
              MetricCalculationSpecKt.details {
                filter =
                  "((has(banner_ad.viewable) && banner_ad.viewable == true)) && (person.age_group == 1)"
                metricFrequencySpec =
                  MetricCalculationSpecKt.metricFrequencySpec {
                    weekly =
                      MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                        dayOfWeek = DayOfWeek.MONDAY
                      }
                  }
                trailingWindow =
                  MetricCalculationSpecKt.trailingWindow {
                    count = 1
                    increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                  }
                metricSpecs += metricSpec {
                  reach =
                    MetricSpecKt.reachParams {
                      multipleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .privacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachParams.multipleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                      singleDataProviderParams =
                        MetricSpecKt.samplingAndPrivacyParams {
                          privacyParams =
                            MetricSpecKt.differentialPrivacyParams {
                              epsilon =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .privacyParams
                                  .epsilon
                              delta =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .privacyParams
                                  .delta
                            }
                          vidSamplingInterval =
                            MetricSpecKt.vidSamplingInterval {
                              start =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .start
                              width =
                                METRIC_SPEC_CONFIG.reachParams.singleDataProviderParams
                                  .vidSamplingInterval
                                  .fixedStart
                                  .width
                            }
                        }
                    }
                }
              }
          },
        )

      val request2 = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "b1234"
      }

      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request2) }

      val identicalMetricCalculationSpecs =
        internalMetricCalculationSpecsService
          .listMetricCalculationSpecs(listMetricCalculationSpecsRequest)
          .metricCalculationSpecsList

      assertThat(identicalMetricCalculationSpecs).hasSize(6)
      assertThat(updatedMetricCalculationSpecs)
        .ignoringRepeatedFieldOrder()
        .containsExactlyElementsIn(identicalMetricCalculationSpecs)
    }

  @Test
  fun `createBasicReport uses line when model line not set and one valid line exists`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      val defaultModelLine = modelLine {
        name = "modelProviders/123/modelSuites/123/modelLines/DEFAULT"
        type = ModelLine.Type.PROD
      }
      whenever(modelLinesServiceMock.enumerateValidModelLines(any()))
        .thenReturn(enumerateValidModelLinesResponse { modelLines += defaultModelLine })

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val basicReport =
        BASIC_REPORT.copy {
          this.campaignGroup = campaignGroupKey.toName()
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 3
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2026
              month = 1
              day = 5
            }
          }
          clearModelLine()
        }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

      assertThat(response.modelLine).isEmpty()
      assertThat(response.effectiveModelLine).isEqualTo(defaultModelLine.name)

      // Verify kingdomModelLinesStub.enumerateValidModelLines was called
      verifyProtoArgument(
          modelLinesServiceMock,
          ModelLinesCoroutineImplBase::enumerateValidModelLines,
        )
        .isEqualTo(
          enumerateValidModelLinesRequest {
            parent = "modelProviders/-/modelSuites/-"
            timeInterval = interval {
              startTime = timestamp { seconds = 1751526000 }
              endTime = timestamp { seconds = 1767600000 }
            }
            dataProviders += DATA_PROVIDER_KEY.toName()
          }
        )

      val listMetricCalculationSpecsRequest = listMetricCalculationSpecsRequest {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        filter =
          ListMetricCalculationSpecsRequestKt.filter {
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        limit = 50
      }

      val createdMetricCalculationSpecs =
        internalMetricCalculationSpecsService
          .listMetricCalculationSpecs(listMetricCalculationSpecsRequest)
          .metricCalculationSpecsList

      assertThat(createdMetricCalculationSpecs.map { it.cmmsModelLine }.distinct())
        .containsExactly(defaultModelLine.name)
    }

  @Test
  fun `createBasicReport uses no line when model line not set and zero valid lines exist`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      // Return no model lines
      whenever(modelLinesServiceMock.enumerateValidModelLines(any()))
        .thenReturn(EnumerateValidModelLinesResponse.getDefaultInstance())

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val basicReport =
        BASIC_REPORT.copy {
          this.campaignGroup = campaignGroupKey.toName()
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 3
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2026
              month = 1
              day = 5
            }
          }
          clearModelLine()
        }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      assertThat(response.modelLine).isEmpty()
      assertThat(response.effectiveModelLine).isEmpty()

      // Verify kingdomModelLinesStub.enumerateValidModelLines was called
      // Verify kingdomModelLinesStub.enumerateValidModelLines was called
      verifyProtoArgument(
          modelLinesServiceMock,
          ModelLinesCoroutineImplBase::enumerateValidModelLines,
        )
        .isEqualTo(
          enumerateValidModelLinesRequest {
            parent = "modelProviders/-/modelSuites/-"
            timeInterval = interval {
              startTime = timestamp { seconds = 1751526000 }
              endTime = timestamp { seconds = 1767600000 }
            }
            dataProviders += DATA_PROVIDER_KEY.toName()
          }
        )

      val listMetricCalculationSpecsRequest = listMetricCalculationSpecsRequest {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        filter =
          ListMetricCalculationSpecsRequestKt.filter {
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        limit = 50
      }

      val createdMetricCalculationSpecs =
        internalMetricCalculationSpecsService
          .listMetricCalculationSpecs(listMetricCalculationSpecsRequest)
          .metricCalculationSpecsList

      assertThat(createdMetricCalculationSpecs.map { it.cmmsModelLine }.distinct())
        .containsExactly("")
    }

  @Test
  fun `createBasicReport allows for IQFs to not be set`(): Unit = runBlocking {
    service =
      BasicReportsService(
        internalBasicReportsService,
        internalImpressionQualificationFiltersService,
        internalReportingSetsService,
        internalMetricCalculationSpecsService,
        reportsService,
        modelLinesService,
        TEST_EVENT_DESCRIPTOR,
        METRIC_SPEC_CONFIG,
        SecureRandom().asKotlinRandom(),
        authorization,
        MEASUREMENT_CONSUMER_CONFIGS,
        listOf(INTERNAL_AMI_IQF.externalImpressionQualificationFilterId),
      )

    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    val campaignGroup =
      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroupKey.toName()
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
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
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      this.basicReport = basicReport
      basicReportId = "a1234"
    }

    val response = withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

    assertThat(response)
      .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        basicReport.copy {
          name = BasicReportKey(measurementConsumerKey, request.basicReportId).toName()
          campaignGroupDisplayName = campaignGroup.displayName
          state = BasicReport.State.RUNNING
          effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
            impressionQualificationFilter =
              ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
                .toName()
          }
        }
      )
    assertThat(response.createTime.seconds).isAtLeast(1)
  }

  @Test
  fun `createBasicReport combines user-set and system-set IQFs`(): Unit = runBlocking {
    service =
      BasicReportsService(
        internalBasicReportsService,
        internalImpressionQualificationFiltersService,
        internalReportingSetsService,
        internalMetricCalculationSpecsService,
        reportsService,
        modelLinesService,
        TEST_EVENT_DESCRIPTOR,
        METRIC_SPEC_CONFIG,
        SecureRandom().asKotlinRandom(),
        authorization,
        MEASUREMENT_CONSUMER_CONFIGS,
        listOf(INTERNAL_AMI_IQF.externalImpressionQualificationFilterId),
      )

    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    val campaignGroup =
      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroupKey.toName()
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        impressionQualificationFilter =
          ImpressionQualificationFilterKey(MRC_IQF.externalImpressionQualificationFilterId).toName()
      }
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
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
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      this.basicReport = basicReport
      basicReportId = "a1234"
    }

    val response = withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

    assertThat(response)
      .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        basicReport.copy {
          name = BasicReportKey(measurementConsumerKey, request.basicReportId).toName()
          campaignGroupDisplayName = campaignGroup.displayName
          state = BasicReport.State.RUNNING
          effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
            impressionQualificationFilter =
              ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
                .toName()
          }
          effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
            impressionQualificationFilter =
              ImpressionQualificationFilterKey(MRC_IQF.externalImpressionQualificationFilterId)
                .toName()
          }
        }
      )
    assertThat(response.createTime.seconds).isAtLeast(1)
  }

  @Test
  fun `createBasicReport combines user-set and system-set IQFs without duplicating`(): Unit =
    runBlocking {
      service =
        BasicReportsService(
          internalBasicReportsService,
          internalImpressionQualificationFiltersService,
          internalReportingSetsService,
          internalMetricCalculationSpecsService,
          reportsService,
          modelLinesService,
          TEST_EVENT_DESCRIPTOR,
          METRIC_SPEC_CONFIG,
          SecureRandom().asKotlinRandom(),
          authorization,
          MEASUREMENT_CONSUMER_CONFIGS,
          listOf(
            INTERNAL_AMI_IQF.externalImpressionQualificationFilterId,
            INTERNAL_MRC_IQF.externalImpressionQualificationFilterId,
          ),
        )

      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      val campaignGroup =
        internalReportingSetsService.createReportingSet(
          createReportingSetRequest {
            reportingSet = internalReportingSet {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
              displayName = "displayName"
              primitive =
                ReportingSetKt.primitive {
                  eventGroupKeys +=
                    ReportingSetKt.PrimitiveKt.eventGroupKey {
                      cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                      cmmsEventGroupId = "1235"
                    }
                }
            }
            externalReportingSetId = campaignGroupKey.reportingSetId
          }
        )

      val basicReport = basicReport {
        title = "title"
        this.campaignGroup = campaignGroupKey.toName()
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(MRC_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
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
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {
            grouping =
              DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

      assertThat(response)
        .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(
          basicReport.copy {
            name = BasicReportKey(measurementConsumerKey, request.basicReportId).toName()
            campaignGroupDisplayName = campaignGroup.displayName
            state = BasicReport.State.RUNNING
            effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
              impressionQualificationFilter =
                ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
                  .toName()
            }
            effectiveImpressionQualificationFilters += reportingImpressionQualificationFilter {
              impressionQualificationFilter =
                ImpressionQualificationFilterKey(MRC_IQF.externalImpressionQualificationFilterId)
                  .toName()
            }
          }
        )
      assertThat(response.createTime.seconds).isAtLeast(1)
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when parent is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = "1234"
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "parent"
        }
      )
  }

  @Test
  fun `createBasicReport throws PERMISSION_DENIED when caller does not have permission`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL.copy { name = "principals/other-mc-user" }, SCOPES) {
            service.createBasicReport(request)
          }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(exception).hasMessageThat().contains(BasicReportsService.Permission.CREATE)
    }

  @Test
  fun `createBasicReport throws PERMISSION_DENIED when caller does not have permission for DEV ModelLine`() =
    runBlocking {
      wheneverBlocking { permissionsServiceMock.checkPermissions(any()) } doAnswer
        { invocation ->
          val request: CheckPermissionsRequest = invocation.getArgument(0)
          checkPermissionsResponse {
            permissions +=
              request.permissionsList.intersect(
                listOf("permissions/${BasicReportsService.Permission.CREATE}")
              )
          }
        }
      val modelLine = modelLine {
        name = ModelLineKey("1234", "1234", "1234").toName()
        type = ModelLine.Type.DEV
      }
      whenever(modelLinesServiceMock.enumerateValidModelLines(any()))
        .thenReturn(enumerateValidModelLinesResponse { modelLines += modelLine })
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")
      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )
      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )
      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport = basicReport {
          title = "title"
          campaignGroup = campaignGroupKey.toName()
          this.modelLine = modelLine.name
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 3
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2026
              month = 1
              day = 5
            }
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            impressionQualificationFilter =
              ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
                .toName()
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                  }
                }
              }
          }
          resultGroupSpecs += resultGroupSpec {
            title = "title"
            reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
            metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
            dimensionSpec = dimensionSpec {
              grouping =
                DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
                  nonCumulative =
                    ResultGroupMetricSpecKt.basicMetricSetSpec {
                      reach = true
                      percentReach = true
                      kPlusReach = 5
                      percentKPlusReach = true
                      averageFrequency = true
                      impressions = true
                      grps = true
                    }
                  cumulative =
                    ResultGroupMetricSpecKt.basicMetricSetSpec {
                      reach = true
                      percentReach = true
                    }
                  stackedIncrementalReach = false
                }
              component =
                ResultGroupMetricSpecKt.componentMetricSetSpec {
                  nonCumulative =
                    ResultGroupMetricSpecKt.basicMetricSetSpec {
                      reach = true
                      percentReach = true
                      kPlusReach = 5
                      percentKPlusReach = true
                      averageFrequency = true
                      impressions = true
                      grps = true
                    }
                  cumulative =
                    ResultGroupMetricSpecKt.basicMetricSetSpec {
                      reach = true
                      percentReach = true
                    }
                  nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                  cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                }
            }
          }
        }
        basicReportId = "a1234"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(exception)
        .hasMessageThat()
        .contains(BasicReportsService.Permission.CREATE_WITH_DEV_MODEL_LINE)
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when basicReportId is missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report_id"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when basicReportId is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
      basicReportId = "1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report_id"
        }
      )
  }

  @Test
  fun `createBasicReport throws ALREADY_EXISTS when basicReport already exists`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
      basicReportId = "a1234"
    }

    val createdBasicReport =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }

    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.ALREADY_EXISTS)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.BASIC_REPORT_ALREADY_EXISTS.name
          metadata[Errors.Metadata.BASIC_REPORT.key] = createdBasicReport.name
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when requestId is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
      basicReportId = "a1234"
      requestId = "1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "request_id"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when campaignGroup is missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.campaign_group"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when campaignGroup is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT.copy { campaignGroup = "1234" }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.campaign_group"
        }
      )
  }

  @Test
  fun `createBasicReport throws FAILED_PRECONDITION when campaignGroup not found`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REPORTING_SET_NOT_FOUND.name
          metadata[Errors.Metadata.REPORTING_SET.key] = campaignGroupKey.toName()
        }
      )
  }

  @Test
  fun `createBasicReport throws FAILED_PRECONDITION when campaignGroup not campaignGroup`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              clearExternalCampaignGroupId()
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )
      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport = BASIC_REPORT.copy { campaignGroup = campaignGroupKey.toName() }
        basicReportId = "a1234"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.CAMPAIGN_GROUP_INVALID.name
            metadata[Errors.Metadata.REPORTING_SET.key] = request.basicReport.campaignGroup
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when modelLine is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          modelLine = "1234"
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.model_line"
        }
      )
  }

  @Test
  fun `createBasicReport throws FAILED_PRECONDITION when modelLine not part of valid list`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      val unusedModelLine = modelLine {
        name = "modelProviders/123/modelSuites/123/modelLines/unused"
        type = ModelLine.Type.PROD
      }
      whenever(modelLinesServiceMock.enumerateValidModelLines(any()))
        .thenReturn(enumerateValidModelLinesResponse { modelLines += unusedModelLine })

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val basicReport =
        BASIC_REPORT.copy {
          this.campaignGroup = campaignGroupKey.toName()
          reportingInterval = reportingInterval {
            reportStart = dateTime {
              year = 2025
              month = 7
              day = 3
              timeZone = timeZone { id = "America/Los_Angeles" }
            }
            reportEnd = date {
              year = 2026
              month = 1
              day = 5
            }
          }
          modelLine = ModelLineKey("a", "b", "c").toName()
        }

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.MODEL_LINE_NOT_ACTIVE.name
            metadata[Errors.Metadata.MODEL_LINE.key] = request.basicReport.modelLine
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportingInterval missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          clearReportingInterval()
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportingInterval reportStart missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            reportingInterval = this.reportingInterval.copy { clearReportStart() }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.reporting_interval.report_start"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportingInterval reportEnd missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            reportingInterval = this.reportingInterval.copy { clearReportEnd() }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_end"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportStart year missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          reportingInterval =
            this.reportingInterval.copy { reportStart = this.reportStart.copy { clearYear() } }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_start"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportStart month missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          reportingInterval =
            this.reportingInterval.copy { reportStart = this.reportStart.copy { clearMonth() } }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_start"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportStart day missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          reportingInterval =
            this.reportingInterval.copy { reportStart = this.reportStart.copy { clearDay() } }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_start"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportStart time zone missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            reportingInterval =
              this.reportingInterval.copy {
                reportStart = this.reportStart.copy { clearTimeZone() }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.reporting_interval.report_start"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportEnd year missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          reportingInterval =
            this.reportingInterval.copy { reportEnd = this.reportEnd.copy { clearYear() } }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_end"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportEnd month missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          reportingInterval =
            this.reportingInterval.copy { reportEnd = this.reportEnd.copy { clearMonth() } }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_end"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportEnd day missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          reportingInterval =
            this.reportingInterval.copy { reportEnd = this.reportEnd.copy { clearDay() } }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.reporting_interval.report_end"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting IQFs missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          impressionQualificationFilters.clear()
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.impression_qualification_filters"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting IQFs have invalid IQF name`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              impressionQualificationFilter = "1234"
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].impression_qualification_filter"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when IQFs have 2 custom filters`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          impressionQualificationFilters.clear()
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                  }
                }
              }
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                  }
                }
              }
          }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.impression_qualification_filters"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom same as non-custom`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          impressionQualificationFilters.clear()
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            impressionQualificationFilter =
              ImpressionQualificationFilterKey(MRC_IQF.externalImpressionQualificationFilterId)
                .toName()
          }
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                  }
                }
              }
          }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.impression_qualification_filters"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when IQFs have custom missing filter specs`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {}
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_spec"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom has 2 filter spec for media type`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { boolValue = true }
                      }
                    }
                  }
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { boolValue = true }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_specs"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec missing filters`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec filters missing terms`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.VIDEO
                    filters += eventFilter {}
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters[0].terms"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec filters terms no path`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.VIDEO
                    filters += eventFilter {
                      terms += eventTemplateField {
                        value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters[0].terms[0].path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec filters terms no value`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField { path = "banner_ad.viewable" }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters[0].terms[0].value"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting IQF no name nor custom`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {}
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].selector"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when result group specs missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          resultGroupSpecs.clear()
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.result_group_specs"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when result group specs reporting unit missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] = resultGroupSpecs[0].copy { clearReportingUnit() }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].reporting_unit"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit missing components`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] = resultGroupSpecs[0].copy { reportingUnit = reportingUnit {} }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].reporting_unit.components"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit component invalid format`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy { reportingUnit = reportingUnit { components += "1234" } }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].reporting_unit.components[0]"
          }
        )
    }

  @Test
  fun `createBasicReport throws FAILED_PRECONDITION when component not in campaign group`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                reportingUnit = reportingUnit { components += DataProviderKey("4321").toName() }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.DATA_PROVIDER_NOT_FOUND_FOR_CAMPAIGN_GROUP.name
            metadata[Errors.Metadata.REPORTING_SET.key] =
              "measurementConsumers/1234/reportingSets/1234"
            metadata[Errors.Metadata.DATA_PROVIDER.key] = "dataProviders/4321"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          resultGroupSpecs[0] = resultGroupSpecs[0].copy { clearDimensionSpec() }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.result_group_specs[0].dimension_spec"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec template fields missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    grouping = DimensionSpecKt.grouping {}
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].dimension_spec.grouping.event_template_fields"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec template field nonexistent`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.height" }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("exist")
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.height"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec templatefield not groupable`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    grouping =
                      DimensionSpecKt.grouping { eventTemplateFields += "banner_ad.viewable" }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("groupable")
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "banner_ad.viewable"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter 0 terms`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {}
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].dimension_spec.filters[2].terms"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter 2 terms`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_TO_18_TO_34" }
                      }
                      terms += eventTemplateField {
                        path = "person.gender"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].dimension_spec.filters[2].terms"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter term no path`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].dimension_spec.filters[2].terms[0].path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter term path not exist`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.height"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.height"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec filter term not filterable`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("filterable")
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "banner_ad.viewable"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec filter term in grouping`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    grouping =
                      DimensionSpecKt.grouping { eventTemplateFields += "person.age_group" }
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].dimension_spec.filters[0].terms[0]"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter term no value`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField { path = "person.age_group" }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].dimension_spec.filters[2].terms[0].value"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension string_value set for Duration`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "video_ad.length"
                        value = EventTemplateFieldKt.fieldValue { stringValue = "1s" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "video_ad.length"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension enum_value set for Duration`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "video_ad.length"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "1s" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "video_ad.length"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension bool_value set for Duration`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "video_ad.length"
                        value = EventTemplateFieldKt.fieldValue { boolValue = true }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "video_ad.length"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension float_value set for Duration`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "video_ad.length"
                        value = EventTemplateFieldKt.fieldValue { floatValue = 1.0f }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "video_ad.length"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec string_value set for Enum`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.gender"
                        value = EventTemplateFieldKt.fieldValue { stringValue = "MALE" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.gender"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when invalid dimension enum_value set for Enum`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.gender"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "dinosaur" }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.gender"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension bool_value set for Enum`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.gender"
                        value = EventTemplateFieldKt.fieldValue { boolValue = true }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.gender"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension float_value set for Enum`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                dimensionSpec =
                  BASIC_REPORT.resultGroupSpecsList[0].dimensionSpec.copy {
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.gender"
                        value = EventTemplateFieldKt.fieldValue { floatValue = 1f }
                      }
                    }
                  }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.gender"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF filter 0 terms`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          impressionQualificationFilters.clear()
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY
                  filters += eventFilter {}
                }
              }
          }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters[0].terms"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF filter 2 terms`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          impressionQualificationFilters.clear()
          impressionQualificationFilters += reportingImpressionQualificationFilter {
            custom =
              ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                filterSpec += impressionQualificationFilterSpec {
                  mediaType = MediaType.DISPLAY
                  filters += eventFilter {
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = true }
                    }
                    terms += eventTemplateField {
                      path = "banner_ad.viewable"
                      value = EventTemplateFieldKt.fieldValue { boolValue = false }
                    }
                  }
                }
              }
          }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters[0].terms"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF filter term path not exist`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.height"
                        value = EventTemplateFieldKt.fieldValue { boolValue = false }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("exist")
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "banner_ad.height"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF filter term not IQ`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "person.gender"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("impression qualification")
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "person.gender"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF field diff media type than spec`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.VIDEO
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { boolValue = true }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception).hasMessageThat().contains("media_type")
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters[0].custom.filter_specs[0].filters[0].terms[0].path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF string_value set for Bool`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { stringValue = "true" }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "banner_ad.viewable"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF enum_value set for Bool`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "TRUE" }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "banner_ad.viewable"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF float_value set for Bool`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.DISPLAY
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "banner_ad.viewable"
                        value = EventTemplateFieldKt.fieldValue { floatValue = 1f }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "banner_ad.viewable"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF bool_value set for Double`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.VIDEO
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "video_ad.viewed_fraction"
                        value = EventTemplateFieldKt.fieldValue { boolValue = true }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "video_ad.viewed_fraction"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom IQF float_value set for Double`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              custom =
                ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
                  filterSpec += impressionQualificationFilterSpec {
                    mediaType = MediaType.VIDEO
                    filters += eventFilter {
                      terms += eventTemplateField {
                        path = "video_ad.viewed_fraction"
                        value = EventTemplateFieldKt.fieldValue { floatValue = 1f }
                      }
                    }
                  }
                }
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.EVENT_TEMPLATE_FIELD_INVALID.name
            metadata[Errors.Metadata.EVENT_TEMPLATE_FIELD_PATH.key] = "video_ad.viewed_fraction"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when result group metric spec missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] = resultGroupSpecs[0].copy { clearResultGroupMetricSpec() }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec"
          }
        )
    }

  @Test
  fun `createBasicReport throws UNIMPLEMENTED when component intersection set`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          resultGroupSpecs[0] =
            resultGroupSpecs[0].copy {
              resultGroupMetricSpec = resultGroupMetricSpec {
                componentIntersection =
                  ResultGroupMetricSpecKt.componentIntersectionMetricSetSpec {}
              }
            }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.UNIMPLEMENTED)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.FIELD_UNIMPLEMENTED.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.result_group_specs[0].result_group_metric_spec.component_intersection"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when metric frequency not set`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
          }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val request = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      basicReport =
        BASIC_REPORT.copy {
          campaignGroup = campaignGroupKey.toName()
          resultGroupSpecs[0] = resultGroupSpecs[0].copy { clearMetricFrequency() }
        }
      basicReportId = "a1234"
    }
    val exception =
      assertFailsWith<StatusRuntimeException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.result_group_specs[0].metric_frequency"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit non cumulative with total`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { total = true }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec {}
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.reporting_unit.non_cumulative"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component non cumulative with total`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { total = true }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec {}
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.component.non_cumulative"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component non cumulative unique with total`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { total = true }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      nonCumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.component.non_cumulative_unique"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportingunit non cumulative 0 kplusReach`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      nonCumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          kPlusReach = 0
                          percentKPlusReach = true
                        }
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.reporting_unit.non_cumulative.k_plus_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when stacked reach set with weekly`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      stackedIncrementalReach = true
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.reporting_unit.stacked_incremental_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws UNIMPLEMENTED when reportingunit cumulative weekly impressions`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  reportingUnit =
                    ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                      cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.UNIMPLEMENTED)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.FIELD_UNIMPLEMENTED.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.reporting_unit.cumulative"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component non cumulative 0 kplusReach`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      nonCumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          kPlusReach = 0
                          percentKPlusReach = true
                        }
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.component.non_cumulative.k_plus_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws UNIMPLEMENTED when component weekly cumulative has impressions`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            resultGroupSpecs[0] =
              resultGroupSpecs[0].copy {
                metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                resultGroupMetricSpec = resultGroupMetricSpec {
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      cumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { impressions = true }
                    }
                }
              }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.UNIMPLEMENTED)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.FIELD_UNIMPLEMENTED.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs[0].result_group_metric_spec.component.cumulative"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting IQFs IQF not found`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
              externalCampaignGroupId = campaignGroupKey.reportingSetId
            }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val badIQF = ImpressionQualificationFilterKey("junk").toName()

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport =
          BASIC_REPORT.copy {
            campaignGroup = campaignGroupKey.toName()
            impressionQualificationFilters.clear()
            impressionQualificationFilters += reportingImpressionQualificationFilter {
              impressionQualificationFilter = badIQF
            }
          }
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception).status().code().isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception)
        .errorInfo()
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER.key] = badIQF
          }
        )
    }

  @Test
  fun `getBasicReport with createBasicReport returns basic report when found`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

    measurementConsumersService.createMeasurementConsumer(
      measurementConsumer {
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
      }
    )

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet = internalReportingSet {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          externalCampaignGroupId = campaignGroupKey.reportingSetId
          displayName = "displayName"
          primitive =
            ReportingSetKt.primitive {
              eventGroupKeys +=
                ReportingSetKt.PrimitiveKt.eventGroupKey {
                  cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                  cmmsEventGroupId = "1235"
                }
            }
        }
        externalReportingSetId = campaignGroupKey.reportingSetId
      }
    )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroupKey.toName()
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2026
          month = 1
          day = 5
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        impressionQualificationFilter =
          ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId).toName()
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
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
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              stackedIncrementalReach = false
            }
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
            }
        }
      }
    }

    val createBasicReportRequest = createBasicReportRequest {
      parent = measurementConsumerKey.toName()
      this.basicReport = basicReport
      basicReportId = "a1234"
    }

    val createdBasicReport =
      withPrincipalAndScopes(PRINCIPAL, SCOPES) {
        service.createBasicReport(createBasicReportRequest)
      }

    val request = getBasicReportRequest { name = createdBasicReport.name }

    val response = withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.getBasicReport(request) }

    assertThat(response).isEqualTo(createdBasicReport)
  }

  @Test
  fun `getBasicReport with createBasicReport with model line returns basic report when found`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID)
      val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "1234")

      val specifiedModelLine = modelLine {
        name = ModelLineKey("1234", "1234", "1234").toName()
        type = ModelLine.Type.PROD
      }
      whenever(modelLinesServiceMock.enumerateValidModelLines(any()))
        .thenReturn(enumerateValidModelLinesResponse { modelLines += specifiedModelLine })

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer {
          cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
        }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet = internalReportingSet {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
            externalCampaignGroupId = campaignGroupKey.reportingSetId
            displayName = "displayName"
            primitive =
              ReportingSetKt.primitive {
                eventGroupKeys +=
                  ReportingSetKt.PrimitiveKt.eventGroupKey {
                    cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
                    cmmsEventGroupId = "1235"
                  }
              }
          }
          externalReportingSetId = campaignGroupKey.reportingSetId
        }
      )

      val basicReport = basicReport {
        title = "title"
        this.campaignGroup = campaignGroupKey.toName()
        reportingInterval = reportingInterval {
          reportStart = dateTime {
            year = 2025
            month = 7
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2026
            month = 1
            day = 5
          }
        }
        modelLine = specifiedModelLine.name
        impressionQualificationFilters += reportingImpressionQualificationFilter {
          impressionQualificationFilter =
            ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId)
              .toName()
        }
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
          metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
          dimensionSpec = dimensionSpec {}
          resultGroupMetricSpec = resultGroupMetricSpec {
            populationSize = true
            reportingUnit =
              ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
            component =
              ResultGroupMetricSpecKt.componentMetricSetSpec {
                nonCumulative = ResultGroupMetricSpecKt.basicMetricSetSpec { reach = true }
              }
          }
        }
      }

      val createBasicReportRequest = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        this.basicReport = basicReport
        basicReportId = "a1234"
      }

      val createdBasicReport =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.createBasicReport(createBasicReportRequest)
        }

      assertThat(createdBasicReport.modelLine).isEqualTo(basicReport.modelLine)
      assertThat(createdBasicReport.effectiveModelLine).isEqualTo(createdBasicReport.modelLine)

      val response =
        withPrincipalAndScopes(PRINCIPAL, SCOPES) {
          service.getBasicReport(getBasicReportRequest { name = createdBasicReport.name })
        }

      assertThat(response).isEqualTo(createdBasicReport)
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

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalCampaignGroupId = reportingSetId
          }
        externalReportingSetId = reportingSetId
      }
    )

    var internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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
                      groupings += internalEventTemplateField {
                        path = "person.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "person.age_group"
                          value =
                            InternalEventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                        }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "person.social_grade_group"
                          value = InternalEventTemplateFieldKt.fieldValue { enumValue = "A_B_C1" }
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
                          nonCumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
                          cumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
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
                          nonCumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 10 }
                          cumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 10 }
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
                      groupings += internalEventTemplateField {
                        path = "person.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "person.age_group"
                          value =
                            InternalEventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
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
                          nonCumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
                          cumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
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
                      groupings += internalEventTemplateField {
                        path = "person.gender"
                        value = InternalEventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                      }

                      filters += internalEventFilter {
                        terms += internalEventTemplateField {
                          path = "person.age_group"
                          value =
                            InternalEventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
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
                          nonCumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
                          cumulativeUnique =
                            InternalResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
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
          campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          state = BasicReport.State.SUCCEEDED

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
          effectiveImpressionQualificationFilters += impressionQualificationFilters

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
                        groupings += eventTemplateField {
                          path = "person.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "person.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
                          }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "person.social_grade_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "A_B_C1" }
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
                            nonCumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
                            cumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
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
                            nonCumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 10 }
                            cumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 10 }
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
                        groupings += eventTemplateField {
                          path = "person.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "person.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
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
                            nonCumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
                            cumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
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
                        groupings += eventTemplateField {
                          path = "person.gender"
                          value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
                        }

                        filters += eventFilter {
                          terms += eventTemplateField {
                            path = "person.age_group"
                            value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
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
                            nonCumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
                            cumulativeUnique =
                              ResultGroupKt.MetricSetKt.uniqueMetricSet { reach = 5 }
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

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
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

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
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

    assertThat(exception).status().code().isEqualTo(Status.Code.NOT_FOUND)
    assertThat(exception)
      .errorInfo()
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

      assertThat(exception).status().code().isEqualTo(Status.Code.PERMISSION_DENIED)
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

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalCampaignGroupId = reportingSetId
          }
        externalReportingSetId = reportingSetId
      }
    )

    var internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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
          campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          createTime = internalBasicReport.createTime
          state = BasicReport.State.SUCCEEDED
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

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              this.externalCampaignGroupId = reportingSetId
            }
          externalReportingSetId = reportingSetId
        }
      )

      var internalBasicReport = internalBasicReport {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalBasicReportId = basicReportId
        externalCampaignGroupId = reportingSetId
        campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalCampaignGroupId = reportingSetId
          }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalCampaignGroupId = reportingSetId
          }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalCampaignGroupId = reportingSetId
          }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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
          campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          createTime = internalBasicReport1.createTime
          state = BasicReport.State.SUCCEEDED
        }
      )
    assertThat(listBasicReportsResponse.nextPageToken)
      .isEqualTo(
        listBasicReportsPageToken {
            lastBasicReport =
              ListBasicReportsPageTokenKt.previousPageEnd {
                createTime = internalBasicReport1.createTime
                externalBasicReportId = internalBasicReport1.externalBasicReportId
                this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              }
          }
          .toByteString()
          .base64UrlEncode()
      )
  }

  @Test
  fun `listBasicReports with createTimeAfter returns next token after prev token used`(): Unit =
    runBlocking {
      val cmmsMeasurementConsumerId = "1234"
      val reportingSetId = "4322"
      val basicReportId = "4321"

      measurementConsumersService.createMeasurementConsumer(
        measurementConsumer { this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId }
      )

      internalReportingSetsService.createReportingSet(
        createReportingSetRequest {
          reportingSet =
            INTERNAL_CAMPAIGN_GROUP.copy {
              this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
              this.externalCampaignGroupId = reportingSetId
            }
          externalReportingSetId = reportingSetId
        }
      )

      val internalBasicReport = internalBasicReport {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        externalBasicReportId = basicReportId
        externalCampaignGroupId = reportingSetId
        campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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
              filter =
                ListBasicReportsRequestKt.filter { createTimeAfter = timestamp { seconds = 1 } }
              pageToken =
                listBasicReportsPageToken {
                    lastBasicReport =
                      ListBasicReportsPageTokenKt.previousPageEnd {
                        createTime = timestamp { seconds = 5 }
                        externalBasicReportId = "1234"
                        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
                      }
                  }
                  .toByteString()
                  .base64UrlEncode()
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
            campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
            title = "title"
            reportingInterval = reportingInterval {
              reportStart = dateTime { day = 3 }
              reportEnd = date { day = 5 }
            }
            createTime = internalBasicReport1.createTime
            state = BasicReport.State.SUCCEEDED
          }
        )
      assertThat(listBasicReportsResponse.nextPageToken)
        .isEqualTo(
          listBasicReportsPageToken {
              lastBasicReport =
                ListBasicReportsPageTokenKt.previousPageEnd {
                  createTime = internalBasicReport1.createTime
                  externalBasicReportId = internalBasicReport1.externalBasicReportId
                  this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
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

    internalReportingSetsService.createReportingSet(
      createReportingSetRequest {
        reportingSet =
          INTERNAL_CAMPAIGN_GROUP.copy {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            this.externalCampaignGroupId = reportingSetId
          }
        externalReportingSetId = reportingSetId
      }
    )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      externalBasicReportId = basicReportId
      externalCampaignGroupId = reportingSetId
      campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
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
          campaignGroupDisplayName = INTERNAL_CAMPAIGN_GROUP.displayName
          title = "title"
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
          }
          createTime = internalBasicReport2.createTime
          state = BasicReport.State.SUCCEEDED
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

    assertThat(exception).status().code().isEqualTo(Status.Code.PERMISSION_DENIED)
    assertThat(exception).hasMessageThat().contains(BasicReportsService.Permission.LIST)
  }

  @Test
  fun `listBasicReports throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val request = listBasicReportsRequest { pageSize = 5 }
    val exception = assertFailsWith<StatusRuntimeException> { service.listBasicReports(request) }

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
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

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
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

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
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

    assertThat(exception).status().code().isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception)
      .errorInfo()
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "page_token"
        }
      )
  }

  companion object {
    @get:ClassRule @JvmStatic val spannerEmulator = SpannerEmulatorRule()

    @get:ClassRule
    @JvmStatic
    val postgresDatabaseProvider =
      PostgresDatabaseProviderRule(PostgresSchemata.REPORTING_CHANGELOG_PATH)

    private val TEST_EVENT_DESCRIPTOR = EventMessageDescriptor(TestEvent.getDescriptor())

    private val SECRETS_DIR =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private val METRIC_SPEC_CONFIG: MetricSpecConfig =
      parseTextProto(
        SECRETS_DIR.resolve("basic_report_metric_spec_config.textproto"),
        MetricSpecConfig.getDefaultInstance(),
      )

    private const val CMMS_MEASUREMENT_CONSUMER_ID = "1234"

    private val CONFIG = measurementConsumerConfig {
      apiKey = "api_key_1234"
      signingCertificateName =
        MeasurementConsumerCertificateKey(CMMS_MEASUREMENT_CONSUMER_ID, "1234").toName()
      signingPrivateKeyPath = "mc_cs_private.der"
    }

    private val MEASUREMENT_CONSUMER_CONFIGS = measurementConsumerConfigs {
      configs[MeasurementConsumerKey(CMMS_MEASUREMENT_CONSUMER_ID).toName()] = CONFIG
    }

    private const val DEFAULT_PAGE_SIZE = 10
    private const val MAX_PAGE_SIZE = 25

    private val ALL_PERMISSIONS =
      setOf(
        BasicReportsService.Permission.GET,
        BasicReportsService.Permission.LIST,
        BasicReportsService.Permission.CREATE,
        BasicReportsService.Permission.CREATE_WITH_DEV_MODEL_LINE,
      )
    private val SCOPES = ALL_PERMISSIONS
    private val PRINCIPAL = principal { name = "principals/mc-user" }

    private val DATA_PROVIDER_KEY = DataProviderKey("1234")

    private val INTERNAL_CAMPAIGN_GROUP = internalReportingSet {
      displayName = "displayName"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
              cmmsEventGroupId = "1235"
            }
        }
    }

    private val INTERNAL_AMI_IQF = internalImpressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalImpressionQualificationFilterSpec.MediaType.DISPLAY
        filters += internalEventFilter {
          terms += internalEventTemplateField {
            path = "banner_ad.viewable"
            value = InternalEventTemplateFieldKt.fieldValue { boolValue = false }
          }
        }
      }
    }

    private val INTERNAL_MRC_IQF = internalImpressionQualificationFilter {
      externalImpressionQualificationFilterId = "mrc"
      filterSpecs += internalImpressionQualificationFilterSpec {
        mediaType = InternalImpressionQualificationFilterSpec.MediaType.DISPLAY
        filters += internalEventFilter {
          terms += internalEventTemplateField {
            path = "banner_ad.viewable"
            value = InternalEventTemplateFieldKt.fieldValue { boolValue = true }
          }
        }
      }
    }

    private val AMI_IQF = impressionQualificationFilter {
      externalImpressionQualificationFilterId = "ami"
      impressionQualificationFilterId = 1
      filterSpecs +=
        ImpressionQualificationFilterConfigKt.impressionQualificationFilterSpec {
          mediaType = ImpressionQualificationFilterSpec.MediaType.DISPLAY
          filters +=
            ImpressionQualificationFilterConfigKt.eventFilter {
              terms +=
                ImpressionQualificationFilterConfigKt.eventTemplateField {
                  path = "banner_ad.viewable"
                  value = fieldValue { boolValue = false }
                }
            }
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
                  path = "banner_ad.viewable"
                  value = fieldValue { boolValue = true }
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

    private val BASIC_REPORT = basicReport {
      title = "title"
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2025
          month = 7
          day = 3
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2026
          month = 1
          day = 5
        }
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        impressionQualificationFilter =
          ImpressionQualificationFilterKey(AMI_IQF.externalImpressionQualificationFilterId).toName()
      }
      impressionQualificationFilters += reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec {
              mediaType = MediaType.DISPLAY
              filters += eventFilter {
                terms += eventTemplateField {
                  path = "banner_ad.viewable"
                  value = EventTemplateFieldKt.fieldValue { boolValue = true }
                }
              }
            }
          }
      }
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.social_grade_group" }
          filters += eventFilter {
            terms += eventTemplateField {
              path = "person.age_group"
              value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_18_TO_34" }
            }
          }
          filters += eventFilter {
            terms += eventTemplateField {
              path = "person.gender"
              value = EventTemplateFieldKt.fieldValue { enumValue = "MALE" }
            }
          }
        }
        resultGroupMetricSpec = resultGroupMetricSpec {
          populationSize = true
          reportingUnit =
            ResultGroupMetricSpecKt.reportingUnitMetricSetSpec {
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              stackedIncrementalReach = false
            }
          component =
            ResultGroupMetricSpecKt.componentMetricSetSpec {
              nonCumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              cumulative =
                ResultGroupMetricSpecKt.basicMetricSetSpec {
                  reach = true
                  percentReach = true
                }
              nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
            }
        }
      }
    }
  }
}
