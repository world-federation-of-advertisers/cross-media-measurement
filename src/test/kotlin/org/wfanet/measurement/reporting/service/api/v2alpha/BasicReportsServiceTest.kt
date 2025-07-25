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
import com.google.type.timeZone
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.StatusRuntimeException
import java.time.Clock
import kotlin.random.Random
import kotlin.test.assertFailsWith
import kotlinx.coroutines.flow.toList
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
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
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
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub as InternalImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt
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
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec as internalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter as internalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval as internalReportingInterval
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.internal.reporting.v2.streamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.common.service.ImpressionQualificationFiltersService
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.SpannerBasicReportsService
import org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.testing.Schemata
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresMeasurementConsumersService
import org.wfanet.measurement.reporting.deploy.v2.postgres.PostgresReportingSetsService
import org.wfanet.measurement.reporting.deploy.v2.postgres.testing.Schemata as PostgresSchemata
import com.google.protobuf.TypeRegistry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Video
import org.wfanet.measurement.reporting.service.api.Errors
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ListBasicReportsRequestKt
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listBasicReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroup
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec

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
    addService(ImpressionQualificationFiltersService(IMPRESSION_QUALIFICATION_FILTER_MAPPING))
  }

  @get:Rule
  val serverRuleChain: TestRule = chainRulesSequentially(spannerDatabase, grpcTestServerRule)

  private lateinit var authorization: Authorization
  private lateinit var measurementConsumersService: MeasurementConsumersCoroutineStub
  private lateinit var internalImpressionQualificationFiltersService:
    InternalImpressionQualificationFiltersCoroutineStub
  private lateinit var internalReportingSetsService: InternalReportingSetsCoroutineStub
  private lateinit var internalBasicReportsService: InternalBasicReportsCoroutineStub

  private lateinit var service: BasicReportsService

  @Before
  fun initService() {
    measurementConsumersService = MeasurementConsumersCoroutineStub(grpcTestServerRule.channel)
    internalImpressionQualificationFiltersService =
      InternalImpressionQualificationFiltersCoroutineStub(grpcTestServerRule.channel)
    internalReportingSetsService = InternalReportingSetsCoroutineStub(grpcTestServerRule.channel)
    internalBasicReportsService = InternalBasicReportsCoroutineStub(grpcTestServerRule.channel)
    authorization =
      Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(grpcTestServerRule.channel))
    val typeRegistry = TypeRegistry.newBuilder().add(listOf(TestEvent.getDescriptor(), Person.getDescriptor(), Banner.getDescriptor(), Video.getDescriptor())).build()

    service =
      BasicReportsService(
        internalBasicReportsService,
        internalImpressionQualificationFiltersService,
        internalReportingSetsService,
        BasicReportsService.buildEventTemplateFieldsMap(typeRegistry.find(TestEvent.getDescriptor().fullName)),
        authorization,
      )
  }

  @Test
  fun `createBasicReport creates new primitive reportingsets only when needed`(): Unit =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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
              filter = "filter"

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

      val existingReportingSets =
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

      assertThat(existingReportingSets).containsExactly(campaignGroup)

      val request = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport = BASIC_REPORT.copy { this.campaignGroup = campaignGroupKey.toName() }
        basicReportId = "a1234"
      }

      assertFailsWith<StatusException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
      }

      val updatedReportingSets =
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

      val request2 = createBasicReportRequest {
        parent = measurementConsumerKey.toName()
        basicReport = BASIC_REPORT.copy { this.campaignGroup = campaignGroupKey.toName() }
        basicReportId = "b1234"
      }

      assertFailsWith<StatusException> {
        withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request2) }
      }

      val identicalReportingSets =
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

      assertThat(identicalReportingSets).hasSize(3)
      assertThat(updatedReportingSets).containsExactlyElementsIn(identicalReportingSets)
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when parent is missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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
  fun `createBasicReport throws INVALID_ARGUMENT when parent is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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
  fun `createBasicReport throws PERMISSION_DENIED when caller does not have permission`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.PERMISSION_DENIED)
      assertThat(exception).hasMessageThat().contains(BasicReportsService.Permission.CREATE)
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when basicReportId is missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.INVALID_FIELD_VALUE.name
          metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report_id"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when requestId is invalid`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")

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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REPORTING_SET_NOT_FOUND.name
          metadata[Errors.Metadata.REPORTING_SET.key] = campaignGroupKey.toName()
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when campaignGroup not campaignGroup`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] = "basic_report.campaign_group"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportingInterval missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
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
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.impression_qualification_filter"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when IQFs have custom missing filter specs`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.custom.filter_spec"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom has 2 filter spec for media type`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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
                        path = "common.age_group"
                        value = EventTemplateFieldKt.fieldValue { enumValue = "18_TO_35" }
                      }
                    }
                  }
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
        basicReportId = "a1234"
      }
      val exception =
        assertFailsWith<StatusRuntimeException> {
          withPrincipalAndScopes(PRINCIPAL, SCOPES) { service.createBasicReport(request) }
        }

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.custom.filter_spec"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec missing filters`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.custom.filter_spec.filters"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec filters missing terms`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec filters terms no path`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms.path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when custom filter spec filters terms no value`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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
                      terms += eventTemplateField { path = "common.age_group" }
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
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.custom.filter_spec.filters.terms.value"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting IQF no name nor custom`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.impression_qualification_filters.selector"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when result group specs missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
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
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.reporting_unit"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit missing components`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.reporting_unit.components"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit component invalid format`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.reporting_unit.components"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component not in campaign group`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.reporting_unit.components"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec missing`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.result_group_specs.dimension_spec"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec template fields missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec template field nonexistent`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("exist")
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec templatefield not groupable`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("groupable")
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.grouping.event_template_fields"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter 0 terms`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter 2 terms`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter term no path`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms.path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter term path not exist`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("exist")
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms.path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec filter term not filterable`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("filterable")
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms.path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec filter term not pop attr`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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
                        path = "video.length"
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.message).contains("population")
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms.path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimensionspec filter term in grouping`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms.path"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when dimension spec filter term no value`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.dimension_spec.filters.terms.value"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when result group metric spec missing`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec"
          }
        )
    }

  @Test
  fun `createBasicReport throws UNIMPLEMENTED when component intersection set`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.UNIMPLEMENTED)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.FIELD_UNIMPLEMENTED.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.result_group_specs.result_group_metric_spec.component_intersection"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when metric frequency not set`() = runBlocking {
    val measurementConsumerKey = MeasurementConsumerKey("1234")
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

    assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
    assertThat(exception.errorInfo)
      .isEqualTo(
        errorInfo {
          domain = Errors.DOMAIN
          reason = Errors.Reason.REQUIRED_FIELD_NOT_SET.name
          metadata[Errors.Metadata.FIELD_NAME.key] =
            "basic_report.result_group_specs.metric_frequency"
        }
      )
  }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit non cumulative with total`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.non_cumulative"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component non cumulative with total`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component non cumulative unique with total`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative_unique"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reportingunit non cumulative 0 kplusReach`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.non_cumulative.k_plus_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting unit cumulative 0 kplusReach`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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
                      cumulative =
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.cumulative.k_plus_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when stacked reach set with weekly`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.reporting_unit.stacked_incremental_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component non cumulative 0 kplusReach`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.component.non_cumulative.k_plus_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when component cumulative 0 kplusReach`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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
                      cumulative =
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

      assertThat(exception.status.code).isEqualTo(Status.Code.INVALID_ARGUMENT)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.INVALID_FIELD_VALUE.name
            metadata[Errors.Metadata.FIELD_NAME.key] =
              "basic_report.result_group_specs.result_group_metric_spec.component.cumulative.k_plus_reach"
          }
        )
    }

  @Test
  fun `createBasicReport throws INVALID_ARGUMENT when reporting IQFs IQF not found`() =
    runBlocking {
      val measurementConsumerKey = MeasurementConsumerKey("1234")
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

      assertThat(exception.status.code).isEqualTo(Status.Code.FAILED_PRECONDITION)
      assertThat(exception.errorInfo)
        .isEqualTo(
          errorInfo {
            domain = Errors.DOMAIN
            reason = Errors.Reason.IMPRESSION_QUALIFICATION_FILTER_NOT_FOUND.name
            metadata[Errors.Metadata.IMPRESSION_QUALIFICATION_FILTER.key] = badIQF
          }
        )
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
                            value =
                              EventTemplateFieldKt.fieldValue { enumValue = "YEARS_TO_18_TO_34" }
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
      setOf(
        BasicReportsService.Permission.GET,
        BasicReportsService.Permission.LIST,
        BasicReportsService.Permission.CREATE,
      )
    private val SCOPES = ALL_PERMISSIONS
    private val PRINCIPAL = principal { name = "principals/mc-user" }

    private val DATA_PROVIDER_KEY = DataProviderKey("1234")

    private val INTERNAL_CAMPAIGN_GROUP = internalReportingSet {
      displayName = "displayName"
      filter = "filter"

      primitive =
        ReportingSetKt.primitive {
          eventGroupKeys +=
            ReportingSetKt.PrimitiveKt.eventGroupKey {
              cmmsDataProviderId = DATA_PROVIDER_KEY.dataProviderId
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
      resultGroupSpecs += resultGroupSpec {
        title = "title"
        reportingUnit = reportingUnit { components += DATA_PROVIDER_KEY.toName() }
        metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
        dimensionSpec = dimensionSpec {
          grouping = DimensionSpecKt.grouping { eventTemplateFields += "person.gender" }
          filters += eventFilter {
            terms += eventTemplateField {
              path = "person.age_group"
              value = EventTemplateFieldKt.fieldValue { enumValue = "YEARS_TO_18_TO_34" }
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
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
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
                  kPlusReach = 5
                  percentKPlusReach = true
                  averageFrequency = true
                  impressions = true
                  grps = true
                }
              nonCumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
              cumulativeUnique = ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
            }
        }
      }
    }
  }
}
