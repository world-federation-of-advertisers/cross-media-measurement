/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.protobuf.timestamp
import com.google.type.DayOfWeek
import com.google.type.Interval
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import java.io.File
import java.nio.file.Paths
import java.time.LocalDate
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
import org.wfanet.measurement.access.service.PermissionKey
import org.wfanet.measurement.access.v1alpha.PoliciesGrpc
import org.wfanet.measurement.access.v1alpha.PolicyKt
import org.wfanet.measurement.access.v1alpha.PrincipalKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpc
import org.wfanet.measurement.access.v1alpha.RolesGrpc
import org.wfanet.measurement.access.v1alpha.createPolicyRequest
import org.wfanet.measurement.access.v1alpha.createPrincipalRequest
import org.wfanet.measurement.access.v1alpha.createRoleRequest
import org.wfanet.measurement.access.v1alpha.policy
import org.wfanet.measurement.access.v1alpha.principal
import org.wfanet.measurement.access.v1alpha.role
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.batchGetEventGroupMetadataDescriptorsRequest
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.testing.MeasurementResultSubject.Companion.assertThat
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInterval
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.integration.common.ALL_EDP_WITHOUT_HMSS_CAPABILITIES_DISPLAY_NAMES
import org.wfanet.measurement.integration.common.ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt as InternalEventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilterSpec as InternalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.ResultGroupKt as InternalResultGroupKt
import org.wfanet.measurement.internal.reporting.v2.basicReport as internalBasicReport
import org.wfanet.measurement.internal.reporting.v2.basicReportDetails
import org.wfanet.measurement.internal.reporting.v2.basicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.eventFilter as internalEventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField as internalEventTemplateField
import org.wfanet.measurement.internal.reporting.v2.impressionQualificationFilterSpec as internalImpressionQualificationFilterSpec
import org.wfanet.measurement.internal.reporting.v2.insertBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec as internalMetricFrequencySpec
import org.wfanet.measurement.internal.reporting.v2.reportingImpressionQualificationFilter as internalReportingImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.reportingInterval as internalReportingInterval
import org.wfanet.measurement.internal.reporting.v2.resultGroup as internalResultGroup
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroupKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.getImpressionQualificationFilterRequest
import org.wfanet.measurement.reporting.v2alpha.getMetricRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.getReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.invalidateMetricRequest
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersRequest
import org.wfanet.measurement.reporting.v2alpha.listImpressionQualificationFiltersResponse
import org.wfanet.measurement.reporting.v2alpha.listMetricsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.resultGroup
import org.wfanet.measurement.reporting.v2alpha.timeIntervals
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAReportIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<
      (
        String, ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub,
      ) -> InProcessDuchy.DuchyDependencies
    >,
  accessServicesFactory: AccessServicesFactory,
  reportingDataServicesProviderRule: ProviderRule<Services>,
) {
  private val inProcessCmmsComponents: InProcessCmmsComponents =
    InProcessCmmsComponents(kingdomDataServicesRule, duchyDependenciesRule, useEdpSimulators = true)

  private val inProcessCmmsComponentsStartup = TestRule { base, _ ->
    object : Statement() {
      override fun evaluate() {
        inProcessCmmsComponents.startDaemons()
        try {
          base.evaluate()
        } finally {
          inProcessCmmsComponents.stopDaemons()
        }
      }
    }
  }

  private val reportingServerRule =
    object : TestRule {
      lateinit var reportingServer: InProcessReportingServer
        private set

      private fun buildReportingServer(): InProcessReportingServer {
        val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
        val measurementConsumer = runBlocking {
          publicKingdomMeasurementConsumersClient
            .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
            .getMeasurementConsumer(
              getMeasurementConsumerRequest { name = measurementConsumerData.name }
            )
        }

        val encryptionKeyPairConfig = encryptionKeyPairConfig {
          principalKeyPairs += principalKeyPairs {
            principal = measurementConsumerData.name
            keyPairs += keyPair {
              publicKeyFile = "mc_enc_public.tink"
              privateKeyFile = "mc_enc_private.tink"
            }
          }
        }
        val measurementConsumerConfig = measurementConsumerConfig {
          apiKey = measurementConsumerData.apiAuthenticationKey
          signingCertificateName = measurementConsumer.certificate
          signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
        }

        return InProcessReportingServer(
          reportingDataServicesProviderRule.value,
          accessServicesFactory,
          inProcessCmmsComponents.kingdom.publicApiChannel,
          encryptionKeyPairConfig,
          SECRETS_DIR,
          measurementConsumerConfig,
          TRUSTED_CERTIFICATES,
          inProcessCmmsComponents.kingdom.knownEventGroupMetadataTypes,
          verboseGrpcLogging = false,
        )
      }

      override fun apply(base: Statement, description: Description): Statement {
        return object : Statement() {
          override fun evaluate() {
            reportingServer = buildReportingServer()
            reportingServer.apply(base, description).evaluate()
          }
        }
      }
    }

  private val reportingServer: InProcessReportingServer
    get() = reportingServerRule.reportingServer

  @get:Rule
  val ruleChain: TestRule =
    chainRulesSequentially(
      inProcessCmmsComponents,
      inProcessCmmsComponentsStartup,
      reportingDataServicesProviderRule,
      reportingServerRule,
    )

  private lateinit var credentials: TrustedPrincipalAuthInterceptor.Credentials

  @Before
  fun createAccessPolicy() {
    val measurementConsumerData: MeasurementConsumerData =
      inProcessCmmsComponents.getMeasurementConsumerData()
    val accessChannel = reportingServer.accessChannel

    val rolesStub = RolesGrpc.newBlockingStub(accessChannel)
    val mcResourceType = "halo.wfanet.org/MeasurementConsumer"
    val mcUserRole =
      rolesStub.createRole(
        createRoleRequest {
          roleId = "mcUser"
          role = role {
            resourceTypes += mcResourceType
            permissions +=
              PERMISSIONS_CONFIG.permissionsMap
                .filterValues { it.protectedResourceTypesList.contains(mcResourceType) }
                .keys
                .map { PermissionKey(it).toName() }
          }
        }
      )
    val rootResourceType = "reporting.halo-cmm.org/Root"
    val kingdomUserRole =
      rolesStub.createRole(
        createRoleRequest {
          roleId = "kingdomUser"
          role = role {
            resourceTypes += rootResourceType
            permissions +=
              PERMISSIONS_CONFIG.permissionsMap
                .filterValues { it.protectedResourceTypesList.contains(rootResourceType) }
                .keys
                .map { PermissionKey(it).toName() }
          }
        }
      )

    val principalsStub = PrincipalsGrpc.newBlockingStub(accessChannel)
    val principal =
      principalsStub.createPrincipal(
        createPrincipalRequest {
          principalId = "mc-user"
          this.principal = principal {
            user =
              PrincipalKt.oAuthUser {
                issuer = "example.com"
                subject = "mc-user@example.com"
              }
          }
        }
      )

    val policiesStub = PoliciesGrpc.newBlockingStub(accessChannel)
    policiesStub.createPolicy(
      createPolicyRequest {
        policyId = "test-mc-policy"
        policy = policy {
          protectedResource = measurementConsumerData.name
          bindings +=
            PolicyKt.binding {
              this.role = mcUserRole.name
              members += principal.name
            }
        }
      }
    )
    policiesStub.createPolicy(
      createPolicyRequest {
        policyId = "test-root-policy"
        policy = policy {
          protectedResource = "" // Root
          bindings +=
            PolicyKt.binding {
              this.role = kingdomUserRole.name
              members += principal.name
            }
        }
      }
    )

    credentials = TrustedPrincipalAuthInterceptor.Credentials(principal, setOf("reporting.*"))
  }

  private val publicKingdomMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicEventGroupMetadataDescriptorsClient by lazy {
    EventGroupMetadataDescriptorsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicMetricCalculationSpecsClient by lazy {
    MetricCalculationSpecsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicMetricsClient by lazy { MetricsCoroutineStub(reportingServer.publicApiChannel) }

  private val publicReportsClient by lazy { ReportsCoroutineStub(reportingServer.publicApiChannel) }

  private val publicReportingSetsClient by lazy {
    ReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicBasicReportsClient by lazy {
    BasicReportsCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicImpressionQualificationFiltersClient by lazy {
    ImpressionQualificationFiltersCoroutineStub(reportingServer.publicApiChannel)
  }

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  suspend fun listMeasurements(): List<Measurement> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    val measurements: List<Measurement> = runBlocking {
      publicMeasurementsClient
        .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
        .listMeasurements(listMeasurementsRequest { parent = measurementConsumerData.name })
        .measurementsList
    }

    return measurements
  }

  @Test
  fun `reporting set is created and then retrieved`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "composite"
      primitive = ReportingSetKt.primitive { cmmsEventGroups.add(eventGroups[0].cmmsEventGroup) }
    }

    val createdPrimitiveReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = primitiveReportingSet
            reportingSetId = "def"
          }
        )

    val retrievedPrimitiveReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .getReportingSet(getReportingSetRequest { name = createdPrimitiveReportingSet.name })

    assertThat(createdPrimitiveReportingSet).isEqualTo(retrievedPrimitiveReportingSet)
  }

  @Test
  fun `report with LLv2 union reach across 2 edps has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups: List<EventGroup> = listEventGroups()

    val llv2EventGroups: List<EventGroup> =
      eventGroups.filter {
        inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(
          it.cmmsDataProvider
        )!! in ALL_EDP_WITHOUT_HMSS_CAPABILITIES_DISPLAY_NAMES
      }

    val hmssEventGroups: List<EventGroup> =
      eventGroups.filter {
        inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(
          it.cmmsDataProvider
        )!! in ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES
      }

    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        llv2EventGroups[0] to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        hmssEventGroups[0] to "person.age_group == ${Person.AgeGroup.YEARS_55_PLUS_VALUE}",
      )

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)

    val measurements: List<Measurement> = listMeasurements()
    assertThat(measurements).hasSize(1)
    assertThat(measurements[0].protocolConfig.protocolsList).hasSize(1)
    assertThat(measurements[0].protocolConfig.protocolsList[0].hasReachOnlyLiquidLegionsV2())
      .isTrue()
  }

  @Test
  fun `report with HMSS union reach across 2 edps has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups: List<EventGroup> = listEventGroups()

    val hmssEventGroups: List<EventGroup> =
      eventGroups.filter {
        inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(
          it.cmmsDataProvider
        )!! in ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES
      }

    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        hmssEventGroups[0] to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        hmssEventGroups[1] to "person.age_group == ${Person.AgeGroup.YEARS_55_PLUS_VALUE}",
      )

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.UNION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)

    val measurements: List<Measurement> = listMeasurements()
    assertThat(measurements).hasSize(1)
    assertThat(measurements[0].protocolConfig.protocolsList).hasSize(1)
    assertThat(measurements[0].protocolConfig.protocolsList[0].hasHonestMajorityShareShuffle())
      .isTrue()
  }

  @Test
  fun `report with unique reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.age_group == ${Person.Gender.MALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[0].name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[1].name
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "unique reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val equivalentFilter =
      "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE} && " +
        "person.age_group == ${Person.Gender.FEMALE_VALUE}"
    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      listOf(buildEventGroupSpec(eventGroup, equivalentFilter, EVENT_RANGE.toInterval()))
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with intersection reach has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.age_group == ${Person.Gender.FEMALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.INTERSECTION
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[0].name
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "intersection reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val equivalentFilter =
      "(${createdPrimitiveReportingSets[0].filter}) && (${createdPrimitiveReportingSets[1].filter})"
    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      listOf(buildEventGroupSpec(eventGroup, equivalentFilter, EVENT_RANGE.toInterval()))
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult =
      retrievedReport.metricCalculationResultsList
        .single()
        .resultAttributesList
        .single()
        .metricResult
        .reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `report with 2 reporting metric entries has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    for (resultAttribute in
      retrievedReport.metricCalculationResultsList.single().resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report across two time intervals has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val eventRangeWithNoReach =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 18)..LocalDate.of(2021, 3, 19))

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += EVENT_RANGE.toInterval()
        timeIntervals += eventRangeWithNoReach.toInterval()
      }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          buildEventGroupSpec(eventGroup, filter, resultAttribute.timeInterval)
        }
      val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with invalidated Metric has state FAILED`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
        eventGroup to "person.age_group == ${Person.Gender.MALE_VALUE}",
      )
    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val compositeReportingSet = reportingSet {
      displayName = "composite"
      composite =
        ReportingSetKt.composite {
          expression =
            ReportingSetKt.setExpression {
              operation = ReportingSet.SetExpression.Operation.DIFFERENCE
              lhs =
                ReportingSetKt.SetExpressionKt.operand {
                  expression =
                    ReportingSetKt.setExpression {
                      operation = ReportingSet.SetExpression.Operation.UNION
                      lhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[0].name
                        }
                      rhs =
                        ReportingSetKt.SetExpressionKt.operand {
                          reportingSet = createdPrimitiveReportingSets[1].name
                        }
                    }
                }
              rhs =
                ReportingSetKt.SetExpressionKt.operand {
                  reportingSet = createdPrimitiveReportingSets[1].name
                }
            }
        }
    }

    val createdCompositeReportingSet =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = compositeReportingSet
            reportingSetId = "def"
          }
        )

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "unique reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdCompositeReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    publicMetricsClient
      .withCallCredentials(credentials)
      .invalidateMetric(
        invalidateMetricRequest {
          name = retrievedReport.metricCalculationResultsList[0].resultAttributesList[0].metric
        }
      )

    val failedReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .getReport(getReportRequest { name = retrievedReport.name })
    assertThat(failedReport.state).isEqualTo(Report.State.FAILED)
  }

  @Test
  fun `report with reporting interval has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  daily = MetricCalculationSpec.MetricFrequencySpec.Daily.getDefaultInstance()
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.DAY
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2021
            month = 3
            day = 15
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2021
            month = 3
            day = 17
          }
        }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          buildEventGroupSpec(eventGroup, filter, resultAttribute.timeInterval)
        }
      val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `report with reporting interval doesn't create metric beyond report_end`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
              metricFrequencySpec =
                MetricCalculationSpecKt.metricFrequencySpec {
                  weekly =
                    MetricCalculationSpecKt.MetricFrequencySpecKt.weekly {
                      dayOfWeek = com.google.type.DayOfWeek.WEDNESDAY
                    }
                }
              trailingWindow =
                MetricCalculationSpecKt.trailingWindow {
                  count = 1
                  increment = MetricCalculationSpec.TrailingWindow.Increment.WEEK
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      reportingInterval =
        ReportKt.reportingInterval {
          reportStart = dateTime {
            year = 2024
            month = 1
            day = 3
            timeZone = timeZone { id = "America/Los_Angeles" }
          }
          reportEnd = date {
            year = 2024
            month = 1
            day = 17
          }
        }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    assertThat(retrievedReport.metricCalculationResultsList[0].resultAttributesList).hasSize(2)
    val sortedResults =
      retrievedReport.metricCalculationResultsList[0].resultAttributesList.sortedBy {
        it.timeInterval.startTime.seconds
      }
    assertThat(sortedResults[0].timeInterval)
      .isEqualTo(
        interval {
          startTime = timestamp {
            seconds = 1704268800 // January 3, 2024 at 12:00 AM, America/Los_Angeles
          }
          endTime = timestamp {
            seconds = 1704873600 // January 10, 2024 at 12:00 AM, America/Los_Angeles
          }
        }
      )
    assertThat(sortedResults[1].timeInterval)
      .isEqualTo(
        interval {
          startTime = timestamp {
            seconds = 1704873600 // January 10, 2024 at 12:00 AM, America/Los_Angeles
          }
          endTime = timestamp {
            seconds = 1705478400 // January 17, 2024 at 12:00 AM, America/Los_Angeles
          }
        }
      )
  }

  @Test
  fun `report with group by has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> = listOf(eventGroup to "")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val grouping1Predicate1 = "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}"
    val grouping1Predicate2 = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
    val grouping2Predicate1 = "person.gender == ${Person.Gender.FEMALE_VALUE}"
    val grouping2Predicate2 = "person.gender == ${Person.Gender.MALE_VALUE}"

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "union reach"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += grouping1Predicate1
                  predicates += grouping1Predicate2
                }
              groupings +=
                MetricCalculationSpecKt.grouping {
                  predicates += grouping2Predicate1
                  predicates += grouping2Predicate2
                }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals { timeIntervals += EVENT_RANGE.toInterval() }
    }

    val createdReport =
      publicReportsClient
        .withCallCredentials(credentials)
        .createReport(
          createReportRequest {
            parent = measurementConsumerData.name
            this.report = report
            reportId = "report"
          }
        )

    val retrievedReport = pollForCompletedReport(createdReport.name)
    assertThat(retrievedReport.state).isEqualTo(Report.State.SUCCEEDED)

    for (resultAttribute in retrievedReport.metricCalculationResultsList[0].resultAttributesList) {
      val reachResult = resultAttribute.metricResult.reach
      val actualResult =
        MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
      val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)

      val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
        eventGroupEntries.map { (eventGroup, filter) ->
          val allFilters =
            (resultAttribute.groupingPredicatesList + filter)
              .filter { it.isNotBlank() }
              .joinToString(" && ")
          buildEventGroupSpec(eventGroup, allFilters, EVENT_RANGE.toInterval())
        }
      val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

      assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
    }
  }

  @Test
  fun `creating 3 reports at once succeeds`() = runBlocking {
    val numReports = 3
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroups.first() to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val createdMetricCalculationSpec =
      publicMetricCalculationSpecsClient
        .withCallCredentials(credentials)
        .createMetricCalculationSpec(
          createMetricCalculationSpecRequest {
            parent = measurementConsumerData.name
            metricCalculationSpec = metricCalculationSpec {
              displayName = "load test"
              metricSpecs += metricSpec {
                reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            }
            metricCalculationSpecId = "fed"
          }
        )

    val report = report {
      reportingMetricEntries +=
        ReportKt.reportingMetricEntry {
          key = createdPrimitiveReportingSet.name
          value =
            ReportKt.reportingMetricCalculationSpec {
              metricCalculationSpecs += createdMetricCalculationSpec.name
            }
        }
      timeIntervals = timeIntervals {
        timeIntervals += interval {
          startTime = timestamp { seconds = 100 }
          endTime = timestamp { seconds = 200 }
        }
      }
    }

    val deferred: MutableList<Deferred<Report>> = mutableListOf()
    repeat(numReports) {
      deferred.add(
        async {
          publicReportsClient
            .withCallCredentials(credentials)
            .createReport(
              createReportRequest {
                parent = measurementConsumerData.name
                this.report = report
                reportId = "report$it"
              }
            )
        }
      )
    }

    deferred.awaitAll()
    val retrievedReports =
      publicReportsClient
        .withCallCredentials(credentials)
        .listReports(
          listReportsRequest {
            parent = measurementConsumerData.name
            pageSize = numReports
          }
        )
        .reportsList

    assertThat(retrievedReports).hasSize(numReports)
    retrievedReports.forEach {
      assertThat(it)
        .ignoringFields(
          Report.NAME_FIELD_NUMBER,
          Report.STATE_FIELD_NUMBER,
          Report.CREATE_TIME_FIELD_NUMBER,
          Report.METRIC_CALCULATION_RESULTS_FIELD_NUMBER,
        )
        .isEqualTo(report)
    }
  }

  @Test
  fun `reach metric result has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `reach metric with single edp params result has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reach =
          MetricSpecKt.reachParams {
            multipleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams = DP_PARAMS
                vidSamplingInterval = VID_SAMPLING_INTERVAL
              }
            singleDataProviderParams =
              MetricSpecKt.samplingAndPrivacyParams {
                privacyParams = SINGLE_DATA_PROVIDER_DP_PARAMS
                vidSamplingInterval = SINGLE_DATA_PROVIDER_VID_SAMPLING_INTERVAL
              }
          }
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `reach-and-frequency metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reachAndFrequency =
          MetricSpecKt.reachAndFrequencyParams {
            reachPrivacyParams = DP_PARAMS
            frequencyPrivacyParams = DP_PARAMS
            maximumFrequency = 5
          }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult =
      calculateExpectedReachAndFrequencyMeasurementResult(
        eventGroupSpecs,
        metric.metricSpec.reachAndFrequency.maximumFrequency,
      )

    val reachAndFrequencyResult = retrievedMetric.result.reachAndFrequency
    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = reachAndFrequencyResult.reach.value }
        frequency =
          MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(
              reachAndFrequencyResult.frequencyHistogram.binsList.associate {
                Pair(it.label.toLong(), it.binResult.value / reachAndFrequencyResult.reach.value)
              }
            )
          }
      }
    val reachTolerance =
      computeErrorMargin(reachAndFrequencyResult.reach.univariateStatistics.standardDeviation)
    val frequencyToleranceMap: Map<Long, Double> =
      reachAndFrequencyResult.frequencyHistogram.binsList.associate { bin ->
        bin.label.toLong() to computeErrorMargin(bin.relativeUnivariateStatistics.standardDeviation)
      }

    assertThat(actualResult).reachValue().isWithin(reachTolerance).of(expectedResult.reach.value)
    assertThat(actualResult)
      .frequencyDistribution()
      .isWithin(frequencyToleranceMap)
      .of(expectedResult.frequency.relativeFrequencyDistributionMap)
  }

  @Test
  fun `impression count metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        impressionCount = MetricSpecKt.impressionCountParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        buildEventGroupSpec(eventGroup, filter, EVENT_RANGE.toInterval())
      }
    val expectedResult =
      calculateExpectedImpressionMeasurementResult(
        eventGroupSpecs,
        createdMetric.metricSpec.impressionCount.maximumFrequencyPerUser,
      )

    val impressionResult = retrievedMetric.result.impressionCount
    val actualResult =
      MeasurementKt.result {
        impression = MeasurementKt.ResultKt.impression { value = impressionResult.value }
      }
    val tolerance = computeErrorMargin(impressionResult.univariateStatistics.standardDeviation)

    assertThat(actualResult)
      .impressionValue()
      .isWithin(tolerance)
      .of(expectedResult.impression.value)
  }

  @Test
  fun `watch duration metric has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        watchDuration = MetricSpecKt.watchDurationParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    // TODO(@tristanvuong2021): Calculate watch duration using synthetic spec.
  }

  @Test
  fun `reach metric with filter has the expected result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = EVENT_RANGE.toInterval()
      metricSpec = metricSpec {
        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
      filters += "person.gender == ${Person.Gender.MALE_VALUE}"
    }

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            this.metric = metric
            metricId = "abc"
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)
    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val eventGroupSpecs: Iterable<EventQuery.EventGroupSpec> =
      eventGroupEntries.map { (eventGroup, filter) ->
        val allFilters =
          (metric.filtersList + filter).filter { it.isNotBlank() }.joinToString(" && ")
        buildEventGroupSpec(eventGroup, allFilters, EVENT_RANGE.toInterval())
      }
    val expectedResult = calculateExpectedReachMeasurementResult(eventGroupSpecs)

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(expectedResult.reach.value)
  }

  @Test
  fun `creating 3 metrics at once succeeds`() = runBlocking {
    val numMetrics = 3
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroups.first() to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 100 }
        endTime = timestamp { seconds = 200 }
      }
      metricSpec = metricSpec {
        reach = MetricSpecKt.reachParams { privacyParams = DP_PARAMS }
        vidSamplingInterval = VID_SAMPLING_INTERVAL
      }
    }

    val deferred: MutableList<Deferred<Metric>> = mutableListOf()
    repeat(numMetrics) {
      deferred.add(
        async {
          publicMetricsClient
            .withCallCredentials(credentials)
            .createMetric(
              createMetricRequest {
                parent = measurementConsumerData.name
                this.metric = metric
                metricId = "abc$it"
              }
            )
        }
      )
    }

    deferred.awaitAll()
    val retrievedMetrics =
      publicMetricsClient
        .withCallCredentials(credentials)
        .listMetrics(
          listMetricsRequest {
            parent = measurementConsumerData.name
            pageSize = numMetrics
          }
        )
        .metricsList

    assertThat(retrievedMetrics).hasSize(numMetrics)
    retrievedMetrics.forEach {
      assertThat(it)
        .ignoringFields(
          Metric.NAME_FIELD_NUMBER,
          Metric.STATE_FIELD_NUMBER,
          Metric.CREATE_TIME_FIELD_NUMBER,
          Metric.RESULT_FIELD_NUMBER,
        )
        .isEqualTo(metric)
    }
  }

  @Test
  fun `creating 25 reporting sets at once succeeds`() = runBlocking {
    val numReportingSets = 25
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()

    val primitiveReportingSet = reportingSet {
      displayName = "primitive"
      filter = "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}"
      primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroups.first().cmmsEventGroup }
    }

    val deferred: MutableList<Deferred<ReportingSet>> = mutableListOf()
    repeat(numReportingSets) {
      deferred.add(
        async {
          publicReportingSetsClient
            .withCallCredentials(credentials)
            .createReportingSet(
              createReportingSetRequest {
                parent = measurementConsumerData.name
                reportingSet = primitiveReportingSet
                reportingSetId = "abc$it"
              }
            )
        }
      )
    }

    deferred.awaitAll()
    val retrievedPrimitiveReportingSets =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .listReportingSets(
          listReportingSetsRequest {
            parent = measurementConsumerData.name
            pageSize = numReportingSets
          }
        )
        .reportingSetsList

    assertThat(retrievedPrimitiveReportingSets).hasSize(numReportingSets)
    retrievedPrimitiveReportingSets.forEach {
      assertThat(it).ignoringFields(ReportingSet.NAME_FIELD_NUMBER).isEqualTo(primitiveReportingSet)
    }
  }

  @Test
  fun `retrieving data provider succeeds`() = runBlocking {
    val eventGroups = listEventGroups()
    val dataProviderName = eventGroups.first().cmmsDataProvider

    val dataProvider =
      publicDataProvidersClient
        .withCallCredentials(credentials)
        .getDataProvider(getDataProviderRequest { name = dataProviderName })

    assertThat(DataProviderCertificateKey.fromName(dataProvider.certificate)).isNotNull()
  }

  @Test
  fun `retrieving metadata descriptors for event groups succeeds`() = runBlocking {
    val eventGroups = listEventGroups()

    val descriptorNames = eventGroups.map { it.metadata.eventGroupMetadataDescriptor }

    val descriptors =
      publicEventGroupMetadataDescriptorsClient
        .withCallCredentials(credentials)
        .batchGetEventGroupMetadataDescriptors(
          batchGetEventGroupMetadataDescriptorsRequest { names += descriptorNames }
        )
        .eventGroupMetadataDescriptorsList

    assertThat(descriptors).hasSize(descriptorNames.size)

    val retrievedDescriptorNames = mutableSetOf<String>()
    for (descriptor in descriptors) {
      retrievedDescriptorNames.add(descriptor.name)
    }

    for (eventGroup in eventGroups) {
      assertThat(
          retrievedDescriptorNames.contains(eventGroup.metadata.eventGroupMetadataDescriptor)
        )
        .isTrue()
    }
  }

  @Test
  fun `getBasicReport returns basic report inserted via internal API`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")

    val dataProvider =
      publicDataProvidersClient
        .withCallCredentials(credentials)
        .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })

    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val dataProviderKey = DataProviderKey.fromName(dataProvider.name)
    val eventGroupKey = EventGroupKey.fromName(eventGroup.name)
    val reportingSetKey = ReportingSetKey.fromName(createdPrimitiveReportingSet.name)

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId =
          MeasurementConsumerKey.fromName(measurementConsumerData.name)!!.measurementConsumerId,
        basicReportId = "basicReport123",
      )

    val internalBasicReport = internalBasicReport {
      this.cmmsMeasurementConsumerId = basicReportKey.cmmsMeasurementConsumerId
      externalBasicReportId = basicReportKey.basicReportId
      externalCampaignGroupId = reportingSetKey!!.reportingSetId
      campaignGroupDisplayName = createdPrimitiveReportingSet.displayName
      details = basicReportDetails {
        title = "title"
        reportingInterval = internalReportingInterval {
          reportStart = dateTime { day = 3 }
          reportEnd = date { day = 5 }
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
                          this.cmmsDataProviderId = dataProviderKey!!.dataProviderId
                          cmmsDataProviderDisplayName = dataProvider.displayName
                          eventGroupSummaries +=
                            InternalResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                              .eventGroupSummary {
                                this.cmmsMeasurementConsumerId =
                                  basicReportKey.cmmsMeasurementConsumerId
                                cmmsEventGroupId = eventGroupKey!!.cmmsEventGroupId
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

              // Numbers below aren't actually calculated.
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
                      key = dataProviderKey!!.dataProviderId
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
                      cmmsDataProviderIds += dataProviderKey!!.dataProviderId
                    }
                }
            }
        }
      }
    }

    val createdInternalBasicReport =
      reportingServer.internalBasicReportsClient.insertBasicReport(
        insertBasicReportRequest { basicReport = internalBasicReport }
      )

    val retrievedPublicBasicReport =
      publicBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = basicReportKey.toName() })

    assertThat(retrievedPublicBasicReport)
      .isEqualTo(
        basicReport {
          name = basicReportKey.toName()
          title = internalBasicReport.details.title
          campaignGroup = createdPrimitiveReportingSet.name
          campaignGroupDisplayName = createdPrimitiveReportingSet.displayName
          reportingInterval = reportingInterval {
            reportStart = dateTime { day = 3 }
            reportEnd = date { day = 5 }
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
                            component = dataProvider.name
                            displayName = dataProvider.displayName
                            eventGroupSummaries +=
                              ResultGroupKt.MetricMetadataKt.ReportingUnitComponentSummaryKt
                                .eventGroupSummary { this.eventGroup = eventGroup.name }
                          }
                      }
                    nonCumulativeMetricStartTime = timestamp { seconds = 10 }
                    cumulativeMetricStartTime = timestamp { seconds = 12 }
                    metricEndTime = timestamp { seconds = 20 }
                    metricFrequency = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
                    dimensionSpecSummary =
                      ResultGroupKt.MetricMetadataKt.dimensionSpecSummary {
                        groupings += eventTemplateField {
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

                // Numbers below aren't actually calculated.
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
                        key = dataProvider.name
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
                        components += dataProvider.name
                      }
                  }
              }
          }

          createTime = createdInternalBasicReport.createTime
        }
      )
  }

  @Test
  fun `getImpressionQualificationFilter retrives ImpressionQualificationFilter`() = runBlocking {
    val impressionQualificationFilter =
      publicImpressionQualificationFiltersClient
        .withCallCredentials(credentials)
        .getImpressionQualificationFilter(
          getImpressionQualificationFilterRequest { name = "impressionQualificationFilters/ami" }
        )

    assertThat(impressionQualificationFilter)
      .isEqualTo(
        impressionQualificationFilter {
          name = "impressionQualificationFilters/ami"
          displayName = "ami"
          filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
          filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
          filterSpecs += impressionQualificationFilterSpec { mediaType = MediaType.OTHER }
        }
      )
  }

  @Test
  fun `listImpressionQualificationFilters with page size and page token retrives ImpressionQualificationFilter`() =
    runBlocking {
      val internalPageToken = listImpressionQualificationFiltersPageToken {
        after =
          ListImpressionQualificationFiltersPageTokenKt.after {
            externalImpressionQualificationFilterId = "ami"
          }
      }

      val listImpressionQualificationFiltersResponse =
        publicImpressionQualificationFiltersClient
          .withCallCredentials(credentials)
          .listImpressionQualificationFilters(
            listImpressionQualificationFiltersRequest {
              pageSize = 1
              pageToken = internalPageToken.toByteString().base64UrlEncode()
            }
          )

      assertThat(listImpressionQualificationFiltersResponse)
        .isEqualTo(
          listImpressionQualificationFiltersResponse {
            impressionQualificationFilters += impressionQualificationFilter {
              name = "impressionQualificationFilters/mrc"
              displayName = "mrc"
              filterSpecs += impressionQualificationFilterSpec {
                mediaType = MediaType.DISPLAY
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "banner_ad.viewable_fraction_1_second"
                    value = EventTemplateFieldKt.fieldValue { floatValue = 0.5F }
                  }
                }
              }
              filterSpecs += impressionQualificationFilterSpec {
                mediaType = MediaType.VIDEO
                filters += eventFilter {
                  terms += eventTemplateField {
                    path = "video.viewable_fraction_1_second"
                    value = EventTemplateFieldKt.fieldValue { floatValue = 1.0F }
                  }
                }
              }
            }
          }
        )
    }

  private suspend fun listEventGroups(): List<EventGroup> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    return publicEventGroupsClient
      .withCallCredentials(credentials)
      .listEventGroups(
        listEventGroupsRequest {
          parent = measurementConsumerData.name
          pageSize = 1000
        }
      )
      .eventGroupsList
  }

  private suspend fun createPrimitiveReportingSets(
    eventGroupEntries: List<Pair<EventGroup, String>>,
    measurementConsumerName: String,
  ): List<ReportingSet> {
    val primitiveReportingSets: List<ReportingSet> =
      eventGroupEntries.mapIndexed { index, (eventGroup, filterExp) ->
        reportingSet {
          displayName = "primitive$index"
          if (filterExp.isNotBlank()) {
            filter = filterExp
          }
          primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
        }
      }

    return primitiveReportingSets.mapIndexed { index, primitiveReportingSet ->
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerName
            reportingSet = primitiveReportingSet
            reportingSetId = "abc$index"
          }
        )
    }
  }

  private suspend fun pollForCompletedReport(reportName: String): Report {
    while (true) {
      val retrievedReport =
        publicReportsClient
          .withCallCredentials(credentials)
          .getReport(getReportRequest { name = reportName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  private suspend fun pollForCompletedMetric(metricName: String): Metric {
    while (true) {
      val retrievedMetric =
        publicMetricsClient
          .withCallCredentials(credentials)
          .getMetric(getMetricRequest { name = metricName })

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
      when (retrievedMetric.state) {
        Metric.State.SUCCEEDED,
        Metric.State.FAILED,
        Metric.State.INVALID -> return retrievedMetric
        Metric.State.RUNNING,
        Metric.State.UNRECOGNIZED,
        Metric.State.STATE_UNSPECIFIED -> delay(5000)
      }
    }
  }

  private suspend fun calculateExpectedReachMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ): Measurement.Result {
    val reach =
      MeasurementResults.computeReach(
        eventGroupSpecs
          .asSequence()
          .flatMap { SYNTHETIC_EVENT_QUERY.getUserVirtualIds(it) }
          .asFlow()
      )
    return MeasurementKt.result {
      this.reach = MeasurementKt.ResultKt.reach { value = reach.toLong() }
    }
  }

  private suspend fun calculateExpectedReachAndFrequencyMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    maxFrequency: Int,
  ): Measurement.Result {
    val reachAndFrequency =
      MeasurementResults.computeReachAndFrequency(
        eventGroupSpecs
          .asSequence()
          .flatMap { SYNTHETIC_EVENT_QUERY.getUserVirtualIds(it) }
          .asFlow(),
        maxFrequency,
      )
    return MeasurementKt.result {
      reach = MeasurementKt.ResultKt.reach { value = reachAndFrequency.reach.toLong() }
      frequency =
        MeasurementKt.ResultKt.frequency {
          relativeFrequencyDistribution.putAll(
            reachAndFrequency.relativeFrequencyDistribution.mapKeys { it.key.toLong() }
          )
        }
    }
  }

  private suspend fun calculateExpectedImpressionMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    maxFrequency: Int,
  ): Measurement.Result {
    val impression =
      MeasurementResults.computeImpression(
        eventGroupSpecs
          .asSequence()
          .flatMap { SYNTHETIC_EVENT_QUERY.getUserVirtualIds(it) }
          .asFlow(),
        maxFrequency,
      )
    return MeasurementKt.result {
      this.impression = MeasurementKt.ResultKt.impression { value = impression }
    }
  }

  private fun buildEventGroupSpec(
    eventGroup: EventGroup,
    filter: String,
    collectionInterval: Interval,
  ): EventQuery.EventGroupSpec {
    val cmmsMetadata =
      CmmsEventGroupKt.metadata {
        eventGroupMetadataDescriptor = eventGroup.metadata.eventGroupMetadataDescriptor
        metadata = eventGroup.metadata.metadata
      }
    val encryptedCmmsMetadata =
      encryptMetadata(cmmsMetadata, InProcessCmmsComponents.MC_ENTITY_CONTENT.encryptionPublicKey)
    val cmmsEventGroup = cmmsEventGroup { encryptedMetadata = encryptedCmmsMetadata }

    val eventFilter = RequisitionSpecKt.eventFilter { expression = filter }

    return EventQuery.EventGroupSpec(
      cmmsEventGroup,
      RequisitionSpecKt.EventGroupEntryKt.value {
        this.collectionInterval = collectionInterval
        this.filter = eventFilter
      },
    )
  }

  /** Computes the margin of error, i.e. half width, of a 99.9% confidence interval. */
  private fun computeErrorMargin(standardDeviation: Double): Double {
    return CONFIDENCE_INTERVAL_MULTIPLIER * standardDeviation
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private val TRUSTED_CERTIFICATES =
      readCertificateCollection(SECRETS_DIR.resolve("all_root_certs.pem")).associateBy {
        it.subjectKeyIdentifier!!
      }

    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"

    private val SYNTHETIC_EVENT_QUERY =
      MetadataSyntheticGeneratorEventQuery(
        SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
        InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
      )

    private val EVENT_RANGE =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2021, 3, 15)..LocalDate.of(2021, 3, 17))

    // Set epsilon and delta higher without exceeding privacy budget so the noise is smaller in the
    // integration test. Check sample values in CompositionTest.kt.
    private val DP_PARAMS =
      MetricSpecKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1e-15
      }

    private val SINGLE_DATA_PROVIDER_DP_PARAMS =
      MetricSpecKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1.01e-15
      }

    private val VID_SAMPLING_INTERVAL =
      MetricSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 1.0f
      }

    private val SINGLE_DATA_PROVIDER_VID_SAMPLING_INTERVAL =
      MetricSpecKt.vidSamplingInterval {
        start = 0.0f
        width = 0.9f
      }

    // For a 99.9% Confidence Interval.
    private const val CONFIDENCE_INTERVAL_MULTIPLIER = 3.291

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
