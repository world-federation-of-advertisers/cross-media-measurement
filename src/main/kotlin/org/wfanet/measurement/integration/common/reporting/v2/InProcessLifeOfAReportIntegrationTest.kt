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
import kotlin.math.max
import kotlin.test.assertNotNull
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
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
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupKt as CmmsEventGroupKt
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.eventGroup as cmmsEventGroup
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
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
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.consent.client.dataprovider.encryptMetadata
import org.wfanet.measurement.dataprovider.MeasurementResults
import org.wfanet.measurement.integration.common.ALL_EDP_WITHOUT_HMSS_CAPABILITIES_DISPLAY_NAMES
import org.wfanet.measurement.integration.common.ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.internal.reporting.v2.ListImpressionQualificationFiltersPageTokenKt
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.internal.reporting.v2.listImpressionQualificationFiltersPageToken
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EventQuery
import org.wfanet.measurement.loadtest.dataprovider.SyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.job.BasicReportsReportsJob
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.DimensionSpecKt
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.Metric
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpec
import org.wfanet.measurement.reporting.v2alpha.MetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportKt
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createMetricCalculationSpecRequest
import org.wfanet.measurement.reporting.v2alpha.createMetricRequest
import org.wfanet.measurement.reporting.v2alpha.createReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
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
import org.wfanet.measurement.reporting.v2alpha.listReportsRequest
import org.wfanet.measurement.reporting.v2alpha.metric
import org.wfanet.measurement.reporting.v2alpha.metricCalculationSpec
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.metricSpec
import org.wfanet.measurement.reporting.v2alpha.report
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec
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

  private lateinit var measurementConsumerConfig: MeasurementConsumerConfig

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
        measurementConsumerConfig = measurementConsumerConfig {
          apiKey = measurementConsumerData.apiAuthenticationKey
          signingCertificateName = measurementConsumer.certificate
          signingPrivateKeyPath = MC_SIGNING_PRIVATE_KEY_PATH
          offlinePrincipal = "principals/mc-user"
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
          TestEvent.getDescriptor(),
          defaultModelLineName = inProcessCmmsComponents.modelLineResourceName,
          verboseGrpcLogging = false,
          populationDataProviderName =
            inProcessCmmsComponents.getPopulationData().populationDataProviderName,
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

  private val syntheticEventQuery: SyntheticGeneratorEventQuery
    get() = inProcessCmmsComponents.eventQuery

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

  private suspend fun listMeasurements(): List<Measurement> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()

    return publicMeasurementsClient
      .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
      .listMeasurements(listMeasurementsRequest { parent = measurementConsumerData.name })
      .measurementsList
  }

  @Test
  fun `population metric for union has correct result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroups[0] to "person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE}",
        eventGroups[1] to "person.age_group <= ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
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

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            metricId = "population"
            metric = metric {
              reportingSet = createdCompositeReportingSet.name
              timeInterval = interval {
                startTime = timestamp { seconds = 1615791600 }
                endTime = timestamp { seconds = 1615964400 }
              }
              metricSpec = metricSpec {
                populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
              }
              filters += "person.gender == ${Person.Gender.MALE_VALUE}"
              modelLine = inProcessCmmsComponents.modelLineResourceName
            }
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)

    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val expectedResult =
      MeasurementResults.computePopulation(
        inProcessCmmsComponents.getPopulationData().populationSpec,
        "(person.gender == ${Person.Gender.MALE_VALUE}) && (person.age_group <= ${Person.AgeGroup.YEARS_35_TO_54_VALUE})",
        TestEvent.getDescriptor(),
      )
    assertThat(retrievedMetric.result.populationCount.value).isEqualTo(expectedResult)
  }

  @Test
  fun `population metric for difference has correct result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(
        eventGroups[0] to "person.age_group <= ${Person.AgeGroup.YEARS_35_TO_54_VALUE}",
        eventGroups[1] to "person.age_group <= ${Person.AgeGroup.YEARS_18_TO_34_VALUE}",
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

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            metricId = "population"
            metric = metric {
              reportingSet = createdCompositeReportingSet.name
              timeInterval = interval {
                startTime = timestamp { seconds = 1615791600 }
                endTime = timestamp { seconds = 1615964400 }
              }
              metricSpec = metricSpec {
                populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
              }
              filters += "person.gender == ${Person.Gender.MALE_VALUE}"
              modelLine = inProcessCmmsComponents.modelLineResourceName
            }
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)

    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val expectedResult =
      MeasurementResults.computePopulation(
        inProcessCmmsComponents.getPopulationData().populationSpec,
        "(person.gender == ${Person.Gender.MALE_VALUE}) && (person.age_group == ${Person.AgeGroup.YEARS_35_TO_54_VALUE})",
        TestEvent.getDescriptor(),
      )
    assertThat(retrievedMetric.result.populationCount.value).isEqualTo(expectedResult)
  }

  @Test
  fun `population metric with no reporting set filters has correct result`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroupEntries: List<Pair<EventGroup, String>> = listOf(eventGroups[0] to "")

    val createdPrimitiveReportingSets: List<ReportingSet> =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name)

    val createdMetric =
      publicMetricsClient
        .withCallCredentials(credentials)
        .createMetric(
          createMetricRequest {
            parent = measurementConsumerData.name
            metricId = "population"
            metric = metric {
              reportingSet = createdPrimitiveReportingSets[0].name
              timeInterval = interval {
                startTime = timestamp { seconds = 1615791600 }
                endTime = timestamp { seconds = 1615964400 }
              }
              metricSpec = metricSpec {
                populationCount = MetricSpec.PopulationCountParams.getDefaultInstance()
              }
              filters += "person.gender == ${Person.Gender.MALE_VALUE}"
              modelLine = inProcessCmmsComponents.modelLineResourceName
            }
          }
        )

    val retrievedMetric = pollForCompletedMetric(createdMetric.name)

    assertThat(retrievedMetric.state).isEqualTo(Metric.State.SUCCEEDED)

    val expectedResult =
      MeasurementResults.computePopulation(
        inProcessCmmsComponents.getPopulationData().populationSpec,
        "(person.gender == ${Person.Gender.MALE_VALUE})",
        TestEvent.getDescriptor(),
      )
    assertThat(retrievedMetric.result.populationCount.value).isEqualTo(expectedResult)
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
        eventGroup to "person.gender == ${Person.Gender.MALE_VALUE}",
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
        "person.gender == ${Person.Gender.FEMALE_VALUE}"
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
        eventGroup to "person.gender == ${Person.Gender.FEMALE_VALUE}",
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
        eventGroup to "person.gender == ${Person.Gender.MALE_VALUE}",
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
  fun `reach-and-frequency metric with no data has a result of 0`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
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

    val reachAndFrequencyResult = retrievedMetric.result.reachAndFrequency
    val actualResult =
      MeasurementKt.result {
        reach = MeasurementKt.ResultKt.reach { value = reachAndFrequencyResult.reach.value }
        frequency =
          MeasurementKt.ResultKt.frequency {
            relativeFrequencyDistribution.putAll(
              reachAndFrequencyResult.frequencyHistogram.binsList.associate {
                Pair(
                  it.label.toLong(),
                  if (reachAndFrequencyResult.reach.value == 0L) {
                    0.0
                  } else {
                    it.binResult.value / reachAndFrequencyResult.reach.value
                  },
                )
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

    val mapWithAllZeroFrequency = buildMap {
      reachAndFrequencyResult.frequencyHistogram.binsList.forEach { bin ->
        put(bin.label.toLong(), 0.0)
      }
    }

    assertThat(actualResult).reachValue().isWithin(reachTolerance).of(0)
    assertThat(actualResult)
      .frequencyDistribution()
      .isWithin(frequencyToleranceMap)
      .of(mapWithAllZeroFrequency)
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
  fun `impression count metric with no data has a result of 0`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
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

    val impressionResult = retrievedMetric.result.impressionCount
    val actualResult =
      MeasurementKt.result {
        impression = MeasurementKt.ResultKt.impression { value = impressionResult.value }
      }
    val tolerance = computeErrorMargin(impressionResult.univariateStatistics.standardDeviation)

    assertThat(actualResult).impressionValue().isWithin(tolerance).of(0)
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
  fun `reach metric with no data has a result of 0`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()
    val eventGroupEntries: List<Pair<EventGroup, String>> =
      listOf(eventGroup to "person.age_group == ${Person.AgeGroup.YEARS_18_TO_34_VALUE}")
    val createdPrimitiveReportingSet: ReportingSet =
      createPrimitiveReportingSets(eventGroupEntries, measurementConsumerData.name).single()

    val metric = metric {
      reportingSet = createdPrimitiveReportingSet.name
      timeInterval = interval {
        startTime = timestamp { seconds = 1 }
        endTime = timestamp { seconds = 2 }
      }
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

    val reachResult = retrievedMetric.result.reach
    val actualResult =
      MeasurementKt.result { reach = MeasurementKt.ResultKt.reach { value = reachResult.value } }
    val tolerance = computeErrorMargin(reachResult.univariateStatistics.standardDeviation)
    assertThat(actualResult).reachValue().isWithin(tolerance).of(0)
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
  fun `getBasicReport returns SUCCEEDED multi edp basic report when basic report is completed`() =
    runBlocking {
      val eventGroups = getHmssEventGroups()
      check(eventGroups.size > 1)

      val createBasicReportRequest =
        buildCreateBasicReportRequest(eventGroups).copy {
          basicReport =
            basicReport.copy {
              resultGroupSpecs.clear()
              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit {
                  components += eventGroups.map { it.cmmsDataProvider }
                }
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
                      nonCumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                      cumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                    }
                }
              }

              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit {
                  components += eventGroups.map { it.cmmsDataProvider }
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
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                          averageFrequency = true
                          kPlusReach = 5
                          percentKPlusReach = true
                          impressions = true
                          grps = true
                        }
                    }
                }
              }
            }
        }

      val createdBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .createBasicReport(createBasicReportRequest)

      val retrievedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedBasicReport)
        .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(
          createBasicReportRequest.basicReport.copy {
            name = createdBasicReport.name
            state = BasicReport.State.RUNNING
            effectiveImpressionQualificationFilters +=
              retrievedBasicReport.impressionQualificationFiltersList
            effectiveModelLine = inProcessCmmsComponents.modelLineResourceName
          }
        )
      assertThat(retrievedBasicReport.createTime).isEqualTo(createdBasicReport.createTime)

      executeBasicReportsReportsJob(createdBasicReport.name)
      executeReportProcessorJob()

      val retrievedCompletedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedCompletedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

      // Check that non cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
              val reportingUnitMetricSet = result.metricSet.reportingUnit.nonCumulative
              val reportingUnitValuesCheck =
                reportingUnitMetricSet.reach > 0 &&
                  reportingUnitMetricSet.percentReach > 0 &&
                  reportingUnitMetricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it } &&
                  reportingUnitMetricSet.percentKPlusReachList
                    .zipWithNext { a, b -> b <= a }
                    .all { it } &&
                  reportingUnitMetricSet.averageFrequency > 0 &&
                  reportingUnitMetricSet.impressions > 0 &&
                  reportingUnitMetricSet.grps > 0

              var componentReach = 0
              var componentPercentReach = 0.0f
              var componentAverageFrequency = 0.0f
              var componentKPlusReachExists = false
              var componentPercentKPlusReachExists = false
              var componentImpressions = 0
              var componentGrps = 0.0f
              var componentUniqueReach = 0

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.nonCumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentAverageFrequency =
                  max(componentAverageFrequency, metricSet.averageFrequency)
                componentKPlusReachExists =
                  componentKPlusReachExists ||
                    metricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentPercentKPlusReachExists =
                  componentPercentKPlusReachExists ||
                    metricSet.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentImpressions = max(componentImpressions, metricSet.impressions)
                componentGrps = max(componentGrps, metricSet.grps)
                componentUniqueReach =
                  max(componentUniqueReach, component.value.nonCumulativeUnique.reach)
              }

              val componentValuesCheck =
                componentReach > 0 &&
                  componentPercentReach > 0 &&
                  componentKPlusReachExists &&
                  componentPercentKPlusReachExists &&
                  componentAverageFrequency > 0 &&
                  componentImpressions > 0 &&
                  componentGrps > 0 &&
                  componentUniqueReach > 0

              reportingUnitValuesCheck &&
                componentValuesCheck &&
                result.metricSet.populationSize > 0
            }
        )
      }

      // Check that cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
              val reportingUnitCumulativeMetricSet = result.metricSet.reportingUnit.cumulative
              val reportingUnitValuesCheck =
                reportingUnitCumulativeMetricSet.reach > 0 &&
                  reportingUnitCumulativeMetricSet.percentReach > 0

              var componentReach = 0
              var componentPercentReach = 0.0f
              var componentUniqueReach = 0

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.cumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentUniqueReach =
                  max(componentUniqueReach, component.value.cumulativeUnique.reach)
              }

              val componentValuesCheck =
                componentReach > 0 && componentPercentReach > 0 && componentUniqueReach > 0

              reportingUnitValuesCheck &&
                componentValuesCheck &&
                result.metricSet.populationSize > 0
            }
        )
      }

      // Check that total results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
            }
            .firstOrNull { result ->
              val reportingUnitCumulativeMetricSet = result.metricSet.reportingUnit.cumulative
              val reportingUnitValuesCheck =
                reportingUnitCumulativeMetricSet.reach > 0 &&
                  reportingUnitCumulativeMetricSet.percentReach > 0 &&
                  reportingUnitCumulativeMetricSet.kPlusReachList
                    .zipWithNext { a, b -> b <= a }
                    .all { it } &&
                  reportingUnitCumulativeMetricSet.percentKPlusReachList
                    .zipWithNext { a, b -> b <= a }
                    .all { it } &&
                  reportingUnitCumulativeMetricSet.averageFrequency > 0 &&
                  reportingUnitCumulativeMetricSet.impressions > 0 &&
                  reportingUnitCumulativeMetricSet.grps > 0 &&
                  result.metricSet.reportingUnit.stackedIncrementalReachList
                    .zipWithNext { a, b -> b >= a }
                    .all { it }

              var componentReach = 0
              var componentPercentReach = 0.0f
              var componentAverageFrequency = 0.0f
              var componentKPlusReachExists = false
              var componentPercentKPlusReachExists = false
              var componentImpressions = 0
              var componentGrps = 0.0f

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.cumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentAverageFrequency =
                  max(componentAverageFrequency, metricSet.averageFrequency)
                componentKPlusReachExists =
                  componentKPlusReachExists ||
                    metricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentPercentKPlusReachExists =
                  componentPercentKPlusReachExists ||
                    metricSet.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentImpressions = max(componentImpressions, metricSet.impressions)
                componentGrps = max(componentGrps, metricSet.grps)
              }

              val componentValuesCheck =
                componentReach > 0 &&
                  componentPercentReach > 0 &&
                  componentKPlusReachExists &&
                  componentPercentKPlusReachExists &&
                  componentAverageFrequency > 0 &&
                  componentImpressions > 0 &&
                  componentGrps > 0

              reportingUnitValuesCheck &&
                componentValuesCheck &&
                result.metricSet.populationSize > 0
            }
        )
      }
    }

  @Test
  fun `getBasicReport returns SUCCEEDED single edp basic report when basic report is completed`() =
    runBlocking {
      val eventGroups = getHmssEventGroups().subList(0, 1)

      val createBasicReportRequest =
        buildCreateBasicReportRequest(eventGroups).copy {
          basicReport =
            basicReport.copy {
              resultGroupSpecs.clear()
              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit {
                  components += eventGroups.map { it.cmmsDataProvider }
                }
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
                      nonCumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                      cumulativeUnique =
                        ResultGroupMetricSpecKt.uniqueMetricSetSpec { reach = true }
                    }
                }
              }

              resultGroupSpecs += resultGroupSpec {
                title = "title"
                reportingUnit = reportingUnit {
                  components += eventGroups.map { it.cmmsDataProvider }
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
                  component =
                    ResultGroupMetricSpecKt.componentMetricSetSpec {
                      cumulative =
                        ResultGroupMetricSpecKt.basicMetricSetSpec {
                          reach = true
                          percentReach = true
                          averageFrequency = true
                          kPlusReach = 5
                          percentKPlusReach = true
                          impressions = true
                          grps = true
                        }
                    }
                }
              }
            }
        }

      val createdBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .createBasicReport(createBasicReportRequest)

      val retrievedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedBasicReport)
        .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
        .isEqualTo(
          createBasicReportRequest.basicReport.copy {
            name = createdBasicReport.name
            state = BasicReport.State.RUNNING
            effectiveImpressionQualificationFilters +=
              retrievedBasicReport.impressionQualificationFiltersList
            effectiveModelLine = inProcessCmmsComponents.modelLineResourceName
          }
        )
      assertThat(retrievedBasicReport.createTime).isEqualTo(createdBasicReport.createTime)

      executeBasicReportsReportsJob(createdBasicReport.name)
      executeReportProcessorJob()

      val retrievedCompletedBasicReport =
        publicBasicReportsClient
          .withCallCredentials(credentials)
          .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

      assertThat(retrievedCompletedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

      // Check that non cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
              var componentReach = 0
              var componentPercentReach = 0.0f
              var componentAverageFrequency = 0.0f
              var componentKPlusReachExists = false
              var componentPercentKPlusReachExists = false
              var componentImpressions = 0
              var componentGrps = 0.0f
              var componentUniqueReach = 0

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.nonCumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentAverageFrequency =
                  max(componentAverageFrequency, metricSet.averageFrequency)
                componentKPlusReachExists =
                  componentKPlusReachExists ||
                    metricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentPercentKPlusReachExists =
                  componentPercentKPlusReachExists ||
                    metricSet.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentImpressions = max(componentImpressions, metricSet.impressions)
                componentGrps = max(componentGrps, metricSet.grps)
                componentUniqueReach =
                  max(componentUniqueReach, component.value.nonCumulativeUnique.reach)
              }

              val componentValuesCheck =
                componentReach > 0 &&
                  componentPercentReach > 0 &&
                  componentKPlusReachExists &&
                  componentPercentKPlusReachExists &&
                  componentAverageFrequency > 0 &&
                  componentImpressions > 0 &&
                  componentGrps > 0 &&
                  componentUniqueReach > 0

              componentValuesCheck && result.metricSet.populationSize > 0
            }
        )
      }

      // Check that cumulative results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.WEEKLY
            }
            .firstOrNull { result ->
              var componentReach = 0
              var componentPercentReach = 0.0f
              var componentUniqueReach = 0

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.cumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentUniqueReach =
                  max(componentUniqueReach, component.value.cumulativeUnique.reach)
              }

              val componentValuesCheck =
                componentReach > 0 && componentPercentReach > 0 && componentUniqueReach > 0

              componentValuesCheck && result.metricSet.populationSize > 0
            }
        )
      }

      // Check that total results are set. Dependent on current test data.
      retrievedBasicReport.resultGroupsList.forEach { resultGroup ->
        assertNotNull(
          resultGroup.resultsList
            .filter {
              it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
            }
            .firstOrNull { result ->
              var componentReach = 0
              var componentPercentReach = 0.0f
              var componentAverageFrequency = 0.0f
              var componentKPlusReachExists = false
              var componentPercentKPlusReachExists = false
              var componentImpressions = 0
              var componentGrps = 0.0f

              result.metricSet.componentsList.forEach { component ->
                val metricSet = component.value.cumulative

                componentReach = max(componentReach, metricSet.reach)
                componentPercentReach = max(componentPercentReach, metricSet.percentReach)
                componentAverageFrequency =
                  max(componentAverageFrequency, metricSet.averageFrequency)
                componentKPlusReachExists =
                  componentKPlusReachExists ||
                    metricSet.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentPercentKPlusReachExists =
                  componentPercentKPlusReachExists ||
                    metricSet.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
                componentImpressions = max(componentImpressions, metricSet.impressions)
                componentGrps = max(componentGrps, metricSet.grps)
              }

              val componentValuesCheck =
                componentReach > 0 &&
                  componentPercentReach > 0 &&
                  componentKPlusReachExists &&
                  componentPercentKPlusReachExists &&
                  componentAverageFrequency > 0 &&
                  componentImpressions > 0 &&
                  componentGrps > 0

              componentValuesCheck && result.metricSet.populationSize > 0
            }
        )
      }
    }

  @Test
  fun `getBasicReport returns basic report when model line system specified`() = runBlocking {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val eventGroups = listEventGroups()
    val eventGroup = eventGroups.first()

    val dataProvider =
      publicDataProvidersClient
        .withCallCredentials(credentials)
        .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })

    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumerData.name)!!

    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "abc123")
    val campaignGroup =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = reportingSet {
              displayName = "campaign group"
              campaignGroup = campaignGroupKey.toName()
              primitive = ReportingSetKt.primitive { cmmsEventGroups += eventGroup.cmmsEventGroup }
            }
            reportingSetId = campaignGroupKey.reportingSetId
          }
        )

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
        basicReportId = "basicreport123",
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroup.name
      campaignGroupDisplayName = campaignGroup.displayName
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2021
          month = 3
          day = 14
          hours = 17
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2021
          month = 3
          day = 15
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
        reportingUnit = reportingUnit { components += dataProvider.name }
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
            }
        }
      }
    }

    val createdBasicReport =
      publicBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(
          createBasicReportRequest {
            parent = measurementConsumerData.name
            basicReportId = basicReportKey.basicReportId
            this.basicReport = basicReport
          }
        )

    val retrievedPublicBasicReport =
      publicBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = basicReportKey.toName() })

    assertThat(retrievedPublicBasicReport).isEqualTo(createdBasicReport)
    assertThat(retrievedPublicBasicReport.modelLine).isEmpty()
    assertThat(retrievedPublicBasicReport.effectiveModelLine)
      .isEqualTo(inProcessCmmsComponents.modelLineResourceName)
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
          filterSpecs += impressionQualificationFilterSpec {
            mediaType = MediaType.DISPLAY
            filters += eventFilter {
              terms += eventTemplateField {
                path = "banner_ad.viewable"
                value = EventTemplateFieldKt.fieldValue { boolValue = false }
              }
            }
          }
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
                    path = "banner_ad.viewable"
                    value = EventTemplateFieldKt.fieldValue { boolValue = true }
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

  private fun calculateExpectedReachMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>
  ): Measurement.Result {
    val reach =
      MeasurementResults.computeReach(
        eventGroupSpecs.asSequence().flatMap { syntheticEventQuery.getUserVirtualIds(it) }
      )
    return MeasurementKt.result {
      this.reach = MeasurementKt.ResultKt.reach { value = reach.toLong() }
    }
  }

  private fun calculateExpectedReachAndFrequencyMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    maxFrequency: Int,
  ): Measurement.Result {
    val reachAndFrequency =
      MeasurementResults.computeReachAndFrequency(
        eventGroupSpecs.asSequence().flatMap { syntheticEventQuery.getUserVirtualIds(it) },
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

  private fun calculateExpectedImpressionMeasurementResult(
    eventGroupSpecs: Iterable<EventQuery.EventGroupSpec>,
    maxFrequency: Int,
  ): Measurement.Result {
    val impression =
      MeasurementResults.computeImpression(
        eventGroupSpecs.asSequence().flatMap { syntheticEventQuery.getUserVirtualIds(it) },
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
    val cmmsEventGroup = cmmsEventGroup {
      name = eventGroup.cmmsEventGroup
      encryptedMetadata = encryptedCmmsMetadata
    }

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

  /** Get EventGroups that are associated with a DataProvider that supports Hmss. */
  private suspend fun getHmssEventGroups(): List<EventGroup> {
    return buildList {
      val includedDataProviders = mutableSetOf<String>()
      val eventGroups = listEventGroups()
      eventGroups.forEach { eventGroup ->
        val dataProvider =
          publicDataProvidersClient
            .withCallCredentials(credentials)
            .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })

        if (
          dataProvider.capabilities.honestMajorityShareShuffleSupported and
            includedDataProviders.contains(dataProvider.name).not()
        ) {
          includedDataProviders.add(dataProvider.name)
          add(eventGroup)
        }
      }
    }
  }

  private suspend fun buildCreateBasicReportRequest(
    eventGroups: List<EventGroup>
  ): CreateBasicReportRequest {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumerData.name)!!

    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, "abc123")
    val campaignGroup =
      publicReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = reportingSet {
              displayName = "campaign group"
              campaignGroup = campaignGroupKey.toName()
              primitive =
                ReportingSetKt.primitive {
                  cmmsEventGroups += eventGroups.map { it.cmmsEventGroup }
                }
            }
            reportingSetId = campaignGroupKey.reportingSetId
          }
        )

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
        basicReportId = "basicreport123",
      )

    val basicReport = basicReport {
      title = "title"
      this.campaignGroup = campaignGroup.name
      campaignGroupDisplayName = campaignGroup.displayName
      modelLine = inProcessCmmsComponents.modelLineResourceName
      reportingInterval = reportingInterval {
        reportStart = dateTime {
          year = 2021
          month = 3
          day = 14
          hours = 17
          timeZone = timeZone { id = "America/Los_Angeles" }
        }
        reportEnd = date {
          year = 2021
          month = 3
          day = 15
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

    return createBasicReportRequest {
      parent = measurementConsumerData.name
      basicReportId = basicReportKey.basicReportId
      this.basicReport = basicReport
    }
  }

  private suspend fun executeBasicReportsReportsJob(basicReportName: String) {
    val basicReportKey = BasicReportKey.fromName(basicReportName)!!

    val internalBasicReport =
      reportingServer.internalBasicReportsClient.getBasicReport(
        internalGetBasicReportRequest {
          cmmsMeasurementConsumerId = basicReportKey.cmmsMeasurementConsumerId
          externalBasicReportId = basicReportKey.basicReportId
        }
      )

    val reportName =
      ReportKey(internalBasicReport.cmmsMeasurementConsumerId, internalBasicReport.externalReportId)
        .toName()

    // BasicReportsReportsJob requires the Report to be SUCCEEDED to advance the BasicReport to
    // the next internal state.
    pollForCompletedReport(reportName)

    val measurementConsumerName = inProcessCmmsComponents.getMeasurementConsumerData().name

    val basicReportsReportsJob =
      BasicReportsReportsJob(
        measurementConsumerConfigs { configs[measurementConsumerName] = measurementConsumerConfig },
        reportingServer.internalBasicReportsClient,
        publicReportsClient,
        reportingServer.internalMetricCalculationSpecsClient,
        reportingServer.internalReportResultsClient,
        EventMessageDescriptor(TestEvent.getDescriptor()),
      )

    basicReportsReportsJob.execute()
  }

  private fun executeReportProcessorJob() {
    val processBuilder =
      ProcessBuilder("python3", POST_PROCESS_REPORT_RESULT_FILE.toPath().toString())

    processBuilder
      .command()
      .add("--internal-api-target=${"localhost:${reportingServer.internalReportingServer.port}"}")
    processBuilder.command().add("--tls-cert-file=${REPORTING_TLS_CERT_FILE.path}")
    processBuilder.command().add("--tls-key-file=${REPORTING_TLS_KEY_FILE.path}")
    processBuilder.command().add("--cert-collection-file=${ALL_ROOT_CERTS_FILE.path}")

    val process = processBuilder.start()

    process.waitFor()
  }

  companion object {
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    val ALL_ROOT_CERTS_FILE: File = SECRETS_DIR.resolve("all_root_certs.pem")

    private val TRUSTED_CERTIFICATES =
      readCertificateCollection(ALL_ROOT_CERTS_FILE).associateBy { it.subjectKeyIdentifier!! }

    private val REPORTING_TLS_CERT_FILE: File = SECRETS_DIR.resolve("reporting_tls.pem")
    private val REPORTING_TLS_KEY_FILE: File = SECRETS_DIR.resolve("reporting_tls.key")

    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"

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

    private val POST_PROCESS_REPORT_RESULT_FILE: File =
      getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "python",
            "wfa",
            "measurement",
            "reporting",
            "deploy",
            "v2",
            "common",
            "job",
            "post_process_report_result_job_executor.zip",
          )
        )!!
        .toFile()

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig()
    }
  }
}
