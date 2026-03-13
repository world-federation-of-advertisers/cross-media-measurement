// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.timestamp
import com.google.type.date
import com.google.type.dateTime
import com.google.type.interval
import com.google.type.timeZone
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import java.time.LocalDate
import java.time.ZoneId
import java.util.logging.Logger
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Before
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
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
import org.wfanet.measurement.api.v2alpha.DataProvider
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.readCertificateCollection
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.keyPair
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfigKt.principalKeyPairs
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.FULFILLER_TOPIC_ID
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.InProcessEdpAggregatorComponents
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.integration.common.PROJECT_ID
import org.wfanet.measurement.integration.common.SUBSCRIPTION_ID
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.job.BasicReportsReportsJob
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetKey
import org.wfanet.measurement.reporting.v2alpha.BasicReport
import org.wfanet.measurement.reporting.v2alpha.BasicReportsGrpcKt.BasicReportsCoroutineStub as ReportingBasicReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.CreateBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as ReportingEventGroupsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventTemplateFieldKt
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as ReportingReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub as ReportingReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ResultGroupMetricSpecKt
import org.wfanet.measurement.reporting.v2alpha.basicReport
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.createBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.createReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.dimensionSpec
import org.wfanet.measurement.reporting.v2alpha.eventFilter
import org.wfanet.measurement.reporting.v2alpha.eventTemplateField
import org.wfanet.measurement.reporting.v2alpha.getBasicReportRequest
import org.wfanet.measurement.reporting.v2alpha.getReportRequest
import org.wfanet.measurement.reporting.v2alpha.impressionQualificationFilterSpec
import org.wfanet.measurement.reporting.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.reporting.v2alpha.metricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.reportingImpressionQualificationFilter
import org.wfanet.measurement.reporting.v2alpha.reportingInterval
import org.wfanet.measurement.reporting.v2alpha.reportingSet
import org.wfanet.measurement.reporting.v2alpha.reportingUnit
import org.wfanet.measurement.reporting.v2alpha.resultGroupMetricSpec
import org.wfanet.measurement.reporting.v2alpha.resultGroupSpec
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

abstract class InProcessEdpAggregatorLifeOfAReportTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  private val accessServicesFactory: AccessServicesFactory,
  private val reportingDataServicesProviderRule: ProviderRule<Services>,
) {

  private val pubSubClient: GooglePubSubEmulatorClient by lazy {
    GooglePubSubEmulatorClient(
      host = pubSubEmulatorProvider.host,
      port = pubSubEmulatorProvider.port,
    )
  }

  private val sharedKmsClient: FakeKmsClient by lazy {
    val kekUri = FakeKmsClient.KEY_URI_PREFIX + "key1"
    val kmsKeyHandle = KeysetHandle.generateNew(KeyTemplates.get("AES128_GCM"))
    val kmsClient = FakeKmsClient()
    kmsClient.setAead(kekUri, kmsKeyHandle.getPrimitive(Aead::class.java))
    kmsClient
  }

  private val inProcessCmmsComponents =
    InProcessCmmsComponents(
      kingdomDataServicesRule,
      duchyDependenciesRule,
      useEdpSimulators = false,
      trusTeeKmsClient = sharedKmsClient,
    )

  @JvmField
  @get:Rule
  val tempPath: Path = run {
    val tempDirectory = TemporaryFolder()
    tempDirectory.create()
    tempDirectory.root.toPath()
  }

  private val syntheticEventGroupMapByEdp =
    mapOf(
      "edp1" to mapOf("edp1-eg-ref-1" to syntheticEventGroupSpec2),
      "edp2" to mapOf("edp2-eg-ref-1" to syntheticEventGroupSpec1),
      "edp3" to mapOf("edp3-eg-ref-1" to syntheticEventGroupSpec2),
    )

  private val inProcessEdpAggregatorComponents: InProcessEdpAggregatorComponents =
    InProcessEdpAggregatorComponents(
      secureComputationDatabaseAdmin = secureComputationDatabaseAdmin,
      storagePath = tempPath,
      pubSubClient = pubSubClient,
      syntheticEventGroupMapByEdp = syntheticEventGroupMapByEdp,
      syntheticPopulationSpec = syntheticPopulationSpec,
      modelLineInfoMap = modelLineInfoMap,
      externalKmsClient = sharedKmsClient,
    )

  private val daemonsStartup = TestRule { base, _ ->
    object : Statement() {
      override fun evaluate() {
        runBlocking {
          pubSubClient.createTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
          pubSubClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, FULFILLER_TOPIC_ID)
        }
        inProcessCmmsComponents.startDaemons()
        modelLineInfoMap.clear()
        modelLineInfoMap[inProcessCmmsComponents.modelLineResourceName] = MODEL_LINE_INFO
        val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
        val edpDisplayNameToResourceMap = inProcessCmmsComponents.edpDisplayNameToResourceMap
        val kingdomChannel = inProcessCmmsComponents.kingdom.publicApiChannel
        val duchyMap =
          inProcessCmmsComponents.duchies.map { it.externalDuchyId to it.publicApiChannel }.toMap()
        inProcessEdpAggregatorComponents.startDaemons(
          kingdomChannel,
          measurementConsumerData,
          edpDisplayNameToResourceMap,
          mapOf(
            "edp1" to
              DataProviderKt.capabilities {
                honestMajorityShareShuffleSupported = true
                trusTeeSupported = false
              },
            "edp2" to
              DataProviderKt.capabilities {
                honestMajorityShareShuffleSupported = true
                trusTeeSupported = true
              },
            "edp3" to
              DataProviderKt.capabilities {
                honestMajorityShareShuffleSupported = false
                trusTeeSupported = true
              },
          ),
          duchyMap,
          edpNoise =
            mapOf(
              "edp1" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
              "edp2" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
              "edp3" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
            ),
        )
        runBlocking {
          registerDataAvailabilityIntervals(kingdomChannel, edpDisplayNameToResourceMap)
        }
        try {
          base.evaluate()
        } finally {
          inProcessCmmsComponents.stopDuchyDaemons()
          inProcessCmmsComponents.stopPopulationRequisitionFulfillerDaemon()
          inProcessEdpAggregatorComponents.stopDaemons()
          runBlocking {
            pubSubClient.deleteTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
            pubSubClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
          }
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
          publicMeasurementConsumersClient
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
      inProcessEdpAggregatorComponents,
      daemonsStartup,
      reportingDataServicesProviderRule,
      reportingServerRule,
    )

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val reportingEventGroupsClient by lazy {
    ReportingEventGroupsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val reportingReportingSetsClient by lazy {
    ReportingReportingSetsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val reportingDataProvidersClient by lazy {
    DataProvidersCoroutineStub(reportingServer.publicApiChannel)
  }
  private val reportingBasicReportsClient by lazy {
    ReportingBasicReportsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val reportingReportsClient by lazy {
    ReportingReportsCoroutineStub(reportingServer.publicApiChannel)
  }

  private lateinit var credentials: TrustedPrincipalAuthInterceptor.Credentials

  @Before
  fun setup() {
    createAccessPolicy()
  }

  private fun createAccessPolicy() {
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
          protectedResource = ""
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

  @Test
  fun `HMSS no noise basic report has the expected result`() = runBlocking {
    val hmssEventGroups = getHmssEventGroups()
    check(hmssEventGroups.size > 1)

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        hmssEventGroups,
        "hmss-campaign",
        "hmss-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    val retrievedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertRunningBasicReport(createBasicReportRequest, createdBasicReport, retrievedBasicReport)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

    val measurements = listMeasurements()
    val hmssProtocolMeasurements =
      measurements.filter { measurement ->
        measurement.protocolConfig.protocolsList.any { it.hasHonestMajorityShareShuffle() }
      }
    assertWithMessage("at least one measurement used HMSS protocol")
      .that(hmssProtocolMeasurements)
      .isNotEmpty()

    assertStructuralResults(completedBasicReport)
    assertNoNoiseResults(
      completedBasicReport,
      expectedCrossPublisherReach = EXPECTED_HMSS_CROSS_PUBLISHER_REACH,
      expectedCrossPublisherImpressions = EXPECTED_HMSS_CROSS_PUBLISHER_IMPRESSIONS,
      expectedKPlusReach = EXPECTED_HMSS_K_PLUS_REACH,
      expectedEdpSpec1Reach = EXPECTED_HMSS_EDP_SPEC1_REACH,
      expectedEdpSpec2Reach = EXPECTED_HMSS_EDP_SPEC2_REACH,
    )
  }

  @Test
  fun `TrusTee basic report has the expected result`() = runBlocking {
    val trusTeeEventGroups = getTrusTeeEventGroups()
    check(trusTeeEventGroups.size > 1)

    val createBasicReportRequest =
      buildCreateBasicReportRequest(
        trusTeeEventGroups,
        "trustee-campaign",
        "trustee-basicreport",
        includeIqfFilter = false,
      )

    val createdBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .createBasicReport(createBasicReportRequest)

    val retrievedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertRunningBasicReport(createBasicReportRequest, createdBasicReport, retrievedBasicReport)

    executeBasicReportsReportsJob(createdBasicReport.name)
    executeReportProcessorJob()

    val completedBasicReport =
      reportingBasicReportsClient
        .withCallCredentials(credentials)
        .getBasicReport(getBasicReportRequest { name = createdBasicReport.name })

    assertThat(completedBasicReport.state).isEqualTo(BasicReport.State.SUCCEEDED)

    val measurements = listMeasurements()
    val trusTeeProtocolMeasurements =
      measurements.filter { measurement ->
        measurement.protocolConfig.protocolsList.any { it.hasTrusTee() }
      }
    assertWithMessage("at least one measurement used TrusTee protocol")
      .that(trusTeeProtocolMeasurements)
      .isNotEmpty()

    assertTrusTeeResults(completedBasicReport)
  }

  protected abstract fun assertTrusTeeResults(basicReport: BasicReport)

  /**
   * Checks structural invariants on basic report results: all metrics are positive, k+ reach is
   * monotonically non-increasing, and component-level metrics are present.
   */
  protected fun assertStructuralResults(basicReport: BasicReport) {
    basicReport.resultGroupsList.forEach { resultGroup ->
      val totalResults =
        resultGroup.resultsList.filter {
          it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
        }
      assertWithMessage("total results").that(totalResults).isNotEmpty()

      totalResults.forEach { result ->
        val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

        assertWithMessage("population size").that(result.metricSet.populationSize).isGreaterThan(0)

        assertWithMessage("reporting unit reach")
          .that(reportingUnitCumulative.reach)
          .isGreaterThan(0L)

        assertWithMessage("reporting unit percent reach")
          .that(reportingUnitCumulative.percentReach)
          .isGreaterThan(0f)

        assertWithMessage("reporting unit impressions")
          .that(reportingUnitCumulative.impressions)
          .isGreaterThan(0)

        assertWithMessage("reporting unit average frequency")
          .that(reportingUnitCumulative.averageFrequency)
          .isGreaterThan(0f)

        assertWithMessage("reporting unit grps")
          .that(reportingUnitCumulative.grps)
          .isGreaterThan(0f)

        assertWithMessage("reporting unit k+ reach is monotonically non-increasing")
          .that(reportingUnitCumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
          .isTrue()

        assertWithMessage("reporting unit percent k+ reach is monotonically non-increasing")
          .that(
            reportingUnitCumulative.percentKPlusReachList.zipWithNext { a, b -> b <= a }.all { it }
          )
          .isTrue()

        assertWithMessage("stacked incremental reach is not empty")
          .that(result.metricSet.reportingUnit.stackedIncrementalReachList)
          .isNotEmpty()

        result.metricSet.componentsList.forEach { component ->
          val cumulative = component.value.cumulative

          assertWithMessage("component ${component.key} reach")
            .that(cumulative.reach)
            .isGreaterThan(0L)

          assertWithMessage("component ${component.key} impressions")
            .that(cumulative.impressions)
            .isGreaterThan(0L)

          assertWithMessage("component ${component.key} k+ reach is monotonically non-increasing")
            .that(cumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
            .isTrue()
        }
      }
    }
  }

  /**
   * Asserts exact metric values for a no-noise basic report.
   *
   * With NoiseMechanism.NONE, protocols produce deterministic results. The test uses 2 EDPs with
   * different synthetic data (small_data_spec and small_data_spec_2 with partially overlapping VID
   * ranges) so that the cross-publisher reach (union of VIDs) is strictly greater than any
   * individual EDP's reach.
   */
  protected fun assertNoNoiseResults(
    basicReport: BasicReport,
    expectedCrossPublisherReach: Long,
    expectedCrossPublisherImpressions: Long,
    expectedKPlusReach: List<Long>,
    expectedEdpSpec1Reach: Long,
    expectedEdpSpec2Reach: Long,
  ) {
    assertWithMessage("result groups").that(basicReport.resultGroupsList).hasSize(1)

    val resultGroup = basicReport.resultGroupsList.single()
    val totalResults =
      resultGroup.resultsList.filter {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    assertWithMessage("total results").that(totalResults).hasSize(1)

    val result = totalResults.single()
    val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

    assertWithMessage("population size").that(result.metricSet.populationSize).isGreaterThan(0)

    assertWithMessage("cross-publisher reach")
      .that(reportingUnitCumulative.reach)
      .isEqualTo(expectedCrossPublisherReach)

    assertWithMessage("cross-publisher percent reach")
      .that(reportingUnitCumulative.percentReach)
      .isGreaterThan(0f)

    assertWithMessage("cross-publisher impressions")
      .that(reportingUnitCumulative.impressions)
      .isEqualTo(expectedCrossPublisherImpressions)

    assertWithMessage("cross-publisher average frequency")
      .that(reportingUnitCumulative.averageFrequency)
      .isGreaterThan(0f)

    assertWithMessage("cross-publisher grps").that(reportingUnitCumulative.grps).isGreaterThan(0f)

    assertWithMessage("cross-publisher k+ reach")
      .that(reportingUnitCumulative.kPlusReachList)
      .containsExactlyElementsIn(expectedKPlusReach)
      .inOrder()

    assertWithMessage("cross-publisher k+ reach is monotonically non-increasing")
      .that(reportingUnitCumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
      .isTrue()

    assertWithMessage("stacked incremental reach is not empty")
      .that(result.metricSet.reportingUnit.stackedIncrementalReachList)
      .isNotEmpty()

    assertWithMessage("number of components").that(result.metricSet.componentsCount).isEqualTo(2)

    val componentReaches = mutableListOf<Long>()
    result.metricSet.componentsList.forEach { component ->
      val cumulative = component.value.cumulative
      componentReaches.add(cumulative.reach)

      assertWithMessage("component ${component.key} reach").that(cumulative.reach).isGreaterThan(0L)

      assertWithMessage("component ${component.key} impressions")
        .that(cumulative.impressions)
        .isGreaterThan(0L)

      assertWithMessage("component ${component.key} k+ reach is monotonically non-increasing")
        .that(cumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
        .isTrue()

      assertWithMessage("cross-publisher reach > component ${component.key} reach")
        .that(reportingUnitCumulative.reach)
        .isGreaterThan(cumulative.reach)

      assertWithMessage("cross-publisher impressions >= component ${component.key} impressions")
        .that(reportingUnitCumulative.impressions)
        .isAtLeast(cumulative.impressions)

      for (k in cumulative.kPlusReachList.indices) {
        assertWithMessage(
            "cross-publisher k+${k + 1} reach >= component ${component.key} k+${k + 1} reach"
          )
          .that(reportingUnitCumulative.kPlusReachList[k])
          .isAtLeast(cumulative.kPlusReachList[k])
      }
    }

    assertWithMessage("component reach values")
      .that(componentReaches.sorted())
      .containsExactly(expectedEdpSpec2Reach, expectedEdpSpec1Reach)
      .inOrder()

    assertWithMessage("cross-publisher reach < sum of individual EDP reaches")
      .that(reportingUnitCumulative.reach)
      .isLessThan(componentReaches.sum())
  }

  /**
   * Asserts that k-anonymity filtering zeroed some or all k+ reach metrics.
   *
   * With a high k-anonymity threshold, frequency buckets with fewer users than the threshold are
   * filtered to zero. The [expectedNonZeroKPlusReachCount] specifies how many leading k+ reach
   * entries should survive.
   *
   * When [expectedNonZeroKPlusReachCount] is 0, all TrusTee metrics (reach, k+ reach, component
   * reaches) are expected to be zeroed. When positive, cross-publisher reach and component reaches
   * should survive while only higher frequency k+ reach entries are zeroed.
   *
   * @param expectedNonZeroKPlusReachCount The number of leading k+ reach entries that should be
   *   positive. All remaining entries are expected to be zero. Since k+ reach is monotonically
   *   non-increasing, the non-zero entries are always at the front.
   */
  protected fun assertKAnonFilteredResults(
    basicReport: BasicReport,
    expectedNonZeroKPlusReachCount: Int,
  ) {
    assertWithMessage("result groups").that(basicReport.resultGroupsList).hasSize(1)

    val resultGroup = basicReport.resultGroupsList.single()
    val totalResults =
      resultGroup.resultsList.filter {
        it.metadata.metricFrequency.selectorCase == MetricFrequencySpec.SelectorCase.TOTAL
      }
    assertWithMessage("total results").that(totalResults).hasSize(1)

    val result = totalResults.single()
    val reportingUnitCumulative = result.metricSet.reportingUnit.cumulative

    if (expectedNonZeroKPlusReachCount > 0) {
      assertWithMessage("cross-publisher reach survives k-anon")
        .that(reportingUnitCumulative.reach)
        .isGreaterThan(0L)
    } else {
      assertWithMessage("cross-publisher reach zeroed by k-anon")
        .that(reportingUnitCumulative.reach)
        .isEqualTo(0L)
    }

    assertWithMessage("cross-publisher impressions")
      .that(reportingUnitCumulative.impressions)
      .isGreaterThan(0L)

    val kPlusReach = reportingUnitCumulative.kPlusReachList
    for (k in kPlusReach.indices) {
      if (k < expectedNonZeroKPlusReachCount) {
        assertWithMessage("k+${k + 1} reach should survive k-anon")
          .that(kPlusReach[k])
          .isGreaterThan(0L)
      } else {
        assertWithMessage("k+${k + 1} reach should be zeroed by k-anon")
          .that(kPlusReach[k])
          .isEqualTo(0L)
      }
    }

    assertWithMessage("k+ reach is monotonically non-increasing")
      .that(kPlusReach.zipWithNext { a, b -> b <= a }.all { it })
      .isTrue()

    assertWithMessage("number of components").that(result.metricSet.componentsCount).isEqualTo(2)

    result.metricSet.componentsList.forEach { component ->
      val cumulative = component.value.cumulative

      if (expectedNonZeroKPlusReachCount > 0) {
        assertWithMessage("component ${component.key} reach survives k-anon")
          .that(cumulative.reach)
          .isGreaterThan(0L)
      } else {
        assertWithMessage("component ${component.key} reach zeroed by k-anon")
          .that(cumulative.reach)
          .isEqualTo(0L)
      }

      assertWithMessage("component ${component.key} impressions")
        .that(cumulative.impressions)
        .isGreaterThan(0L)

      assertWithMessage("component ${component.key} k+ reach is monotonically non-increasing")
        .that(cumulative.kPlusReachList.zipWithNext { a, b -> b <= a }.all { it })
        .isTrue()
    }
  }

  private fun assertRunningBasicReport(
    createBasicReportRequest: CreateBasicReportRequest,
    createdBasicReport: BasicReport,
    retrievedBasicReport: BasicReport,
  ) {
    assertThat(retrievedBasicReport)
      .ignoringFields(BasicReport.CREATE_TIME_FIELD_NUMBER)
      .isEqualTo(
        createBasicReportRequest.basicReport.copy {
          this.name = createdBasicReport.name
          this.state = BasicReport.State.RUNNING
          this.effectiveImpressionQualificationFilters +=
            retrievedBasicReport.impressionQualificationFiltersList
          this.effectiveModelLine = inProcessCmmsComponents.modelLineResourceName
          this.reportingInterval =
            this.reportingInterval.copy {
              this.effectiveReportStart =
                createBasicReportRequest.basicReport.reportingInterval.reportStart
            }
        }
      )
    assertThat(retrievedBasicReport.createTime).isEqualTo(createdBasicReport.createTime)
  }

  private suspend fun buildCreateBasicReportRequest(
    eventGroups: List<EventGroup>,
    campaignGroupId: String,
    basicReportId: String,
    includeIqfFilter: Boolean = true,
  ): CreateBasicReportRequest {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumerData.name)!!

    val campaignGroupKey = ReportingSetKey(measurementConsumerKey, campaignGroupId)
    val campaignGroup =
      reportingReportingSetsClient
        .withCallCredentials(credentials)
        .createReportingSet(
          createReportingSetRequest {
            parent = measurementConsumerData.name
            reportingSet = reportingSet {
              displayName = "campaign group"
              campaignGroup = campaignGroupKey.toName()
              primitive =
                ReportingSetKt.primitive {
                  cmmsEventGroups += eventGroups.take(2).map { it.cmmsEventGroup }
                }
            }
            reportingSetId = campaignGroupKey.reportingSetId
          }
        )

    val basicReportKey =
      BasicReportKey(
        cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId,
        basicReportId = basicReportId,
      )

    return createBasicReportRequest {
      parent = measurementConsumerData.name
      this.basicReportId = basicReportKey.basicReportId
      basicReport = basicReport {
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
            day = 18
          }
        }
        impressionQualificationFilters +=
          if (includeIqfFilter) IMPRESSION_QUALIFICATION_FILTER
          else PASSTHROUGH_IMPRESSION_QUALIFICATION_FILTER
        resultGroupSpecs += resultGroupSpec {
          title = "title"
          reportingUnit = reportingUnit {
            components += eventGroups.take(2).map { it.cmmsDataProvider }
          }
          metricFrequency = metricFrequencySpec { total = true }
          dimensionSpec = dimensionSpec {}
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
    }
  }

  private suspend fun listMeasurements(): List<Measurement> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    return publicMeasurementsClient
      .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
      .listMeasurements(listMeasurementsRequest { parent = measurementConsumerData.name })
      .measurementsList
  }

  private suspend fun listReportingEventGroups(): List<EventGroup> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    return reportingEventGroupsClient
      .withCallCredentials(credentials)
      .listEventGroups(
        listEventGroupsRequest {
          parent = measurementConsumerData.name
          pageSize = 1000
        }
      )
      .eventGroupsList
  }

  private suspend fun pollForCompletedReport(reportName: String): Report {
    while (true) {
      val retrievedReport =
        reportingReportsClient
          .withCallCredentials(credentials)
          .getReport(getReportRequest { name = reportName })
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (retrievedReport.state) {
        Report.State.SUCCEEDED,
        Report.State.FAILED -> return retrievedReport
        Report.State.RUNNING,
        Report.State.UNRECOGNIZED,
        Report.State.STATE_UNSPECIFIED -> delay(5000)
      }
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
    pollForCompletedReport(reportName)
    val measurementConsumerName = inProcessCmmsComponents.getMeasurementConsumerData().name
    BasicReportsReportsJob(
        measurementConsumerConfigs { configs[measurementConsumerName] = measurementConsumerConfig },
        reportingServer.internalBasicReportsClient,
        reportingReportsClient,
        reportingServer.internalMetricCalculationSpecsClient,
        reportingServer.internalReportResultsClient,
        EventMessageDescriptor(TestEvent.getDescriptor()),
      )
      .execute()
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

  private suspend fun registerDataAvailabilityIntervals(
    kingdomChannel: io.grpc.Channel,
    edpDisplayNameToResourceMap:
      Map<String, org.wfanet.measurement.loadtest.resourcesetup.Resources.Resource>,
  ) {
    val dataAvailabilityInterval = computeDataAvailabilityInterval()
    val modelLineName = inProcessCmmsComponents.modelLineResourceName
    for ((_, resource) in edpDisplayNameToResourceMap) {
      val dataProvidersStub =
        DataProvidersCoroutineStub(kingdomChannel).withPrincipalName(resource.name)
      dataProvidersStub.replaceDataAvailabilityIntervals(
        replaceDataAvailabilityIntervalsRequest {
          name = resource.name
          dataAvailabilityIntervals +=
            DataProviderKt.dataAvailabilityMapEntry {
              key = modelLineName
              value = dataAvailabilityInterval
            }
        }
      )
    }
  }

  private fun computeDataAvailabilityInterval(): com.google.type.Interval {
    val dateSpec = syntheticEventGroupSpec1.dateSpecsList.first()
    val dateRange = dateSpec.dateRange
    val startTime =
      LocalDate.of(dateRange.start.year, dateRange.start.month, dateRange.start.day)
        .atStartOfDay(ZoneId.of("UTC"))
        .toInstant()
    val endTime =
      LocalDate.of(
          dateRange.endExclusive.year,
          dateRange.endExclusive.month,
          dateRange.endExclusive.day,
        )
        .atStartOfDay(ZoneId.of("UTC"))
        .toInstant()
    return interval {
      this.startTime = timestamp { seconds = startTime.epochSecond }
      this.endTime = timestamp { seconds = endTime.epochSecond }
    }
  }

  private suspend fun getEventGroupsByCapability(
    isCapable: (DataProvider.Capabilities) -> Boolean
  ): List<EventGroup> {
    return buildList {
      val includedDataProviders = mutableSetOf<String>()
      val eventGroups = listReportingEventGroups()
      eventGroups.forEach { eventGroup ->
        val dataProvider =
          reportingDataProvidersClient
            .withCallCredentials(credentials)
            .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })
        if (
          isCapable(dataProvider.capabilities) && !includedDataProviders.contains(dataProvider.name)
        ) {
          includedDataProviders.add(dataProvider.name)
          add(eventGroup)
        }
      }
    }
  }

  private suspend fun getHmssEventGroups(): List<EventGroup> = getEventGroupsByCapability {
    it.honestMajorityShareShuffleSupported
  }

  private suspend fun getTrusTeeEventGroups(): List<EventGroup> = getEventGroupsByCapability {
    it.trusTeeSupported
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()
    private val ALL_ROOT_CERTS_FILE: File = SECRETS_DIR.resolve("all_root_certs.pem")
    private val TRUSTED_CERTIFICATES =
      readCertificateCollection(ALL_ROOT_CERTS_FILE).associateBy { it.subjectKeyIdentifier!! }
    private val REPORTING_TLS_CERT_FILE: File = SECRETS_DIR.resolve("reporting_tls.pem")
    private val REPORTING_TLS_KEY_FILE: File = SECRETS_DIR.resolve("reporting_tls.key")
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
    private const val MC_SIGNING_PRIVATE_KEY_PATH = "mc_cs_private.der"
    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
      )
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!
    private val TEST_RESULTS_FULFILLER_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
        "edpaggregator",
        "resultsfulfiller",
        "testing",
      )
    private val TEST_RESULTS_FULFILLER_DATA_RUNTIME_PATH =
      getRuntimePath(TEST_RESULTS_FULFILLER_DATA_PATH)!!
    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec1: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec2: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec_2.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val populationSpec: PopulationSpec =
      parseTextProto(
        TEST_RESULTS_FULFILLER_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto")
          .toFile(),
        PopulationSpec.getDefaultInstance(),
      )
    private val MODEL_LINE_INFO =
      ModelLineInfo(
        populationSpec = populationSpec,
        vidIndexMap = InMemoryVidIndexMap.build(populationSpec),
        eventDescriptor = TestEvent.getDescriptor(),
        localAlias = null,
      )
    val modelLineInfoMap: MutableMap<String, ModelLineInfo> = mutableMapOf()
    private val PASSTHROUGH_IMPRESSION_QUALIFICATION_FILTER =
      reportingImpressionQualificationFilter {
        custom =
          ReportingImpressionQualificationFilterKt.customImpressionQualificationFilterSpec {
            filterSpec += impressionQualificationFilterSpec { mediaType = MediaType.DISPLAY }
            filterSpec += impressionQualificationFilterSpec { mediaType = MediaType.VIDEO }
          }
      }

    private val IMPRESSION_QUALIFICATION_FILTER = reportingImpressionQualificationFilter {
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
    private const val EXPECTED_HMSS_CROSS_PUBLISHER_REACH = 5371L
    private const val EXPECTED_HMSS_CROSS_PUBLISHER_IMPRESSIONS = 9124L
    private val EXPECTED_HMSS_K_PLUS_REACH = listOf(5371L, 2678L, 682L, 394L, 0L)
    private const val EXPECTED_HMSS_EDP_SPEC1_REACH = 4473L
    private const val EXPECTED_HMSS_EDP_SPEC2_REACH = 3338L

    const val EXPECTED_TRUSTEE_CROSS_PUBLISHER_REACH = 5369L
    const val EXPECTED_TRUSTEE_CROSS_PUBLISHER_IMPRESSIONS = 9122L
    val EXPECTED_TRUSTEE_K_PLUS_REACH = listOf(5369L, 2677L, 682L, 394L, 0L)
    const val EXPECTED_TRUSTEE_EDP_SPEC1_REACH = 4472L
    const val EXPECTED_TRUSTEE_EDP_SPEC2_REACH = 3338L

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()
  }
}
