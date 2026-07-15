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
import com.google.protobuf.TypeRegistry
import com.google.protobuf.struct
import com.google.protobuf.timestamp
import com.google.protobuf.value
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
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
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
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroup as CmmsEventGroup
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.Measurement
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig as PublicProtocolConfig
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getDataProviderRequest
import org.wfanet.measurement.api.v2alpha.getMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.listMeasurementsRequest
import org.wfanet.measurement.api.v2alpha.replaceDataAvailabilityIntervalsRequest
import org.wfanet.measurement.api.v2alpha.unpack
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
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.encryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfig
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroup as EdpaEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.AdMetadataKt.campaignMetadata as edpaCampaignMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.MetadataKt.adMetadata as edpaAdMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.entityKey as edpaEntityKey
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.EventGroupKt.metadata as edpaEventGroupMetadata
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.MappedEventGroup
import org.wfanet.measurement.edpaggregator.eventgroups.v1alpha.eventGroup as edpaEventGroup
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.EntityKeySpec
import org.wfanet.measurement.integration.common.EventGroupConfig
import org.wfanet.measurement.integration.common.FULFILLER_TOPIC_ID
import org.wfanet.measurement.integration.common.InProcessCmmsComponents
import org.wfanet.measurement.integration.common.InProcessDuchy
import org.wfanet.measurement.integration.common.InProcessEdpAggregatorComponents
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.integration.common.PROJECT_ID
import org.wfanet.measurement.integration.common.SUBSCRIPTION_ID
import org.wfanet.measurement.internal.kingdom.HmssProtocolConfigConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfig
import org.wfanet.measurement.internal.kingdom.ProtocolConfigKt
import org.wfanet.measurement.internal.kingdom.TrusTeeProtocolConfigConfig
import org.wfanet.measurement.internal.kingdom.hmssProtocolConfigConfig
import org.wfanet.measurement.internal.kingdom.trusTeeProtocolConfigConfig
import org.wfanet.measurement.internal.reporting.v2.getBasicReportRequest as internalGetBasicReportRequest
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
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
import org.wfanet.measurement.reporting.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.reporting.v2alpha.MediaType
import org.wfanet.measurement.reporting.v2alpha.MetricFrequencySpec
import org.wfanet.measurement.reporting.v2alpha.Report
import org.wfanet.measurement.reporting.v2alpha.ReportingImpressionQualificationFilterKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
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
  private val duchyNames: List<String> = ALL_DUCHY_NAMES,
  private val hmssEnabled: Boolean,
  private val trusTeeEnabled: Boolean,
  private val multiEdpDisplayNames: Set<String> = emptySet(),
) {

  protected val expectedProtocol: PublicProtocolConfig.Protocol.ProtocolCase =
    when {
      hmssEnabled && trusTeeEnabled ->
        error("hmssEnabled and trusTeeEnabled are mutually exclusive")
      hmssEnabled -> PublicProtocolConfig.Protocol.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE
      trusTeeEnabled -> PublicProtocolConfig.Protocol.ProtocolCase.TRUS_TEE
      else -> PublicProtocolConfig.Protocol.ProtocolCase.DIRECT
    }

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
      duchyNames = duchyNames,
      hmssEnabled = hmssEnabled,
      trusTeeEnabled = trusTeeEnabled,
      trusTeeKmsClient = sharedKmsClient,
    )

  @JvmField
  @get:Rule
  val tempPath: Path = run {
    val tempDirectory = TemporaryFolder()
    tempDirectory.create()
    tempDirectory.root.toPath()
  }

  // Each event group config combines the synthetic data spec with entity key configuration.
  //   - edp1-eg-ref-1: no entity key (legacy; Kingdom defaults entity_type="campaign").
  //   - edp1-eg-creative-1: entity_type="creative-id".
  //   - edp1-eg-multi-creative: two creative-id entity keys in the same blob.
  //   - edp2-eg-ref-1, edp3-eg-ref-1: entity_type="campaign" + entity_id.
  //   - edp2-eg-creative-1: entity_type="creative-id".
  //   - edp4-eg-ref-1: entity_type="ad_group" (non-default type round-trip).
  // listReportingEventGroups() filters entity_type_in=["campaign", "ad_group", "creative-id"]
  // so all event groups are visible.
  protected val eventGroupConfigsByEdp: Map<String, Map<String, EventGroupConfig>> =
    mapOf(
      "edp1" to
        mapOf(
          "edp1-eg-ref-1" to EventGroupConfig.LegacySpec(syntheticEventGroupSpec2),
          EDP1_CREATIVE_EVENT_GROUP_REF_ID to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_CREATIVE_EVENT_GROUP_REF_ID),
                    syntheticEventGroupSpec2,
                    ENTITY_METADATA,
                  )
                )
            ),
          "multi-creative" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_A_ID),
                    syntheticEventGroupSpec2,
                    ENTITY_METADATA,
                  ),
                  EntityKeySpec(
                    EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP1_MULTI_CREATIVE_B_ID),
                    syntheticEventGroupSpec2,
                    ENTITY_METADATA,
                  ),
                )
            ),
        ),
      "edp2" to
        mapOf(
          "campaign/edp2-eg-ref-1" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey("campaign", "edp2-eg-ref-1"),
                    syntheticEventGroupSpec1,
                    ENTITY_METADATA,
                  )
                )
            ),
          EDP2_CREATIVE_EVENT_GROUP_REF_ID to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey(CREATIVE_ID_ENTITY_TYPE, EDP2_CREATIVE_EVENT_GROUP_REF_ID),
                    syntheticEventGroupSpec1,
                    ENTITY_METADATA,
                  )
                )
            ),
        ),
      "edp3" to
        mapOf(
          "campaign/edp3-eg-ref-1" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey("campaign", "edp3-eg-ref-1"),
                    syntheticEventGroupSpec2,
                    ENTITY_METADATA,
                  )
                )
            )
        ),
      AD_GROUP_EDP_DISPLAY_NAME to
        mapOf(
          AD_GROUP_EDP_EVENT_GROUP_REF_ID to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey("ad_group", AD_GROUP_EDP_EVENT_GROUP_REF_ID),
                    syntheticEventGroupSpec1,
                    ENTITY_METADATA,
                  )
                )
            )
        ),
    )

  private val inProcessEdpAggregatorComponents: InProcessEdpAggregatorComponents =
    InProcessEdpAggregatorComponents(
      secureComputationDatabaseAdmin = secureComputationDatabaseAdmin,
      storagePath = tempPath,
      pubSubClient = pubSubClient,
      eventGroupConfigsByEdp = eventGroupConfigsByEdp,
      populationSpec = populationSpec,
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
                honestMajorityShareShuffleSupported = hmssEnabled
                trusTeeSupported = trusTeeEnabled
              },
            "edp2" to
              DataProviderKt.capabilities {
                honestMajorityShareShuffleSupported = hmssEnabled
                trusTeeSupported = trusTeeEnabled
              },
            "edp3" to
              DataProviderKt.capabilities {
                honestMajorityShareShuffleSupported = hmssEnabled
                trusTeeSupported = trusTeeEnabled
              },
            "edp4" to
              DataProviderKt.capabilities {
                honestMajorityShareShuffleSupported = hmssEnabled
                trusTeeSupported = trusTeeEnabled
              },
          ),
          duchyMap,
          edpNoise =
            mapOf(
              "edp1" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
              "edp2" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
              "edp3" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
              "edp4" to ResultsFulfillerParams.NoiseParams.NoiseType.NONE,
            ),
          edpMultiPartyNoiseTypes =
            mapOf(
              "edp4" to listOf(ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN)
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
          metricSpecConfigOverride = NO_SAMPLING_METRIC_SPEC_CONFIG,
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
  protected val reportingBasicReportsClient by lazy {
    ReportingBasicReportsCoroutineStub(reportingServer.publicApiChannel)
  }
  private val reportingReportsClient by lazy {
    ReportingReportsCoroutineStub(reportingServer.publicApiChannel)
  }

  protected lateinit var credentials: TrustedPrincipalAuthInterceptor.Credentials

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
  // Protected so subclasses can call this from their assertTrusTeeMetricResults overrides
  // with different expected values (e.g. after fold-down changes k+ reach).
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

  protected fun assertSingleEdpNoNoiseResults(
    basicReport: BasicReport,
    expectedReach: Long,
    expectedImpressions: Long,
    expectedKPlusReach: List<Long>,
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

    assertWithMessage("reach").that(reportingUnitCumulative.reach).isEqualTo(expectedReach)

    assertWithMessage("impressions")
      .that(reportingUnitCumulative.impressions)
      .isEqualTo(expectedImpressions)

    assertWithMessage("k+ reach")
      .that(reportingUnitCumulative.kPlusReachList)
      .containsExactlyElementsIn(expectedKPlusReach)
      .inOrder()

    assertWithMessage("number of components").that(result.metricSet.componentsCount).isEqualTo(1)

    val component = result.metricSet.componentsList.single()
    val componentCumulative = component.value.cumulative

    assertWithMessage("component reach").that(componentCumulative.reach).isEqualTo(expectedReach)

    assertWithMessage("component impressions")
      .that(componentCumulative.impressions)
      .isEqualTo(expectedImpressions)
  }

  protected fun assertRunningBasicReport(
    createBasicReportRequest: CreateBasicReportRequest,
    createdBasicReport: BasicReport,
    retrievedBasicReport: BasicReport,
    // Defaults to the caller-supplied campaign_group (the DataProvider-component case, where it
    // equals effective_campaign_group). For ReportingSet components campaign_group is empty and the
    // server synthesizes one, so callers pass the synthesized effective_campaign_group explicitly.
    expectedEffectiveCampaignGroup: String = createBasicReportRequest.basicReport.campaignGroup,
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
          this.effectiveCampaignGroup = expectedEffectiveCampaignGroup
          this.reportingInterval =
            this.reportingInterval.copy {
              this.effectiveReportStart =
                createBasicReportRequest.basicReport.reportingInterval.reportStart
            }
        }
      )
    assertThat(retrievedBasicReport.createTime).isEqualTo(createdBasicReport.createTime)
  }

  protected suspend fun buildCreateBasicReportRequest(
    eventGroups: List<EventGroup>,
    campaignGroupId: String?,
    basicReportId: String,
    includeIqfFilter: Boolean = true,
  ): CreateBasicReportRequest {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    val measurementConsumerKey = MeasurementConsumerKey.fromName(measurementConsumerData.name)!!

    // A null campaignGroupId means the caller omits campaign_group so the server synthesizes one,
    // which is required for ReportingSet-component reporting units.
    val campaignGroup: ReportingSet? =
      if (campaignGroupId == null) {
        null
      } else {
        val campaignGroupKey = ReportingSetKey(measurementConsumerKey, campaignGroupId)
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
      }

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
        if (campaignGroup != null) {
          this.campaignGroup = campaignGroup.name
          campaignGroupDisplayName = campaignGroup.displayName
        }
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
            components += eventGroups.map { it.cmmsDataProvider }.distinct()
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

  /**
   * Creates a primitive [ReportingSet] spanning [cmmsEventGroups], for use as a [ReportingUnit]
   * component.
   *
   * The `campaign_group` field is intentionally left empty: a ReportingSet component does not
   * reference the campaign group, which the server synthesizes when `campaign_group` is not
   * specified on the [CreateBasicReportRequest].
   */
  protected suspend fun createPrimitiveReportingSet(
    reportingSetId: String,
    displayName: String,
    cmmsEventGroups: List<String>,
  ): ReportingSet {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    return reportingReportingSetsClient
      .withCallCredentials(credentials)
      .createReportingSet(
        createReportingSetRequest {
          parent = measurementConsumerData.name
          reportingSet = reportingSet {
            this.displayName = displayName
            primitive = ReportingSetKt.primitive { this.cmmsEventGroups += cmmsEventGroups }
          }
          this.reportingSetId = reportingSetId
        }
      )
  }

  protected suspend fun listMeasurements(): List<Measurement> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    return publicMeasurementsClient
      .withAuthenticationKey(measurementConsumerData.apiAuthenticationKey)
      .listMeasurements(listMeasurementsRequest { parent = measurementConsumerData.name })
      .measurementsList
  }

  protected suspend fun getMeasurementsForBasicReport(basicReportName: String): List<Measurement> {
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
    return listMeasurements().filter {
      it.measurementSpec.unpack<MeasurementSpec>().reportingMetadata.report == reportName
    }
  }

  protected fun assertExpectedProtocolUsed(measurements: List<Measurement>) {
    assertWithMessage("measurements").that(measurements).isNotEmpty()
    var expectedProtocolFound = false
    for (measurement in measurements) {
      val protocol = measurement.protocolConfig.protocolsList.single()
      assertWithMessage("protocol for ${measurement.name}")
        .that(protocol.protocolCase)
        .isAnyOf(PublicProtocolConfig.Protocol.ProtocolCase.DIRECT, expectedProtocol)
      if (protocol.protocolCase == expectedProtocol) expectedProtocolFound = true
    }
    assertWithMessage("at least one $expectedProtocol measurement")
      .that(expectedProtocolFound)
      .isTrue()
  }

  /** Resource name of the single MeasurementConsumer used by these tests. */
  protected fun measurementConsumerName(): String =
    inProcessCmmsComponents.getMeasurementConsumerData().name

  /**
   * Builds a valid EDPA-side source [EdpaEventGroup] for driving [EventGroupSync] directly in
   * migration tests. Sets whichever of `event_group_reference_id` / `entity_key` is provided
   * (mirroring how an EDP evolves a row across the #4175 migration), plus the fields
   * `EventGroupSync.validateEventGroup` requires: media type, data-availability interval, and
   * metadata. `campaign` names the campaign metadata so a mutation is observable across syncs.
   */
  protected fun buildMigrationSourceEventGroup(
    referenceId: String?,
    entityType: String?,
    entityId: String?,
    campaign: String,
  ): EdpaEventGroup = edpaEventGroup {
    if (!referenceId.isNullOrEmpty()) {
      eventGroupReferenceId = referenceId
    }
    measurementConsumer = measurementConsumerName()
    if (entityType != null && entityId != null) {
      entityKey = edpaEntityKey {
        this.entityType = entityType
        this.entityId = entityId
      }
    }
    eventGroupMetadata = edpaEventGroupMetadata {
      adMetadata = edpaAdMetadata {
        campaignMetadata = edpaCampaignMetadata {
          brand = "migration-brand"
          this.campaign = campaign
        }
      }
    }
    dataAvailabilityInterval = interval {
      startTime = timestamp { seconds = 200 }
      endTime = timestamp { seconds = 300 }
    }
    mediaTypes += EdpaEventGroup.MediaType.VIDEO
  }

  /**
   * Re-runs [EventGroupSync] for a single EDP against the running in-process Kingdom. Test-only
   * seam for driving repeated migration syncs after daemon startup.
   */
  protected fun syncEventGroups(
    edpAggregatorShortName: String,
    sources: List<EdpaEventGroup>,
    entityKeyTypes: List<String> = emptyList(),
  ): List<MappedEventGroup> =
    inProcessEdpAggregatorComponents.syncEventGroups(
      edpAggregatorShortName,
      sources,
      entityKeyTypes,
    )

  /**
   * Lists the raw CMMS [CmmsEventGroup]s an EDP owns on the Kingdom (bypassing the Reporting API).
   * Used to assert the exact Kingdom row count and resource-name set, proving no EventGroup
   * explosion / duplicate rows during migration.
   */
  protected fun listCmmsEventGroups(edpAggregatorShortName: String): List<CmmsEventGroup> =
    inProcessEdpAggregatorComponents.listCmmsEventGroups(edpAggregatorShortName)

  protected suspend fun listReportingEventGroups(): List<EventGroup> {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    return reportingEventGroupsClient
      .withCallCredentials(credentials)
      .listEventGroups(
        listEventGroupsRequest {
          parent = measurementConsumerData.name
          pageSize = 1000
          // Default entity_type_in is ["campaign"], which would hide AD_GROUP_EDP's EventGroup.
          // Include both so HMSS/TrusTee correctness tests still see every expected EDP.
          structuredFilter =
            ListEventGroupsRequestKt.filter {
              entityTypeIn += "campaign"
              entityTypeIn += "ad_group"
              entityTypeIn += CREATIVE_ID_ENTITY_TYPE
            }
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

  protected suspend fun executeBasicReportsReportsJob(basicReportName: String) {
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

  protected fun executeReportProcessorJob() {
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

  private suspend fun getEventGroupsByEdp(
    includeEdpDisplayNames: Set<String>,
    excludeEdpDisplayNames: Set<String> = emptySet(),
  ): List<EventGroup> {
    return buildList {
      val includedDataProviders = mutableSetOf<String>()
      val eventGroups =
        listReportingEventGroups().filter {
          it.eventGroupReferenceId !in EDP1_ALL_ENTITY_KEY_REF_IDS
        }
      eventGroups.forEach { eventGroup ->
        val dataProvider =
          reportingDataProvidersClient
            .withCallCredentials(credentials)
            .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })
        val displayName =
          inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(dataProvider.name)
        if (
          !includedDataProviders.contains(dataProvider.name) &&
            displayName !in excludeEdpDisplayNames &&
            (includeEdpDisplayNames.isEmpty() || displayName in includeEdpDisplayNames)
        ) {
          includedDataProviders.add(dataProvider.name)
          add(eventGroup)
        }
      }
    }
  }

  protected suspend fun getMultiEdpEventGroups(): List<EventGroup> =
    getEventGroupsByEdp(multiEdpDisplayNames)

  protected suspend fun getMultiEdpEventGroupsIncludingRestrictedEdp(): List<EventGroup> {
    val allGroups = getEventGroupsByEdp(emptySet())
    var restrictedGroup: EventGroup? = null
    var unrestrictedGroup: EventGroup? = null
    for (eventGroup in allGroups) {
      val dataProvider =
        reportingDataProvidersClient
          .withCallCredentials(credentials)
          .getDataProvider(getDataProviderRequest { name = eventGroup.cmmsDataProvider })
      val displayName =
        inProcessCmmsComponents.getDataProviderDisplayNameFromDataProviderName(dataProvider.name)
      if (displayName == RESTRICTED_EDP_DISPLAY_NAME && restrictedGroup == null) {
        restrictedGroup = eventGroup
      } else if (displayName != RESTRICTED_EDP_DISPLAY_NAME && unrestrictedGroup == null) {
        unrestrictedGroup = eventGroup
      }
      if (restrictedGroup != null && unrestrictedGroup != null) break
    }
    check(restrictedGroup != null) {
      "No event group found for restricted EDP '$RESTRICTED_EDP_DISPLAY_NAME'"
    }
    check(unrestrictedGroup != null) { "No unrestricted event group found" }
    return listOf(unrestrictedGroup, restrictedGroup)
  }

  protected suspend fun getCreativeIdOnlyEventGroups(): List<EventGroup> {
    return listReportingEventGroups().filter {
      it.entityKey.entityType == CREATIVE_ID_ENTITY_TYPE &&
        it.eventGroupReferenceId !in EDP1_MULTI_ENTITY_KEY_REF_IDS
    }
  }

  protected suspend fun getReferenceIdOnlyEventGroups(): List<EventGroup> {
    return listReportingEventGroups().filter { it.entityKey.entityId.isEmpty() }
  }

  protected suspend fun getMixedEntityKeyEventGroups(): List<EventGroup> {
    val allEventGroups = listReportingEventGroups()
    val edp1RefIdOnly =
      allEventGroups.first { it.eventGroupReferenceId == EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID }
    val edp1CreativeId =
      allEventGroups.first {
        it.eventGroupReferenceId == "$CREATIVE_ID_ENTITY_TYPE/$EDP1_CREATIVE_EVENT_GROUP_REF_ID"
      }
    return listOf(edp1RefIdOnly, edp1CreativeId)
  }

  companion object {
    // edp1 has no entity_key/entity_metadata override (legacy path).
    // edp4 is configured with multi-party noise CONTINUOUS_GAUSSIAN, so it's the "restricted"
    // EDP for the no-noise failure path tests; the same EDP also carries the non-default
    // entity_type ("ad_group") in this fixture.
    internal const val EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID = "edp1-eg-ref-1"
    internal const val AD_GROUP_EDP_DISPLAY_NAME = "edp4"
    internal const val AD_GROUP_EDP_EVENT_GROUP_REF_ID = "ad_group/edp4-eg-ref-1"
    private const val RESTRICTED_EDP_DISPLAY_NAME = AD_GROUP_EDP_DISPLAY_NAME
    internal const val CREATIVE_ID_ENTITY_TYPE = "creative-id"
    internal const val EDP1_MULTI_CREATIVE_A_ID = "multi-creative-A"
    internal const val EDP1_MULTI_CREATIVE_B_ID = "multi-creative-B"
    internal val EDP1_CREATIVE_EVENT_GROUP_REF_ID = "$CREATIVE_ID_ENTITY_TYPE/edp1-eg-creative-1"
    private val EDP2_CREATIVE_EVENT_GROUP_REF_ID = "$CREATIVE_ID_ENTITY_TYPE/edp2-eg-creative-1"
    internal val EDP1_MULTI_ENTITY_KEY_REF_IDS =
      setOf(
        "$CREATIVE_ID_ENTITY_TYPE/$EDP1_MULTI_CREATIVE_A_ID",
        "$CREATIVE_ID_ENTITY_TYPE/$EDP1_MULTI_CREATIVE_B_ID",
      )
    private val EDP1_ALL_ENTITY_KEY_REF_IDS =
      EDP1_MULTI_ENTITY_KEY_REF_IDS + "$CREATIVE_ID_ENTITY_TYPE/$EDP1_CREATIVE_EVENT_GROUP_REF_ID"
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

    private val POPULATION_SPEC_TYPE_REGISTRY: TypeRegistry =
      TypeRegistry.newBuilder().add(Person.getDescriptor()).build()

    val populationSpec: PopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        PopulationSpec.getDefaultInstance(),
        POPULATION_SPEC_TYPE_REGISTRY,
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
    // All computation methods (HMSS, TrusTee, etc.) are expected to produce exactly the same
    // results when using the same input data and no noise.
    internal const val EXPECTED_CROSS_PUBLISHER_REACH = 5330L
    internal const val EXPECTED_CROSS_PUBLISHER_IMPRESSIONS = 8860L
    internal val EXPECTED_CROSS_PUBLISHER_K_PLUS_REACH = listOf(5330L, 2572L, 647L, 311L, 0L)
    internal const val EXPECTED_EDP_SPEC1_REACH = 3937L
    internal const val EXPECTED_EDP_SPEC2_REACH = 3638L

    internal const val EXPECTED_SINGLE_EDP_SPEC2_REACH = 3638L
    internal const val EXPECTED_SINGLE_EDP_SPEC2_IMPRESSIONS = 4276L
    internal val EXPECTED_SINGLE_EDP_SPEC2_K_PLUS_REACH = listOf(3638L, 638L, 0L, 0L, 0L)

    internal const val EXPECTED_TWO_ENTITY_KEY_IMPRESSIONS = 8552L
    internal val EXPECTED_TWO_ENTITY_KEY_K_PLUS_REACH = listOf(3638L, 3638L, 638L, 638L, 0L)

    // Placeholder values required by the MeasurementSpec. Unused since NoiseMechanism is NONE.
    private val NO_NOISE_PRIVACY_PARAMS =
      MetricSpecConfigKt.differentialPrivacyParams {
        epsilon = 1.0
        delta = 1e-15
      }

    /** MetricSpecConfig with width=1.0 so there's no VID sampling variance. */
    private val NO_SAMPLING_METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams = NO_NOISE_PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          singleDataProviderParams = multipleDataProviderParams
        }

      reachAndFrequencyParams =
        MetricSpecConfigKt.reachAndFrequencyParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.reachAndFrequencySamplingAndPrivacyParams {
              reachPrivacyParams = NO_NOISE_PRIVACY_PARAMS
              frequencyPrivacyParams = NO_NOISE_PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          singleDataProviderParams = multipleDataProviderParams
          maximumFrequency = 10
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams = NO_NOISE_PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          maximumFrequencyPerUser = 60
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams = NO_NOISE_PRIVACY_PARAMS
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 1.0f
                    }
                }
            }
          maximumWatchDurationPerUser = com.google.protobuf.util.Durations.fromSeconds(4000)
        }

      populationCountParams =
        org.wfanet.measurement.config.reporting.MetricSpecConfig.PopulationCountParams
          .getDefaultInstance()
    }

    private val NO_NOISE_TRUSTEE_PROTOCOL_CONFIG_CONFIG: TrusTeeProtocolConfigConfig =
      trusTeeProtocolConfigConfig {
        protocolConfig =
          ProtocolConfigKt.trusTee { noiseMechanism = ProtocolConfig.NoiseMechanism.NONE }
        duchyId = "aggregator"
      }

    private val NO_NOISE_HMSS_PROTOCOL_CONFIG_CONFIG: HmssProtocolConfigConfig =
      hmssProtocolConfigConfig {
        protocolConfig =
          ProtocolConfigKt.honestMajorityShareShuffle {
            noiseMechanism = ProtocolConfig.NoiseMechanism.NONE
            reachAndFrequencyRingModulus = 127
            reachRingModulus = 127
          }
        firstNonAggregatorDuchyId = "worker1"
        secondNonAggregatorDuchyId = "worker2"
        aggregatorDuchyId = "aggregator"
      }

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig = NO_NOISE_TRUSTEE_PROTOCOL_CONFIG_CONFIG,
        hmssProtocolConfigConfig = NO_NOISE_HMSS_PROTOCOL_CONFIG_CONFIG,
      )
    }

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    internal val ENTITY_METADATA = struct {
      fields["placement"] = value { stringValue = "homepage_top" }
      fields["objective"] = value { stringValue = "awareness" }
    }
  }
}
