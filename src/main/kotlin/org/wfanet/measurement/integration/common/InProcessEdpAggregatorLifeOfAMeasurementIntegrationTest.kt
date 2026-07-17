// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.integration.common

import com.google.crypto.tink.Aead
import com.google.crypto.tink.KeyTemplates
import com.google.crypto.tink.KeysetHandle
import com.google.protobuf.TypeRegistry
import com.google.protobuf.struct
import com.google.protobuf.value
import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Rule
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.common.crypto.tink.testing.FakeKmsClient
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.edpaggregator.resultsfulfiller.ModelLineInfo
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common.InMemoryVidIndexMap
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorClient
import org.wfanet.measurement.gcloud.pubsub.testing.GooglePubSubEmulatorProvider
import org.wfanet.measurement.gcloud.spanner.testing.SpannerDatabaseAdmin
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.EntityKey
import org.wfanet.measurement.loadtest.measurementconsumer.EdpAggregatorMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportKey
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessEdpAggregatorLifeOfAMeasurementIntegrationTest(
  kingdomDataServicesRule: ProviderRule<DataServices>,
  duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  secureComputationDatabaseAdmin: SpannerDatabaseAdmin,
  hmssEnabled: Boolean,
  trusTeeEnabled: Boolean,
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

  @get:Rule
  val inProcessCmmsComponents =
    InProcessCmmsComponents(
      kingdomDataServicesRule,
      duchyDependenciesRule,
      useEdpSimulators = false,
      trusTeeKmsClient = sharedKmsClient,
      hmssEnabled = hmssEnabled,
      trusTeeEnabled = trusTeeEnabled,
    )

  @JvmField
  @get:Rule
  val tempPath: Path = run {
    val tempDirectory = TemporaryFolder()
    tempDirectory.create()
    tempDirectory.root.toPath()
  }

  // edp1 is intentionally created without entity key (legacy path: Kingdom defaults
  // entity_type="campaign", entity_id and entity_metadata unset). The simulator's
  // ListEventGroups uses the default filter (entity_type_in defaults to ["campaign"]),
  // so edp1 stays visible to existing measurement tests.
  protected val eventGroupConfigsByEdp: Map<String, Map<String, EventGroupConfig>> =
    mapOf(
      "edp1" to
        mapOf(
          EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID to
            EventGroupConfig.LegacySpec(syntheticEventGroupSpec)
        ),
      "edp2" to
        mapOf(
          "ad_group/edpa-eg-reference-id-2" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey("ad_group", "edpa-eg-reference-id-2"),
                    syntheticEventGroupSpec,
                    ENTITY_METADATA,
                  )
                )
            ),
          "multi-creative" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey(CREATIVE_ID_ENTITY_TYPE, MULTI_CREATIVE_A_ID),
                    syntheticEventGroupSpec,
                    ENTITY_METADATA,
                  ),
                  EntityKeySpec(
                    EntityKey(CREATIVE_ID_ENTITY_TYPE, MULTI_CREATIVE_B_ID),
                    syntheticEventGroupSpec2,
                    ENTITY_METADATA,
                  ),
                )
            ),
        ),
      "edp3" to
        mapOf(
          "ad_group/edpa-eg-reference-id-3" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey("ad_group", "edpa-eg-reference-id-3"),
                    syntheticEventGroupSpec,
                    ENTITY_METADATA,
                  )
                )
            )
        ),
      "edp4" to
        mapOf(
          "ad_group/edpa-eg-reference-id-4" to
            EventGroupConfig.MultiEntityKey(
              entityKeySpecs =
                listOf(
                  EntityKeySpec(
                    EntityKey("ad_group", "edpa-eg-reference-id-4"),
                    syntheticEventGroupSpec,
                    ENTITY_METADATA,
                  )
                )
            )
        ),
    )

  private val syntheticEventGroupMap: Map<String, EventGroupConfig> =
    eventGroupConfigsByEdp.values
      .flatMap { it.entries }
      .flatMap { (refId, config) ->
        when (config) {
          is EventGroupConfig.LegacySpec -> listOf(refId to config)
          is EventGroupConfig.MultiEntityKey ->
            config.entityKeySpecs.map { spec ->
              "${spec.entityKey.entityType}/${spec.entityKey.entityId}" to
                EventGroupConfig.MultiEntityKey(entityKeySpecs = listOf(spec))
            }
        }
      }
      .toMap()

  @get:Rule
  val inProcessEdpAggregatorComponents: InProcessEdpAggregatorComponents =
    InProcessEdpAggregatorComponents(
      secureComputationDatabaseAdmin = secureComputationDatabaseAdmin,
      storagePath = tempPath,
      pubSubClient = pubSubClient,
      eventGroupConfigsByEdp = eventGroupConfigsByEdp,
      populationSpec = populationSpec,
      modelLineInfoMap = modelLineInfoMap,
      externalKmsClient = sharedKmsClient,
    )

  @Before
  fun setup() {
    runBlocking {
      pubSubClient.createTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
      pubSubClient.createSubscription(PROJECT_ID, SUBSCRIPTION_ID, FULFILLER_TOPIC_ID)
    }
    inProcessCmmsComponents.startDaemons()
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
        "edp4" to
          DataProviderKt.capabilities {
            honestMajorityShareShuffleSupported = true
            trusTeeSupported = true
          },
      ),
      duchyMap,
      edpNoise =
        mapOf(
          "edp1" to ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
          "edp2" to ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
          "edp3" to ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
          "edp4" to ResultsFulfillerParams.NoiseParams.NoiseType.CONTINUOUS_GAUSSIAN,
        ),
      edpMultiPartyNoiseTypes =
        mapOf("edp4" to listOf(ResultsFulfillerParams.NoiseParams.NoiseType.NONE)),
    )
    initMcSimulator()
  }

  protected lateinit var mcSimulator: EdpAggregatorMeasurementConsumerSimulator
  protected lateinit var mcName: String
  protected lateinit var mcApiKey: String

  private val publicMeasurementsClient by lazy {
    MeasurementsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicCertificatesClient by lazy {
    CertificatesCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  protected val publicEventGroupsClient by lazy {
    EventGroupsCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }
  private val publicDataProvidersClient by lazy {
    DataProvidersCoroutineStub(inProcessCmmsComponents.kingdom.publicApiChannel)
  }

  private fun initMcSimulator() {
    val measurementConsumerData = inProcessCmmsComponents.getMeasurementConsumerData()
    mcName = measurementConsumerData.name
    mcApiKey = measurementConsumerData.apiAuthenticationKey
    mcSimulator =
      EdpAggregatorMeasurementConsumerSimulator(
        MeasurementConsumerData(
          measurementConsumerData.name,
          InProcessCmmsComponents.MC_ENTITY_CONTENT.signingKey,
          InProcessCmmsComponents.MC_ENCRYPTION_PRIVATE_KEY,
          measurementConsumerData.apiAuthenticationKey,
        ),
        OUTPUT_DP_PARAMS,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicMeasurementConsumersClient,
        publicCertificatesClient,
        InProcessCmmsComponents.TRUSTED_CERTIFICATES,
        TestEvent.getDefaultInstance(),
        NoiseMechanism.CONTINUOUS_GAUSSIAN,
        populationSpec,
        syntheticEventGroupMap,
        ReportKey(
            MeasurementConsumerKey.fromName(measurementConsumerData.name)!!.measurementConsumerId,
            "some-report-id",
          )
          .toName(),
        modelLineName = modelLineName,
        listEventGroupsEntityTypes = listOf("campaign", "ad_group", "creative-id"),
      )
  }

  @After
  fun tearDown() {
    inProcessCmmsComponents.stopDuchyDaemons()
    inProcessCmmsComponents.stopPopulationRequisitionFulfillerDaemon()
    inProcessEdpAggregatorComponents.stopDaemons()
    runBlocking {
      pubSubClient.deleteTopic(PROJECT_ID, FULFILLER_TOPIC_ID)
      pubSubClient.deleteSubscription(PROJECT_ID, SUBSCRIPTION_ID)
    }
  }

  companion object {
    // edp1 deliberately has no entity_key/entity_metadata override (legacy path); it also
    // happens
    // to be the EDP that requires no measurement noise on the HMSS protocol, used by the
    // HMSS-failure path test.
    internal const val EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID = "edpa-eg-reference-id-1"
    private const val HMSS_NO_NOISE_EDP_EVENT_GROUP_REF_ID = EDP_NO_ENTITY_KEY_EVENT_GROUP_REF_ID
    private const val TRUSTEE_NO_NOISE_EDP_EVENT_GROUP_REF_ID = "ad_group/edpa-eg-reference-id-3"
    internal const val MULTIPARTY_NO_NOISE_EDP_EVENT_GROUP_REF_ID =
      "ad_group/edpa-eg-reference-id-4"

    internal val MULTI_ENTITY_KEY_REF_IDS =
      setOf(
        "$CREATIVE_ID_ENTITY_TYPE/$MULTI_CREATIVE_A_ID",
        "$CREATIVE_ID_ENTITY_TYPE/$MULTI_CREATIVE_B_ID",
      )
    private const val MULTI_CREATIVE_A_ID = "creative-a"
    private const val MULTI_CREATIVE_B_ID = "creative-b"
    private const val CREATIVE_ID_ENTITY_TYPE = "creative-id"

    /** EventGroups whose EDPs cannot satisfy a single-party HMSS measurement's noise contract. */
    internal val HMSS_NO_NOISE_EVENT_GROUP_REF_IDS =
      setOf(HMSS_NO_NOISE_EDP_EVENT_GROUP_REF_ID, MULTIPARTY_NO_NOISE_EDP_EVENT_GROUP_REF_ID)

    /**
     * EventGroups whose EDPs cannot satisfy a single-party TrusTee measurement's noise contract.
     */
    internal val TRUSTEE_NO_NOISE_EVENT_GROUP_REF_IDS =
      setOf(TRUSTEE_NO_NOISE_EDP_EVENT_GROUP_REF_ID, MULTIPARTY_NO_NOISE_EDP_EVENT_GROUP_REF_ID)

    private val modelLineName =
      "modelProviders/AAAAAAAAAHs/modelSuites/AAAAAAAAAHs/modelLines/AAAAAAAAAHs"
    // Epsilon can vary from 0.0001 to 1.0, delta = 1e-15 is a realistic value.
    // Set epsilon higher without exceeding privacy budget so the noise is smaller in the
    // integration test. Check sample values in CompositionTest.kt.
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1e-15
    }

    // This is the relative location from which population and data spec textprotos are read.
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
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec2: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec_2.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val modelLineInfoMap =
      mapOf(
        modelLineName to
          ModelLineInfo(
            populationSpec = populationSpec,
            vidIndexMap = InMemoryVidIndexMap.build(populationSpec),
            eventDescriptor = TestEvent.getDescriptor(),
            localAlias = null,
          )
      )

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      InProcessCmmsComponents.initConfig(
        trusTeeProtocolConfigConfig = TRUSTEE_PROTOCOL_CONFIG_CONFIG_NOISE_NO_THRESHOLDS,
        hmssProtocolConfigConfig = HMSS_PROTOCOL_CONFIG_CONFIG,
      )
    }

    @get:ClassRule @JvmStatic val pubSubEmulatorProvider = GooglePubSubEmulatorProvider()

    private val ENTITY_METADATA = struct {
      fields["placement"] = value { stringValue = "homepage_top" }
      fields["objective"] = value { stringValue = "awareness" }
    }
  }
}
