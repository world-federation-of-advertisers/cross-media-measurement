// Copyright 2020 The Cross-Media Measurement Authors
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

import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub as PublicAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub as PublicApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub as PublicRequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.DuchyCertConfig
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.frontend.FrontendSimulator
import org.wfanet.measurement.loadtest.frontend.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.DuchyCert
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient

private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
  epsilon = 1.0
  delta = 1.0
}
private const val REDIRECT_URI = "https://localhost:2048"
private val RESULT_POLLING_DELAY = Duration.ofSeconds(10)

/**
 * Test that everything is wired up properly.
 *
 * This is abstract so that different implementations of dependencies can all run the same tests
 * easily.
 */
abstract class InProcessLifeOfAMeasurementIntegrationTest {
  abstract val kingdomDataServicesRule: ProviderRule<DataServices>

  /** Provides a function from Duchy to the dependencies needed to start the Duchy to the test. */
  abstract val duchyDependenciesRule: ProviderRule<(String) -> InProcessDuchy.DuchyDependencies>

  abstract val storageClient: StorageClient

  private val kingdomDataServices: DataServices
    get() = kingdomDataServicesRule.value

  private val kingdom: InProcessKingdom =
    InProcessKingdom(
      dataServicesProvider = { kingdomDataServices },
      verboseGrpcLogging = false,
      REDIRECT_URI,
    )

  private val duchies: List<InProcessDuchy> by lazy {
    ALL_DUCHY_NAMES.map {
      InProcessDuchy(
        externalDuchyId = it,
        kingdomSystemApiChannel = kingdom.systemApiChannel,
        duchyDependenciesProvider = { duchyDependenciesRule.value(it) },
        trustedCertificates = TRUSTED_CERTIFICATES,
        verboseGrpcLogging = false,
      )
    }
  }

  private val edpSimulators: List<InProcessEdpSimulator> by lazy {
    edpDisplayNameToResourceNameMap.map { (displayName, resourceName) ->
      InProcessEdpSimulator(
        displayName = displayName,
        resourceName = resourceName,
        mcResourceName = mcResourceName,
        storageClient = storageClient,
        kingdomPublicApiChannel = kingdom.publicApiChannel,
        duchyPublicApiChannel = duchies[1].publicApiChannel,
        eventTemplateNames = EVENT_TEMPLATES_TO_FILTERS_MAP.keys.toList(),
        trustedCertificates = TRUSTED_CERTIFICATES,
      )
    }
  }

  @get:Rule
  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      kingdomDataServicesRule,
      kingdom,
      duchyDependenciesRule,
      *duchies.toTypedArray()
    )
  }

  private val publicMeasurementsClient by lazy {
    PublicMeasurementsCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicMeasurementConsumersClient by lazy {
    PublicMeasurementConsumersCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicCertificatesClient by lazy {
    PublicCertificatesCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicEventGroupsClient by lazy {
    PublicEventGroupsCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicDataProvidersClient by lazy {
    PublicDataProvidersCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicRequisitionsClient by lazy {
    PublicRequisitionsCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicAccountsClient by lazy { PublicAccountsCoroutineStub(kingdom.publicApiChannel) }
  private val publicApiKeysClient by lazy { PublicApiKeysCoroutineStub(kingdom.publicApiChannel) }

  private lateinit var mcResourceName: String
  private lateinit var apiAuthenticationKey: String
  private lateinit var edpDisplayNameToResourceNameMap: Map<String, String>
  private lateinit var duchyCertMap: Map<String, String>
  private lateinit var frontendSimulator: FrontendSimulator
  private lateinit var eventGroups: List<EventGroup>

  private suspend fun createAllResources() {
    val resourceSetup =
      ResourceSetup(
        internalAccountsClient = kingdom.internalAccountsClient,
        internalDataProvidersClient = kingdom.internalDataProvidersClient,
        accountsClient = publicAccountsClient,
        apiKeysClient = publicApiKeysClient,
        internalCertificatesClient = kingdom.internalCertificatesClient,
        measurementConsumersClient = publicMeasurementConsumersClient,
        runId = "12345",
        requiredDuchies = listOf("worker1", "worker2")
      )
    // Create the MC.
    val (measurementConsumer, apiKey) =
      resourceSetup.createMeasurementConsumer(
        MC_ENTITY_CONTENT,
        resourceSetup.createAccountWithRetries()
      )
    mcResourceName = measurementConsumer.name
    apiAuthenticationKey = apiKey
    // Create all EDPs
    edpDisplayNameToResourceNameMap =
      ALL_EDP_DISPLAY_NAMES.associateWith {
        val edp = createEntityContent(it)
        resourceSetup.createInternalDataProvider(edp)
      }
    // Create all duchy certificates.
    duchyCertMap =
      ALL_DUCHY_NAMES.associateWith {
        resourceSetup
          .createDuchyCertificate(DuchyCert(it, loadTestCertDerFile("${it}_cs_cert.der")))
          .name
      }

    frontendSimulator =
      FrontendSimulator(
        MeasurementConsumerData(
          mcResourceName,
          MC_ENTITY_CONTENT.signingKey,
          MC_ENCRYPTION_PRIVATE_KEY,
          apiAuthenticationKey
        ),
        OUTPUT_DP_PARAMS,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicRequisitionsClient,
        publicMeasurementConsumersClient,
        publicCertificatesClient,
        SketchStore(storageClient),
        RESULT_POLLING_DELAY,
        TRUSTED_CERTIFICATES,
        EVENT_TEMPLATES_TO_FILTERS_MAP
      )
  }

  @Before
  fun startDaemons() = runBlocking {
    // Create all resources
    createAllResources()
    eventGroups = edpSimulators.map { it.createEventGroup() }

    // Start daemons. Mills and EDP simulators can only be started after resources have been
    // created.
    duchies.forEach {
      it.startHerald()
      it.startLiquidLegionsV2mill(duchyCertMap)
    }
    edpSimulators.forEach { it.start() }
  }

  @After fun stopEdpSimulators() = runBlocking { edpSimulators.forEach { it.stop() } }

  @After
  fun stopDuchyDaemons() = runBlocking {
    for (duchy in duchies) {
      duchy.stopHerald()
      duchy.stopLiquidLegionsV2Mill()
    }
  }

  @Test
  fun `create a RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      frontendSimulator.executeReachAndFrequency("1234")
    }

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      frontendSimulator.executeDirectReachAndFrequency("1234")
    }

  @Test
  fun `create a reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      frontendSimulator.executeReachOnly("1234")
    }

  @Test
  fun `create an impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its result.
      frontendSimulator.executeImpression("1234")
    }

  @Test
  fun `create a duration measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a duration measurement and verify its result.
      frontendSimulator.executeDuration("1234")
    }

  @Test
  fun `create a RF measurement of invalid params and check the result contains error info`() =
    runBlocking {
      // Use frontend simulator to create an invalid reach and frequency measurement and verify
      // its error info.
      frontendSimulator.executeInvalidReachAndFrequency("1234")
    }

  companion object {
    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
    private val MC_ENCRYPTION_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")

    private val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      loadTestCertCollection("all_root_certs.pem").associateBy {
        checkNotNull(it.subjectKeyIdentifier)
      }

    @BeforeClass
    @JvmStatic
    fun initConfig() {
      DuchyIds.setForTest(ALL_DUCHIES)
      Llv2ProtocolConfig.setForTest(
        LLV2_PROTOCOL_CONFIG_CONFIG.protocolConfig,
        LLV2_PROTOCOL_CONFIG_CONFIG.duchyProtocolConfig,
        setOf("aggregator"),
        2
      )
      DuchyInfo.initializeFromConfig(
        loadTextProto("duchy_cert_config.textproto", DuchyCertConfig.getDefaultInstance())
      )
    }
  }
}
