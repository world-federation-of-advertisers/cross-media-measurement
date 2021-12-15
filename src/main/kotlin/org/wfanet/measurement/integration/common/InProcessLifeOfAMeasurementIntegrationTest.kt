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

import com.google.common.truth.Truth.assertThat
import java.time.Clock
import kotlinx.coroutines.runBlocking
import org.junit.After
import org.junit.Before
import org.junit.BeforeClass
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TestRule
import org.wfanet.measurement.api.v2alpha.AccountKey
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub as PublicAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub as PublicRequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.activateAccountRequest
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.pollFor
import org.wfanet.measurement.consent.crypto.keystore.KeyStore
import org.wfanet.measurement.consent.crypto.keystore.testing.InMemoryKeyStore
import org.wfanet.measurement.internal.kingdom.account
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.service.api.v2alpha.withIdToken
import org.wfanet.measurement.loadtest.frontend.FrontendSimulator
import org.wfanet.measurement.loadtest.frontend.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.DuchyCert
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.tools.generateIdToken

private const val MC_CONSENT_SIGNALING_PRIVATE_KEY_ID = "mc-cs-private-key"
private const val MC_ENCRYPTION_PRIVATE_KEY_ID = "mc-enc-private-key"
private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
  epsilon = 1.0
  delta = 1.0
}
private const val REDIRECT_URI = "https://localhost:2048"

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

  /**
   * The keyStore used in the test. The kingdom, duchies and EDP simulators use their own keyStore
   * in case of key collision.
   */
  abstract val simulatorKeyStore: KeyStore

  abstract val storageClient: StorageClient

  private val kingdomDataServices: DataServices
    get() = kingdomDataServicesRule.value

  private val kingdom: InProcessKingdom =
    InProcessKingdom(
      dataServicesProvider = { kingdomDataServices },
      verboseGrpcLogging = false,
      REDIRECT_URI
    )

  private val duchies: List<InProcessDuchy> by lazy {
    ALL_DUCHY_NAMES.map {
      InProcessDuchy(
        externalDuchyId = it,
        kingdomSystemApiChannel = kingdom.systemApiChannel,
        duchyDependenciesProvider = { duchyDependenciesRule.value(it) },
        verboseGrpcLogging = false,
      )
    }
  }

  private val edpSimulators: List<InProcessEdpSimulator> by lazy {
    ALL_EDP_DISPLAY_NAMES.map {
      InProcessEdpSimulator(
        displayName = it,
        keyStore = InMemoryKeyStore(),
        storageClient = storageClient,
        kingdomPublicApiChannel = kingdom.publicApiChannel,
        duchyPublicApiChannel = duchies[1].publicApiChannel
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
  private val publicEventGroupsClient by lazy {
    PublicEventGroupsCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicDataProvidersClient by lazy {
    PublicDataProvidersCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicRequisitionsClient by lazy {
    PublicRequisitionsCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicCertificatesClient by lazy {
    PublicCertificatesCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicAccountsClient by lazy { PublicAccountsCoroutineStub(kingdom.publicApiChannel) }

  private lateinit var mcResourceName: String
  private lateinit var edpDisplayNameToResourceNameMap: Map<String, String>
  private lateinit var duchyCertMap: Map<String, String>

  private suspend fun createAllResources() {
    val resourceSetup =
      ResourceSetup(
        keyStore = simulatorKeyStore,
        dataProvidersClient = publicDataProvidersClient,
        certificatesClient = publicCertificatesClient,
        measurementConsumersClient = publicMeasurementConsumersClient,
        runId = "12345"
      )
    // Create the MC.
    val mc = createEntityContent(MC_DISPLAY_NAME)
    simulatorKeyStore.storePrivateKeyDer(mc.displayName, mc.consentSignalPrivateKeyDer)

    val account = kingdom.internalAccountsClient.createAccount(account {})
    val authenticationResponse =
      publicAccountsClient.authenticate(authenticateRequest { issuer = "https://self-issued.me" })
    val idToken =
      generateIdToken(authenticationResponse.authenticationRequestUri, Clock.systemUTC())
    publicAccountsClient
      .withIdToken(idToken)
      .activateAccount(
        activateAccountRequest {
          name = AccountKey(externalIdToApiId(account.externalAccountId)).toName()
          activationToken = externalIdToApiId(account.activationToken)
        }
      )

    mcResourceName =
      resourceSetup.createMeasurementConsumer(
          mc,
          externalIdToApiId(account.measurementConsumerCreationToken),
          idToken
        )
        .name
    // Create all EDPs
    edpDisplayNameToResourceNameMap =
      ALL_EDP_DISPLAY_NAMES.associateWith {
        val edp = createEntityContent(it)
        simulatorKeyStore.storePrivateKeyDer(edp.displayName, edp.consentSignalPrivateKeyDer)
        resourceSetup.createDataProvider(edp).name
      }
    // Create all duchy certificates.
    duchyCertMap =
      ALL_DUCHY_NAMES.associateWith {
        resourceSetup.createDuchyCertificate(
            DuchyCert(it, loadTestCertDerFile("${it}_cs_cert.der"))
          )
          .name
      }
    // Store two keys required when creating a measurement.
    simulatorKeyStore.storePrivateKeyDer(
      MC_CONSENT_SIGNALING_PRIVATE_KEY_ID,
      loadTestCertDerFile("${MC_DISPLAY_NAME}_cs_private.der")
    )
    simulatorKeyStore.storePrivateKeyDer(
      MC_ENCRYPTION_PRIVATE_KEY_ID,
      loadTestCertDerFile("${MC_DISPLAY_NAME}_enc_private.der")
    )
  }

  @Before
  fun createResourcesAndStartMillsAndDataProviders() = runBlocking {
    // Create all resources
    createAllResources()

    // Start all Mills and all EDPs, which can only be started after the resources are created.
    duchies.forEach { it.startLiquidLegionsV2mill(duchyCertMap) }
    edpSimulators.forEach {
      it.start(edpDisplayNameToResourceNameMap.getValue(it.displayName), mcResourceName)
    }
  }

  @After fun stopAllEdpSimulators() = runBlocking { edpSimulators.forEach { it.stop() } }

  @Test
  fun `create a measurement and check the result is equal to the expected result`() = runBlocking {
    // Wait until all EDPs finish creating eventGroups before the test starts.
    val eventGroupList =
      pollFor(timeoutMillis = 10_000) {
        val eventGroups =
          publicEventGroupsClient.listEventGroups(
              listEventGroupsRequest {
                parent = "dataProviders/-"
                filter = ListEventGroupsRequestKt.filter { measurementConsumers += mcResourceName }
              }
            )
            .eventGroupsList
        if (eventGroups.size == ALL_EDP_DISPLAY_NAMES.size) eventGroups else null
      }
    assertThat(eventGroupList).isNotNull()

    // Runs the frontend simulator, which will
    //   1. create a measurement.
    //   2. keep polling from the kingdom until the measurement is done.
    //   3. read raw sketches and compute the expected result.
    //   4. assert that the computed result is equal to the expected result (within error tolerance)
    FrontendSimulator(
        MeasurementConsumerData(
          mcResourceName,
          MC_CONSENT_SIGNALING_PRIVATE_KEY_ID,
          MC_ENCRYPTION_PRIVATE_KEY_ID
        ),
        OUTPUT_DP_PARAMS,
        simulatorKeyStore,
        publicDataProvidersClient,
        publicEventGroupsClient,
        publicMeasurementsClient,
        publicRequisitionsClient,
        publicMeasurementConsumersClient,
        SketchStore(storageClient),
        "1234"
      )
      .process()
  }

  companion object {
    @BeforeClass
    @JvmStatic
    fun initConfig() {
      DuchyIds.setForTest(ALL_DUCHY_NAMES)
      Llv2ProtocolConfig.setForTest(
        LLV2_PROTOCOL_CONFIG_CONFIG.protocolConfig,
        LLV2_PROTOCOL_CONFIG_CONFIG.duchyProtocolConfig
      )
      DuchyInfo.setForTest(ALL_DUCHY_NAMES.toSet())
    }
  }
}
