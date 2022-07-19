// Copyright 2022 The Cross-Media Measurement Authors
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
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub as PublicAccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub as PublicApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub as PublicRequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.testing.pollFor
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

/** Rule that sets up the Kingdom, Edps,and Duchies. */
class InProcessKingdomResourcesSetup(
  private val kingdomDataServicesRule: ProviderRule<DataServices>,
  private val duchyDependenciesRule: ProviderRule<(String) -> InProcessDuchy.DuchyDependencies>,
  private val storageClient: StorageClient
) : TestRule {

  private val kingdomDataServices: DataServices
    get() = kingdomDataServicesRule.value

  val kingdom: InProcessKingdom =
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

  private val edpSimulatorsLazy = lazy {
    ALL_EDP_DISPLAY_NAMES.map {
      InProcessEdpSimulator(
        displayName = it,
        storageClient = storageClient,
        kingdomPublicApiChannel = kingdom.publicApiChannel,
        duchyPublicApiChannel = duchies[1].publicApiChannel,
        eventTemplateNames = EVENT_TEMPLATES_TO_FILTERS_MAP.keys.toList()
      )
    }
  }

  private val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      kingdomDataServicesRule,
      kingdom,
      duchyDependenciesRule,
      *duchies.toTypedArray(),
      ResourceSetupAndComponentsStartup()
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

  lateinit var mcResourceName: String
  private lateinit var apiAuthenticationKey: String
  private lateinit var edpDisplayNameToResourceNameMap: Map<String, String>
  private lateinit var duchyCertMap: Map<String, String>
  lateinit var frontendSimulator: FrontendSimulator

  private suspend fun createAllResources() {
    val resourceSetup =
      ResourceSetup(
        internalAccountsClient = kingdom.internalAccountsClient,
        internalDataProvidersClient = kingdom.internalDataProvidersClient,
        accountsClient = publicAccountsClient,
        apiKeysClient = publicApiKeysClient,
        internalCertificatesClient = kingdom.internalCertificatesClient,
        measurementConsumersClient = publicMeasurementConsumersClient,
        runId = "12345"
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
          loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink"),
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
        EVENT_TEMPLATES_TO_FILTERS_MAP
      )
  }

  private fun createResourcesAndStartMillsAndDataProviders() = runBlocking {
    // Create all resources
    createAllResources()

    // Start all Mills and all EDPs, which can only be started after the resources are created.
    duchies.forEach { it.startLiquidLegionsV2mill(duchyCertMap) }
    edpSimulatorsLazy.value.forEach {
      it.start(edpDisplayNameToResourceNameMap.getValue(it.displayName), mcResourceName)
    }

    val eventGroupsList = pollForEventGroups()
    assertThat(eventGroupsList).isNotNull()
  }

  private fun stopAllEdpSimulators() = runBlocking { edpSimulatorsLazy.value.forEach { it.stop() } }

  private suspend fun pollForEventGroups() {
    pollFor(timeoutMillis = 10_000) {
      val eventGroups =
        publicEventGroupsClient
          .withAuthenticationKey(apiAuthenticationKey)
          .listEventGroups(
            listEventGroupsRequest {
              parent = "dataProviders/-"
              filter = ListEventGroupsRequestKt.filter { measurementConsumers += mcResourceName }
            }
          )
          .eventGroupsList
      if (eventGroups.size == ALL_EDP_DISPLAY_NAMES.size) eventGroups else null
    }
  }

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        try {
          ruleChain.apply(base, description).evaluate()
        } finally {
          if (edpSimulatorsLazy.isInitialized()) {
            stopAllEdpSimulators()
          }
        }
      }
    }
  }

  private inner class ResourceSetupAndComponentsStartup : TestRule {
    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          createResourcesAndStartMillsAndDataProviders()
          base.evaluate()
        }
      }
    }
  }

  companion object {
    private val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
  }
}
