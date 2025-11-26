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

package org.wfanet.measurement.integration.common

import com.google.protobuf.ByteString
import java.security.cert.X509Certificate
import java.time.ZoneOffset
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProviderCertificateKey
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MediaType
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpcKt
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationsGrpcKt
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.createPopulationRequest
import org.wfanet.measurement.api.v2alpha.eventGroupMetadata
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.api.v2alpha.population
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.subjectKeyIdentifier
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.testing.ProviderRule
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.config.DuchyCertConfig
import org.wfanet.measurement.dataprovider.DataProviderData
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.loadtest.dataprovider.toPopulationSpec
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.PopulationData
import org.wfanet.measurement.loadtest.resourcesetup.DuchyCert
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub

class InProcessCmmsComponents(
  private val kingdomDataServicesRule: ProviderRule<DataServices>,
  private val duchyDependenciesRule:
    ProviderRule<(String, ComputationLogEntriesCoroutineStub) -> InProcessDuchy.DuchyDependencies>,
  private val syntheticPopulationSpec: SyntheticPopulationSpec =
    SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
  private val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec> =
    SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL,
  private val useEdpSimulators: Boolean,
) : TestRule {
  private val kingdomDataServices: DataServices
    get() = kingdomDataServicesRule.value

  val kingdom: InProcessKingdom =
    InProcessKingdom(
      dataServicesProvider = { kingdomDataServices },
      REDIRECT_URI,
      verboseGrpcLogging = false,
    )

  val eventQuery by lazy {
    EventQuery(
      syntheticPopulationSpec,
      syntheticEventGroupSpecs,
      edpDisplayNameToResourceMap.values.map { it.name },
    )
  }

  val duchies: List<InProcessDuchy> by lazy {
    ALL_DUCHY_NAMES.map {
      InProcessDuchy(
        externalDuchyId = it,
        kingdomSystemApiChannel = kingdom.systemApiChannel,
        kingdomPublicApiChannel = kingdom.publicApiChannel,
        duchyDependenciesRule = duchyDependenciesRule,
        trustedCertificates = TRUSTED_CERTIFICATES,
        verboseGrpcLogging = false,
      )
    }
  }

  private val edpSimulators: List<InProcessEdpSimulator> by lazy {
    edpDisplayNameToResourceMap.entries.map { (displayName, resource) ->
      val certificateKey = DataProviderCertificateKey.fromName(resource.dataProvider.certificate)!!
      val eventGroupOptions =
        InProcessEdpSimulator.EventGroupOptions(
          "",
          eventQuery.eventGroupSpecByDataProvider.getValue(certificateKey.parentKey),
          EVENT_GROUP_MEDIA_TYPES,
          eventGroupMetadata {
            adMetadata =
              EventGroupMetadataKt.adMetadata {
                campaignMetadata =
                  EventGroupMetadataKt.AdMetadataKt.campaignMetadata {
                    campaignName = "$displayName campaign"
                    brandName = "Brand"
                  }
              }
          },
        )

      InProcessEdpSimulator(
        displayName = displayName,
        resourceName = resource.name,
        certificateKey = certificateKey,
        mcResourceName = mcResourceName,
        kingdomPublicApiChannel = kingdom.publicApiChannel,
        duchyPublicApiChannelMap =
          mapOf(
            duchies[1].externalDuchyId to duchies[1].publicApiChannel,
            duchies[2].externalDuchyId to duchies[2].publicApiChannel,
          ),
        trustedCertificates = TRUSTED_CERTIFICATES,
        eventGroupOptions = eventGroupOptions,
        eventQuery = eventQuery,
        honestMajorityShareShuffleSupported =
          (displayName in ALL_EDP_WITH_HMSS_CAPABILITIES_DISPLAY_NAMES),
      )
    }
  }

  private val populationRequisitionFulfiller: InProcessPopulationRequisitionFulfiller by lazy {
    InProcessPopulationRequisitionFulfiller(
      pdpData =
        DataProviderData(
          populationDataProviderResource.name,
          loadEncryptionPrivateKey("${PDP_DISPLAY_NAME}_enc_private.tink"),
          loadSigningKey("${PDP_DISPLAY_NAME}_cs_cert.der", "${PDP_DISPLAY_NAME}_cs_private.der"),
          DataProviderCertificateKey.fromName(
            populationDataProviderResource.dataProvider.certificate
          )!!,
        ),
      populationDataProviderResource.name,
      kingdom.publicApiChannel,
      TRUSTED_CERTIFICATES,
    )
  }

  val ruleChain: TestRule by lazy {
    chainRulesSequentially(
      kingdomDataServicesRule,
      kingdom,
      duchyDependenciesRule,
      *duchies.toTypedArray(),
    )
  }

  private val publicMeasurementConsumersClient by lazy {
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(kingdom.publicApiChannel)
  }
  private val publicAccountsClient by lazy {
    AccountsGrpcKt.AccountsCoroutineStub(kingdom.publicApiChannel)
      // Fail faster.
      .withDeadlineAfter(5L, TimeUnit.SECONDS)
  }
  private val publicApiKeysClient by lazy {
    ApiKeysGrpcKt.ApiKeysCoroutineStub(kingdom.publicApiChannel)
  }

  lateinit var mcResourceName: String
  private lateinit var apiAuthenticationKey: String
  lateinit var edpDisplayNameToResourceMap: Map<String, Resources.Resource>
    private set

  private lateinit var duchyCertMap: Map<String, String>
  private lateinit var eventGroups: List<EventGroup>
  private lateinit var populationDataProviderResource: Resources.Resource
  private lateinit var population: Population
  private lateinit var modelProviderResource: Resources.Resource
  private lateinit var modelLineName: String

  val populationResourceName: String
    get() = population.name

  val modelProviderResourceName: String
    get() = modelProviderResource.name

  val modelLineResourceName: String
    get() = modelLineName

  private suspend fun createAllResources() {
    val resourceSetup =
      ResourceSetup(
        internalAccountsClient = kingdom.internalAccountsClient,
        internalDataProvidersClient = kingdom.internalDataProvidersClient,
        internalCertificatesClient = kingdom.internalCertificatesClient,
        internalModelProvidersClient = kingdom.internalModelProvidersClient,
        internalModelSuitesClient = kingdom.internalModelSuitesClient,
        internalModelLinesClient = kingdom.internalModelLinesClient,
        accountsClient = publicAccountsClient,
        apiKeysClient = publicApiKeysClient,
        measurementConsumersClient = publicMeasurementConsumersClient,
        runId = "12345",
        requiredDuchies = listOf("worker1", "worker2"),
      )
    // Create the MC.
    val (measurementConsumer, apiKey) =
      resourceSetup.createMeasurementConsumer(
        MC_ENTITY_CONTENT,
        resourceSetup.createAccountWithRetries(),
      )
    mcResourceName = measurementConsumer.name
    apiAuthenticationKey = apiKey
    // Create all EDPs
    edpDisplayNameToResourceMap =
      ALL_EDP_DISPLAY_NAMES.associateWith {
        resourceSetup.createDataProviderResource(createEntityContent(it))
      }

    createPopulationResources(resourceSetup)

    // Create all duchy certificates.
    duchyCertMap =
      ALL_DUCHY_NAMES.associateWith {
        resourceSetup
          .createDuchyCertificate(DuchyCert(it, loadTestCertDerFile("${it}_cs_cert.der")))
          .name
      }
  }

  private suspend fun createPopulationResources(resourceSetup: ResourceSetup) {
    populationDataProviderResource =
      resourceSetup.createDataProviderResource(createEntityContent(PDP_DISPLAY_NAME))
    modelProviderResource =
      resourceSetup.createModelProviderResource(MP_ROOT_CERT.subjectKeyIdentifier!!)
    modelLineName =
      resourceSetup
        .createModelLineResource(
          apiIdToExternalId(ModelProviderKey.fromName(modelProviderResource.name)!!.modelProviderId)
        )
        .name

    val populationsClient = PopulationsGrpcKt.PopulationsCoroutineStub(kingdom.publicApiChannel)
    population =
      populationsClient
        .withPrincipalName(populationDataProviderResource.name)
        .createPopulation(
          createPopulationRequest {
            parent = populationDataProviderResource.name
            population = population { populationSpec = POPULATION_SPEC }
          }
        )

    val modelSuiteName = ModelLineKey.fromName(modelLineName)!!.parentKey.toName()

    val modelReleasesClient =
      ModelReleasesGrpcKt.ModelReleasesCoroutineStub(kingdom.publicApiChannel)
    val modelRelease =
      modelReleasesClient
        .withPrincipalName(modelProviderResourceName)
        .createModelRelease(
          createModelReleaseRequest {
            parent = modelSuiteName
            modelRelease = modelRelease {
              this.population = this@InProcessCmmsComponents.population.name
            }
          }
        )

    val modelLinesClient = ModelLinesGrpcKt.ModelLinesCoroutineStub(kingdom.publicApiChannel)
    val modelLine =
      modelLinesClient
        .withPrincipalName(modelProviderResourceName)
        .getModelLine(getModelLineRequest { name = modelLineName })

    val modelRolloutsClient =
      ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub(kingdom.publicApiChannel)
    modelRolloutsClient
      .withPrincipalName(modelProviderResourceName)
      .createModelRollout(
        createModelRolloutRequest {
          parent = modelLineName
          modelRollout = modelRollout {
            instantRolloutDate =
              modelLine.activeStartTime
                .toInstant()
                .atZone(ZoneOffset.UTC)
                .toLocalDate()
                .toProtoDate()
            this.modelRelease = modelRelease.name
          }
        }
      )
  }

  fun getMeasurementConsumerData(): MeasurementConsumerData {
    return MeasurementConsumerData(
      mcResourceName,
      MC_ENTITY_CONTENT.signingKey,
      MC_ENCRYPTION_PRIVATE_KEY,
      apiAuthenticationKey,
    )
  }

  fun getPopulationData(): PopulationData {
    return PopulationData(
      populationDataProviderName = populationDataProviderResource.name,
      POPULATION_SPEC,
    )
  }

  /**
   * Retrieves the data provider display name associated with a given data provider name.
   *
   * This function searches the `edpDisplayNameToResourceMap` for an entry where the `name` property
   * of the entry's value exactly matches the provided [dataProviderName]. If such an entry is
   * found, its key (which represents the data provider display name) is returned. If no match is
   * found, null is returned.
   *
   * @param dataProviderName The exact name of the data provider name to search for.
   * @return The corresponding data provider display name if an exact match for the
   *   [dataProviderName] is found in the map's values; otherwise, null.
   */
  fun getDataProviderDisplayNameFromDataProviderName(dataProviderName: String): String? {
    return edpDisplayNameToResourceMap.entries
      .find { entry -> dataProviderName.equals(entry.value.name) }
      ?.key
  }

  fun getDataProviderResourceNames(): List<String> {
    return edpDisplayNameToResourceMap.values.map { it.name }
  }

  fun startDaemons() = runBlocking {
    // Create all resources
    createAllResources()
    // Start daemons. Mills and EDP simulators can only be started after resources have been
    // created.
    if (useEdpSimulators) {
      eventGroups = edpSimulators.map { it.ensureEventGroup() }
      edpSimulators.forEach { it.start() }
      edpSimulators.forEach { it.waitUntilHealthy() }
    }

    duchies.forEach {
      it.startHerald()
      it.startMill(duchyCertMap)
    }
    populationRequisitionFulfiller.start()
  }

  fun stopEdpSimulators() = runBlocking { edpSimulators.forEach { it.stop() } }

  fun stopDuchyDaemons() = runBlocking {
    for (duchy in duchies) {
      duchy.stopHerald()
      duchy.stopMill()
    }
  }

  fun stopPopulationRequisitionFulfillerDaemon() = runBlocking {
    populationRequisitionFulfiller.stop()
  }

  fun stopDaemons() {
    if (useEdpSimulators) {
      stopEdpSimulators()
    }
    stopDuchyDaemons()
    stopPopulationRequisitionFulfillerDaemon()
  }

  override fun apply(statement: Statement, description: Description): Statement {
    return ruleChain.apply(statement, description)
  }

  companion object {
    private const val REDIRECT_URI = "https://localhost:2048"
    private const val PDP_DISPLAY_NAME = "pdp1"
    val MC_ENTITY_CONTENT: EntityContent = createEntityContent(MC_DISPLAY_NAME)
    val MC_ENCRYPTION_PRIVATE_KEY: TinkPrivateKeyHandle =
      loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
    private val MP_ROOT_CERT: X509Certificate =
      readCertificate(SECRET_FILES_PATH.resolve("mp1_root.pem").toFile())
    val TRUSTED_CERTIFICATES: Map<ByteString, X509Certificate> =
      loadTestCertCollection("all_root_certs.pem").associateBy {
        checkNotNull(it.subjectKeyIdentifier)
      }
    private val EVENT_GROUP_MEDIA_TYPES = setOf(MediaType.VIDEO, MediaType.DISPLAY)
    private val POPULATION_SPEC: PopulationSpec =
      SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_LARGE.toPopulationSpec(
        TestEvent.getDescriptor()
      )

    @JvmStatic
    fun initConfig() {
      DuchyIds.setForTest(ALL_DUCHIES)
      Llv2ProtocolConfig.setForTest(
        LLV2_PROTOCOL_CONFIG_CONFIG.protocolConfig,
        LLV2_PROTOCOL_CONFIG_CONFIG.duchyProtocolConfig,
        setOf("aggregator"),
        2,
      )
      RoLlv2ProtocolConfig.setForTest(
        RO_LLV2_PROTOCOL_CONFIG_CONFIG.protocolConfig,
        RO_LLV2_PROTOCOL_CONFIG_CONFIG.duchyProtocolConfig,
        setOf("aggregator"),
        2,
      )
      HmssProtocolConfig.setForTest(
        HMSS_PROTOCOL_CONFIG_CONFIG.protocolConfig,
        "worker1",
        "worker2",
        "aggregator",
      )
      DuchyInfo.initializeFromConfig(
        loadTextProto("duchy_cert_config.textproto", DuchyCertConfig.getDefaultInstance())
      )
    }
  }
}
