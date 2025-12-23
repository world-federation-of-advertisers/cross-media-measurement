/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.util.JsonFormat
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.kubernetes.client.common.KubernetesObject
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.util.ClientBuilder
import java.io.File
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.KeyPair
import java.security.Security
import java.security.cert.X509Certificate
import java.time.Duration
import java.time.ZoneOffset
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withContext
import okhttp3.OkHttpClient
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import okhttp3.tls.decodeCertificatePem
import org.jetbrains.annotations.Blocking
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLineKey
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpc
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpc
import org.wfanet.measurement.api.v2alpha.Population
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelRolloutRequest
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelRollout
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.authorityKeyIdentifier
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.OpenIdProvider
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.KubernetesClientImpl
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.common.k8s.testing.Processes
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.config.access.OpenIdProvidersConfig
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.MC_DISPLAY_NAME
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadTestCertDerFile
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator
import org.wfanet.measurement.loadtest.resourcesetup.DuchyCert
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt

/**
 * Test for correctness of the CMMS on a single "empty" Kubernetes cluster using the `local`
 * configuration.
 *
 * This will push the images to the container registry and populate the K8s cluster prior to running
 * the test methods. The cluster must already exist with the `KUBECONFIG` environment variable
 * pointing to its kubeconfig.
 *
 * This assumes that the `tar` and `kubectl` executables are the execution path. The latter is only
 * used for `kustomize`, as the Kubernetes API is used to interact with the cluster.
 */
@RunWith(JUnit4::class)
class EmptyClusterCorrectnessTest : AbstractCorrectnessTest(measurementSystem) {
  private class Images : TestRule {
    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          pushImages()
          base.evaluate()
        }
      }
    }

    private fun pushImages() {
      val pusherRuntimePath = getRuntimePath(IMAGE_PUSHER_PATH)
      Processes.runCommand(pusherRuntimePath.toString())
    }
  }

  private data class ResourceInfo(
    val aggregatorCert: String,
    val worker1Cert: String,
    val worker2Cert: String,
    val measurementConsumer: String,
    val measurementConsumerCert: String,
    val measurementConsumerSigningKey: SigningKeyHandle,
    val measurementConsumerEncryptionKey: TinkPrivateKeyHandle,
    val measurementConsumerApiKey: String,
    /** Map of DataProvider display name to resource. */
    val dataProviders: Map<String, Resources.Resource>,
    val modelProvider: String,
    val modelLine: String,
  ) {
    val measurementConsumerData =
      MeasurementConsumerData(
        measurementConsumer,
        measurementConsumerSigningKey,
        measurementConsumerEncryptionKey,
        measurementConsumerApiKey,
      )

    companion object {
      fun from(
        resources: Iterable<Resources.Resource>,
        measurementConsumerSigningKey: SigningKeyHandle,
        measurementConsumerEncryptionKey: TinkPrivateKeyHandle,
      ): ResourceInfo {
        var aggregatorCert: String? = null
        var worker1Cert: String? = null
        var worker2Cert: String? = null
        var measurementConsumer: String? = null
        var measurementConsumerCert: String? = null
        var apiKey: String? = null
        val dataProviders = mutableMapOf<String, Resources.Resource>()
        var modelProvider: String? = null
        var modelLine: String? = null

        for (resource in resources) {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
          when (resource.resourceCase) {
            Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER -> {
              measurementConsumer = resource.name
              apiKey = resource.measurementConsumer.apiKey
              measurementConsumerCert = resource.measurementConsumer.certificate
            }
            Resources.Resource.ResourceCase.DATA_PROVIDER -> {
              val displayName = resource.dataProvider.displayName
              require(dataProviders.putIfAbsent(displayName, resource) == null) {
                "Entry already exists for DataProvider $displayName"
              }
            }
            Resources.Resource.ResourceCase.DUCHY_CERTIFICATE -> {
              when (val duchyId = resource.duchyCertificate.duchyId) {
                "aggregator" -> aggregatorCert = resource.name
                "worker1" -> worker1Cert = resource.name
                "worker2" -> worker2Cert = resource.name
                else -> error("Unhandled Duchy $duchyId")
              }
            }
            Resources.Resource.ResourceCase.MODEL_PROVIDER -> {
              modelProvider = resource.name
            }
            Resources.Resource.ResourceCase.MODEL_LINE -> {
              modelLine = resource.name
            }
            Resources.Resource.ResourceCase.RESOURCE_NOT_SET -> error("Unhandled type")
          }
        }

        return ResourceInfo(
          aggregatorCert = requireNotNull(aggregatorCert),
          worker1Cert = requireNotNull(worker1Cert),
          worker2Cert = requireNotNull(worker2Cert),
          measurementConsumer = requireNotNull(measurementConsumer),
          measurementConsumerCert = requireNotNull(measurementConsumerCert),
          measurementConsumerSigningKey = measurementConsumerSigningKey,
          measurementConsumerEncryptionKey = measurementConsumerEncryptionKey,
          measurementConsumerApiKey = requireNotNull(apiKey),
          dataProviders = dataProviders,
          modelProvider = requireNotNull(modelProvider),
          modelLine = requireNotNull(modelLine),
        )
      }
    }
  }

  /** [TestRule] which populates a K8s cluster with the components of the CMMS. */
  class LocalMeasurementSystem(
    k8sClient: Lazy<KubernetesClient>,
    tempDir: Lazy<TemporaryFolder>,
    runId: Lazy<String>,
  ) : TestRule, MeasurementSystem() {
    override val syntheticPopulationSpec: SyntheticPopulationSpec =
      SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL
    override val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec> =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_SMALL

    private val portForwarders = mutableListOf<PortForwarder>()
    private val channels = mutableListOf<ManagedChannel>()

    private val k8sClient: KubernetesClient by k8sClient
    private val tempDir: TemporaryFolder by tempDir
    override val runId: String by runId

    private lateinit var _testHarness: EventQueryMeasurementConsumerSimulator
    override val testHarness: EventQueryMeasurementConsumerSimulator
      get() = _testHarness

    private lateinit var _reportingTestHarness: ReportingUserSimulator
    override val reportingTestHarness: ReportingUserSimulator
      get() = _reportingTestHarness

    private lateinit var _populationDataProviderName: String
    override val populationDataProviderName: String
      get() = _populationDataProviderName

    private lateinit var _modelLineName: String
    override val modelLineName: String
      get() = _modelLineName

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            runBlocking {
              withTimeout(Duration.ofMinutes(5)) {
                val resourceInfo: ResourceInfo = populateCluster()
                _populationDataProviderName =
                  resourceInfo.dataProviders.getValue(PDP_DISPLAY_NAME).name

                val kingdomPublicApiTarget = forwardKingdomPublicApiService().toTarget()
                val mcKingdomPublicApiChannel =
                  buildKingdomPublicApiChannel(
                    kingdomPublicApiTarget,
                    MEASUREMENT_CONSUMER_SIGNING_CERTS,
                  )
                val mpKingdomPublicApiChannel =
                  buildKingdomPublicApiChannel(kingdomPublicApiTarget, MP_SIGNING_CERTS)
                val pdpKingdomPublicApiChannel =
                  buildKingdomPublicApiChannel(kingdomPublicApiTarget, PDP_SIGNING_CERTS)

                _modelLineName = resourceInfo.modelLine
                ensureModelLine(pdpKingdomPublicApiChannel, mpKingdomPublicApiChannel)

                _testHarness = createTestHarness(mcKingdomPublicApiChannel, resourceInfo)
                _reportingTestHarness =
                  createReportingUserSimulator(resourceInfo.measurementConsumerData)
              }
            }
            base.evaluate()
          } finally {
            stopPortForwarding()
          }
        }
      }
    }

    private suspend fun populateCluster(): ResourceInfo {
      val apiClient = k8sClient.apiClient
      apiClient.httpClient =
        apiClient.httpClient.newBuilder().readTimeout(Duration.ofHours(1L)).build()
      Configuration.setDefaultApiClient(apiClient)

      val duchyCerts =
        ALL_DUCHY_NAMES.map { DuchyCert(it, loadTestCertDerFile("${it}_cs_cert.der")) }
      val dataProviderContents: List<EntityContent> =
        withContext(Dispatchers.IO) {
          EDP_DISPLAY_NAMES.map { createEntityContent(it) } + createEntityContent(PDP_DISPLAY_NAME)
        }
      val measurementConsumerContent =
        withContext(Dispatchers.IO) { createEntityContent(MC_DISPLAY_NAME) }

      // Wait until default service account has been created. See
      // https://github.com/kubernetes/kubernetes/issues/66689.
      k8sClient.waitForServiceAccount("default", timeout = READY_TIMEOUT)

      loadKingdom()
      val resourceSetupOutput: ResourceSetupOutput =
        runResourceSetup(duchyCerts, dataProviderContents, measurementConsumerContent)
      val encryptionPrivateKey: TinkPrivateKeyHandle =
        withContext(Dispatchers.IO) {
          loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
        }
      val resourceInfo =
        ResourceInfo.from(
          resourceSetupOutput.resources,
          measurementConsumerContent.signingKey,
          encryptionPrivateKey,
        )
      loadFullCmms(
        resourceInfo,
        resourceSetupOutput.akidPrincipalMap,
        resourceSetupOutput.measurementConsumerConfig,
        resourceSetupOutput.encryptionKeyPairConfig,
      )

      return resourceInfo
    }

    private fun createTestHarness(
      publicApiChannel: Channel,
      resourceInfo: ResourceInfo,
    ): EventQueryMeasurementConsumerSimulator {
      val eventGroupsClient = EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel)

      return EventQueryMeasurementConsumerSimulator(
        resourceInfo.measurementConsumerData,
        OUTPUT_DP_PARAMS,
        DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        eventGroupsClient,
        MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
        CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
        MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
        buildEventQuery(resourceInfo.dataProviders.values.map { it.name }),
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    }

    @Blocking
    private fun ensureModelLine(
      pdpKingdomPublicApiChannel: Channel,
      mpKingdomPublicApiChannel: Channel,
    ) {
      val population: Population = ensurePopulation(pdpKingdomPublicApiChannel)

      val modelLinesStub = ModelLinesGrpc.newBlockingStub(mpKingdomPublicApiChannel)
      val modelLine = modelLinesStub.getModelLine(getModelLineRequest { name = modelLineName })

      val modelSuiteName = ModelLineKey.fromName(modelLine.name)!!.parentKey.toName()

      val modelReleasesStub = ModelReleasesGrpc.newBlockingStub(mpKingdomPublicApiChannel)
      val modelRelease =
        modelReleasesStub.createModelRelease(
          createModelReleaseRequest {
            parent = modelSuiteName
            modelRelease = modelRelease { this.population = population.name }
          }
        )

      val modelRolloutsStub = ModelRolloutsGrpc.newBlockingStub(mpKingdomPublicApiChannel)
      modelRolloutsStub.createModelRollout(
        createModelRolloutRequest {
          parent = modelLine.name
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

    private suspend fun forwardKingdomPublicApiService(): InetSocketAddress {
      val kingdomPublicPod: V1Pod = getPod(KINGDOM_PUBLIC_DEPLOYMENT_NAME)

      val publicApiForwarder = PortForwarder(kingdomPublicPod, SERVER_PORT)
      portForwarders.add(publicApiForwarder)
      return withContext(Dispatchers.IO) { publicApiForwarder.start() }
    }

    private fun buildKingdomPublicApiChannel(target: String, clientCerts: SigningCerts): Channel {
      return buildMutualTlsChannel(target, clientCerts).also { channels.add(it) }
    }

    private suspend fun createReportingUserSimulator(
      measurementConsumerData: MeasurementConsumerData
    ): ReportingUserSimulator {
      val reportingPublicPod: V1Pod = getPod(REPORTING_PUBLIC_DEPLOYMENT_NAME)

      val publicApiForwarder = PortForwarder(reportingPublicPod, SERVER_PORT)
      portForwarders.add(publicApiForwarder)

      val publicApiAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { publicApiForwarder.start() }
      val publicApiChannel: Channel =
        buildMutualTlsChannel(publicApiAddress.toTarget(), REPORTING_SIGNING_CERTS).also {
          channels.add(it)
        }

      val reportingGatewayPod: V1Pod = getPod(REPORTING_GATEWAY_DEPLOYMENT_NAME)
      val gatewayForwarder = PortForwarder(reportingGatewayPod, SERVER_PORT)
      portForwarders.add(gatewayForwarder)

      val gatewayAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { gatewayForwarder.start() }

      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val reportingRootCert = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()

      val clientCertificate: X509Certificate = cert.readText().decodeCertificatePem()
      val keyAlgorithm = clientCertificate.publicKey.algorithm
      val certificates =
        HandshakeCertificates.Builder()
          .addTrustedCertificate(reportingRootCert.readText().decodeCertificatePem())
          .heldCertificate(
            HeldCertificate(
              KeyPair(clientCertificate.publicKey, readPrivateKey(key, keyAlgorithm)),
              clientCertificate,
            )
          )
          .build()

      val okHttpReportingClient =
        OkHttpClient.Builder()
          .sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager)
          .build()

      val accessPublicPod: V1Pod = getPod(ACCESS_PUBLIC_API_DEPLOYMENT_NAME)
      val accessPublicApiForwarder = PortForwarder(accessPublicPod, SERVER_PORT)
      portForwarders.add(accessPublicApiForwarder)

      val accessPublicApiAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { accessPublicApiForwarder.start() }
      val accessPublicApiChannel: Channel =
        buildMutualTlsChannel(accessPublicApiAddress.toTarget(), ACCESS_SIGNING_CERTS).also {
          channels.add(it)
        }

      val openIdProvidersConfigBuilder = OpenIdProvidersConfig.newBuilder()
      JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(OPEN_ID_PROVIDERS_CONFIG_JSON_FILE.readText(), openIdProvidersConfigBuilder)
      val openIdProvidersConfig = openIdProvidersConfigBuilder.build()

      val principal =
        createAccessPrincipal(
          measurementConsumerData.name,
          accessPublicApiChannel,
          openIdProvidersConfig.providerConfigByIssuerMap.keys.first(),
        )

      val getAccessToken = {
        OpenIdProvider(
            principal.user.issuer,
            TinkProtoKeysetFormat.parseKeyset(
              OPEN_ID_PROVIDERS_TINK_FILE.readBytes(),
              InsecureSecretKeyAccess.get(),
            ),
          )
          .generateCredentials(
            audience = openIdProvidersConfig.audience,
            subject = principal.user.subject,
            scopes =
              setOf(
                "reporting.basicReports.create",
                "reporting.reports.create",
                "reporting.metrics.create",
                "reporting.basicReports.get",
              ),
            ttl = Duration.ofMinutes(60),
          )
          .token
      }

      return ReportingUserSimulator(
        measurementConsumerName = measurementConsumerData.name,
        dataProvidersClient = DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        eventGroupsClient =
          org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub(
            publicApiChannel
          ),
        reportingSetsClient = ReportingSetsGrpcKt.ReportingSetsCoroutineStub(publicApiChannel),
        okHttpReportingClient = okHttpReportingClient,
        reportingGatewayHost = gatewayAddress.hostName,
        reportingGatewayPort = gatewayAddress.port,
        getReportingAccessToken = getAccessToken,
        modelLineName = modelLineName,
      )
    }

    fun stopPortForwarding() {
      for (channel in channels) {
        channel.shutdown()
      }
      for (portForwarder in portForwarders) {
        portForwarder.close()
      }
    }

    private suspend fun loadFullCmms(
      resourceInfo: ResourceInfo,
      akidPrincipalMap: File,
      measurementConsumerConfig: File,
      encryptionKeyPairConfig: File,
    ) {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("cmms")
          extractTar(getRuntimePath(LOCAL_K8S_TESTING_PATH.resolve("cmms.tar")).toFile(), outputDir)

          val configFilesDir = outputDir.toPath().resolve(CONFIG_FILES_PATH).toFile()
          logger.info("Copying $akidPrincipalMap to $CONFIG_FILES_PATH")
          akidPrincipalMap.copyTo(configFilesDir.resolve(akidPrincipalMap.name))

          logger.info("Copying $encryptionKeyPairConfig to $CONFIG_FILES_PATH")
          encryptionKeyPairConfig.copyTo(configFilesDir.resolve(encryptionKeyPairConfig.name))

          val mcConfigDir = outputDir.toPath().resolve(MC_CONFIG_PATH).toFile()
          logger.info("Copying $measurementConsumerConfig to $MC_CONFIG_PATH")
          measurementConsumerConfig.copyTo(mcConfigDir.resolve(measurementConsumerConfig.name))

          val configTemplate: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir.toPath().resolve(LOCAL_K8S_TESTING_PATH).resolve("cmms").toFile(),
            configTemplate,
          )

          val pdpResource: Resources.Resource =
            resourceInfo.dataProviders.getValue(PDP_DISPLAY_NAME)
          val configContent =
            configTemplate
              .readText(StandardCharsets.UTF_8)
              .replace("{aggregator_cert_name}", resourceInfo.aggregatorCert)
              .replace("{worker1_cert_name}", resourceInfo.worker1Cert)
              .replace("{worker2_cert_name}", resourceInfo.worker2Cert)
              .replace("{mc_name}", resourceInfo.measurementConsumer)
              .replace("{mc_api_key}", resourceInfo.measurementConsumerApiKey)
              .replace("{mc_cert_name}", resourceInfo.measurementConsumerCert)
              .replace("{pdp_name}", pdpResource.name)
              .replace("{pdp_cert_name}", pdpResource.dataProvider.certificate)
              .let {
                var config = it
                for ((displayName, resource) in resourceInfo.dataProviders) {
                  config =
                    config
                      .replace("{${displayName}_name}", resource.name)
                      .replace("{${displayName}_cert_name}", resource.dataProvider.certificate)
                }
                config
              }

          kubectlApply(configContent)
        }

      waitUntilDeploymentsComplete(appliedObjects)
    }

    private suspend fun loadKingdom() {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("kingdom-setup")
          extractTar(
            getRuntimePath(LOCAL_K8S_PATH.resolve("kingdom_setup.tar")).toFile(),
            outputDir,
          )
          val config: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir.toPath().resolve(LOCAL_K8S_PATH).resolve("kingdom_setup").toFile(),
            config,
          )

          kubectlApply(config)
        }

      waitUntilDeploymentsComplete(appliedObjects)
    }

    private suspend fun runResourceSetup(
      duchyCerts: List<DuchyCert>,
      dataProviderContents: List<EntityContent>,
      measurementConsumerContent: EntityContent,
    ): ResourceSetupOutput {
      val outputDir = withContext(Dispatchers.IO) { tempDir.newFolder("resource-setup") }

      val kingdomInternalPod: V1Pod = getPod(KINGDOM_INTERNAL_DEPLOYMENT_NAME)
      val kingdomPublicPod: V1Pod = getPod(KINGDOM_PUBLIC_DEPLOYMENT_NAME)

      val resources =
        PortForwarder(kingdomInternalPod, SERVER_PORT).use { internalForward ->
          val internalAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { internalForward.start() }
          val internalChannel =
            buildMutualTlsChannel(internalAddress.toTarget(), KINGDOM_SIGNING_CERTS)
          PortForwarder(kingdomPublicPod, SERVER_PORT)
            .use { publicForward ->
              val publicAddress: InetSocketAddress =
                withContext(Dispatchers.IO) { publicForward.start() }
              val publicChannel =
                buildMutualTlsChannel(publicAddress.toTarget(), KINGDOM_SIGNING_CERTS)
              val resourceSetup =
                ResourceSetup(
                  AccountsGrpcKt.AccountsCoroutineStub(internalChannel),
                  org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
                    .DataProvidersCoroutineStub(internalChannel),
                  org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
                    .CertificatesCoroutineStub(internalChannel),
                  org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub(
                    publicChannel
                  ),
                  ApiKeysGrpcKt.ApiKeysCoroutineStub(publicChannel),
                  MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicChannel),
                  runId,
                  internalModelProvidersClient =
                    ModelProvidersGrpcKt.ModelProvidersCoroutineStub(internalChannel),
                  internalModelSuitesClient =
                    ModelSuitesGrpcKt.ModelSuitesCoroutineStub(internalChannel),
                  internalModelLinesClient =
                    ModelLinesGrpcKt.ModelLinesCoroutineStub(internalChannel),
                  requiredDuchies = listOf("aggregator", "worker1", "worker2"),
                  outputDir = outputDir,
                )
              withContext(Dispatchers.IO) {
                resourceSetup
                  .process(
                    dataProviderContents,
                    measurementConsumerContent,
                    duchyCerts,
                    checkNotNull(
                      MP_SIGNING_CERTS.privateKeyHandle.certificate.authorityKeyIdentifier
                    ),
                  )
                  .also { publicChannel.shutdown() }
              }
            }
            .also { internalChannel.shutdown() }
        }

      return ResourceSetupOutput(
        resources,
        outputDir.resolve(ResourceSetup.AKID_PRINCIPAL_MAP_FILE),
        outputDir.resolve(ResourceSetup.MEASUREMENT_CONSUMER_CONFIG_FILE),
        outputDir.resolve(ResourceSetup.ENCRYPTION_KEY_PAIR_CONFIG_FILE),
      )
    }

    private fun extractTar(archive: File, outputDirectory: File) {
      Processes.runCommand("tar", "-xf", archive.toString(), "-C", outputDirectory.toString())
    }

    @Blocking
    private fun kustomize(kustomizationDir: File, output: File) {
      Processes.runCommand(
        "kubectl",
        "kustomize",
        kustomizationDir.toString(),
        "--output",
        output.toString(),
      )
    }

    @Blocking
    private fun kubectlApply(config: File): List<KubernetesObject> {
      return k8sClient
        .kubectlApply(config)
        .onEach { logger.info { "Applied ${it.kind} ${it.metadata.name}" } }
        .toList()
    }

    @Blocking
    private fun kubectlApply(config: String): List<KubernetesObject> {
      return k8sClient
        .kubectlApply(config)
        .onEach { logger.info { "Applied ${it.kind} ${it.metadata.name}" } }
        .toList()
    }

    private suspend fun waitUntilDeploymentComplete(name: String): V1Deployment {
      logger.info { "Waiting for Deployment $name to be complete..." }
      return k8sClient.waitUntilDeploymentComplete(name, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $name complete" }
      }
    }

    private suspend fun waitUntilDeploymentsComplete(appliedObjects: Iterable<KubernetesObject>) {
      appliedObjects.filterIsInstance<V1Deployment>().forEach {
        waitUntilDeploymentComplete(checkNotNull(it.metadata?.name))
      }
    }

    /**
     * Returns the first Pod from current ReplicaSet of the Deployment with name [deploymentName].
     *
     * This assumes that the Deployment is complete.
     */
    private suspend fun getPod(deploymentName: String): V1Pod {
      val deployment = checkNotNull(k8sClient.getDeployment(deploymentName))
      val replicaSet = checkNotNull(k8sClient.getNewReplicaSet(deployment))
      return k8sClient.listPods(replicaSet).items.first()
    }

    data class ResourceSetupOutput(
      val resources: List<Resources.Resource>,
      val akidPrincipalMap: File,
      val measurementConsumerConfig: File,
      val encryptionKeyPairConfig: File,
    )
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    init {
      // Ensure that JCE provider is installed before Kubernetes API client is instantiated.
      checkNotNull(Security.getProvider(jceProvider.name)) {
        "JCE provider ${jceProvider.name} is not installed"
      }
    }

    private const val SERVER_PORT: Int = 8443
    private const val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    private const val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    private const val REPORTING_PUBLIC_DEPLOYMENT_NAME =
      "reporting-v2alpha-public-api-server-deployment"
    private const val REPORTING_GATEWAY_DEPLOYMENT_NAME = "reporting-grpc-gateway-deployment"
    private const val ACCESS_PUBLIC_API_DEPLOYMENT_NAME = "access-public-api-server-deployment"
    private const val NUM_DATA_PROVIDERS = 6
    private val EDP_DISPLAY_NAMES: List<String> = (1..NUM_DATA_PROVIDERS).map { "edp$it" }
    private val READY_TIMEOUT = Duration.ofMinutes(2L)

    private val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")
    private val LOCAL_K8S_TESTING_PATH = LOCAL_K8S_PATH.resolve("testing")
    private val CONFIG_FILES_PATH = LOCAL_K8S_TESTING_PATH.resolve("config_files")
    private val MC_CONFIG_PATH = LOCAL_K8S_TESTING_PATH.resolve("mc_config")
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_local_images.bash")

    private val tempDir = TemporaryFolder()

    private val measurementSystem =
      LocalMeasurementSystem(
        lazy { KubernetesClientImpl(ClientBuilder.defaultClient()) },
        lazy { tempDir },
        lazy { UUID.randomUUID().toString() },
      )

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, Images(), measurementSystem)
  }
}

private fun InetSocketAddress.toTarget(): String {
  return "$hostName:$port"
}
