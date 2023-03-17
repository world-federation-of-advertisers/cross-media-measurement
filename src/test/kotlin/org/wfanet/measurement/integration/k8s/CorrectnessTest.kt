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

import io.grpc.Channel
import io.grpc.ManagedChannel
import io.kubernetes.client.common.KubernetesObject
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod
import java.io.File
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.security.Security
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.Blocking
import org.junit.AfterClass
import org.junit.BeforeClass
import org.junit.ClassRule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ApiKeysGrpcKt.ApiKeysCoroutineStub
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.testing.KindCluster
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.MC_DISPLAY_NAME
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadTestCertDerFile
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub
import org.wfanet.measurement.loadtest.config.EventFilters
import org.wfanet.measurement.loadtest.frontend.FrontendSimulator
import org.wfanet.measurement.loadtest.frontend.MeasurementConsumerData
import org.wfanet.measurement.loadtest.resourcesetup.DuchyCert
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.loadtest.resourcesetup.Resources
import org.wfanet.measurement.loadtest.storage.SketchStore
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient

/** Test for correctness of the CMMS on [KinD](https://kind.sigs.k8s.io/). */
@RunWith(JUnit4::class)
class CorrectnessTest {
  @Test(timeout = 1 * 60 * 1000)
  fun `impression measurement completes with expected result`() = runBlocking {
    testHarness.executeImpression("$runId-impression")
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `duration measurement completes with expected result`() = runBlocking {
    testHarness.executeDuration("$runId-duration")
  }

  @Test(timeout = 8 * 60 * 1000)
  fun `reach and frequency measurement completes with expected result`() = runBlocking {
    testHarness.executeReachAndFrequency("$runId-reach-and-freq")
  }

  private data class ResourceInfo(
    val aggregatorCert: String,
    val worker1Cert: String,
    val worker2Cert: String,
    val measurementConsumer: String,
    val apiKey: String,
    val dataProviders: Map<String, String>
  ) {
    companion object {
      fun from(resources: Iterable<Resources.Resource>): ResourceInfo {
        var aggregatorCert: String? = null
        var worker1Cert: String? = null
        var worker2Cert: String? = null
        var measurementConsumer: String? = null
        var apiKey: String? = null
        val dataProviders = mutableMapOf<String, String>()

        for (resource in resources) {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
          when (resource.resourceCase) {
            Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER -> {
              measurementConsumer = resource.name
              apiKey = resource.measurementConsumer.apiKey
            }
            Resources.Resource.ResourceCase.DATA_PROVIDER -> {
              val displayName = resource.dataProvider.displayName
              require(dataProviders.putIfAbsent(displayName, resource.name) == null) {
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
            Resources.Resource.ResourceCase.RESOURCE_NOT_SET -> error("Unhandled type")
          }
        }

        return ResourceInfo(
          requireNotNull(aggregatorCert),
          requireNotNull(worker1Cert),
          requireNotNull(worker2Cert),
          requireNotNull(measurementConsumer),
          requireNotNull(apiKey),
          dataProviders
        )
      }
    }
  }

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to KinD (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    private val logger = Logger.getLogger(this::class.java.name)

    private const val SERVER_PORT: Int = 8443
    private val DEFAULT_RPC_DEADLINE = Duration.ofSeconds(30)
    private const val CONFIG_FILES_NAME = "config-files"
    private const val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    private const val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    private const val FORWARDED_STORAGE_DEPLOYMENT_NAME = "fake-storage-server-deployment"
    private val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.1
      delta = 0.000001
    }
    private const val NUM_DATA_PROVIDERS = 6
    private val EDP_DISPLAY_NAMES: List<String> = (1..NUM_DATA_PROVIDERS).map { "edp$it" }
    private val READY_TIMEOUT = Duration.ofMinutes(2L)
    private val BAZEL_TEST_OUTPUTS_DIR: Path? =
      System.getenv("TEST_UNDECLARED_OUTPUTS_DIR")?.let { Paths.get(it) }

    private val TESTING_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "local", "testing")
    private val EMULATORS_CONFIG_PATH = TESTING_PATH.resolve("emulators_config.yaml")
    private val KINGDOM_CONFIG_PATH = TESTING_PATH.resolve("kingdom_config.yaml")
    private val DUCHIES_AND_EDPS_CONFIG_PATH = TESTING_PATH.resolve("duchies_and_edps_config.yaml")
    private val SECRET_FILES_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")

    private val EMULATOR_IMAGE_ARCHIVES =
      listOf(
        Paths.get(
          "wfa_common_jvm",
          "src",
          "main",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "storage",
          "filesystem",
          "server_image.tar"
        ),
      )

    private val MEASUREMENT_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
      )

    private val KINGDOM_DEPLOY_PATH = MEASUREMENT_PATH.resolve(Paths.get("kingdom", "deploy"))
    private val KINGDOM_IMAGE_ARCHIVES =
      listOf(
        KINGDOM_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "server", "gcp_kingdom_data_server_image.tar")
        ),
        KINGDOM_DEPLOY_PATH.resolve(Paths.get("common", "server", "system_api_server_image.tar")),
        KINGDOM_DEPLOY_PATH.resolve(
          Paths.get("common", "server", "v2alpha_public_api_server_image.tar")
        ),
        KINGDOM_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "spanner", "tools", "update_schema_image.tar")
        ),
      )

    private val DUCHY_DEPLOY_PATH = MEASUREMENT_PATH.resolve(Paths.get("duchy", "deploy"))
    private val DUCHY_IMAGE_ARCHIVES =
      listOf(
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get(
            "common",
            "daemon",
            "mill",
            "liquidlegionsv2",
            "forwarded_storage_liquid_legions_v2_mill_daemon_image.tar"
          )
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get(
            "common",
            "server",
            "forwarded_storage_computation_control_server_image.tar",
          )
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get(
            "common",
            "server",
            "forwarded_storage_requisition_fulfillment_server_image.tar",
          )
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("common", "server", "async_computation_control_server_image.tar")
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("common", "daemon", "herald", "herald_daemon_image.tar")
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "server", "forwarded_storage_spanner_computations_server_image.tar")
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "spanner", "tools", "update_schema_image.tar")
        ),
      )

    private val SIMULATOR_IMAGE_ARCHIVES =
      listOf(
        MEASUREMENT_PATH.resolve(
          Paths.get("loadtest", "dataprovider", "forwarded_storage_edp_simulator_runner_image.tar")
        )
      )

    private val kingdomSigningCerts: SigningCerts by lazy {
      val secretFiles = checkNotNull(getRuntimePath(SECRET_FILES_PATH))
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("kingdom_tls.pem").toFile()
      val key = secretFiles.resolve("kingdom_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    private val measurementConsumerSigningCerts: SigningCerts by lazy {
      val secretFiles = checkNotNull(getRuntimePath(SECRET_FILES_PATH))
      val trustedCerts = secretFiles.resolve("mc_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    @ClassRule @JvmField val kindCluster = KindCluster()

    private val runId: String
      get() = kindCluster.uuid.toString()

    private val k8sClient: KubernetesClient
      get() = kindCluster.k8sClient

    @ClassRule @JvmField val tempDir = TemporaryFolder()

    private val portForwarders = mutableListOf<PortForwarder>()
    private val channels = mutableListOf<ManagedChannel>()
    private lateinit var testHarness: FrontendSimulator

    @BeforeClass
    @JvmStatic
    fun initCluster() = runBlocking {
      withTimeout(Duration.ofMinutes(5)) {
        val apiClient = k8sClient.apiClient
        apiClient.httpClient =
          apiClient.httpClient.newBuilder().readTimeout(Duration.ofHours(1L)).build()
        Configuration.setDefaultApiClient(apiClient)

        val duchyCerts =
          ALL_DUCHY_NAMES.map { DuchyCert(it, loadTestCertDerFile("${it}_cs_cert.der")) }
        val edpEntityContents = EDP_DISPLAY_NAMES.map { createEntityContent(it) }
        val measurementConsumerContent =
          withContext(Dispatchers.IO) { createEntityContent(MC_DISPLAY_NAME) }

        loadEmulators()
        val resourceInfo =
          ResourceInfo.from(loadKingdom(duchyCerts, edpEntityContents, measurementConsumerContent))
        val encryptionPrivateKey: TinkPrivateKeyHandle =
          withContext(Dispatchers.IO) {
            loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
          }
        val measurementConsumerData =
          MeasurementConsumerData(
            resourceInfo.measurementConsumer,
            measurementConsumerContent.signingKey,
            encryptionPrivateKey,
            resourceInfo.apiKey
          )

        loadDuchiesAndEdps(resourceInfo)

        testHarness = createTestHarness(measurementConsumerData)
      }
    }

    @AfterClass
    @JvmStatic
    fun stopPortForwarding() {
      for (channel in channels) {
        channel.shutdown()
      }
      for (portForwarder in portForwarders) {
        portForwarder.close()
      }
    }

    @AfterClass
    @JvmStatic
    fun exportKindLogs() {
      if (BAZEL_TEST_OUTPUTS_DIR == null) {
        return
      }

      kindCluster.exportLogs(BAZEL_TEST_OUTPUTS_DIR)
    }

    private suspend fun loadDuchiesAndEdps(resourceInfo: ResourceInfo) {
      withContext(Dispatchers.IO) {
        for (imageArchivePath in DUCHY_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
        for (imageArchivePath in SIMULATOR_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
      }

      val duchiesAndEdpsConfig =
        checkNotNull(getRuntimePath(DUCHIES_AND_EDPS_CONFIG_PATH))
          .toFile()
          .readText(StandardCharsets.UTF_8)
          .replace("{aggregator_cert_name}", resourceInfo.aggregatorCert)
          .replace("{worker1_cert_name}", resourceInfo.worker1Cert)
          .replace("{worker2_cert_name}", resourceInfo.worker2Cert)
          .replace("{mc_name}", resourceInfo.measurementConsumer)
          .let {
            var config = it
            for ((displayName, resourceName) in resourceInfo.dataProviders) {
              config = config.replace("{${displayName}_name}", resourceName)
            }
            config
          }
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) { kubectlApply(duchiesAndEdpsConfig) }
      appliedObjects.filterIsInstance(V1Deployment::class.java).forEach {
        waitUntilDeploymentReady(it)
      }
    }

    private suspend fun createTestHarness(
      measurementConsumerData: MeasurementConsumerData
    ): FrontendSimulator {
      val kingdomPublicPod: V1Pod =
        k8sClient
          .listPodsByMatchLabels(waitUntilDeploymentReady(KINGDOM_PUBLIC_DEPLOYMENT_NAME))
          .items
          .first()

      val publicApiForwarder = PortForwarder(kingdomPublicPod, SERVER_PORT)
      portForwarders.add(publicApiForwarder)

      val forwardedStoragePod: V1Pod =
        k8sClient
          .listPodsByMatchLabels(waitUntilDeploymentReady(FORWARDED_STORAGE_DEPLOYMENT_NAME))
          .items
          .first()
      val storageForwarder = PortForwarder(forwardedStoragePod, SERVER_PORT)
      portForwarders.add(storageForwarder)

      val publicApiAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { publicApiForwarder.start() }
      val publicApiChannel: Channel =
        buildMutualTlsChannel(publicApiAddress.toTarget(), measurementConsumerSigningCerts)
          .also { channels.add(it) }
          .withDefaultDeadline(DEFAULT_RPC_DEADLINE)
      val storageAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { storageForwarder.start() }
      val storageChannel: Channel =
        buildMutualTlsChannel(storageAddress.toTarget(), kingdomSigningCerts)
          .also { channels.add(it) }
          .withDefaultDeadline(DEFAULT_RPC_DEADLINE)
      val storageClient = ForwardedStorageClient(ForwardedStorageCoroutineStub(storageChannel))
      val eventGroupsClient = EventGroupsCoroutineStub(publicApiChannel)

      return FrontendSimulator(
          measurementConsumerData,
          OUTPUT_DP_PARAMS,
          DataProvidersCoroutineStub(publicApiChannel),
          eventGroupsClient,
          MeasurementsCoroutineStub(publicApiChannel),
          RequisitionsCoroutineStub(publicApiChannel),
          MeasurementConsumersCoroutineStub(publicApiChannel),
          CertificatesCoroutineStub(publicApiChannel),
          SketchStore(storageClient),
          Duration.ofSeconds(10L),
          measurementConsumerSigningCerts.trustedCertificates,
          EventFilters.EVENT_TEMPLATES_TO_FILTERS_MAP
        )
        .also {
          eventGroupsClient.waitForEventGroups(
            measurementConsumerData.name,
            measurementConsumerData.apiAuthenticationKey
          )
        }
    }

    private suspend fun loadEmulators() {
      withContext(Dispatchers.IO) {
        for (imageArchivePath in EMULATOR_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
      }

      // Wait until default service account has been created. See
      // https://github.com/kubernetes/kubernetes/issues/66689.
      k8sClient.waitForServiceAccount("default", timeout = READY_TIMEOUT)

      withContext(Dispatchers.IO) {
        val emulatorsConfig: File = checkNotNull(getRuntimePath(EMULATORS_CONFIG_PATH)).toFile()
        kubectlApply(emulatorsConfig)
      }
    }

    private suspend fun loadKingdom(
      duchyCerts: List<DuchyCert>,
      edpEntityContents: List<EntityContent>,
      measurementConsumerContent: EntityContent
    ): List<Resources.Resource> {
      withContext(Dispatchers.IO) {
        for (imageArchivePath in KINGDOM_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
      }

      withContext(Dispatchers.IO) {
        val kingdomConfig: File = checkNotNull(getRuntimePath(KINGDOM_CONFIG_PATH)).toFile()
        kubectlApply(kingdomConfig)
      }
      val resourceSetupOutput = withContext(Dispatchers.IO) { tempDir.newFolder("resource-setup") }

      val kingdomInternalPod =
        k8sClient
          .listPodsByMatchLabels(waitUntilDeploymentReady(KINGDOM_INTERNAL_DEPLOYMENT_NAME))
          .items
          .first()
      val kingdomPublicPod =
        k8sClient
          .listPodsByMatchLabels(waitUntilDeploymentReady(KINGDOM_PUBLIC_DEPLOYMENT_NAME))
          .items
          .first()

      val resources =
        PortForwarder(kingdomInternalPod, SERVER_PORT).use { internalForward ->
          val internalAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { internalForward.start() }
          val internalChannel =
            buildMutualTlsChannel(internalAddress.toTarget(), kingdomSigningCerts)
          PortForwarder(kingdomPublicPod, SERVER_PORT)
            .use { publicForward ->
              val publicAddress: InetSocketAddress =
                withContext(Dispatchers.IO) { publicForward.start() }
              val publicChannel =
                buildMutualTlsChannel(publicAddress.toTarget(), kingdomSigningCerts)
              val resourceSetup =
                ResourceSetup(
                  InternalAccountsCoroutineStub(internalChannel),
                  InternalDataProvidersCoroutineStub(internalChannel),
                  AccountsCoroutineStub(publicChannel),
                  ApiKeysCoroutineStub(publicChannel),
                  InternalCertificatesCoroutineStub(internalChannel),
                  MeasurementConsumersCoroutineStub(publicChannel),
                  runId,
                  outputDir = resourceSetupOutput,
                  requiredDuchies = listOf("aggregator", "worker1", "worker2")
                )
              withContext(Dispatchers.IO) {
                resourceSetup
                  .process(edpEntityContents, measurementConsumerContent, duchyCerts)
                  .also { publicChannel.shutdown() }
              }
            }
            .also { internalChannel.shutdown() }
        }

      val akidPrincipalMap: String =
        withContext(Dispatchers.IO) {
          resourceSetupOutput
            .resolve(ResourceSetup.AKID_PRINCIPAL_MAP_FILE)
            .readText(StandardCharsets.UTF_8)
        }

      k8sClient.updateConfigMap(
        CONFIG_FILES_NAME,
        ResourceSetup.AKID_PRINCIPAL_MAP_FILE,
        akidPrincipalMap
      )

      // Restart public API server to pick up updated ConfigMap.
      k8sClient.restartDeployment(KINGDOM_PUBLIC_DEPLOYMENT_NAME)

      return resources
    }

    private suspend fun EventGroupsCoroutineStub.waitForEventGroups(
      measurementConsumer: String,
      apiKey: String
    ) {
      logger.info { "Waiting for all event groups to be created..." }
      while (currentCoroutineContext().isActive) {
        val eventGroups =
          withAuthenticationKey(apiKey)
            .listEventGroups(
              listEventGroupsRequest {
                parent = "dataProviders/-"
                filter =
                  ListEventGroupsRequestKt.filter { measurementConsumers += measurementConsumer }
              }
            )
            .eventGroupsList

        // Each EDP simulator creates one event group, so we wait until there are as many event
        // groups as EDP simulators.
        if (eventGroups.size == NUM_DATA_PROVIDERS) {
          logger.info { "All event groups created" }
          return
        }

        delay(Duration.ofSeconds(1))
      }
    }

    @Blocking
    private fun loadImage(archivePath: Path) {
      logger.info("Loading image archive $archivePath")
      val runtimePath: Path = checkNotNull(getRuntimePath(archivePath))
      kindCluster.loadImage(runtimePath)
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

    private suspend fun waitUntilDeploymentReady(name: String): V1Deployment {
      logger.info { "Waiting for Deployment $name to be ready..." }
      return k8sClient.waitUntilDeploymentReady(name, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $name ready" }
      }
    }

    private suspend fun waitUntilDeploymentReady(deployment: V1Deployment): V1Deployment {
      val deploymentName = requireNotNull(deployment.metadata?.name)
      logger.info { "Waiting for Deployment $deploymentName to be ready..." }
      return k8sClient.waitUntilDeploymentReady(deployment, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $deploymentName ready" }
      }
    }
  }
}

private fun InetSocketAddress.toTarget(): String {
  return "$hostName:$port"
}
