/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.integration.k8s

import com.google.protobuf.Any
import com.google.protobuf.kotlin.toByteString
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
import java.security.Security
import java.time.Duration
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.Blocking
import org.junit.AfterClass
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.KubernetesClientImpl
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.common.k8s.testing.Processes
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt
import org.wfanet.measurement.loadtest.panelmatch.EntitiesData
import org.wfanet.measurement.loadtest.panelmatch.PanelMatchSimulator
import org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.loadtest.forwardedStorageConfig
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.storage.testing.FakeTinkKeyStorageProvider

/**
 * Test for correctness of the CMMS for panel exchange on a single "empty" Kubernetes cluster using
 * the `local` configuration.
 *
 * This will push the images to the container registry and populate the K8s cluster prior to running
 * the test methods. The cluster must already exist with the `KUBECONFIG` environment variable
 * pointing to its kubeconfig.
 *
 * This assumes that the `tar` and `kubectl` executables are the execution path. The latter is only
 * used for `kustomize`, as the Kubernetes API is used to interact with the cluster.
 */
class EmptyClusterPanelMatchCorrectnessTest : AbstractPanelMatchCorrectnessTest(localSystem) {

  class Images : TestRule {
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
      val panelMatchPusherRuntimePath = getRuntimePath(IMAGE_PANEL_MATCH_PUSHER_PATH)
      Processes.runCommand(panelMatchPusherRuntimePath.toString())
    }
  }

  /**
   * [TestRule] which populates a K8s cluster with the components of the CMMS and daemons for
   * PanelMatch.
   */
  class LocalSystem(
    k8sClient: Lazy<KubernetesClient>,
    tempDir: Lazy<TemporaryFolder>,
    runId: Lazy<String>,
  ) : TestRule, PanelMatchSystem {
    private val k8sClient: KubernetesClient by k8sClient
    private val tempDir: TemporaryFolder by tempDir

    private val channels = mutableListOf<ManagedChannel>()
    private val portForwarders = mutableListOf<PortForwarder>()

    private lateinit var dataProviderPrivateStorageDetails: StorageDetails
    private lateinit var dataProviderSharedStorageDetails: StorageDetails
    private lateinit var modelProviderPrivateStorageDetails: StorageDetails
    private lateinit var modelProviderSharedStorageDetails: StorageDetails
    private lateinit var dpForwardedStorage: StorageClient
    private lateinit var mpForwardedStorage: StorageClient
    private lateinit var publicChannel: ManagedChannel
    private lateinit var dataProviderDefaults: DaemonStorageClientDefaults
    private lateinit var modelProviderDefaults: DaemonStorageClientDefaults

    override val runId: String by runId

    private lateinit var _testHarness: PanelMatchSimulator
    override val testHarness: PanelMatchSimulator
      get() = _testHarness

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking {
            withTimeout(Duration.ofMinutes(5)) {
              _testHarness = createTestHarness(populateCluster())
            }
          }
          base.evaluate()
        }
      }
    }

    private suspend fun populateCluster(): EntitiesData {
      val apiClient = k8sClient.apiClient
      apiClient.httpClient =
        apiClient.httpClient.newBuilder().readTimeout(Duration.ofHours(1L)).build()
      Configuration.setDefaultApiClient(apiClient)

      // Wait until default service account has been created. See
      // https://github.com/kubernetes/kubernetes/issues/66689.
      k8sClient.waitForServiceAccount("default", timeout = READY_TIMEOUT)

      loadKingdomForSetup()
      val dataProviderEntityContent = withContext(Dispatchers.IO) { createEntityContent("edp1") }
      val modelProviderEntityContent = withContext(Dispatchers.IO) { createEntityContent("mp1") }
      val resourceSetupOutput =
        runResourceSetup(dataProviderEntityContent, modelProviderEntityContent)
      loadCmms(resourceSetupOutput.akidPrincipalMap)

      return resourceSetupOutput.entitiesData
    }

    /** Runs resource setup, leaving port forwarding active for forwarded storage. */
    private suspend fun runResourceSetup(
      dataProviderContent: EntityContent,
      modelProviderContent: EntityContent,
    ): ResourceSetupOutput {
      val outputDir = withContext(Dispatchers.IO) { tempDir.newFolder("resource-setup") }

      val kingdomInternalPod = getPod(KINGDOM_INTERNAL_DEPLOYMENT_NAME)
      val mpPrivateStoragePod = getPod(MP_PRIVATE_STORAGE_DEPLOYMENT_NAME)
      val dpPrivateStoragePod = getPod(DP_PRIVATE_STORAGE_DEPLOYMENT_NAME)
      val sharedStoragePod = getPod(SHARED_STORAGE_DEPLOYMENT_NAME)

      return PortForwarder(kingdomInternalPod, SERVER_PORT).use { internalForward ->
        val internalAddress: InetSocketAddress =
          withContext(Dispatchers.IO) { internalForward.start() }
        val internalChannel =
          buildMutualTlsChannel(internalAddress.toTarget(), KINGDOM_SIGNING_CERTS).also {
            channels.add(it)
          }

        val panelMatchResourceSetup =
          PanelMatchResourceSetup(
            DataProvidersGrpcKt.DataProvidersCoroutineStub(internalChannel),
            ModelProvidersGrpcKt.ModelProvidersCoroutineStub(internalChannel),
            RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub(internalChannel),
            outputDir,
          )
        val panelMatchResourceKey =
          panelMatchResourceSetup
            .process(
              dataProviderContent = dataProviderContent,
              modelProviderContent = modelProviderContent,
              exchangeDate = EXCHANGE_DATE.toProtoDate(),
              exchangeSchedule = SCHEDULE,
            )
            .also { internalChannel.shutdown() }

        dataProviderKey = panelMatchResourceKey.dataProviderKey
        modelProviderKey = panelMatchResourceKey.modelProviderKey

        val dpStorageForwarder = PortForwarder(dpPrivateStoragePod, SERVER_PORT)
        portForwarders.add(dpStorageForwarder)
        val dpStorageAddress: InetSocketAddress =
          withContext(Dispatchers.IO) { dpStorageForwarder.start() }
        val dpStorageChannel =
          buildMutualTlsChannel(dpStorageAddress.toTarget(), EDP_SIGNING_CERTS).also {
            channels.add(it)
          }
        dpForwardedStorage =
          ForwardedStorageClient(
            ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub(dpStorageChannel)
          )
        dataProviderDefaults =
          DaemonStorageClientDefaults(dpForwardedStorage, "", FakeTinkKeyStorageProvider())

        dataProviderDefaults.rootCertificates.put(
          panelMatchResourceKey.dataProviderKey.toName(),
          TestCertificateManager.CERTIFICATE.encoded.toByteString(),
        )

        val dpPrivateForwarderStorage = forwardedStorageConfig {
          target = dpPrivateStoragePod.status?.podIP + ":8443"
          certCollectionPath = "/var/run/secrets/files/edp_trusted_certs.pem"
          forwardedStorageCertHost = "localhost"
        }
        dataProviderPrivateStorageDetails = storageDetails {
          custom = StorageDetailsKt.customStorage { details = Any.pack(dpPrivateForwarderStorage) }
          visibility = StorageDetails.Visibility.PRIVATE
        }
        val dpSharedForwarderStorage = forwardedStorageConfig {
          target = sharedStoragePod.status?.podIP + ":8443"
          certCollectionPath = "/var/run/secrets/files/edp_trusted_certs.pem"
          forwardedStorageCertHost = "localhost"
        }
        dataProviderSharedStorageDetails = storageDetails {
          custom = StorageDetailsKt.customStorage { details = Any.pack(dpSharedForwarderStorage) }
          visibility = StorageDetails.Visibility.SHARED
        }

        logger.info { "DataProvider setup completed" }

        // Setup model provider resources
        val mpStorageForwarder = PortForwarder(mpPrivateStoragePod, SERVER_PORT)
        portForwarders.add(mpStorageForwarder)
        val mpStorageAddress: InetSocketAddress =
          withContext(Dispatchers.IO) { mpStorageForwarder.start() }
        val mpStorageChannel =
          buildMutualTlsChannel(mpStorageAddress.toTarget(), MP_SIGNING_CERTS).also {
            channels.add(it)
          }
        mpForwardedStorage =
          ForwardedStorageClient(
            ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub(mpStorageChannel)
          )
        modelProviderDefaults =
          DaemonStorageClientDefaults(mpForwardedStorage, "", FakeTinkKeyStorageProvider())

        modelProviderDefaults.rootCertificates.put(
          panelMatchResourceKey.modelProviderKey.toName(),
          TestCertificateManager.CERTIFICATE.encoded.toByteString(),
        )
        val mpPrivateForwarderStorage = forwardedStorageConfig {
          target = mpPrivateStoragePod.status?.podIP + ":8443"
          certCollectionPath = "/var/run/secrets/files/mp_trusted_certs.pem"
          forwardedStorageCertHost = "localhost"
        }
        modelProviderPrivateStorageDetails = storageDetails {
          custom = StorageDetailsKt.customStorage { details = Any.pack(mpPrivateForwarderStorage) }
          visibility = StorageDetails.Visibility.PRIVATE
        }

        val mpSharedForwarderStorage = forwardedStorageConfig {
          target = sharedStoragePod.status?.podIP + ":8443"
          certCollectionPath = "/var/run/secrets/files/mp_trusted_certs.pem"
          forwardedStorageCertHost = "localhost"
        }
        modelProviderSharedStorageDetails = storageDetails {
          custom = StorageDetailsKt.customStorage { details = Any.pack(mpSharedForwarderStorage) }
          visibility = StorageDetails.Visibility.SHARED
        }

        val akidPrincipalMap = outputDir.resolve(PanelMatchResourceSetup.AKID_PRINCIPAL_MAP_FILE)

        loadDpDaemonForPanelMatch(
          k8sClient,
          panelMatchResourceKey.dataProviderKey,
          akidPrincipalMap,
        )
        loadMpDaemonForPanelMatch(
          k8sClient,
          panelMatchResourceKey.modelProviderKey,
          akidPrincipalMap,
        )

        ResourceSetupOutput(
          EntitiesData(
            panelMatchResourceKey.dataProviderKey,
            panelMatchResourceKey.modelProviderKey,
          ),
          akidPrincipalMap,
        )
      }
    }

    private suspend fun loadKingdomForSetup() {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("kingdom-for-panelmatch-setup")
          extractTar(
            getRuntimePath(LOCAL_K8S_PATH.resolve("kingdom_for_panelmatch_setup.tar")).toFile(),
            outputDir,
          )
          val config: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir
              .toPath()
              .resolve(LOCAL_K8S_PATH)
              .resolve("kingdom_for_panelmatch_setup")
              .toFile(),
            config,
          )
          kubectlApply(config, k8sClient)
        }

      waitUntilDeploymentsComplete(appliedObjects)
    }

    private suspend fun loadCmms(akidPrincipalMap: File) {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("cmms")
          extractTar(
            getRuntimePath(LOCAL_K8S_TESTING_PATH.resolve("cmms_for_panelmatch.tar")).toFile(),
            outputDir,
          )

          val configFilesDir = outputDir.toPath().resolve(CONFIG_FILES_PATH).toFile()
          logger.info("Copying $akidPrincipalMap to $CONFIG_FILES_PATH")
          akidPrincipalMap.copyTo(configFilesDir.resolve(akidPrincipalMap.name))

          val configFile: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir
              .toPath()
              .resolve(LOCAL_K8S_TESTING_PATH)
              .resolve("cmms_for_panelmatch")
              .toFile(),
            configFile,
          )

          kubectlApply(configFile, k8sClient)
        }

      waitUntilDeploymentsComplete(appliedObjects)
    }

    private suspend fun loadDpDaemonForPanelMatch(
      k8sClient: KubernetesClient,
      dataProviderKey: DataProviderKey,
      akidPrincipalMap: File,
    ) {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("edp_daemon")
          extractTar(
            getRuntimePath(LOCAL_K8S_PANELMATCH_PATH.resolve("edp_daemon.tar")).toFile(),
            outputDir,
          )

          val configFilesDir = outputDir.toPath().resolve(PANELMATCH_CONFIG_FILES_PATH).toFile()
          akidPrincipalMap.copyTo(configFilesDir.resolve(akidPrincipalMap.name))
          val configTemplate: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir.toPath().resolve(LOCAL_K8S_PANELMATCH_PATH).resolve("edp_daemon").toFile(),
            configTemplate,
          )

          val configContent =
            configTemplate
              .readText(StandardCharsets.UTF_8)
              .replace("{party_name}", dataProviderKey.dataProviderId)

          kubectlApply(configContent, k8sClient)
        }

      waitUntilDeploymentsComplete(appliedObjects)
    }

    private suspend fun loadMpDaemonForPanelMatch(
      k8sClient: KubernetesClient,
      modelProviderKey: ModelProviderKey,
      akidPrincipalMap: File,
    ) {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("mp_daemon")
          extractTar(
            getRuntimePath(LOCAL_K8S_PANELMATCH_PATH.resolve("mp_daemon.tar")).toFile(),
            outputDir,
          )

          val configFilesDir = outputDir.toPath().resolve(PANELMATCH_CONFIG_FILES_PATH).toFile()
          akidPrincipalMap.copyTo(configFilesDir.resolve(akidPrincipalMap.name))

          val configTemplate: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir.toPath().resolve(LOCAL_K8S_PANELMATCH_PATH).resolve("mp_daemon").toFile(),
            configTemplate,
          )
          val configContent =
            configTemplate
              .readText(StandardCharsets.UTF_8)
              .replace("{party_name}", modelProviderKey.modelProviderId)

          kubectlApply(configContent, k8sClient)
        }

      waitUntilDeploymentsComplete(appliedObjects)
    }

    private suspend fun createTestHarness(entitiesData: EntitiesData): PanelMatchSimulator {

      val publicForward = PortForwarder(getPod(KINGDOM_PUBLIC_DEPLOYMENT_NAME), SERVER_PORT)
      val publicAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { publicForward.start() }
          .also { portForwarders.add(publicForward) }

      publicChannel =
        buildMutualTlsChannel(publicAddress.toTarget(), MP_SIGNING_CERTS).also { channels.add(it) }

      val internalForward = PortForwarder(getPod(KINGDOM_INTERNAL_DEPLOYMENT_NAME), SERVER_PORT)
      val internalAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { internalForward.start() }
          .also { portForwarders.add(internalForward) }
      val internalChannel =
        buildMutualTlsChannel(internalAddress.toTarget(), KINGDOM_SIGNING_CERTS).also {
          channels.add(it)
        }

      return PanelMatchSimulator(
        entitiesData,
        RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub(internalChannel),
        ExchangesGrpcKt.ExchangesCoroutineStub(publicChannel),
        ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub(publicChannel),
        SCHEDULE,
        API_VERSION,
        EXCHANGE_DATE,
        dataProviderPrivateStorageDetails,
        modelProviderPrivateStorageDetails,
        dataProviderSharedStorageDetails,
        modelProviderSharedStorageDetails,
        dpForwardedStorage,
        mpForwardedStorage,
        dataProviderDefaults,
        modelProviderDefaults,
      )
    }

    fun cleanChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
      for (portForwarder in portForwarders) {
        portForwarder.stop()
      }
    }

    private data class ResourceSetupOutput(
      val entitiesData: EntitiesData,
      val akidPrincipalMap: File,
    )
  }

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to cluster (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    var dataProviderKey: DataProviderKey? = null
    var modelProviderKey: ModelProviderKey? = null

    private val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")
    private val LOCAL_K8S_TESTING_PATH = LOCAL_K8S_PATH.resolve("testing")
    private val CONFIG_FILES_PATH = LOCAL_K8S_TESTING_PATH.resolve("config_files_for_panel_match")
    private val LOCAL_K8S_PANELMATCH_PATH = Paths.get("src", "main", "k8s", "panelmatch", "local")
    private val PANELMATCH_CONFIG_FILES_PATH = LOCAL_K8S_PANELMATCH_PATH.resolve("config_files")

    private const val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    private const val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    private const val MP_PRIVATE_STORAGE_DEPLOYMENT_NAME = "mp-private-storage-server-deployment"
    private const val DP_PRIVATE_STORAGE_DEPLOYMENT_NAME = "dp-private-storage-server-deployment"
    private const val SHARED_STORAGE_DEPLOYMENT_NAME = "shared-storage-server-deployment"

    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_local_images.bash")
    private val IMAGE_PANEL_MATCH_PUSHER_PATH =
      Paths.get("src", "main", "docker", "panel_exchange_client", "push_all_images.bash")

    private const val API_VERSION = "v2alpha"
    private const val SCHEDULE = "@daily"

    private val tempDir = TemporaryFolder()

    private val localSystem =
      LocalSystem(
        lazy { KubernetesClientImpl(ClientBuilder.defaultClient()) },
        lazy { tempDir },
        lazy { UUID.randomUUID().toString() },
      )

    @ClassRule @JvmField val chainedRule = chainRulesSequentially(tempDir, Images(), localSystem)

    @JvmStatic
    @AfterClass
    fun tearDownClass() {
      localSystem.cleanChannels()
    }

    @Blocking
    private fun kubectlApply(config: String, k8sClient: KubernetesClient): List<KubernetesObject> {
      return k8sClient
        .kubectlApply(config)
        .onEach { logger.info { "Applied ${it.kind} ${it.metadata.name}" } }
        .toList()
    }

    @Blocking
    private fun kubectlApply(config: File, k8sClient: KubernetesClient): List<KubernetesObject> {
      return k8sClient
        .kubectlApply(config)
        .onEach { logger.info { "Applied ${it.kind} ${it.metadata.name}" } }
        .toList()
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

    private fun extractTar(archive: File, outputDirectory: File) {
      Processes.runCommand("tar", "-xf", archive.toString(), "-C", outputDirectory.toString())
    }

    private suspend fun KubernetesClient.waitUntilDeploymentComplete(name: String): V1Deployment {
      logger.info { "Waiting for Deployment $name to be complete..." }
      return waitUntilDeploymentComplete(name, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $name complete" }
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

    private suspend fun waitUntilDeploymentsComplete(appliedObjects: Iterable<KubernetesObject>) {
      appliedObjects.filterIsInstance<V1Deployment>().forEach {
        k8sClient.waitUntilDeploymentComplete(checkNotNull(it.metadata?.name))
      }
    }
  }
}
