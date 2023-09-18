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

import com.google.privatemembership.batch.Shared
import com.google.protobuf.Any.pack
import com.google.protobuf.ByteString
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import com.google.protobuf.kotlin.toByteString
import io.grpc.Channel
import io.grpc.ManagedChannel
import io.grpc.StatusException
import io.kubernetes.client.common.KubernetesObject
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.util.ClientBuilder
import java.io.File
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Path
import java.nio.file.Paths
import java.security.Security
import java.time.Duration
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.Blocking
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProviderKey
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.Exchange
import org.wfanet.measurement.api.v2alpha.ExchangeKey
import org.wfanet.measurement.api.v2alpha.ExchangeStep
import org.wfanet.measurement.api.v2alpha.ExchangeStepsGrpcKt
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.ExchangesGrpcKt
import org.wfanet.measurement.api.v2alpha.ListExchangeStepsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelProviderKey
import org.wfanet.measurement.api.v2alpha.authenticateRequest
import org.wfanet.measurement.api.v2alpha.createMeasurementConsumerRequest
import org.wfanet.measurement.api.v2alpha.getCertificateRequest
import org.wfanet.measurement.api.v2alpha.getExchangeRequest
import org.wfanet.measurement.api.v2alpha.listExchangeStepsRequest
import org.wfanet.measurement.api.v2alpha.withPrincipal
import org.wfanet.measurement.api.withIdToken
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.identity.withPrincipalName
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.common.k8s.testing.Processes
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.testing.ForwardedStorageGrpcKt
import org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.storage.forwarded.ForwardedStorageClient
import org.wfanet.panelmatch.client.deploy.DaemonStorageClientDefaults
import org.wfanet.panelmatch.client.storage.StorageDetails
import org.wfanet.panelmatch.client.storage.StorageDetailsKt.customStorage
import org.wfanet.panelmatch.client.storage.StorageDetailsKt.forwardedStorage
import org.wfanet.panelmatch.client.storage.storageDetails
import org.wfanet.panelmatch.common.certificates.testing.TestCertificateManager
import org.wfanet.panelmatch.common.storage.testing.FakeTinkKeyStorageProvider

abstract class AbstractPanelMatchCorrectnessTest {

  private val channels = mutableListOf<ManagedChannel>()
  private val portForwarders = mutableListOf<PortForwarder>()
  private val TERMINAL_EXCHANGE_STATES = setOf(Exchange.State.SUCCEEDED, Exchange.State.FAILED)
  private lateinit var exchangeKey: ExchangeKey
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
      val panelMatchPusherRuntimePath = getRuntimePath(IMAGE_PANEL_MATCH_PUSHER_PATH)
      Processes.runCommand(panelMatchPusherRuntimePath.toString())
    }
  }

  protected suspend fun runResourceSetup(
    dataProviderContent: EntityContent,
    workflow: ExchangeWorkflow,
    initialDataProviderInputs: Map<String, ByteString>
  ) {
    val outputDir = withContext(Dispatchers.IO) { tempDir.newFolder("resource-setup") }
    val k8sClient = KubernetesClient(ClientBuilder.defaultClient())

    val kingdomInternalPod =
      k8sClient
        .listPodsByMatchLabels(
          k8sClient.waitUntilDeploymentReady(KINGDOM_INTERNAL_DEPLOYMENT_NAME)
        )
        .items
        .first()
    val kingdomPublicPod =
      k8sClient
        .listPodsByMatchLabels(k8sClient.waitUntilDeploymentReady(KINGDOM_PUBLIC_DEPLOYMENT_NAME))
        .items
        .first()
    val mpPrivateStoragePod =
      k8sClient
        .listPodsByMatchLabels(k8sClient.waitUntilDeploymentReady(MP_PRIVATE_STORAGE_DEPLOYMENT_NAME))
        .items
        .first()
    val dpPrivateStoragePod =
      k8sClient
        .listPodsByMatchLabels(k8sClient.waitUntilDeploymentReady(DP_PRIVATE_STORAGE_DEPLOYMENT_NAME))
        .items
        .first()
    val sharedStoragePod =
      k8sClient
        .listPodsByMatchLabels(k8sClient.waitUntilDeploymentReady(SHARED_STORAGE_DEPLOYMENT_NAME))
        .items
        .first()


    PortForwarder(kingdomInternalPod,
      SERVER_PORT
    ).use { internalForward ->
      val internalAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { internalForward.start() }
      logger.info { "=========================================== GET internal channel target" }
      val internalChannel =
        buildMutualTlsChannel(internalAddress.toTarget(), KINGDOM_SIGNING_CERTS)
      PortForwarder(kingdomPublicPod, SERVER_PORT)
        .use { publicForward ->
          val publicAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { publicForward.start() }
          logger.info { "=========================================== GET public channel target" }
          val publicChannel =
            buildMutualTlsChannel(
              publicAddress.toTarget(),
              KINGDOM_SIGNING_CERTS
            )
          val panelMatchResourceSetup =
            PanelMatchResourceSetup(
              DataProvidersGrpcKt.DataProvidersCoroutineStub(internalChannel),
              ModelProvidersGrpcKt.ModelProvidersCoroutineStub(internalChannel),
              RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub(internalChannel),
              outputDir
            )
          val panelMatchResourceKey = withContext(Dispatchers.IO) {
            panelMatchResourceSetup
              .process(
                dataProviderContent,
                EXCHANGE_DATE.toProtoDate(),
                workflow,
                SCHEDULE,
                API_VERSION
              )
          }
          exchangeKey =
            ExchangeKey(
              recurringExchangeId = panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
              exchangeId = EXCHANGE_DATE.toString()
            )
          val sharedStorageForwarder = PortForwarder(sharedStoragePod, SERVER_PORT)
          portForwarders.add(sharedStorageForwarder)
          val sharedStorageAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { sharedStorageForwarder.start() }
          // Setup data provider resources
          val dpStorageForwarder = PortForwarder(dpPrivateStoragePod, SERVER_PORT)
          portForwarders.add(dpStorageForwarder)
          val dpStorageAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { dpStorageForwarder.start() }
          val dpStorageChannel: Channel =
            buildMutualTlsChannel(dpStorageAddress.toTarget(), KINGDOM_SIGNING_CERTS)
              .also { channels.add(it) }
              .withDefaultDeadline(DEFAULT_RPC_DEADLINE)
          val dpForwardedStorage = ForwardedStorageClient(
            ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub(dpStorageChannel)
          )
          val dataProviderDefaults by lazy {
            DaemonStorageClientDefaults(
              dpForwardedStorage,
              "",
              FakeTinkKeyStorageProvider()
            )
          }
          dataProviderDefaults.validExchangeWorkflows.put(
            panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
            workflow.toByteString()
          )
          dataProviderDefaults.rootCertificates.put(
            panelMatchResourceKey.dataProviderKey.toName(),
            TestCertificateManager.CERTIFICATE.encoded.toByteString()
          )
          val dpPrivateForwarderStorage = forwardedStorage {
            target = dpStorageAddress.toTarget()
            certCollectionPath = "/var/run/secrets/files/trusted_certs.pem"
            forwardedStorageCertHost = "localhost"
          }
          val dataProviderPrivateStorageDetails = storageDetails {
            custom = customStorage {
              details = pack(dpPrivateForwarderStorage)
            }
            visibility = StorageDetails.Visibility.PRIVATE
          }
          dataProviderDefaults.privateStorageInfo.put(
            panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
            dataProviderPrivateStorageDetails
          )
          val dpSharedForwarderStorage = forwardedStorage {
            target = sharedStorageAddress.toTarget()
            certCollectionPath = "/var/run/secrets/files/trusted_certs.pem"
            forwardedStorageCertHost = "localhost"
          }
          val dataProviderSharedStorageDetails = storageDetails {
            custom = customStorage {
              details = pack(dpSharedForwarderStorage)
            }
            visibility = StorageDetails.Visibility.SHARED
          }
          dataProviderDefaults.sharedStorageInfo.put(
            panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
            dataProviderSharedStorageDetails
          )
          for ((blobKey, value) in initialDataProviderInputs) {
            dpForwardedStorage.writeBlob(blobKey, value)
          }
          logger.info { "------------------------------------- dp setup completed" }
          // Setup model provider resources
          val mpStorageForwarder = PortForwarder(mpPrivateStoragePod, SERVER_PORT)
          portForwarders.add(mpStorageForwarder)
          val mpStorageAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { mpStorageForwarder.start() }
          val mpStorageChannel: Channel =
            buildMutualTlsChannel(mpStorageAddress.toTarget(), KINGDOM_SIGNING_CERTS)
              .also { channels.add(it) }
              .withDefaultDeadline(DEFAULT_RPC_DEADLINE)
          val mpForwardedStorage = ForwardedStorageClient(
            ForwardedStorageGrpcKt.ForwardedStorageCoroutineStub(mpStorageChannel)
          )
          val modelProviderDefaults by lazy {
            DaemonStorageClientDefaults(
              mpForwardedStorage,
              "",
              FakeTinkKeyStorageProvider()
            )
          }
          modelProviderDefaults.validExchangeWorkflows.put(
            panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
            workflow.toByteString()
          )
          modelProviderDefaults.rootCertificates.put(
            panelMatchResourceKey.modelProviderKey.toName(),
            TestCertificateManager.CERTIFICATE.encoded.toByteString()
          )
          val mpPrivateForwarderStorage = forwardedStorage {
            target = mpStorageAddress.toTarget()
            certCollectionPath = "/var/run/secrets/files/trusted_certs.pem"
            forwardedStorageCertHost = "localhost"
          }
          val modelProviderPrivateStorageDetails = storageDetails {
            custom = customStorage {
              details = pack(mpPrivateForwarderStorage)
            }
            visibility = StorageDetails.Visibility.PRIVATE
          }
          modelProviderDefaults.privateStorageInfo.put(
            panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
            modelProviderPrivateStorageDetails
          )
          val mpSharedForwarderStorage = forwardedStorage {
            target = sharedStorageAddress.toTarget()
            certCollectionPath = "/var/run/secrets/files/trusted_certs.pem"
            forwardedStorageCertHost = "localhost"
          }
          val modelProviderSharedStorageDetails = storageDetails {
            custom = customStorage {
              details = pack(mpSharedForwarderStorage)
            }
            visibility = StorageDetails.Visibility.SHARED
          }
          modelProviderDefaults.sharedStorageInfo.put(
            panelMatchResourceKey.recurringExchangeKey.recurringExchangeId,
            modelProviderSharedStorageDetails
          )
          for ((blobKey, value) in initialDataProviderInputs) {
            mpForwardedStorage.writeBlob(blobKey, value)
          }
          logger.info { "------------------------------------- mp setup completed" }
          loadDpDaemonForPanelMatch(k8sClient, panelMatchResourceKey.dataProviderKey)
          loadMpDaemonForPanelMatch(k8sClient, panelMatchResourceKey.modelProviderKey)
          logger.info { "------------------------------------- daemons created" }
          val exchangeClient = ExchangesGrpcKt.ExchangesCoroutineStub(publicChannel)
          val exchangeStepsClient = ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub(publicChannel)

          logger.info { "------------------------------------- ATTEMPT EXCHANGE CALL" }
          val result = exchangeClient.withPrincipalName(panelMatchResourceKey.dataProviderKey.toName()).getExchange(getExchangeRequest { name = exchangeKey.toName() })
          logger.info { "------------------------------------- TEST SUCCESFUL: ${result.date}" }
          while (!isDone(exchangeClient, exchangeStepsClient)) {
            delay(500)
          }
          publicChannel.shutdown()
          internalChannel.shutdown()
        }
      // Setup share storage
      for (channel in channels) {
        channel.shutdown()
      }
      for (portForwarder in portForwarders) {
        portForwarder.stop()
      }
    }

    logger.info { "------------------------------------- EXIT" }

  }

  private suspend fun loadDpDaemonForPanelMatch(k8sClient: KubernetesClient, dataProviderKey: DataProviderKey) {
    withContext(Dispatchers.IO) {
      val outputDir = tempDir.newFolder("edp_daemon")
      extractTar(
        getRuntimePath(LOCAL_K8S_PANELMATCH_PATH.resolve("edp_daemon.tar"))
          .toFile(), outputDir)
      val configTemplate: File = outputDir.resolve("config.yaml")
      kustomize(
        outputDir.toPath().resolve(LOCAL_K8S_PANELMATCH_PATH).resolve("edp_daemon").toFile(),
        configTemplate
      )

      val configContent =
        configTemplate
          .readText(StandardCharsets.UTF_8)
          .replace("{party_name}", dataProviderKey.dataProviderId)

      //logger.info { "----------------------------- CONTENT!" }
      //logger.info { configContent }

      kubectlApply(configContent, k8sClient)
    }
  }

  private suspend fun loadMpDaemonForPanelMatch(k8sClient: KubernetesClient, modelProviderKey: ModelProviderKey) {
    withContext(Dispatchers.IO) {
      val outputDir = tempDir.newFolder("mp_daemon")
      extractTar(
        getRuntimePath(LOCAL_K8S_PANELMATCH_PATH.resolve("mp_daemon.tar"))
          .toFile(), outputDir)
      val configTemplate: File = outputDir.resolve("config.yaml")
      kustomize(
        outputDir.toPath().resolve(LOCAL_K8S_PANELMATCH_PATH).resolve("mp_daemon").toFile(),
        configTemplate
      )
      val configContent =
        configTemplate
          .readText(StandardCharsets.UTF_8)
          .replace("{party_name}", modelProviderKey.modelProviderId)

      kubectlApply(configContent, k8sClient)
    }
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
      output.toString()
    )
  }

  @Blocking
  private fun kubectlApply(config: String,  k8sClient: KubernetesClient): List<KubernetesObject> {
    return k8sClient
      .kubectlApply(config)
      .onEach { logger.info { "Applied ${it.kind} ${it.metadata.name}" } }
      .toList()
  }

  private suspend fun isDone(exchangesClient: ExchangesGrpcKt.ExchangesCoroutineStub, exchangeStepsClient: ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub): Boolean {
    logger.info { "------------------------------------- IS DONE 1" }
    val request = getExchangeRequest { name = exchangeKey.toName() }
    logger.info { "------------------------------------- IS DONE 2" }
    return try {
      logger.info { "------------------------------------- IS DONE 3 (request): ${request}" }
      val exchange = exchangesClient.getExchange(request)
      logger.info { "------------------------------------- IS DONE 4" }
      val steps = getSteps(exchangeStepsClient)
      //logStepStates(steps)
      //assertNotDeadlocked(steps)

      logger.info("Exchange is in state: ${exchange.state}.")
      exchange.state in TERMINAL_EXCHANGE_STATES
    } catch (e: StatusException) {
      false
    }
  }

  private suspend fun getSteps(exchangeStepsClient: ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub): List<ExchangeStep> {
    return exchangeStepsClient
      .listExchangeSteps(
        listExchangeStepsRequest {
          parent = exchangeKey.toName()
          pageSize = 50
          filter = ListExchangeStepsRequestKt.filter { exchangeDates += EXCHANGE_DATE.toProtoDate() }
        }
      )
      .exchangeStepsList
      .sortedBy { step -> step.stepIndex }
  }

  class LocalSystem(k8sClient: Lazy<KubernetesClient>, tempDir: Lazy<TemporaryFolder>) : TestRule {
    private val portForwarders = mutableListOf<PortForwarder>()
    private val channels = mutableListOf<ManagedChannel>()

    private val k8sClient: KubernetesClient by k8sClient
    private val tempDir: TemporaryFolder by tempDir
    /*override val runId: String by runId
    private lateinit var _testHarness: MeasurementConsumerSimulator
    override val testHarness: MeasurementConsumerSimulator
      get() = _testHarness*/

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            runBlocking {
              withTimeout(Duration.ofMinutes(5)) {
                populateCluster()
              }
            }
            base.evaluate()
          } finally {
            stopPortForwarding()
          }
        }
      }
    }

    private suspend fun populateCluster() {
      val apiClient = k8sClient.apiClient
      apiClient.httpClient =
        apiClient.httpClient.newBuilder().readTimeout(Duration.ofHours(1L)).build()
      Configuration.setDefaultApiClient(apiClient)

      // Wait until default service account has been created. See
      // https://github.com/kubernetes/kubernetes/issues/66689.
      k8sClient.waitForServiceAccount("default", timeout = READY_TIMEOUT)

      loadKingdomForPanelMatch()

      //val edpEntityContent = createEntityContent("edp1")
      //runResourceSetup(edpEntityContent)


    }

    private suspend fun loadKingdomForPanelMatch() {
      withContext(Dispatchers.IO) {
        val outputDir = tempDir.newFolder("kingdom-for-panelmatch-setup")
        extractTar(
          getRuntimePath(LOCAL_K8S_PATH.resolve("kingdom_for_panelmatch_setup.tar"))
            .toFile(), outputDir)
        val config: File = outputDir.resolve("config.yaml")
        kustomize(
          outputDir.toPath().resolve(LOCAL_K8S_PATH).resolve("kingdom_for_panelmatch_setup").toFile(),
          config
        )
        kubectlApply(config)
      }
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
        output.toString()
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

    fun stopPortForwarding() {
      for (channel in channels) {
        channel.shutdown()
      }
      for (portForwarder in portForwarders) {
        portForwarder.close()
      }
    }

  }

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to cluster (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    val logger = Logger.getLogger(this::class.java.name)

    private const val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    private const val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    private const val MP_PRIVATE_STORAGE_DEPLOYMENT_NAME = "mp-private-storage-server-deployment"
    private const val DP_PRIVATE_STORAGE_DEPLOYMENT_NAME = "dp-private-storage-server-deployment"
    private const val SHARED_STORAGE_DEPLOYMENT_NAME = "shared-storage-server-deployment"
    private const val SERVER_PORT: Int = 8443
    private const val API_VERSION = "v2alpha"
    private const val SCHEDULE = "@daily"
    private val typeRegistry =
      TypeRegistry.newBuilder().add(Shared.Parameters.getDescriptor()).build()

    val EXCHANGE_DATE: LocalDate = LocalDate.now()

    private val DEFAULT_RPC_DEADLINE = Duration.ofSeconds(30)
    /*private val DEFAULT_RPC_DEADLINE = Duration.ofSeconds(30)
    private const val NUM_DATA_PROVIDERS = 6
    private val EDP_DISPLAY_NAMES: List<String> = (1..NUM_DATA_PROVIDERS).map { "edp$it" }*/
    private val READY_TIMEOUT = Duration.ofMinutes(2L)

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    private val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")
    private val LOCAL_K8S_PANELMATCH_PATH = Paths.get("src", "main", "k8s", "panelmatch", "local")
    //private val LOCAL_K8S_TESTING_PATH = LOCAL_K8S_PATH.resolve("testing")
    //private val CONFIG_FILES_PATH = LOCAL_K8S_TESTING_PATH.resolve("config_files")
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_local_images")
    private val IMAGE_PANEL_MATCH_PUSHER_PATH = Paths.get("src", "main", "docker", "panel_exchange_client", "push_all_images")
    private val TEST_DATA_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "panelmatch", "testing", "data")

    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val KINGDOM_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles =
        getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("kingdom_tls.pem").toFile()
      val key = secretFiles.resolve("kingdom_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    private val tempDir = TemporaryFolder()

    private val localSystem =
      LocalSystem(
        lazy { KubernetesClient(ClientBuilder.defaultClient()) },
        lazy { tempDir }
      )

    @JvmStatic
    protected fun <T : Message> loadTestData(fileName: String, defaultInstance: T): T {
      val testDataRuntimePath = org.wfanet.measurement.common.getRuntimePath(
        TEST_DATA_PATH
      )!!
      return parseTextProto(testDataRuntimePath.resolve(fileName).toFile(), defaultInstance)
    }

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, Images(), localSystem)

    /*private suspend fun EventGroupsGrpcKt.EventGroupsCoroutineStub.waitForEventGroups(
      measurementConsumer: String,
      apiKey: String
    ) {
      logger.info { "Waiting for all event groups to be created..." }
      while (currentCoroutineContext().isActive) {
        val eventGroups =
          withAuthenticationKey(apiKey)
            .listEventGroups(
              listEventGroupsRequest {
                parent = measurementConsumer
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
    }*/

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }

    private suspend fun KubernetesClient.waitUntilDeploymentReady(name: String): V1Deployment {
      logger.info { "Waiting for Deployment $name to be ready..." }
      return waitUntilDeploymentReady(name, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $name ready" }
      }
    }

    /*private suspend fun KubernetesClient.waitUntilDeploymentReady(
      deployment: V1Deployment
    ): V1Deployment {
      val deploymentName = requireNotNull(deployment.metadata?.name)
      logger.info { "Waiting for Deployment $deploymentName to be ready..." }
      return waitUntilDeploymentReady(deployment, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $deploymentName ready" }
      }
    }*/
  }

}

private fun InetSocketAddress.toTarget(): String {
  AbstractPanelMatchCorrectnessTest.logger.info { "=========================================== TARGET: ${hostName} - ${port}" }
  return "$hostName:$port"
}
