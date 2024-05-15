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
import io.grpc.StatusException
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
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.currentCoroutineContext
import kotlinx.coroutines.isActive
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.time.delay
import kotlinx.coroutines.time.withTimeout
import kotlinx.coroutines.withContext
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
import org.wfanet.measurement.api.v2alpha.ListEventGroupsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.listEventGroupsRequest
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.crypto.tink.TinkPrivateKeyHandle
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.common.k8s.testing.Processes
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.integration.common.ALL_DUCHY_NAMES
import org.wfanet.measurement.integration.common.MC_DISPLAY_NAME
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadTestCertDerFile
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MetadataSyntheticGeneratorEventQuery
import org.wfanet.measurement.loadtest.resourcesetup.DuchyCert
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.ResourceSetup
import org.wfanet.measurement.loadtest.resourcesetup.Resources

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
    val apiKey: String,
    val dataProviders: Map<String, Resources.Resource>,
  ) {
    companion object {
      fun from(resources: Iterable<Resources.Resource>): ResourceInfo {
        var aggregatorCert: String? = null
        var worker1Cert: String? = null
        var worker2Cert: String? = null
        var measurementConsumer: String? = null
        var apiKey: String? = null
        val dataProviders = mutableMapOf<String, Resources.Resource>()

        for (resource in resources) {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
          when (resource.resourceCase) {
            Resources.Resource.ResourceCase.MEASUREMENT_CONSUMER -> {
              measurementConsumer = resource.name
              apiKey = resource.measurementConsumer.apiKey
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
            Resources.Resource.ResourceCase.RESOURCE_NOT_SET -> error("Unhandled type")
          }
        }

        return ResourceInfo(
          requireNotNull(aggregatorCert),
          requireNotNull(worker1Cert),
          requireNotNull(worker2Cert),
          requireNotNull(measurementConsumer),
          requireNotNull(apiKey),
          dataProviders,
        )
      }
    }
  }

  /** [TestRule] which populates a K8s cluster with the components of the CMMS. */
  class LocalMeasurementSystem(
    k8sClient: Lazy<KubernetesClient>,
    tempDir: Lazy<TemporaryFolder>,
    runId: Lazy<String>,
  ) : TestRule, MeasurementSystem {
    private val portForwarders = mutableListOf<PortForwarder>()
    private val channels = mutableListOf<ManagedChannel>()

    private val k8sClient: KubernetesClient by k8sClient
    private val tempDir: TemporaryFolder by tempDir
    override val runId: String by runId

    private lateinit var _testHarness: MeasurementConsumerSimulator
    override val testHarness: MeasurementConsumerSimulator
      get() = _testHarness

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            runBlocking {
              withTimeout(Duration.ofMinutes(5)) {
                val measurementConsumerData = populateCluster()
                _testHarness = createTestHarness(measurementConsumerData)
              }
            }
            base.evaluate()
          } finally {
            stopPortForwarding()
          }
        }
      }
    }

    private suspend fun populateCluster(): MeasurementConsumerData {
      val apiClient = k8sClient.apiClient
      apiClient.httpClient =
        apiClient.httpClient.newBuilder().readTimeout(Duration.ofHours(1L)).build()
      Configuration.setDefaultApiClient(apiClient)

      val duchyCerts =
        ALL_DUCHY_NAMES.map { DuchyCert(it, loadTestCertDerFile("${it}_cs_cert.der")) }
      val edpEntityContents = EDP_DISPLAY_NAMES.map { createEntityContent(it) }
      val measurementConsumerContent =
        withContext(Dispatchers.IO) { createEntityContent(MC_DISPLAY_NAME) }

      // Wait until default service account has been created. See
      // https://github.com/kubernetes/kubernetes/issues/66689.
      k8sClient.waitForServiceAccount("default", timeout = READY_TIMEOUT)

      loadKingdom()
      val resourceSetupOutput =
        runResourceSetup(duchyCerts, edpEntityContents, measurementConsumerContent)
      val resourceInfo = ResourceInfo.from(resourceSetupOutput.resources)
      loadFullCmms(resourceInfo, resourceSetupOutput.akidPrincipalMap)

      val encryptionPrivateKey: TinkPrivateKeyHandle =
        withContext(Dispatchers.IO) {
          loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
        }
      return MeasurementConsumerData(
        resourceInfo.measurementConsumer,
        measurementConsumerContent.signingKey,
        encryptionPrivateKey,
        resourceInfo.apiKey,
      )
    }

    private suspend fun createTestHarness(
      measurementConsumerData: MeasurementConsumerData
    ): MeasurementConsumerSimulator {
      val kingdomPublicPod: V1Pod = getPod(KINGDOM_PUBLIC_DEPLOYMENT_NAME)

      val publicApiForwarder = PortForwarder(kingdomPublicPod, SERVER_PORT)
      portForwarders.add(publicApiForwarder)

      val publicApiAddress: InetSocketAddress =
        withContext(Dispatchers.IO) { publicApiForwarder.start() }
      val publicApiChannel: Channel =
        buildMutualTlsChannel(publicApiAddress.toTarget(), MEASUREMENT_CONSUMER_SIGNING_CERTS)
          .also { channels.add(it) }
          .withDefaultDeadline(DEFAULT_RPC_DEADLINE)
      val eventGroupsClient = EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel)

      return MeasurementConsumerSimulator(
          measurementConsumerData,
          OUTPUT_DP_PARAMS,
          DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
          eventGroupsClient,
          MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
          MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
          CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
          MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
          MetadataSyntheticGeneratorEventQuery(
            SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_SMALL,
            MC_ENCRYPTION_PRIVATE_KEY,
          ),
          ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
        )
        .also {
          try {
            eventGroupsClient.waitForEventGroups(
              measurementConsumerData.name,
              measurementConsumerData.apiAuthenticationKey,
            )
          } catch (e: StatusException) {
            throw Exception("Error waiting for EventGroups", e)
          }
        }
    }

    fun stopPortForwarding() {
      for (channel in channels) {
        channel.shutdown()
      }
      for (portForwarder in portForwarders) {
        portForwarder.close()
      }
    }

    private suspend fun loadFullCmms(resourceInfo: ResourceInfo, akidPrincipalMap: File) {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("cmms")
          extractTar(getRuntimePath(LOCAL_K8S_TESTING_PATH.resolve("cmms.tar")).toFile(), outputDir)

          val configFilesDir = outputDir.toPath().resolve(CONFIG_FILES_PATH).toFile()
          logger.info("Copying $akidPrincipalMap to $CONFIG_FILES_PATH")
          akidPrincipalMap.copyTo(configFilesDir.resolve(akidPrincipalMap.name))

          val configTemplate: File = outputDir.resolve("config.yaml")
          kustomize(
            outputDir.toPath().resolve(LOCAL_K8S_TESTING_PATH).resolve("cmms").toFile(),
            configTemplate,
          )

          val configContent =
            configTemplate
              .readText(StandardCharsets.UTF_8)
              .replace("{aggregator_cert_name}", resourceInfo.aggregatorCert)
              .replace("{worker1_cert_name}", resourceInfo.worker1Cert)
              .replace("{worker2_cert_name}", resourceInfo.worker2Cert)
              .replace("{mc_name}", resourceInfo.measurementConsumer)
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
      edpEntityContents: List<EntityContent>,
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
                  org.wfanet.measurement.api.v2alpha.AccountsGrpcKt.AccountsCoroutineStub(
                    publicChannel
                  ),
                  ApiKeysGrpcKt.ApiKeysCoroutineStub(publicChannel),
                  org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt
                    .CertificatesCoroutineStub(internalChannel),
                  MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicChannel),
                  runId,
                  outputDir = outputDir,
                  requiredDuchies = listOf("aggregator", "worker1", "worker2"),
                )
              withContext(Dispatchers.IO) {
                resourceSetup
                  .process(edpEntityContents, measurementConsumerContent, duchyCerts)
                  .also { publicChannel.shutdown() }
              }
            }
            .also { internalChannel.shutdown() }
        }

      return ResourceSetupOutput(
        resources,
        outputDir.resolve(ResourceSetup.AKID_PRINCIPAL_MAP_FILE),
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
    )
  }

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to cluster (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    private val logger = Logger.getLogger(this::class.java.name)

    private const val SERVER_PORT: Int = 8443
    private val DEFAULT_RPC_DEADLINE = Duration.ofSeconds(30)
    private const val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    private const val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    private const val NUM_DATA_PROVIDERS = 6
    private val EDP_DISPLAY_NAMES: List<String> = (1..NUM_DATA_PROVIDERS).map { "edp$it" }
    private val READY_TIMEOUT = Duration.ofMinutes(2L)

    private val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")
    private val LOCAL_K8S_TESTING_PATH = LOCAL_K8S_PATH.resolve("testing")
    private val CONFIG_FILES_PATH = LOCAL_K8S_TESTING_PATH.resolve("config_files")
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_local_images.bash")

    private val tempDir = TemporaryFolder()

    private val measurementSystem =
      LocalMeasurementSystem(
        lazy { KubernetesClient(ClientBuilder.defaultClient()) },
        lazy { tempDir },
        lazy { UUID.randomUUID().toString() },
      )

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, Images(), measurementSystem)

    private suspend fun EventGroupsGrpcKt.EventGroupsCoroutineStub.waitForEventGroups(
      measurementConsumer: String,
      apiKey: String,
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
    }
  }
}

private fun InetSocketAddress.toTarget(): String {
  return "$hostName:$port"
}
