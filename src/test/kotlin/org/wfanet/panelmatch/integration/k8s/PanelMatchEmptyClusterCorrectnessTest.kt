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

package org.wfanet.panelmatch.integration.k8s

import io.grpc.ManagedChannel
import io.kubernetes.client.common.KubernetesObject
import io.kubernetes.client.openapi.Configuration
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.util.ClientBuilder
import java.io.File
import java.net.InetSocketAddress
import java.nio.charset.StandardCharsets
import java.nio.file.Paths
import java.security.Security
import java.time.Duration
import java.util.*
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
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
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.k8s.testing.PortForwarder
import org.wfanet.measurement.common.k8s.testing.Processes
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.integration.common.createEntityContent
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt
import org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup
import org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup.Companion.AKID_PRINCIPAL_MAP_FILE
import org.wfanet.measurement.loadtest.resourcesetup.EntityContent
import org.wfanet.measurement.loadtest.resourcesetup.Resources

@RunWith(JUnit4::class)
class PanelMatchEmptyClusterCorrectnessTest : PanelMatchAbstractCorrectnessTest() {

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
      val pusherRuntimePath =
        getRuntimePath(IMAGE_PUSHER_PATH)
      Processes.runCommand(pusherRuntimePath.toString())
    }
  }

  private data class ResourceInfo(
    val dataProviders: Map<String, String>
  ) {
    companion object {
      fun from(resources: Iterable<Resources.Resource>): ResourceInfo {
        val dataProviders = mutableMapOf<String, String>()

        for (resource in resources) {
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields cannot be null.
          when (resource.resourceCase) {
            Resources.Resource.ResourceCase.DATA_PROVIDER -> {
              val displayName = resource.dataProvider.displayName
              require(dataProviders.putIfAbsent(displayName, resource.name) == null) {
                "Entry already exists for DataProvider $displayName"
              }
            }
            Resources.Resource.ResourceCase.RESOURCE_NOT_SET -> error("Unhandled type")
          }
        }

        return ResourceInfo(
          dataProviders
        )
      }
    }
  }

  /** [TestRule] which populates a K8s cluster with the components of the CMMS. */
  class LocalMeasurementSystem(k8sClient: Lazy<KubernetesClient>, tempDir: Lazy<TemporaryFolder>) : TestRule {
    private val portForwarders = mutableListOf<PortForwarder>()
    private val channels = mutableListOf<ManagedChannel>()

    private val k8sClient: KubernetesClient by k8sClient

    private val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")

    private val tempDir: TemporaryFolder by tempDir

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

      val edpEntityContent = createEntityContent(EDP_DISPLAY_NAME)

      // Wait until default service account has been created. See
      // https://github.com/kubernetes/kubernetes/issues/66689.
      k8sClient.waitForServiceAccount("default", timeout = READY_TIMEOUT)

      loadKingdom()
      val resourceSetupOutput =
        runResourceSetup(edpEntityContent)
      val resourceInfo = ResourceInfo.from(resourceSetupOutput.resources)
      loadCmmsForPanelMatch(resourceInfo, resourceSetupOutput.akidPrincipalMap)

      /*val encryptionPrivateKey: TinkPrivateKeyHandle =
        withContext(Dispatchers.IO) {
          loadEncryptionPrivateKey("${MC_DISPLAY_NAME}_enc_private.tink")
        }
      return MeasurementConsumerData(
        resourceInfo.measurementConsumer,
        measurementConsumerContent.signingKey,
        encryptionPrivateKey,
        resourceInfo.apiKey
      )*/
    }

    private suspend fun loadCmmsForPanelMatch(resourceInfo: ResourceInfo, akidPrincipalMap: File) {
      val appliedObjects: List<KubernetesObject> =
        withContext(Dispatchers.IO) {
          val outputDir = tempDir.newFolder("panel_match")
          extractTar(
            getRuntimePath(
              LOCAL_K8S_TESTING_PATH.resolve(
                "panel_match.tar"
              )
            ).toFile(), outputDir)

          val configFilesDir = outputDir.toPath().resolve(CONFIG_FILES_PATH).toFile()
          logger.info("Copying $akidPrincipalMap to ${CONFIG_FILES_PATH}")
          akidPrincipalMap.copyTo(configFilesDir.resolve(akidPrincipalMap.name))
          val configTemplate: File = outputDir.resolve("config.yaml")

          kustomize(
            outputDir.toPath().resolve(LOCAL_K8S_TESTING_PATH).resolve("panel_match").toFile(),
            configTemplate
          )
          println("------------------------------------------------- KUSTOMIZE")

          val configContent =
            configTemplate
              .readText(StandardCharsets.UTF_8)
              .let {
                var config = it
                for ((displayName, resourceName) in resourceInfo.dataProviders) {
                  config = config.replace("{${displayName}_name}", resourceName)
                }
                config
              }

          kubectlApply(configContent)
        }

      appliedObjects.filterIsInstance(V1Deployment::class.java).forEach {
        k8sClient.waitUntilDeploymentReady(it)
      }
    }

    private suspend fun loadKingdom() {
      withContext(Dispatchers.IO) {
        val outputDir = tempDir.newFolder("kingdom-setup")
        extractTar(
          getRuntimePath(
            LOCAL_K8S_PATH.resolve(
              "kingdom_setup.tar"
            )
          ).toFile(), outputDir)
        val config: File = outputDir.resolve("config.yaml")
        kustomize(
          outputDir.toPath().resolve(LOCAL_K8S_PATH).resolve("kingdom_setup").toFile(),
          config
        )

        kubectlApply(config)
      }
    }

    private suspend fun runResourceSetup(edpEntityContent: EntityContent): ResourceSetupOutput {
      val outputDir = withContext(Dispatchers.IO) { tempDir.newFolder("resource-setup") }

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

      val resources =
        PortForwarder(kingdomInternalPod,
          SERVER_PORT
        ).use { internalForward ->
          val internalAddress: InetSocketAddress =
            withContext(Dispatchers.IO) { internalForward.start() }
          val internalChannel =
            buildMutualTlsChannel(internalAddress.toTarget(),
              KINGDOM_SIGNING_CERTS
            )
          PortForwarder(kingdomPublicPod, SERVER_PORT)
            .use { publicForward ->
              val publicAddress: InetSocketAddress =
                withContext(Dispatchers.IO) { publicForward.start() }
              val publicChannel =
                buildMutualTlsChannel(publicAddress.toTarget(),
                  KINGDOM_SIGNING_CERTS
                )
              val panelMatchResourceSetup =
                PanelMatchResourceSetup(
                  DataProvidersGrpcKt
                    .DataProvidersCoroutineStub(internalChannel),
                  ModelProvidersGrpcKt
                    .ModelProvidersCoroutineStub(internalChannel),
                  RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub(publicChannel),
                  outputDir = outputDir
                )
              withContext(Dispatchers.IO) {
                panelMatchResourceSetup
                  .process(edpEntityContent)
                  .also { publicChannel.shutdown() }
              }
            }
            .also { internalChannel.shutdown() }
        }

      return ResourceSetupOutput(
        resources,
        outputDir.resolve(AKID_PRINCIPAL_MAP_FILE)
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

    data class ResourceSetupOutput(
      val resources: List<Resources.Resource>,
      val akidPrincipalMap: File
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
    private const val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    private const val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    private val EDP_DISPLAY_NAME: String = "edp1"
    private val READY_TIMEOUT = Duration.ofMinutes(2L)

    private val LOCAL_K8S_PATH = Paths.get("src", "main", "k8s", "local")
    private val LOCAL_K8S_TESTING_PATH = LOCAL_K8S_PATH.resolve("testing")
    private val CONFIG_FILES_PATH = LOCAL_K8S_TESTING_PATH.resolve("config_files")
    private val IMAGE_PUSHER_PATH = Paths.get("src", "main", "docker", "push_all_local_images")

    private val tempDir = TemporaryFolder()

    private val measurementSystem =
      LocalMeasurementSystem(
        lazy { KubernetesClient(ClientBuilder.defaultClient()) },
        lazy { tempDir }
      )

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, Images(), measurementSystem)

    private suspend fun KubernetesClient.waitUntilDeploymentReady(name: String): V1Deployment {
      logger.info { "Waiting for Deployment $name to be ready..." }
      return waitUntilDeploymentReady(name, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $name ready" }
      }
    }

    private suspend fun KubernetesClient.waitUntilDeploymentReady(
      deployment: V1Deployment
    ): V1Deployment {
      val deploymentName = requireNotNull(deployment.metadata?.name)
      logger.info { "Waiting for Deployment $deploymentName to be ready..." }
      return waitUntilDeploymentReady(deployment, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $deploymentName ready" }
      }
    }

  }

}

private fun InetSocketAddress.toTarget(): String {
  return "$hostName:$port"
}
