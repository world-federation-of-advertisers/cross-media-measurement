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

import com.google.protobuf.Message
import io.kubernetes.client.openapi.models.V1Deployment
import io.kubernetes.client.openapi.models.V1Pod
import io.kubernetes.client.util.ClientBuilder
import java.net.InetSocketAddress
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
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.k8s.KubernetesClient
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.loadtest.panelmatch.PanelMatchSimulator

abstract class AbstractPanelMatchCorrectnessTest(private val localSystem: PanelMatchSystem) {

  /*private val channels = mutableListOf<ManagedChannel>()
  private val portForwarders = mutableListOf<PortForwarder>()*/
  //private lateinit var exchangeKey: ExchangeKey
  //protected abstract val initialDataProviderInputs: Map<String, ByteString>
  //protected abstract val initialModelProviderInputs: Map<String, ByteString>
  // TODO(@marcopremier): Add abstract expected output here to be checked during test validation
  //protected abstract val workflow: ExchangeWorkflow

  // private val TERMINAL_STEP_STATES = setOf(ExchangeStep.State.SUCCEEDED, ExchangeStep.State.FAILED)
  /*private val READY_STEP_STATES =
    setOf(
      ExchangeStep.State.IN_PROGRESS,
      ExchangeStep.State.READY,
      ExchangeStep.State.READY_FOR_RETRY
    )
  private val TERMINAL_EXCHANGE_STATES = setOf(Exchange.State.SUCCEEDED, Exchange.State.FAILED)*/

  //private val API_VERSION = "v2alpha"
  //private val SCHEDULE = "@daily"

  //val EXCHANGE_DATE: LocalDate = LocalDate.now()

  //private val DEFAULT_RPC_DEADLINE = Duration.ofSeconds(30)

  private val runId: String
    get() = localSystem.runId

  private val testHarness: PanelMatchSimulator
    get() = localSystem.testHarness

  @Test(timeout = 3 * 60 * 1000)
  fun `Double blind workflow completes with expected result`() = runBlocking {
    val workflow: ExchangeWorkflow by lazy {
      loadTestData("double_blind_exchange_workflow.textproto", ExchangeWorkflow.getDefaultInstance())
        .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
    }
    logger.info { "-------------------------------------- RUN TEST 1 !!!" }
    testHarness.executeDoubleBlindExchangeWorkflow(workflow)
    logger.info { "-------------------------------------- RUN TEST 2 !!!" }
  }

  interface PanelMatchSystem {
    val runId: String
    val testHarness: PanelMatchSimulator
  }


      // TODO(@marcopremier): Make dpForwardedStorage and mpForwardedStorage as abstract val

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to cluster (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    val k8sClient = KubernetesClient(ClientBuilder.defaultClient())
    @JvmStatic
    protected val READY_TIMEOUT = Duration.ofMinutes(2L)
    private val TEST_DATA_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "panelmatch", "testing", "data")

    @JvmStatic
    protected val logger = Logger.getLogger(this::class.java.name)

    @JvmStatic
    protected val SERVER_PORT: Int = 8443
    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")

    @JvmStatic
    protected val KINGDOM_INTERNAL_DEPLOYMENT_NAME = "gcp-kingdom-data-server-deployment"
    @JvmStatic
    protected val KINGDOM_PUBLIC_DEPLOYMENT_NAME = "v2alpha-public-api-server-deployment"
    @JvmStatic
    protected val MP_PRIVATE_STORAGE_DEPLOYMENT_NAME = "mp-private-storage-server-deployment"
    @JvmStatic
    protected val DP_PRIVATE_STORAGE_DEPLOYMENT_NAME = "dp-private-storage-server-deployment"
    @JvmStatic
    protected val SHARED_STORAGE_DEPLOYMENT_NAME = "shared-storage-server-deployment"
    @JvmStatic
    protected val EXCHANGE_DATE: LocalDate = LocalDate.now()

    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")
    val PANELMATCH_SECRET_FILES_PATH: Path =
      Paths.get("src", "main", "k8s", "panelmatch", "testing", "secretfiles")

    val KINGDOM_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("kingdom_tls.pem").toFile()
      val key = secretFiles.resolve("kingdom_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val EDP_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(PANELMATCH_SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("edp_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("edp1_tls.pem").toFile()
      val key = secretFiles.resolve("edp1_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val MP_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(PANELMATCH_SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("mp_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mp1_tls.pem").toFile()
      val key = secretFiles.resolve("mp1_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    @JvmStatic
    private fun <T : Message> loadTestData(fileName: String, defaultInstance: T): T {
      val testDataRuntimePath = org.wfanet.measurement.common.getRuntimePath(TEST_DATA_PATH)!!
      return parseTextProto(testDataRuntimePath.resolve(fileName).toFile(), defaultInstance)
    }

    protected suspend fun KubernetesClient.waitUntilDeploymentReady(name: String): V1Deployment {
      logger.info { "Waiting for Deployment $name to be ready..." }
      return waitUntilDeploymentReady(name, timeout = READY_TIMEOUT).also {
        logger.info { "Deployment $name ready" }
      }
    }

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }

    @JvmStatic
    protected suspend fun getPod(deploymentName: String): V1Pod {
      return k8sClient
        .listPodsByMatchLabels(k8sClient.waitUntilDeploymentReady(deploymentName))
        .items
        .first()
    }
  }
}

fun InetSocketAddress.toTarget(): String {
  return "$hostName:$port"
}
