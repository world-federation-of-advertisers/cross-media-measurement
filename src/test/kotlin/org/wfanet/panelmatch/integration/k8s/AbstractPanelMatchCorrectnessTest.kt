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
import com.google.protobuf.Message
import com.google.protobuf.TypeRegistry
import io.kubernetes.client.util.ClientBuilder
import java.net.InetSocketAddress
import java.nio.file.Path
import java.nio.file.Paths
import java.security.Security
import java.time.Duration
import java.time.LocalDate
import java.util.logging.Logger
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.ExchangeWorkflow
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.k8s.KubernetesClientImpl
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.loadtest.panelmatch.PanelMatchSimulator

abstract class AbstractPanelMatchCorrectnessTest(private val localSystem: PanelMatchSystem) {

  private val runId: String
    get() = localSystem.runId

  private val testHarness: PanelMatchSimulator
    get() = localSystem.testHarness

  @Test(timeout = 3 * 60 * 1000)
  fun `Double blind workflow completes with expected result`() = runBlocking {
    logger.info { "Run Double Blind workflow test" }
    val workflow: ExchangeWorkflow by lazy {
      testHarness.populateWorkflow(
        loadTestData(
          "double_blind_exchange_workflow.textproto",
          ExchangeWorkflow.getDefaultInstance(),
        )
      )
    }
    testHarness.executeDoubleBlindExchangeWorkflow(workflow)
  }

  @Test(timeout = 3 * 60 * 1000)
  fun `Mini workflow completes with expected result`() = runBlocking {
    logger.info { "Run Mini workflow test" }
    val workflow: ExchangeWorkflow by lazy {
      testHarness.populateWorkflow(
        loadTestData("mini_exchange_workflow.textproto", ExchangeWorkflow.getDefaultInstance())
      )
    }
    testHarness.executeMiniWorkflow(workflow)
  }

  /*@Test(timeout = 25 * 60 * 1000)
  fun `Full test with preprocessing workflow completes with expected result`() = runBlocking {
    logger.info { "Run Full test with preprocessing workflow test" }
    val workflow: ExchangeWorkflow by lazy {
      loadTestData("full_with_preprocessing.textproto", ExchangeWorkflow.getDefaultInstance())
        .copy { firstExchangeDate = EXCHANGE_DATE.toProtoDate() }
    }
    testHarness.executeFullWithPreprocessingWorkflow(workflow)
  }*/

  interface PanelMatchSystem {
    val runId: String
    val testHarness: PanelMatchSimulator
  }

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to cluster (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    val k8sClient = KubernetesClientImpl(ClientBuilder.defaultClient())
    @JvmStatic protected val READY_TIMEOUT = Duration.ofMinutes(2L)
    private val TEST_DATA_PATH =
      Paths.get("wfa_measurement_system", "src", "main", "k8s", "panelmatch", "testing", "data")

    @JvmStatic protected val logger = Logger.getLogger(this::class.java.name)

    @JvmStatic protected val SERVER_PORT: Int = 8443
    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")

    @JvmStatic protected val EXCHANGE_DATE: LocalDate = LocalDate.now()

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

    private val typeRegistry =
      TypeRegistry.newBuilder().add(Shared.Parameters.getDescriptor()).build()

    @JvmStatic
    private fun <T : Message> loadTestData(fileName: String, defaultInstance: T): T {
      val testDataRuntimePath = org.wfanet.measurement.common.getRuntimePath(TEST_DATA_PATH)!!
      return parseTextProto(
        testDataRuntimePath.resolve(fileName).toFile(),
        defaultInstance,
        typeRegistry,
      )
    }

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }
  }
}

fun InetSocketAddress.toTarget(): String {
  return "$hostName:$port"
}
