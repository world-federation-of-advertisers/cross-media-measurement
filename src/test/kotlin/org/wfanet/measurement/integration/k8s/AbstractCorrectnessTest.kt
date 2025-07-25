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

import java.nio.file.Path
import java.nio.file.Paths
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.wfanet.measurement.api.v2alpha.DataProviderKt
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.integration.common.EventQuery
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator

/** Test for correctness of the CMMS on Kubernetes. */
abstract class AbstractCorrectnessTest(private val measurementSystem: MeasurementSystem) {
  private val runId: String
    get() = measurementSystem.runId

  private val testHarness: EventQueryMeasurementConsumerSimulator
    get() = measurementSystem.testHarness

  private val reportingTestHarness: ReportingUserSimulator
    get() = measurementSystem.reportingTestHarness

  @Test(timeout = 1 * 60 * 1000)
  fun `impression measurement completes with expected result`() = runBlocking {
    testHarness.testImpression("$runId-impression")
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `duration measurement completes with expected result`() = runBlocking {
    testHarness.testDuration("$runId-duration")
  }

  @Test
  fun `HMSS reach and frequency measurement completes with expected result`() = runBlocking {
    testHarness.testReachAndFrequency(
      "$runId-hmss-reach-and-freq",
      DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
    )
  }

  @Test
  fun `LLv2 reach and frequency measurement completes with expected result`() = runBlocking {
    testHarness.testReachAndFrequency(
      "$runId-llv2-reach-and-freq",
      DataProviderKt.capabilities { honestMajorityShareShuffleSupported = false },
    )
  }

  @Test(timeout = 1 * 60 * 1000)
  fun `report can be created`() = runBlocking {
    reportingTestHarness.testCreateReport("$runId-test-report")
  }

  interface MeasurementSystem {
    val runId: String
    val testHarness: EventQueryMeasurementConsumerSimulator
    val reportingTestHarness: ReportingUserSimulator

    /**
     * Synthetic population spec.
     *
     * This must match the spec used by EDP simulators.
     */
    val syntheticPopulationSpec: SyntheticPopulationSpec

    /**
     * Synthetic event group specs.
     *
     * These must match the specs used by the EDP simulators, in order.
     */
    val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec>

    fun buildEventQuery(dataProviderNames: Iterable<String>) =
      EventQuery(syntheticPopulationSpec, syntheticEventGroupSpecs, dataProviderNames)
  }

  companion object {
    private const val MC_ENCRYPTION_PRIVATE_KEY_NAME = "mc_enc_private.tink"
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")
    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.1
      delta = 0.000001
    }

    val KINGDOM_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("kingdom_root.pem").toFile()
      val cert = secretFiles.resolve("kingdom_tls.pem").toFile()
      val key = secretFiles.resolve("kingdom_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val MEASUREMENT_CONSUMER_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("mc_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val REPORTING_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    val MC_ENCRYPTION_PRIVATE_KEY: PrivateKeyHandle by lazy {
      loadEncryptionPrivateKey(MC_ENCRYPTION_PRIVATE_KEY_NAME)
    }

    val MC_SIGNING_KEY: SigningKeyHandle by lazy {
      loadSigningKey(MC_CS_CERT_DER_NAME, MC_CS_PRIVATE_KEY_DER_NAME)
    }

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }
  }
}
