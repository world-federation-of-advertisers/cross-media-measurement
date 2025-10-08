/*
 * Copyright 2025 The Cross-Media Measurement Authors
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
import org.wfanet.measurement.api.v2alpha.EventGroup
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator

abstract class AbstractEdpAggregatorCorrectnessTest(
  private val measurementSystem: MeasurementSystem
) {

  private val mcSimulator: MeasurementConsumerSimulator
    get() = measurementSystem.mcSimulator

  protected abstract val EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS:
    ((EventGroup) -> Boolean)?
  protected abstract val EVENT_GROUP_FILTERING_LAMBDA_HMSS: ((EventGroup) -> Boolean)?

  @Test
  fun `create a Hmss reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachOnly(
        "1231",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_HMSS,
      )
    }

  @Test
  fun `create a Hmss RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a reach and frequency measurement and verify its result.
      mcSimulator.testReachAndFrequency(
        "1232",
        DataProviderKt.capabilities { honestMajorityShareShuffleSupported = true },
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_HMSS,
      )
    }

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachAndFrequency(
        "1233",
        1,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a direct reach-only measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachOnly(
        "1234",
        1,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create incremental direct reach only measurements in same report and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create N incremental direct reach and frequency measurements and
      // verify its result.
      mcSimulator.testDirectReachOnly(
        runId = "1235",
        numMeasurements = 3,
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  @Test
  fun `create a impression measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create an impression measurement and verify its
      // result.
      mcSimulator.testImpression(
        "1236",
        eventGroupFilter = EVENT_GROUP_FILTERING_LAMBDA_DIRECT_MEASUREMENTS,
      )
    }

  interface MeasurementSystem {
    val runId: String
    val mcSimulator: MeasurementConsumerSimulator
  }

  companion object {
    private const val MC_ENCRYPTION_PRIVATE_KEY_NAME = "mc_enc_private.tink"
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"

    val OUTPUT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 0.1
      delta = 0.000001
    }

    val MC_ENCRYPTION_PRIVATE_KEY: PrivateKeyHandle by lazy {
      loadEncryptionPrivateKey(MC_ENCRYPTION_PRIVATE_KEY_NAME)
    }

    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val MC_SIGNING_KEY: SigningKeyHandle by lazy {
      loadSigningKey(MC_CS_CERT_DER_NAME, MC_CS_PRIVATE_KEY_DER_NAME)
    }

    val MEASUREMENT_CONSUMER_SIGNING_CERTS: SigningCerts by lazy {
      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val trustedCerts = secretFiles.resolve("mc_trusted_certs.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()
      SigningCerts.fromPemFiles(cert, key, trustedCerts)
    }

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }
  }
}
