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
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.integration.common.loadEncryptionPrivateKey
import org.wfanet.measurement.integration.common.loadSigningKey
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerSimulator
import org.wfanet.measurement.common.crypto.PrivateKeyHandle
import org.wfanet.measurement.common.crypto.SigningCerts
import java.net.URI
import java.net.http.HttpClient
import java.net.http.HttpRequest
import java.net.http.HttpResponse
import java.time.Duration

abstract class AbstractEdpAggregatorCorrectnessTest(private val measurementSystem: MeasurementSystem) {

  private val mcSimulator: MeasurementConsumerSimulator
    get() = measurementSystem.mcSimulator

  @Test
  fun `create a direct RF measurement and check the result is equal to the expected result`() =
    runBlocking {
      // Use frontend simulator to create a direct reach and frequency measurement and verify its
      // result.
      mcSimulator.testDirectReachAndFrequency("1234", 1)
    }

  interface MeasurementSystem {
    val runId: String
    val mcSimulator: MeasurementConsumerSimulator
  }

  companion object {
    
    private const val MC_CS_CERT_DER_NAME = "mc_cs_cert.der"
    private const val MC_CS_PRIVATE_KEY_DER_NAME = "mc_cs_private.der"

    val SECRET_FILES_PATH: Path = Paths.get("src", "main", "k8s", "testing", "secretfiles")

    val MC_SIGNING_KEY: SigningKeyHandle by lazy {
      loadSigningKey(MC_CS_CERT_DER_NAME, MC_CS_PRIVATE_KEY_DER_NAME)
    }

    private val WORKSPACE_PATH: Path = Paths.get("wfa_measurement_system")

    fun getRuntimePath(workspaceRelativePath: Path): Path {
      return checkNotNull(
        org.wfanet.measurement.common.getRuntimePath(WORKSPACE_PATH.resolve(workspaceRelativePath))
      )
    }

  }

}
