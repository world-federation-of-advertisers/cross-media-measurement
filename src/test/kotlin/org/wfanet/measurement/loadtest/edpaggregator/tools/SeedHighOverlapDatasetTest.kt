// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import java.io.File
import java.time.Duration
import java.util.concurrent.TimeUnit
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SeedHighOverlapDatasetTest {
  @get:Rule val tempFolder = TemporaryFolder()

  @Test
  fun `main reports missing EDP KMS configuration before accessing cloud credentials`() {
    val outputFile = tempFolder.newFile("output.txt")
    val processBuilder =
      ProcessBuilder(
          File(System.getProperty("java.home"), "bin/java").path,
          "-Xmx256m",
          "-XX:ActiveProcessorCount=1",
          "-cp",
          System.getProperty("java.class.path"),
          "org.wfanet.measurement.loadtest.edpaggregator.tools.SeedHighOverlapDatasetKt",
          "--kingdom-public-api-target=localhost:1",
          "--kingdom-trusted-certs-file=unused.pem",
          "--pdp-tls-cert-file=unused.pem",
          "--pdp-tls-key-file=unused.key",
          "--model-provider-tls-cert-file=unused.pem",
          "--model-provider-tls-key-file=unused.key",
          "--population-data-provider=dataProviders/test-pdp",
          "--model-line=modelProviders/test-mp/modelSuites/test-suite/modelLines/test-line",
          "--measurement-consumer=measurementConsumers/test-mc",
          "--storage-bucket=test-bucket",
          "--google-project-id=test-project",
          "--google-cloud-kms-kek-uri=gcp-kms://projects/test-project/locations/global/keyRings/test/cryptoKeys/test",
        )
        .redirectErrorStream(true)
        .redirectOutput(outputFile)
    processBuilder.environment()["GOOGLE_APPLICATION_CREDENTIALS"] =
      tempFolder.root.resolve("missing-credentials.json").path
    val process = processBuilder.start()
    try {
      assertThat(process.waitFor(Duration.ofSeconds(30).toMillis(), TimeUnit.MILLISECONDS)).isTrue()
      assertThat(process.exitValue()).isNotEqualTo(0)
      assertThat(outputFile.readText())
        .contains("No KMS configuration for high overlap EDP(s) [edpa_meta]")
    } finally {
      process.destroyForcibly()
    }
  }
}
