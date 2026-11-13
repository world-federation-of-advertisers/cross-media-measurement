/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.devtools.build.runfiles.Runfiles
import java.io.File
import java.nio.file.Paths
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.wfanet.measurement.common.getRuntimePath

class ValidateDataWatcherConfigTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `main accepts a route without application parameters`() {
    assertSuccess(
      execute(
        """
        watched_paths {
          identifier: "requisitions"
          control_plane_queue_sink {}
        }
        """
          .trimIndent()
      )
    )
  }

  @Test
  fun `main accepts an EventGroupSync route`() {
    assertSuccess(execute(route("EventGroupSyncParams")))
  }

  @Test
  fun `main accepts an external data availability route`() {
    assertSuccess(execute(route("ExternalDataAvailabilitySyncParams")))
  }

  @Test
  fun `main accepts mixed route parameter types`() {
    assertSuccess(
      execute(route("EventGroupSyncParams") + route("ExternalDataAvailabilitySyncParams"))
    )
  }

  @Test
  fun `main accepts the deployed dev configuration`() {
    assertSuccess(executeDeployedConfig("dev"))
  }

  @Test
  fun `main accepts the deployed head configuration`() {
    assertSuccess(executeDeployedConfig("head"))
  }

  @Test
  fun `main accepts the deployed qa configuration`() {
    assertSuccess(executeDeployedConfig("qa"))
  }

  @Test
  fun `main rejects a legacy ResultsFulfiller route`() {
    val result = execute(route("ResultsFulfillerParams"))

    assertThat(result.exitCode).isEqualTo(1)
    assertThat(result.output).contains("uses legacy ResultsFulfiller dispatch")
  }

  @Test
  fun `main rejects an unsupported route parameter type`() {
    val result = execute(route("UnsupportedParams"))

    assertThat(result.exitCode).isEqualTo(1)
    assertThat(result.output).contains("Unable to parse Any of type")
    assertThat(result.output).contains("UnsupportedParams")
  }

  private fun assertSuccess(result: ValidationResult) {
    assertWithMessage(result.output).that(result.exitCode).isEqualTo(0)
  }

  private fun executeDeployedConfig(environment: String): ValidationResult {
    val path =
      Paths.get(
        "wfa_measurement_system",
        "src/main/terraform/gcloud/cmms/envs/$environment/data-watcher-config.textproto",
      )
    val configFile =
      checkNotNull(RUNFILES.getRuntimePath(path)) { "Runfile not found: $path" }.toFile()
    return execute(configFile)
  }

  private fun execute(config: String): ValidationResult {
    val configFile = temporaryFolder.newFile()
    configFile.writeText(config)
    return execute(configFile)
  }

  private fun execute(configFile: File): ValidationResult {
    val binaryPath =
      Paths.get(
        "wfa_measurement_system",
        "src/main/kotlin/org/wfanet/measurement/securecomputation/deploy/gcloud/datawatcher/ValidateDataWatcherConfig",
      )
    val binary =
      checkNotNull(RUNFILES.getRuntimePath(binaryPath)) { "Runfile not found: $binaryPath" }
    val process =
      ProcessBuilder(binary.toString(), "--data-watcher-config=${configFile.path}")
        .redirectErrorStream(true)
        .start()
    try {
      val output = process.inputStream.bufferedReader().use { it.readText() }
      return ValidationResult(process.waitFor(), output)
    } finally {
      process.destroyForcibly()
    }
  }

  private fun route(typeName: String): String =
    """
    watched_paths {
      identifier: "test"
      control_plane_queue_sink {
        queue: "test-queue"
        serialization_key: "test-provider"
        app_params {
          [type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.$typeName] {}
        }
      }
    }

    """
      .trimIndent()

  private data class ValidationResult(val exitCode: Int, val output: String)

  companion object {
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
  }
}
