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

package org.wfanet.measurement.common.k8s.testing

import io.kubernetes.client.util.Config
import java.io.InputStream
import java.nio.file.Path
import java.util.UUID
import java.util.logging.Logger
import org.jetbrains.annotations.Blocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.k8s.KubernetesClient

/**
 * [TestRule] for a Kubernetes cluster in [KinD](https://kind.sigs.k8s.io/).
 *
 * This assumes that you have KinD installed and in your path. This creates a cluster using `kind
 * create cluster` and attempts to delete it using `kind delete cluster`. These commands may have
 * other side effects.
 */
class KindCluster : TestRule {
  lateinit var uuid: UUID
    private set
  val name: String by lazy { "kind-$uuid" }
  val nameOption: String by lazy { "--name=$name" }

  lateinit var k8sClient: KubernetesClient
    private set

  override fun apply(base: Statement, description: Description): Statement {
    return object : Statement() {
      override fun evaluate() {
        uuid = UUID.randomUUID()
        runCommand("kind", "create", "cluster", nameOption)
        try {
          val apiClient =
            runCommand("kind", "get", "kubeconfig", nameOption) { Config.fromConfig(it) }
          k8sClient = KubernetesClient(apiClient)
          base.evaluate()
        } finally {
          runCommand("kind", "delete", "cluster", nameOption)
        }
      }
    }
  }

  @Blocking
  fun loadImage(archivePath: Path) {
    runCommand("kind", "load", "image-archive", archivePath.toString(), nameOption)
  }

  @Blocking
  fun exportLogs(outputDir: Path) {
    runCommand("kind", "export", "logs", outputDir.toString(), nameOption)
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)

    @Blocking
    private fun runCommand(vararg command: String) {
      logger.fine { "Running `${command.joinToString(" ")}`" }

      val process: Process =
        ProcessBuilder(*command)
          .redirectOutput(ProcessBuilder.Redirect.INHERIT)
          .redirectError(ProcessBuilder.Redirect.INHERIT)
          .start()
      val exitCode: Int = process.waitFor()
      check(exitCode == 0) { "`${command.joinToString(" ")}` failed with exit code $exitCode" }
    }

    @Blocking
    private inline fun <T> runCommand(
      vararg command: String,
      consumeOutput: (InputStream) -> T
    ): T {
      logger.fine { "Running `${command.joinToString(" ")}`" }

      val process: Process = ProcessBuilder(*command).start()
      val result = process.inputStream.use { consumeOutput(it) }

      val exitCode: Int = process.waitFor()
      check(exitCode == 0) {
        val errOutput = process.errorStream.bufferedReader().use { it.readText() }
        "`${command.joinToString(" ")}` failed with exit code $exitCode\n$errOutput"
      }

      return result
    }
  }
}
