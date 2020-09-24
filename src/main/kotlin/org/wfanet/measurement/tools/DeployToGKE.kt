// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.tools

import java.nio.file.Paths
import java.util.concurrent.Callable
import java.util.logging.Logger
import kotlin.system.exitProcess
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.getRuntimePath
import picocli.CommandLine
import picocli.CommandLine.Command

@Command(
  name = "deploy_to_kind",
  description = ["Builds container images from source and deploys them to a local kind cluster."]
)
class DeployToGke() : Callable<Int> {
  private val yamlFile = "kingdom_and_three_duchies_from_cue_gke.yaml"
  private val clusterName = "om-test-cluster"
  private fun String.runAsProcess(
    // A lot of tools write things that aren't errors to stderr.
    redirectErrorStream: Boolean = true,
    exitOnFail: Boolean = true
  ) {
    logger.info("*** RUNNING: $this ***")

    val process = ProcessBuilder(this.split("\\s".toRegex()))
      .redirectErrorStream(redirectErrorStream)
      .start()

    runBlocking {
      joinAll(
        launch { process.errorStream.bufferedReader().forEachLine(logger::severe) },
        launch { process.inputStream.bufferedReader().forEachLine(logger::info) }
      )
    }

    process.waitFor()

    if (exitOnFail && process.exitValue() != 0) {
      logger.severe("*** FAILURE: Something went wrong. Aborting. ****")
      System.exit(process.exitValue())
    }
  }

  override fun call(): Int {
    val manifestPath =
      checkNotNull(
        getRuntimePath(
          Paths.get(
            "wfa_measurement_system",
            "src",
            "main",
            "k8s",
            yamlFile
          )
        )
      )
    logger.info("*** STARTING ***")

    "gcloud container clusters get-credentials $clusterName".runAsProcess()

    // Create the pods and services.
    "kubectl apply -f $manifestPath".runAsProcess()

    logger.info("*** DONE: Completed successfully. ***")

    return 0
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

fun main(args: Array<String>) {
  exitProcess(CommandLine(DeployToGke()).execute(*args))
}
