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

import com.google.devtools.build.runfiles.Runfiles
import java.nio.file.Paths
import java.util.concurrent.Callable
import java.util.logging.Logger
import kotlin.system.exitProcess
import kotlinx.coroutines.joinAll
import kotlinx.coroutines.launch
import kotlinx.coroutines.runBlocking
import picocli.CommandLine
import picocli.CommandLine.Command

@Command(
  name = "deploy_to_kind",
  description = ["Builds container images from source and deploys them to a local kind cluster."]
)
class DeployToKind() : Callable<Int> {
  private val duchyFilePath = "src/main/kotlin/org/wfanet/measurement"
  private val yamlFile = "kingdom_and_three_duchies_from_cue_local.yaml"

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
    logger.info("*** STARTING ***")

    val runfiles = Runfiles.create()

    // Load Duchy images.
    val duchyRunfiles = Paths.get(
      runfiles.rlocation("wfa_measurement_system/$duchyFilePath")
    ).toFile()
    duchyRunfiles
      .walk().filter { !it.isDirectory && it.extension == "tar" }
      .forEach { imageFile ->
        logger.info("*** LOADING IMAGE: ${imageFile.absolutePath} ***")

        // Figure out what the image name is.
        val repository = "bazel/$duchyFilePath/${imageFile.parentFile.relativeTo(duchyRunfiles)}"
        val tag = imageFile.nameWithoutExtension
        val imageName = "$repository:$tag"

        // Remove images from Docker if they already exist.
        // This is not strictly necessary but Docker keeps the old image in memory otherwise.
        logger.info(
          "*** FYI: If the image doesn't exist the next command fails. " +
            "This is expected and not a big deal. ***"
        )
        "docker rmi $imageName".runAsProcess(exitOnFail = false)

        // Load the image into Docker.
        "docker load -i ${imageFile.absolutePath}".runAsProcess(redirectErrorStream = false)

        // Load the image into Kind.
        "kind load docker-image $imageName".runAsProcess()

        logger.info("*** DONE LOADING IMAGE: $imageFile.absolutePath ***")
      }

    logger.info("*** DONE LOADING ALL IMAGES ***")

    val yaml = Paths.get(
      runfiles.rlocation(
        "wfa_measurement_system/src/main/k8s/$yamlFile"
      )
    ).toFile()

    // kubectl apply does not necessarily overwrite previous configuration.
    // Delete existing pods/services to be safe.
    logger.info(
      "*** FYI: If the pods don't exist the next command fails. " +
        "This is expected and not a big deal. ***"
    )
    "kubectl delete -f ${yaml.absolutePath}".runAsProcess(exitOnFail = false)

    // Create the pods and services.
    "kubectl apply -f ${yaml.absolutePath}".runAsProcess()

    logger.info("*** DONE: Completed successfully. ***")

    return 0
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

fun main(args: Array<String>) {
  exitProcess(CommandLine(DeployToKind()).execute(*args))
}
