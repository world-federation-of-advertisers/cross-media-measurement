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
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.mapConcurrently
import picocli.CommandLine
import picocli.CommandLine.Command

@Command(
  name = "deploy_to_kind",
  description = ["Builds container images from source and deploys them to a local kind cluster."]
)
class DeployToKind() : Callable<Int> {
  private val defaultClusterName = "kind"

  override fun call(): Int {
    logger.info("*** STARTING ***")

    loadImages(defaultClusterName)

    // kubectl apply does not necessarily overwrite previous configuration.
    // Delete existing pods/services to be safe.
    logger.info(
      "*** FYI: If the pods don't exist the next command fails. " +
        "This is expected and not a big deal. ***"
    )
    runSubprocess(
      "kubectl delete -f $manifestPath --context kind-$defaultClusterName",
      exitOnFail = false
    )

    // Create the pods and services.
    runSubprocess("kubectl apply -f $manifestPath --context kind-$defaultClusterName")

    logger.info("*** DONE: Completed successfully. ***")

    return 0
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
    private const val kotlinRelativePath = "src/main/kotlin"
    private const val yamlFile = "kingdom_and_three_duchies_from_cue_local.yaml"
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

    fun loadImages(clusterName: String) {
      logger.info("*** DONE LOADING ALL IMAGES ***")

      val runfiles =
        checkNotNull(
          getRuntimePath(
            Paths.get(
              "wfa_measurement_system",
              kotlinRelativePath
            )
          )
        ).toFile()

      runBlocking {
        runfiles
          .walk()
          .asFlow()
          .filter { !it.isDirectory && it.extension == "tar" }
          .mapConcurrently(CoroutineScope(coroutineContext), 8) { imageFile ->
            val absolutePath = imageFile.absolutePath

            logger.info("*** LOADING IMAGE: $absolutePath ***")

            // Figure out what the image name is.
            val repository =
              "bazel/$kotlinRelativePath/${imageFile.parentFile.relativeTo(runfiles)}"
            val tag = imageFile.nameWithoutExtension
            val imageName = "$repository:$tag"

            // Remove images from Docker if they already exist.
            // This is not strictly necessary but Docker keeps the old image in memory otherwise.
            val deleteWarning =
              "*** FYI: If the image doesn't exist the next command fails. " +
                "This is expected and not a big deal. ***"
            runSubprocess(
              "echo \"$deleteWarning\" && docker rmi $imageName", exitOnFail = false
            )

            // Load the image into Docker.
            runSubprocess("docker load -i $absolutePath", redirectErrorStream = false)

            // Load the image into Kind.
            runSubprocess("kind load docker-image $imageName --name $clusterName")

            logger.info("*** DONE LOADING IMAGE: $absolutePath ***")
          }
          .collect()
      }

      logger.info("*** DONE LOADING ALL IMAGES ***")
    }
  }
}

fun main(args: Array<String>) {
  CommandLine(DeployToKind()).execute(*args)
}
