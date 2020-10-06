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
import org.wfanet.measurement.common.getRuntimePath
import picocli.CommandLine
import picocli.CommandLine.Command

@Command(
  name = "deploy_to_kind",
  description = ["Deploys containers on gcr.io to GKE"]
)
class DeployToGke() : Callable<Int> {
  private val yamlFile = "kingdom_and_three_duchies_from_cue_gke.yaml"
  private val clusterName = "om-test-cluster"

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

    // Obtain a credential
    runSubprocess("gcloud container clusters get-credentials $clusterName")

    // Create the pods and services.
    runSubprocess("kubectl apply -f $manifestPath")

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
