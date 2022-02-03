// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.panelmatch.tools

import java.nio.file.Paths
import java.util.concurrent.Callable
import java.util.logging.Logger
import kotlin.system.exitProcess
import org.wfanet.measurement.common.getRuntimePath
import picocli.CommandLine
import picocli.CommandLine.Command

@Command(name = "deploy_to_gke", description = ["Deploys containers on gcr.io to GKE"])
class DeployToGke : Callable<Int> {

  @CommandLine.Option(
    names = ["--yaml-file"],
    description = ["The yaml file to deploy."],
    required = true
  )
  lateinit var yamlFile: String
    private set

  @CommandLine.Option(
    names = ["--cluster-name"],
    description = ["The name of the Kubernetes cluster to deploy in."],
    required = true
  )
  lateinit var clusterName: String
    private set

  @CommandLine.Option(
    names = ["--environment"],
    description = ["The environment of the deployment."],
    required = true
  )
  lateinit var environment: String
    private set

  override fun call(): Int {
    val manifestPath =
      checkNotNull(
        getRuntimePath(Paths.get("panel_exchange_client", "src", "main", "k8s", "dev", yamlFile))
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
