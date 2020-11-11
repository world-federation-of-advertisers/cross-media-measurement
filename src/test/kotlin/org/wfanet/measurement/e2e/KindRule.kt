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

package org.wfanet.measurement.e2e

import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.tools.DeployToKind
import org.wfanet.measurement.tools.runSubprocess

class KindRule : TestRule {
  override fun apply(base: Statement, description: Description): Statement =
    object : Statement() {
      override fun evaluate() {
        // Tear down the cluster if it already exists.
        runSubprocess("kind delete cluster --name $clusterName", exitOnFail = false)
        runSubprocess("kind create cluster --name $clusterName")
        // Load the Docker images.
        DeployToKind.loadImages(clusterName)
        // Create the pods and services.
        runSubprocess(
          "kubectl apply -f ${DeployToKind.manifestPath} --context kind-$clusterName"
        )
        try {
          // Run the test.
          base.evaluate()
        } finally {
          // Tear down the cluster.
          runSubprocess("kind delete cluster --name $clusterName")
        }
      }
    }

  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
    const val clusterName = "measurement-e2e"
  }
}
