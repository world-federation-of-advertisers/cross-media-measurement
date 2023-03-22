/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import java.security.Security
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.jetbrains.annotations.Blocking
import org.junit.AfterClass
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.crypto.jceProvider
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.k8s.testing.KindCluster
import org.wfanet.measurement.common.testing.chainRulesSequentially

/** Test for correctness of the CMMS on [KinD](https://kind.sigs.k8s.io/). */
@RunWith(JUnit4::class)
class KindCorrectnessTest : CorrectnessTest(measurementSystem) {
  private class Images : TestRule {
    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          runBlocking { loadImages() }
          base.evaluate()
        }
      }
    }

    private suspend fun loadImages() {
      withContext(Dispatchers.IO) {
        for (imageArchivePath in EMULATOR_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
        for (imageArchivePath in KINGDOM_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
        for (imageArchivePath in DUCHY_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
        for (imageArchivePath in SIMULATOR_IMAGE_ARCHIVES) {
          loadImage(imageArchivePath)
        }
      }
    }

    @Blocking
    private fun loadImage(archivePath: Path) {
      logger.info("Loading image archive $archivePath")
      val runtimePath: Path = checkNotNull(getRuntimePath(archivePath))
      kindCluster.loadImage(runtimePath)
    }
  }

  companion object {
    init {
      // Remove Conscrypt provider so underlying OkHttp client won't use it and fail on unsupported
      // certificate algorithms when connecting to KinD (ECFieldF2m).
      Security.removeProvider(jceProvider.name)
    }

    private val logger = Logger.getLogger(this::class.java.name)

    private val BAZEL_TEST_OUTPUTS_DIR: Path? =
      System.getenv("TEST_UNDECLARED_OUTPUTS_DIR")?.let { Paths.get(it) }

    private val EMULATOR_IMAGE_ARCHIVES =
      listOf(
        Paths.get(
          "wfa_common_jvm",
          "src",
          "main",
          "kotlin",
          "org",
          "wfanet",
          "measurement",
          "storage",
          "filesystem",
          "server_image.tar"
        ),
      )

    private val MEASUREMENT_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "kotlin",
        "org",
        "wfanet",
        "measurement",
      )

    private val KINGDOM_DEPLOY_PATH = MEASUREMENT_PATH.resolve(Paths.get("kingdom", "deploy"))
    private val KINGDOM_IMAGE_ARCHIVES =
      listOf(
        KINGDOM_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "server", "gcp_kingdom_data_server_image.tar")
        ),
        KINGDOM_DEPLOY_PATH.resolve(Paths.get("common", "server", "system_api_server_image.tar")),
        KINGDOM_DEPLOY_PATH.resolve(
          Paths.get("common", "server", "v2alpha_public_api_server_image.tar")
        ),
        KINGDOM_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "spanner", "tools", "update_schema_image.tar")
        ),
      )

    private val DUCHY_DEPLOY_PATH = MEASUREMENT_PATH.resolve(Paths.get("duchy", "deploy"))
    private val DUCHY_IMAGE_ARCHIVES =
      listOf(
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get(
            "common",
            "daemon",
            "mill",
            "liquidlegionsv2",
            "forwarded_storage_liquid_legions_v2_mill_daemon_image.tar"
          )
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get(
            "common",
            "server",
            "forwarded_storage_computation_control_server_image.tar",
          )
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get(
            "common",
            "server",
            "forwarded_storage_requisition_fulfillment_server_image.tar",
          )
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("common", "server", "async_computation_control_server_image.tar")
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("common", "daemon", "herald", "herald_daemon_image.tar")
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "server", "forwarded_storage_spanner_computations_server_image.tar")
        ),
        DUCHY_DEPLOY_PATH.resolve(
          Paths.get("gcloud", "spanner", "tools", "update_schema_image.tar")
        ),
      )

    private val SIMULATOR_IMAGE_ARCHIVES =
      listOf(
        MEASUREMENT_PATH.resolve(
          Paths.get("loadtest", "dataprovider", "forwarded_storage_edp_simulator_runner_image.tar")
        )
      )

    private val kindCluster = KindCluster()
    private val tempDir = TemporaryFolder()
    private val measurementSystem =
      MeasurementSystem(lazy { kindCluster.k8sClient }, lazy { tempDir }, lazy { runId })

    @ClassRule
    @JvmField
    val chainedRule = chainRulesSequentially(tempDir, kindCluster, Images(), measurementSystem)

    private val runId: String
      get() = kindCluster.uuid.toString()

    @AfterClass
    @JvmStatic
    fun exportKindLogs() {
      if (BAZEL_TEST_OUTPUTS_DIR == null) {
        return
      }

      kindCluster.exportLogs(BAZEL_TEST_OUTPUTS_DIR)
    }
  }
}
