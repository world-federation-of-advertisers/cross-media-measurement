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

package org.wfanet.measurement.edpaggregator.tools

import com.google.devtools.build.runfiles.Runfiles
import java.io.File
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.ClassRule
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.testing.CommandLineTesting
import org.wfanet.measurement.common.testing.ExitInterceptingSecurityManager
import org.wfanet.measurement.gcloud.gcs.testing.StorageEmulatorRule

@RunWith(JUnit4::class)
class ReplayDataAvailabilitySyncTest {
  @get:Rule val tempDir = TemporaryFolder()

  @Test
  fun `main exits nonzero when mode is missing`() {
    val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        connectionArgs(configFile) + "--blob-prefix=$EDP_IMPRESSION_PATH",
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput).err().contains("--mode")
  }

  @Test
  fun `main exits nonzero when activity mode configuration is missing`() {
    val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))

    val capturedOutput =
      CommandLineTesting.capturingOutput(
        requiredArgs(configFile, ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY),
        ::main,
      )

    CommandLineTesting.assertThat(capturedOutput).status().isNotEqualTo(0)
    CommandLineTesting.assertThat(capturedOutput)
      .err()
      .contains("event_group_activity_publishing must be set")
  }

  @Test
  fun `main exits zero in activity only mode when no completion blobs exist`() {
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = true))

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, ReplayDataAvailabilitySync.Mode.EVENT_GROUP_ACTIVITY_ONLY) +
            storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  @Test
  fun `main exits zero in full sync mode without activity configuration`() {
    storageEmulator.createBucket(BUCKET_NAME)
    try {
      val configFile = writeConfigFile(validConfig(includeActivityPublishing = false))

      val capturedOutput =
        CommandLineTesting.capturingOutput(
          requiredArgs(configFile, ReplayDataAvailabilitySync.Mode.FULL_SYNC) + storageArgs,
          ::main,
        )

      CommandLineTesting.assertThat(capturedOutput).status().isEqualTo(0)
    } finally {
      storageEmulator.deleteBucketRecursive(BUCKET_NAME)
    }
  }

  private val storageArgs: Array<String>
    get() =
      arrayOf(
        "--storage-api-endpoint=${storageEmulator.storage.options.host}",
        "--throttler-minimum-interval=0s",
      )

  private fun connectionArgs(configFile: File): Array<String> =
    arrayOf(
      "--config-file=${configFile.path}",
      "--kingdom-public-api-target=localhost:1",
      "--impression-metadata-api-target=localhost:1",
    )

  private fun requiredArgs(configFile: File, mode: ReplayDataAvailabilitySync.Mode): Array<String> =
    connectionArgs(configFile) + arrayOf("--mode=$mode", "--blob-prefix=$EDP_IMPRESSION_PATH")

  private fun validConfig(includeActivityPublishing: Boolean): String =
    buildString {
        appendLine(
          """
          data_provider: "dataProviders/test-provider"
          data_availability_storage {
            gcs {
              project_id: "test-project"
              bucket_name: "$BUCKET_NAME"
            }
          }
          cmms_connection {
            cert_file_path: "$SECRETS_DIR/kingdom_tls.pem"
            private_key_file_path: "$SECRETS_DIR/kingdom_tls.key"
            cert_collection_file_path: "$SECRETS_DIR/kingdom_root.pem"
          }
          impression_metadata_storage_connection {
            cert_file_path: "$SECRETS_DIR/kingdom_tls.pem"
            private_key_file_path: "$SECRETS_DIR/kingdom_tls.key"
            cert_collection_file_path: "$SECRETS_DIR/kingdom_root.pem"
          }
          edp_impression_path: "$EDP_IMPRESSION_PATH"
          """
            .trimIndent()
        )
        if (includeActivityPublishing) {
          appendLine(
            """
            event_group_activity_publishing {
              market_time_zone { id: "UTC" }
              event_group_map_blob_uri: "gs://$BUCKET_NAME/event-group-map.binpb"
              event_group_map_storage {
                gcs {
                  project_id: "test-project"
                  bucket_name: "$BUCKET_NAME"
                }
              }
            }
            """
              .trimIndent()
          )
        }
      }
      .trim()

  private fun writeConfigFile(contents: String): File =
    tempDir
      .newFile("data-availability-sync-config-${tempDir.root.listFiles().size}.textproto")
      .apply { writeText(contents) }

  @Suppress("DEPRECATION")
  companion object {
    // CommandLineTesting uses this security manager to observe the real main function's exit code.
    init {
      System.setSecurityManager(ExitInterceptingSecurityManager)
    }

    @get:JvmStatic @get:ClassRule val storageEmulator = StorageEmulatorRule()

    private const val MODULE_REPO_NAME = "wfa_measurement_system"
    private const val BUCKET_NAME = "replay-test"
    private const val EDP_IMPRESSION_PATH = "edp/test/vid-labeled-impressions"
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
    private val SECRETS_DIR: Path =
      checkNotNull(
        RUNFILES.getRuntimePath(
          Paths.get(MODULE_REPO_NAME, "src", "main", "k8s", "testing", "secretfiles")
        )
      )
  }
}
