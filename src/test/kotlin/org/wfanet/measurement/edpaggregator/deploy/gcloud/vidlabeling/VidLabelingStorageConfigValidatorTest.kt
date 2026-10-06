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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TextFormat
import com.google.protobuf.struct
import com.google.protobuf.value
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.gcsStorage
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.httpEndpointSink
import org.wfanet.measurement.config.securecomputation.dataWatcherConfig
import org.wfanet.measurement.config.securecomputation.watchedPath
import picocli.CommandLine

@RunWith(JUnit4::class)
class VidLabelingStorageConfigValidatorTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `validate accepts matching output paths not watched by DataWatcher`() {
    VidLabelingStorageConfigValidator.validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH),
      watcherConfig("gs://bucket/edp/event-groups/.*"),
    )
  }

  @Test
  fun `validate rejects different VID labeling and DataAvailabilitySync paths`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(OTHER_OUTPUT_PATH),
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("edp_impression_path values differ")
  }

  @Test
  fun `validate rejects DataWatcher route matching VidLabeler-managed output`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*/.*/done"),
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `CLI validates deployment textprotos`() {
    val vidConfigFile = temporaryFolder.newFile("vid-labeling.textproto")
    vidConfigFile.writeText(TextFormat.printer().printToString(vidConfigs(LABELED_OUTPUT_PATH)))
    val syncConfigFile = temporaryFolder.newFile("data-availability.textproto")
    syncConfigFile.writeText(TextFormat.printer().printToString(syncConfigs(LABELED_OUTPUT_PATH)))
    val watcherConfigFile = temporaryFolder.newFile("data-watcher.textproto")
    watcherConfigFile.writeText(
      TextFormat.printer().printToString(watcherConfig("gs://bucket/edp/event-groups/.*"))
    )

    val exitCode =
      CommandLine(ValidateVidLabelingStorageConfig())
        .execute(
          "--vid-labeling-config=${vidConfigFile.path}",
          "--vid-labeling-monitor-config=${vidConfigFile.path}",
          "--data-availability-sync-config=${syncConfigFile.path}",
          "--data-watcher-config=${watcherConfigFile.path}",
        )

    assertThat(exitCode).isEqualTo(0)
  }

  private fun vidConfigs(path: String) = vidLabelingConfigs {
    configs += vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      edpImpressionPath = path
      vidLabeledImpressionsStorageParams = storageParams {
        gcs = gcsStorage { bucketName = "bucket" }
      }
    }
  }

  private fun syncConfigs(path: String) = dataAvailabilitySyncConfigs {
    configs += dataAvailabilitySyncConfig {
      dataProvider = DATA_PROVIDER
      edpImpressionPath = path
      dataAvailabilityStorage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
    }
  }

  private fun watcherConfig(regex: String) = dataWatcherConfig {
    watchedPaths += availabilityRoute(regex)
  }

  private fun availabilityRoute(regex: String) = watchedPath {
    sourcePathRegex = regex
    httpEndpointSink = httpEndpointSink {
      endpointUri = "https://example.test/sync"
      appParams = struct { fields["dataProvider"] = value { stringValue = DATA_PROVIDER } }
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp7"
    private const val LABELED_OUTPUT_PATH = "edp/vid-labeler"
    private const val OTHER_OUTPUT_PATH = "edp/other"
  }
}
