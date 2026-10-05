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
  fun `validate accepts existing disjoint internal and external paths`() {
    VidLabelingStorageConfigValidator.validate(
      vidConfigs(INTERNAL_PATH),
      vidConfigs(INTERNAL_PATH),
      syncConfigs(EXTERNAL_PATH),
      watcherConfig("gs://bucket/$EXTERNAL_PATH/model-line/.*/.*/done"),
    )
  }

  @Test
  fun `validate rejects overlapping internal and external paths`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs("$EXTERNAL_PATH/internal"),
          vidConfigs("$EXTERNAL_PATH/internal"),
          syncConfigs(EXTERNAL_PATH),
          watcherConfig("gs://bucket/$EXTERNAL_PATH/model-line/.*/.*/done"),
        )
      }

    assertThat(error).hasMessageThat().contains("overlap")
  }

  @Test
  fun `validate rejects DataWatcher route matching internal output`() {
    val watcherConfig = dataWatcherConfig {
      watchedPaths += availabilityRoute("gs://bucket/$EXTERNAL_PATH/model-line/.*/.*/done")
      watchedPaths += availabilityRoute("gs://bucket/edp/.*/model-line/.*/.*/done")
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(INTERNAL_PATH),
          vidConfigs(INTERNAL_PATH),
          syncConfigs(EXTERNAL_PATH),
          watcherConfig,
        )
      }

    assertThat(error).hasMessageThat().contains("matches the internal")
  }

  @Test
  fun `CLI validates deployment textprotos`() {
    val vidConfigFile = temporaryFolder.newFile("vid-labeling.textproto")
    vidConfigFile.writeText(TextFormat.printer().printToString(vidConfigs(INTERNAL_PATH)))
    val syncConfigFile = temporaryFolder.newFile("data-availability.textproto")
    syncConfigFile.writeText(TextFormat.printer().printToString(syncConfigs(EXTERNAL_PATH)))
    val watcherConfigFile = temporaryFolder.newFile("data-watcher.textproto")
    watcherConfigFile.writeText(
      TextFormat.printer()
        .printToString(watcherConfig("gs://bucket/$EXTERNAL_PATH/model-line/.*/.*/done"))
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
    private const val INTERNAL_PATH = "edp/internal"
    private const val EXTERNAL_PATH = "edp/external"
  }
}
