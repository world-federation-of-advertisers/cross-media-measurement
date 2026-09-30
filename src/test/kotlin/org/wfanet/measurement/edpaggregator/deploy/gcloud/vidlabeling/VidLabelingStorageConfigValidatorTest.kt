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
  fun `validate accepts disjoint internal and external paths`() {
    VidLabelingStorageConfigValidator.validate(
      vidConfigs(internalPath = "edp/edp7/internal"),
      syncConfigs(externalPath = "edp/edp7/external", internalPath = "edp/edp7/internal"),
      watcherConfig("gs://bucket/edp/edp7/external/model-line/.*/.*/done"),
    )
  }

  @Test
  fun `validate rejects overlapping paths`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(internalPath = "edp/edp7/external/internal"),
          syncConfigs(
            externalPath = "edp/edp7/external",
            internalPath = "edp/edp7/external/internal",
          ),
          watcherConfig("gs://bucket/edp/edp7/external/model-line/.*/.*/done"),
        )
      }

    assertThat(error).hasMessageThat().contains("overlap")
  }

  @Test
  fun `validate rejects DataWatcher route matching internal path`() {
    val watcherConfig = dataWatcherConfig {
      watchedPaths +=
        this@VidLabelingStorageConfigValidatorTest
          .watcherConfig("gs://bucket/edp/edp7/external/model-line/.*/.*/done")
          .watchedPathsList
      watchedPaths += watchedPath {
        sourcePathPrefix = "gs://bucket/edp/edp7"
        sourcePathRegex = "gs://bucket/edp/edp7/(.*)"
        this.httpEndpointSink = httpEndpointSink {
          endpointUri = "https://example.test/sync"
          appParams = struct { fields["dataProvider"] = value { stringValue = DATA_PROVIDER } }
        }
      }
    }
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(internalPath = "edp/edp7/internal"),
          syncConfigs(externalPath = "edp/edp7/external", internalPath = "edp/edp7/internal"),
          watcherConfig,
        )
      }

    assertThat(error).hasMessageThat().contains("matches the internal")
  }

  @Test
  fun `validate rejects mismatched internal paths`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(internalPath = "edp/edp7/internal-a"),
          syncConfigs(externalPath = "edp/edp7/external", internalPath = "edp/edp7/internal-b"),
          watcherConfig("gs://bucket/edp/edp7/external/model-line/.*/.*/done"),
        )
      }

    assertThat(error).hasMessageThat().contains("differs")
  }

  @Test
  fun `validate rejects different dispatcher and monitor providers`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(internalPath = "edp/edp7/internal"),
          vidConfigs(internalPath = "edp/edp8/internal", dataProvider = "dataProviders/edp8"),
          syncConfigs(externalPath = "edp/edp7/external", internalPath = "edp/edp7/internal"),
          watcherConfig("gs://bucket/edp/edp7/external/model-line/.*/.*/done"),
        )
      }

    assertThat(error).hasMessageThat().contains("data providers differ")
  }

  @Test
  fun `CLI validates storage routing textprotos`() {
    val vidConfigFile = temporaryFolder.newFile("vid-labeling.textproto")
    vidConfigFile.writeText(
      TextFormat.printer().printToString(vidConfigs(internalPath = "edp/edp7/internal"))
    )
    val syncConfigFile = temporaryFolder.newFile("data-availability.textproto")
    syncConfigFile.writeText(
      TextFormat.printer()
        .printToString(
          syncConfigs(externalPath = "edp/edp7/external", internalPath = "edp/edp7/internal")
        )
    )
    val watcherConfigFile = temporaryFolder.newFile("data-watcher.textproto")
    watcherConfigFile.writeText(
      TextFormat.printer()
        .printToString(watcherConfig("gs://bucket/edp/edp7/external/model-line/.*/.*/done"))
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

  private fun vidConfigs(internalPath: String, dataProvider: String = DATA_PROVIDER) =
    vidLabelingConfigs {
    configs += vidLabelingConfig {
      this.dataProvider = dataProvider
      edpImpressionPath = "edp/edp7/external"
      dataAvailabilitySyncTasksEnabled = true
      vidLabelingOutputPath = internalPath
      vidLabeledImpressionsStorageParams = storageParams {
        gcs = gcsStorage { bucketName = "bucket" }
      }
      }
    }

  private fun syncConfigs(externalPath: String, internalPath: String) =
    dataAvailabilitySyncConfigs {
      configs += dataAvailabilitySyncConfig {
        dataProvider = DATA_PROVIDER
        edpImpressionPath = externalPath
        vidLabelingOutputPath = internalPath
        dataAvailabilityStorage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
      }
    }

  private fun watcherConfig(
    regex: String,
    sourcePathPrefix: String = "gs://bucket/edp/edp7/external",
  ) = dataWatcherConfig {
    watchedPaths += watchedPath {
      sourcePathRegex = regex
      this.sourcePathPrefix = sourcePathPrefix
      this.httpEndpointSink = httpEndpointSink {
        endpointUri = "https://example.test/sync"
        appParams = struct { fields["dataProvider"] = value { stringValue = DATA_PROVIDER } }
      }
    }
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp7"
  }
}
