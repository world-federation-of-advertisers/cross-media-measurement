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
import com.google.protobuf.Any
import com.google.protobuf.Struct
import com.google.protobuf.TextFormat
import com.google.protobuf.struct
import com.google.protobuf.util.JsonFormat
import com.google.protobuf.value
import java.io.StringWriter
import kotlin.test.assertFailsWith
import org.junit.Rule
import org.junit.Test
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityMonitorConfigs
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.fileSystemStorage
import org.wfanet.measurement.config.edpaggregator.StorageParamsKt.gcsStorage
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.config.edpaggregator.copy
import org.wfanet.measurement.config.edpaggregator.dataAvailabilityMonitorConfig
import org.wfanet.measurement.config.edpaggregator.dataAvailabilityMonitorConfigs
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfig
import org.wfanet.measurement.config.edpaggregator.dataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.storageParams
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfig
import org.wfanet.measurement.config.edpaggregator.vidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.controlPlaneQueueSink
import org.wfanet.measurement.config.securecomputation.WatchedPathKt.httpEndpointSink
import org.wfanet.measurement.config.securecomputation.dataWatcherConfig
import org.wfanet.measurement.config.securecomputation.watchedPath
import org.wfanet.measurement.edpaggregator.v1alpha.externalDataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.vidlabeling.DataAvailabilitySyncWorkItems
import picocli.CommandLine

@RunWith(JUnit4::class)
class VidLabelingStorageConfigValidatorTest {
  @get:Rule val temporaryFolder = TemporaryFolder()

  @Test
  fun `validate accepts matching output paths not watched by DataWatcher`() {
    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH),
      watcherConfig("gs://bucket/edp/event-groups/.*"),
    )
  }

  @Test
  fun `validate accepts separate internal and external paths for one provider`() {
    val syncConfigs = dataAvailabilitySyncConfigs {
      configs +=
        syncConfigs(OTHER_OUTPUT_PATH).configsList.single().copy {
          vidLabeledImpressionPath = LABELED_OUTPUT_PATH
        }
    }

    VidLabelingStorageConfigValidator.validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs,
      watcherConfig("gs://bucket/$OTHER_OUTPUT_PATH/model-line/.*/.*/done"),
    )
  }

  @Test
  fun `validate accepts raw upload dispatch beside an external availability route`() {
    val watcher = dataWatcherConfig {
      watchedPaths += queuedAvailabilityRoute(externalDoneRegex())
      watchedPaths += watchedPath {
        identifier = "vid-labeling-dispatch"
        sourcePathRegex = "^gs://bucket/$RAW_INPUT_PATH/[^/]+/done$"
        httpEndpointSink = httpEndpointSink {
          endpointUri = "https://example.test/dispatcher"
          appParams = struct { fields["dataProvider"] = value { stringValue = DATA_PROVIDER } }
        }
      }
    }

    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      watcher,
      monitoringConfigs(EXTERNAL_OUTPUT_PATH),
    )
  }

  @Test
  fun `validate rejects overlapping internal and external paths`() {
    val syncConfigs = dataAvailabilitySyncConfigs {
      configs +=
        syncConfigs(LABELED_OUTPUT_PATH).configsList.single().copy {
          edpImpressionPath = LABELED_OUTPUT_PATH
          vidLabeledImpressionPath = "$LABELED_OUTPUT_PATH/internal"
        }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs("$LABELED_OUTPUT_PATH/internal"),
          vidConfigs("$LABELED_OUTPUT_PATH/internal"),
          syncConfigs,
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("paths overlap")
  }

  @Test
  fun `validate rejects different VID labeling and DataAvailabilitySync paths`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(OTHER_OUTPUT_PATH),
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("vid_labeled_impression_path differ")
  }

  @Test
  fun `validate rejects different dispatcher and monitor raw prefixes`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH, rawPrefix = "raw/other"),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("storage routes differ")
  }

  @Test
  fun `validate allows configured labeled descendant of raw root`() {
    val overlappingConfigs = vidConfigs(LABELED_OUTPUT_PATH, rawPrefix = "edp")

    validate(
      overlappingConfigs,
      overlappingConfigs,
      syncConfigs(LABELED_OUTPUT_PATH),
      watcherConfig("gs://bucket/edp/event-groups/.*"),
    )
  }

  @Test
  fun `validate rejects raw prefix within labeled output`() {
    val overlappingConfigs = vidConfigs(LABELED_OUTPUT_PATH, rawPrefix = "$LABELED_OUTPUT_PATH/raw")

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          overlappingConfigs,
          overlappingConfigs,
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("is within labeled output")
  }

  @Test
  fun `validate rejects DataWatcher route matching VidLabeler-managed output`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*/.*/done"),
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate rejects exact internal child excluded by sample matching`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("^gs://bucket/$LABELED_OUTPUT_PATH/model-line/foo/2026-01-01/done$"),
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate rejects broad ancestor route`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("^gs://bucket/edp/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate rejects overlap hidden by an optional literal`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig("^gs://bucket/edp/vidX?-labeler/model-line/[^/]+/[^/]+/done$"),
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate rejects overlap hidden by alternation`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcherConfig(
            "^gs://bucket/edp/external/.*|" + "^gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*"
          ),
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate rejects unresolved provider in an overlapping route`() {
    val watcher = dataWatcherConfig {
      watchedPaths +=
        availabilityRoute(
          "gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*/.*/done",
          dataProvider = "dataProviders/other",
        )
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcher,
        )
      }

    assertThat(error).hasMessageThat().contains("app_params must resolve")
    assertThat(error).hasMessageThat().contains(OTHER_DATA_PROVIDER)
  }

  @Test
  fun `validate rejects overlapping route with no data provider`() {
    val watcher = dataWatcherConfig {
      watchedPaths += watchedPath {
        sourcePathRegex = "gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*/.*/done"
        httpEndpointSink = httpEndpointSink { endpointUri = "https://example.test/sync" }
      }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcher,
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate rejects overlapping control plane route`() {
    val watcher = dataWatcherConfig {
      watchedPaths += watchedPath {
        sourcePathRegex = "gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*/.*/done"
        controlPlaneQueueSink = controlPlaneQueueSink { queue = "availability-queue" }
      }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH),
          watcher,
        )
      }

    assertThat(error).hasMessageThat().contains("matches the VidLabeler-managed output path")
  }

  @Test
  fun `validate accepts a regex with a provably disjoint literal prefix`() {
    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH),
      watcherConfig("^gs://bucket/edp/external/model-line/.*/.*/done$"),
    )
  }

  @Test
  fun `validate accepts matching external watcher and monitor route`() {
    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      availabilityWatcherConfig(externalDoneRegex()),
      monitoringConfigs(EXTERNAL_OUTPUT_PATH),
    )
  }

  @Test
  fun `validate rejects missing external availability monitor entry`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
        )
      }

    assertThat(error).hasMessageThat().contains("DataAvailabilityMonitorConfigs.configs")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects misspelled external monitor prefix`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
          monitoringConfigs("edp/external-typo"),
        )
      }

    assertThat(error).hasMessageThat().contains("DataAvailabilityMonitorConfig.edp_impression_path")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects external monitor bucket mismatch`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
          monitoringConfigs(EXTERNAL_OUTPUT_PATH, bucket = "other-bucket"),
        )
      }

    assertThat(error).hasMessageThat().contains("DataAvailabilityMonitorConfig.storage")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects external monitor provider mismatch`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
          monitoringConfigs(EXTERNAL_OUTPUT_PATH, dataProvider = OTHER_DATA_PROVIDER),
        )
      }

    assertThat(error).hasMessageThat().contains("data_provider_name")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects external monitor storage backend mismatch`() {
    val availabilityConfigs = dataAvailabilityMonitorConfigs {
      configs += dataAvailabilityMonitorConfig {
        dataProviderName = DATA_PROVIDER
        edpImpressionPath = EXTERNAL_OUTPUT_PATH
        storage = storageParams { fileSystem = fileSystemStorage {} }
      }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
          availabilityConfigs,
        )
      }

    assertThat(error).hasMessageThat().contains("DataAvailabilityMonitorConfig.storage")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate accepts additional monitor location for same provider`() {
    val availabilityConfigs = dataAvailabilityMonitorConfigs {
      configs += monitoringConfigs(EXTERNAL_OUTPUT_PATH).configsList
      configs += dataAvailabilityMonitorConfig {
        dataProviderName = DATA_PROVIDER
        edpImpressionPath = "edp/additional"
        storage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
      }
    }

    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      availabilityWatcherConfig(externalDoneRegex()),
      availabilityConfigs,
    )
  }

  @Test
  fun `validate rejects missing external DataWatcher route`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig("^gs://bucket/edp/other/model-line/[^/]+/[^/]+/done$"),
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("DataWatcherConfig.watched_paths")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects external DataWatcher route restricted to representative models`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(
            "^gs://bucket/$EXTERNAL_OUTPUT_PATH/model-line/model-[ab]/[^/]+/done$"
          ),
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("source_path_regex")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects additional misspelled external DataWatcher route`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths +=
        availabilityRoute("^gs://bucket/edp/external-typo/model-line/[^/]+/[^/]+/done$")
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("exactly one route for")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects canonical route with wrong identifier`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex(), identifier = "data-availability-typo")
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("watched_paths.identifier")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects extra legacy route with wrong identifier and prefix`() {
    val syncConfigs = syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH)
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths +=
        legacyAvailabilityRoute(
          "^gs://bucket/edp/external-typo/model-line/[^/]+/[^/]+/done$",
          syncConfigs.configsList.single(),
          identifier = "data-availability-typo",
        )
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs,
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("exactly one route for")
  }

  @Test
  fun `validate rejects extra incomplete HTTP route with misspelled prefix`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths +=
        availabilityRoute(
          "^gs://bucket/edp/external-typo/model-line/[^/]+/[^/]+/done$",
          typeUrl = null,
        )
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("watched_paths.app_params")
  }

  @Test
  fun `validate rejects extra wrong-type HTTP route with misspelled prefix`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths +=
        availabilityRoute(
          "^gs://bucket/edp/external-typo/model-line/[^/]+/[^/]+/done$",
          typeUrl = "type.googleapis.com/example.UnsupportedParams",
        )
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("watched_paths.app_params")
  }

  @Test
  fun `validate rejects extra wrong-type queued route with misspelled prefix`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths += watchedPath {
        identifier = "data-availability"
        sourcePathRegex = "^gs://bucket/edp/external-typo/model-line/[^/]+/[^/]+/done$"
        controlPlaneQueueSink = controlPlaneQueueSink {
          queue = DataAvailabilitySyncWorkItems.QUEUE
          serializationKey = "data-availability:$DATA_PROVIDER"
          appParams = Any.getDefaultInstance()
        }
      }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("watched_paths.app_params")
  }

  @Test
  fun `validate rejects reserved route with missing sink`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          dataWatcherConfig {
            watchedPaths += watchedPath {
              identifier = "data-availability"
              sourcePathRegex = externalDoneRegex()
            }
          },
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("watched_paths.sink_config")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate accepts canonical queued route`() {
    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      dataWatcherConfig { watchedPaths += queuedAvailabilityRoute(externalDoneRegex()) },
      monitoringConfigs(EXTERNAL_OUTPUT_PATH),
    )
  }

  @Test
  fun `validate rejects queued route with wrong queue`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          dataWatcherConfig {
            watchedPaths += queuedAvailabilityRoute(externalDoneRegex(), queue = "wrong-queue")
          },
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("control_plane_queue_sink.queue")
  }

  @Test
  fun `validate rejects queued route with wrong serialization key`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          dataWatcherConfig {
            watchedPaths +=
              queuedAvailabilityRoute(
                externalDoneRegex(),
                serializationKey = "data-availability:wrong",
              )
          },
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("serialization_key")
  }

  @Test
  fun `validate rejects incomplete legacy HTTP payload`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          dataWatcherConfig {
            watchedPaths += availabilityRoute(externalDoneRegex(), typeUrl = null)
          },
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("app_params must resolve")
  }

  @Test
  fun `validate rejects unsupported typed HTTP payload`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          dataWatcherConfig {
            watchedPaths +=
              availabilityRoute(
                externalDoneRegex(),
                typeUrl = "type.googleapis.com/example.UnsupportedParams",
              )
          },
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("app_params must resolve")
  }

  @Test
  fun `validate accepts complete legacy HTTP payload`() {
    val syncConfigs = syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH)

    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs,
      dataWatcherConfig {
        watchedPaths +=
          legacyAvailabilityRoute(externalDoneRegex(), syncConfigs.configsList.single())
      },
      monitoringConfigs(EXTERNAL_OUTPUT_PATH),
    )
  }

  @Test
  fun `validate rejects orphan reserved route for another data provider`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths +=
        availabilityRoute(
          "^gs://bucket/edp/other/model-line/[^/]+/[^/]+/done$",
          dataProvider = OTHER_DATA_PROVIDER,
        )
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          watcher,
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("watched_paths.app_params")
    assertThat(error).hasMessageThat().contains(OTHER_DATA_PROVIDER)
  }

  @Test
  fun `validate accepts fallback external route for non-VID provider`() {
    validate(
      vidLabelingConfigs {},
      vidLabelingConfigs {},
      externalOnlySyncConfigs(LABELED_OUTPUT_PATH),
      availabilityWatcherConfig("^gs://bucket/$LABELED_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$"),
      monitoringConfigs(),
    )
  }

  @Test
  fun `validate rejects missing provider on fallback external route`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidLabelingConfigs {},
          vidLabelingConfigs {},
          externalOnlySyncConfigs(LABELED_OUTPUT_PATH, dataProvider = ""),
          dataWatcherConfig {
            watchedPaths +=
              availabilityRoute(
                "^gs://bucket/$LABELED_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$",
                dataProvider = "",
              )
          },
          monitoringConfigs(),
        )
      }

    assertThat(error).hasMessageThat().contains("DataAvailabilitySyncConfig.data_provider")
  }

  @Test
  fun `validate accepts equal explicit paths for non-VID provider`() {
    validate(
      vidLabelingConfigs {},
      vidLabelingConfigs {},
      externalOnlySyncConfigs(LABELED_OUTPUT_PATH, LABELED_OUTPUT_PATH),
      availabilityWatcherConfig("^gs://bucket/$LABELED_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$"),
      monitoringConfigs(LABELED_OUTPUT_PATH),
    )
  }

  @Test
  fun `validate rejects fallback queued route with wrong queue`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidLabelingConfigs {},
          vidLabelingConfigs {},
          externalOnlySyncConfigs(LABELED_OUTPUT_PATH),
          dataWatcherConfig {
            watchedPaths +=
              queuedAvailabilityRoute(
                "^gs://bucket/$LABELED_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$",
                queue = "wrong-queue",
              )
          },
          monitoringConfigs(),
        )
      }

    assertThat(error).hasMessageThat().contains("control_plane_queue_sink.queue")
  }

  @Test
  fun `validate ignores unrelated descendant watcher route`() {
    val watcher = dataWatcherConfig {
      watchedPaths += availabilityRoute(externalDoneRegex())
      watchedPaths += watchedPath {
        identifier = "event-groups"
        sourcePathRegex = "^gs://bucket/$EXTERNAL_OUTPUT_PATH/event-groups/.*"
        controlPlaneQueueSink = controlPlaneQueueSink {
          queue = "event-group-sync-queue"
          appParams = Any.getDefaultInstance()
        }
      }
    }

    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      watcher,
      monitoringConfigs(EXTERNAL_OUTPUT_PATH),
    )
  }

  @Test
  fun `validate rejects external DataWatcher bucket mismatch`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(
            "^gs://other-bucket/$EXTERNAL_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$"
          ),
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("DataWatcherConfig.watched_paths")
  }

  @Test
  fun `validate rejects equal external and internal paths`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, LABELED_OUTPUT_PATH),
          availabilityWatcherConfig(
            "^gs://bucket/$LABELED_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$"
          ),
          monitoringConfigs(LABELED_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("external_impression_path must differ")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects non-normalized external sync prefix`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, "$EXTERNAL_OUTPUT_PATH/"),
          availabilityWatcherConfig(externalDoneRegex()),
          monitoringConfigs(EXTERNAL_OUTPUT_PATH),
        )
      }

    assertThat(error).hasMessageThat().contains("external_impression_path must not start or end")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate rejects non-normalized external monitor prefix`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
          monitoringConfigs("$EXTERNAL_OUTPUT_PATH/"),
        )
      }

    assertThat(error).hasMessageThat().contains("edp_impression_path must not start or end")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `validate does not require internal path in DataAvailabilityMonitor`() {
    val availabilityConfigs = monitoringConfigs(EXTERNAL_OUTPUT_PATH)

    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      availabilityWatcherConfig(externalDoneRegex()),
      availabilityConfigs,
    )

    assertThat(availabilityConfigs.configsList.map { it.edpImpressionPath })
      .containsExactly(EXTERNAL_OUTPUT_PATH)
  }

  @Test
  fun `validate accepts external and internal monitors for their provider`() {
    val availabilityConfigs = dataAvailabilityMonitorConfigs {
      configs += monitoringConfigs(EXTERNAL_OUTPUT_PATH).configsList
      configs += monitoringConfigs(LABELED_OUTPUT_PATH).configsList
    }
    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
      availabilityWatcherConfig(externalDoneRegex()),
      availabilityConfigs,
    )
  }

  @Test
  fun `validate rejects internal DataAvailabilityMonitor assigned to another provider`() {
    val availabilityConfigs = dataAvailabilityMonitorConfigs {
      configs += monitoringConfigs(EXTERNAL_OUTPUT_PATH).configsList
      configs += dataAvailabilityMonitorConfig {
        dataProviderName = OTHER_DATA_PROVIDER
        storage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
        edpImpressionPath = LABELED_OUTPUT_PATH
      }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH),
          availabilityWatcherConfig(externalDoneRegex()),
          availabilityConfigs,
        )
      }

    assertThat(error).hasMessageThat().contains("data_provider_name must equal")
    assertThat(error).hasMessageThat().contains(DATA_PROVIDER)
  }

  @Test
  fun `CLI rejects a URI prefix in the dispatcher config`() {
    val errors = StringWriter()
    val exitCode = validateRawPrefixes("gs://other-bucket/raw", "raw", errors)

    assertThat(exitCode).isEqualTo(1)
    assertThat(errors.toString()).contains("raw_impressions_blob_prefix")
    assertThat(errors.toString()).contains("bucket-relative")
  }

  @Test
  fun `CLI rejects a URI prefix in the monitor config`() {
    val errors = StringWriter()
    val exitCode = validateRawPrefixes("raw", "gs://other-bucket/raw", errors)

    assertThat(exitCode).isEqualTo(1)
    assertThat(errors.toString()).contains("raw_impressions_blob_prefix")
    assertThat(errors.toString()).contains("bucket-relative")
  }

  @Test
  fun `CLI rejects matching malformed raw prefixes`() {
    for (prefix in listOf("raw//2026", "raw/./2026", "raw/../2026")) {
      val errors = StringWriter()
      val exitCode = validateRawPrefixes(prefix, prefix, errors)

      assertThat(exitCode).isEqualTo(1)
      assertThat(errors.toString()).contains("raw_impressions_blob_prefix")
      assertThat(errors.toString()).contains("path segments")
    }
  }

  private fun validateRawPrefixes(
    dispatcherPrefix: String,
    monitorPrefix: String,
    errors: StringWriter,
  ): Int {
    val dispatcherConfigFile = temporaryFolder.newFile()
    dispatcherConfigFile.writeText(
      TextFormat.printer()
        .printToString(vidConfigs(LABELED_OUTPUT_PATH, rawPrefix = dispatcherPrefix))
    )
    val monitorConfigFile = temporaryFolder.newFile()
    monitorConfigFile.writeText(
      TextFormat.printer().printToString(vidConfigs(LABELED_OUTPUT_PATH, rawPrefix = monitorPrefix))
    )
    val syncConfigFile = temporaryFolder.newFile()
    syncConfigFile.writeText(
      TextFormat.printer().printToString(syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH))
    )
    val watcherConfigFile = temporaryFolder.newFile()
    watcherConfigFile.writeText(
      TextFormat.printer().printToString(availabilityWatcherConfig(externalDoneRegex()))
    )

    val availabilityMonitorConfigFile = temporaryFolder.newFile()
    availabilityMonitorConfigFile.writeText(
      TextFormat.printer().printToString(monitoringConfigs(EXTERNAL_OUTPUT_PATH))
    )

    val process =
      ProcessBuilder(
          "${System.getProperty("java.home")}/bin/java",
          "-cp",
          System.getProperty("java.class.path"),
          ValidateVidLabelingStorageConfig::class.java.name,
          "--vid-labeling-config=${dispatcherConfigFile.path}",
          "--vid-labeling-monitor-config=${monitorConfigFile.path}",
          "--data-availability-sync-config=${syncConfigFile.path}",
          "--data-availability-monitor-config=${availabilityMonitorConfigFile.path}",
          "--data-watcher-config=${watcherConfigFile.path}",
        )
        .redirectErrorStream(true)
        .start()

    errors.write(process.inputStream.bufferedReader().use { it.readText() })
    return process.waitFor()
  }

  @Test
  fun `CLI validates deployment textprotos`() {
    val vidConfigFile = temporaryFolder.newFile("vid-labeling.textproto")
    vidConfigFile.writeText(TextFormat.printer().printToString(vidConfigs(LABELED_OUTPUT_PATH)))
    val syncConfigFile = temporaryFolder.newFile("data-availability.textproto")
    syncConfigFile.writeText(
      TextFormat.printer().printToString(syncConfigs(LABELED_OUTPUT_PATH, EXTERNAL_OUTPUT_PATH))
    )
    val availabilityMonitorConfigFile =
      temporaryFolder.newFile("data-availability-monitor.textproto")
    availabilityMonitorConfigFile.writeText(
      TextFormat.printer().printToString(monitoringConfigs(EXTERNAL_OUTPUT_PATH))
    )
    val watcherConfigFile = temporaryFolder.newFile("data-watcher.textproto")
    watcherConfigFile.writeText(
      TextFormat.printer().printToString(availabilityWatcherConfig(externalDoneRegex()))
    )

    val exitCode =
      CommandLine(ValidateVidLabelingStorageConfig())
        .execute(
          "--vid-labeling-config=${vidConfigFile.path}",
          "--vid-labeling-monitor-config=${vidConfigFile.path}",
          "--data-availability-sync-config=${syncConfigFile.path}",
          "--data-availability-monitor-config=${availabilityMonitorConfigFile.path}",
          "--data-watcher-config=${watcherConfigFile.path}",
        )

    assertThat(exitCode).isEqualTo(0)
  }

  @Test
  fun `validate requires an explicit internal path when VID labeling is configured`() {
    val syncConfigs = dataAvailabilitySyncConfigs {
      configs +=
        syncConfigs(LABELED_OUTPUT_PATH).configsList.single().copy {
          edpImpressionPath = LABELED_OUTPUT_PATH
          vidLabeledImpressionPath = ""
        }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs,
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("vid_labeled_impression_path is missing")
  }

  @Test
  fun `validate accepts an external-only provider with VID labeling disabled`() {
    val syncConfigs = dataAvailabilitySyncConfigs {
      configs +=
        syncConfigs(LABELED_OUTPUT_PATH).configsList.single().copy { vidLabeledImpressionPath = "" }
    }

    VidLabelingStorageConfigValidator.validate(
      vidLabelingConfigs {},
      vidLabelingConfigs {},
      syncConfigs,
      watcherConfig("gs://bucket/$OTHER_OUTPUT_PATH/model-line/.*/.*/done"),
    )
  }

  @Test
  fun `validate rejects a blank internal path`() {
    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs(" "),
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("vid_labeled_impression_path is missing")
  }

  @Test
  fun `CLI rejects VID labeling without an explicit internal availability path`() {
    val vidConfigFile = temporaryFolder.newFile("vid-labeling.textproto")
    vidConfigFile.writeText(TextFormat.printer().printToString(vidConfigs(LABELED_OUTPUT_PATH)))
    val syncConfigFile = temporaryFolder.newFile("data-availability.textproto")
    syncConfigFile.writeText(
      TextFormat.printer()
        .printToString(
          dataAvailabilitySyncConfigs {
            configs +=
              syncConfigs(LABELED_OUTPUT_PATH).configsList.single().copy {
                edpImpressionPath = LABELED_OUTPUT_PATH
                vidLabeledImpressionPath = ""
              }
          }
        )
    )
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

    assertThat(exitCode).isEqualTo(1)
  }

  @Test
  fun `validate rejects identical internal and external paths`() {
    val syncConfigs = dataAvailabilitySyncConfigs {
      configs +=
        syncConfigs(LABELED_OUTPUT_PATH).configsList.single().copy {
          edpImpressionPath = LABELED_OUTPUT_PATH
        }
    }

    val error =
      assertFailsWith<IllegalArgumentException> {
        VidLabelingStorageConfigValidator.validate(
          vidConfigs(LABELED_OUTPUT_PATH),
          vidConfigs(LABELED_OUTPUT_PATH),
          syncConfigs,
          watcherConfig("gs://bucket/edp/event-groups/.*"),
        )
      }

    assertThat(error).hasMessageThat().contains("paths overlap")
  }

  @Test
  fun `queued external route cannot match internal VID Labeler outputs`() {
    val watcher = dataWatcherConfig {
      watchedPaths += watchedPath {
        sourcePathRegex = "gs://bucket/$LABELED_OUTPUT_PATH/model-line/.*/.*/done"
        controlPlaneQueueSink = controlPlaneQueueSink {
          queue = "data-availability-sync-queue"
          appParams = Any.pack(externalDataAvailabilitySyncParams { dataProvider = DATA_PROVIDER })
        }
      }
    }
    assertFailsWith<IllegalArgumentException> {
      validate(
        vidConfigs(LABELED_OUTPUT_PATH),
        vidConfigs(LABELED_OUTPUT_PATH),
        syncConfigs(LABELED_OUTPUT_PATH),
        watcher,
      )
    }
  }

  @Test
  fun `queued external route on a separate prefix is accepted`() {
    val watcher = dataWatcherConfig {
      watchedPaths += watchedPath {
        sourcePathRegex = "gs://bucket/edp/external/model-line/.*/.*/done"
        controlPlaneQueueSink = controlPlaneQueueSink {
          queue = "data-availability-sync-queue"
          appParams = Any.pack(externalDataAvailabilitySyncParams { dataProvider = DATA_PROVIDER })
        }
      }
    }
    validate(
      vidConfigs(LABELED_OUTPUT_PATH),
      vidConfigs(LABELED_OUTPUT_PATH),
      syncConfigs(LABELED_OUTPUT_PATH),
      watcher,
    )
  }

  private fun vidConfigs(path: String, rawPrefix: String = RAW_INPUT_PATH) = vidLabelingConfigs {
    configs += vidLabelingConfig {
      dataProvider = DATA_PROVIDER
      edpImpressionPath = path
      rawImpressionsBlobPrefix = rawPrefix
      rawImpressionsStorageParams = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
      vidLabeledImpressionsStorageParams = storageParams {
        gcs = gcsStorage { bucketName = "bucket" }
      }
    }
  }

  private fun syncConfigs(
    path: String,
    externalPath: String = "",
    dataProvider: String = DATA_PROVIDER,
  ) = dataAvailabilitySyncConfigs {
    configs += dataAvailabilitySyncConfig {
      this.dataProvider = dataProvider
      edpImpressionPath = externalPath.ifEmpty { OTHER_OUTPUT_PATH }
      vidLabeledImpressionPath = path
      externalImpressionPath = externalPath
      dataAvailabilityStorage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
    }
  }

  private fun externalOnlySyncConfigs(
    path: String,
    externalPath: String = "",
    dataProvider: String = DATA_PROVIDER,
  ) = dataAvailabilitySyncConfigs {
    configs += dataAvailabilitySyncConfig {
      this.dataProvider = dataProvider
      edpImpressionPath = path
      externalImpressionPath = externalPath
      dataAvailabilityStorage = storageParams { gcs = gcsStorage { bucketName = "bucket" } }
    }
  }

  private fun monitoringConfigs(
    path: String? = null,
    bucket: String = "bucket",
    dataProvider: String = DATA_PROVIDER,
  ) = dataAvailabilityMonitorConfigs {
    if (path != null) {
      configs += dataAvailabilityMonitorConfig {
        dataProviderName = dataProvider
        edpImpressionPath = path
        storage = storageParams { gcs = gcsStorage { bucketName = bucket } }
      }
    }
  }

  private fun watcherConfig(regex: String) = dataWatcherConfig {
    watchedPaths += watchedPath {
      identifier = "unrelated"
      sourcePathRegex = regex
      httpEndpointSink = httpEndpointSink { endpointUri = "https://example.test/unrelated" }
    }
  }

  private fun availabilityWatcherConfig(regex: String) = dataWatcherConfig {
    watchedPaths += availabilityRoute(regex)
  }

  private fun availabilityRoute(
    regex: String,
    dataProvider: String = DATA_PROVIDER,
    identifier: String = "data-availability",
    typeUrl: String? = DATA_AVAILABILITY_SYNC_PARAMS_TYPE_URL,
  ) = watchedPath {
    this.identifier = identifier
    sourcePathRegex = regex
    httpEndpointSink = httpEndpointSink {
      endpointUri = "https://example.test/sync"
      appParams = struct {
        if (typeUrl != null) {
          fields["@type"] = value { stringValue = typeUrl }
        }
        fields["dataProvider"] = value { stringValue = dataProvider }
      }
    }
  }

  private fun legacyAvailabilityRoute(
    regex: String,
    syncConfig: DataAvailabilitySyncConfig,
    identifier: String = "data-availability",
  ) = watchedPath {
    this.identifier = identifier
    sourcePathRegex = regex
    httpEndpointSink = httpEndpointSink {
      endpointUri = "https://example.test/sync"
      appParams =
        Struct.newBuilder()
          .apply { JsonFormat.parser().merge(JsonFormat.printer().print(syncConfig), this) }
          .build()
    }
  }

  private fun queuedAvailabilityRoute(
    regex: String,
    queue: String = DataAvailabilitySyncWorkItems.QUEUE,
    serializationKey: String = "data-availability:$DATA_PROVIDER",
  ) = watchedPath {
    identifier = "data-availability"
    sourcePathRegex = regex
    controlPlaneQueueSink = controlPlaneQueueSink {
      this.queue = queue
      this.serializationKey = serializationKey
      appParams = Any.pack(externalDataAvailabilitySyncParams { dataProvider = DATA_PROVIDER })
    }
  }

  private fun externalDoneRegex(): String =
    "^gs://bucket/$EXTERNAL_OUTPUT_PATH/model-line/[^/]+/[^/]+/done$"

  private fun validate(
    dispatcherConfigs: VidLabelingConfigs,
    vidLabelingMonitorConfigs: VidLabelingConfigs,
    syncConfigs: DataAvailabilitySyncConfigs,
    watcherConfig: DataWatcherConfig,
    availabilityMonitorConfigs: DataAvailabilityMonitorConfigs = monitoringConfigs(),
  ) {
    VidLabelingStorageConfigValidator.validate(
      dispatcherConfigs,
      vidLabelingMonitorConfigs,
      syncConfigs,
      watcherConfig,
      availabilityMonitorConfigs,
    )
  }

  companion object {
    private const val DATA_PROVIDER = "dataProviders/edp7"
    private const val OTHER_DATA_PROVIDER = "dataProviders/other"
    private const val LABELED_OUTPUT_PATH = "edp/vid-labeler"
    private const val EXTERNAL_OUTPUT_PATH = "edp/external"
    private const val OTHER_OUTPUT_PATH = "edp/other"
    private const val RAW_INPUT_PATH = "raw-impressions/edp/edp7"
    private const val DATA_AVAILABILITY_SYNC_PARAMS_TYPE_URL =
      "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams"
  }
}
