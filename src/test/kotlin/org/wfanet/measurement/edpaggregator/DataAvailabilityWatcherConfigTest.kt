// Copyright 2026 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     https://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.edpaggregator

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import com.google.devtools.build.runfiles.Runfiles
import com.google.protobuf.TypeRegistry
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling.VidLabelingStorageConfigValidator
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.ExternalDataAvailabilitySyncParams
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

@RunWith(JUnit4::class)
class DataAvailabilityWatcherConfigTest {

  @Test
  fun `data availability watcher events select runtime configs`() {
    for (environment in ENVIRONMENTS) {
      val configDirectory = "src/main/terraform/gcloud/cmms/envs/$environment"
      val watcherConfig =
        parseTextProto(
          resolveRunfile("$configDirectory/data-watcher-config.textproto").toFile(),
          DataWatcherConfig.getDefaultInstance(),
          TYPE_REGISTRY,
        )
      val runtimeConfigs =
        parseTextProto(
          resolveRunfile("$configDirectory/data-availability-sync-config.textproto").toFile(),
          DataAvailabilitySyncConfigs.getDefaultInstance(),
        )
      val runtimeConfigsByDataProvider = runtimeConfigs.configsList.associateBy { it.dataProvider }
      val dataAvailabilityWatchedPaths =
        watcherConfig.watchedPathsList.filter { it.identifier == DATA_AVAILABILITY_IDENTIFIER }

      assertWithMessage("$environment data availability watched paths")
        .that(dataAvailabilityWatchedPaths)
        .isNotEmpty()
      for (watchedPath in dataAvailabilityWatchedPaths) {
        assertThat(watchedPath.hasControlPlaneQueueSink()).isTrue()
        val sink = watchedPath.controlPlaneQueueSink
        assertThat(sink.queue).isEqualTo("data-availability-sync-queue")
        assertThat(sink.appParams.`is`(ExternalDataAvailabilitySyncParams::class.java)).isTrue()
        val params = sink.appParams.unpack(ExternalDataAvailabilitySyncParams::class.java)
        val dataProvider = params.dataProvider
        assertThat(sink.serializationKey).isEqualTo("data-availability:$dataProvider")
        assertWithMessage("$environment runtime config for $dataProvider")
          .that(runtimeConfigsByDataProvider)
          .containsKey(dataProvider)
        val config = runtimeConfigsByDataProvider.getValue(dataProvider)
        val prefix = config.externalImpressionPath.ifEmpty { config.edpImpressionPath }
        val example =
          "gs://${config.dataAvailabilityStorage.gcs.bucketName}/$prefix/model-line/example/2000-01-01/done"
        assertWithMessage("$environment external route for $dataProvider")
          .that(watchedPath.sourcePathRegex.toRegex().matches(example))
          .isTrue()
      }
      VidLabelingStorageConfigValidator.validate(
        parseTextProto(
          resolveRunfile("$configDirectory/vid-labeling-dispatcher-config.textproto").toFile(),
          VidLabelingConfigs.getDefaultInstance(),
        ),
        parseTextProto(
          resolveRunfile("$configDirectory/vid-labeling-monitor-config.textproto").toFile(),
          VidLabelingConfigs.getDefaultInstance(),
        ),
        runtimeConfigs,
        watcherConfig,
      )
    }
  }

  private fun resolveRunfile(relativePath: String): Path =
    checkNotNull(RUNFILES.getRuntimePath(Paths.get("wfa_measurement_system", relativePath))) {
      "Runfile not found: $relativePath"
    }

  companion object {
    private const val DATA_AVAILABILITY_IDENTIFIER = "data-availability"
    private val ENVIRONMENTS = listOf("dev", "head", "qa")
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
    private val TYPE_REGISTRY =
      TypeRegistry.newBuilder()
        .add(ResultsFulfillerParams.getDescriptor())
        .add(EventGroupSyncParams.getDescriptor())
        .add(ExternalDataAvailabilitySyncParams.getDescriptor())
        .build()
  }
}
