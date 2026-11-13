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
import com.google.protobuf.util.JsonFormat
import java.nio.file.Path
import java.nio.file.Paths
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.DataAvailabilityMonitorConfigs
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.deploy.gcloud.vidlabeling.VidLabelingStorageConfigValidator
import org.wfanet.measurement.edpaggregator.v1alpha.EventGroupSyncParams
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
      val dispatcherConfigs =
        parseTextProto(
          resolveRunfile("$configDirectory/vid-labeling-dispatcher-config.textproto").toFile(),
          VidLabelingConfigs.getDefaultInstance(),
        )
      val monitorConfigs =
        parseTextProto(
          resolveRunfile("$configDirectory/vid-labeling-monitor-config.textproto").toFile(),
          VidLabelingConfigs.getDefaultInstance(),
        )
      VidLabelingStorageConfigValidator.validate(
        dispatcherConfigs,
        monitorConfigs,
        runtimeConfigs,
        watcherConfig,
      )
      val deleteConfig =
        parseTextProto(
          resolveRunfile("$configDirectory/data-watcher-delete-config.textproto").toFile(),
          DataWatcherConfig.getDefaultInstance(),
        )
      val availabilityMonitorConfigs =
        parseTextProto(
          resolveRunfile("$configDirectory/data-availability-monitor-config.textproto").toFile(),
          DataAvailabilityMonitorConfigs.getDefaultInstance(),
        )
      val runtimeConfigsByDataProvider = runtimeConfigs.configsList.associateBy { it.dataProvider }
      val dataAvailabilityWatchedPaths =
        watcherConfig.watchedPathsList.filter { it.identifier == DATA_AVAILABILITY_IDENTIFIER }

      assertWithMessage("$environment data availability watched paths")
        .that(dataAvailabilityWatchedPaths)
        .isNotEmpty()
      for (watchedPath in dataAvailabilityWatchedPaths) {
        val params = watchedPath.httpEndpointSink.appParams
        assertWithMessage("$environment ${watchedPath.sourcePathRegex} parameter fields")
          .that(params.fieldsMap.keys)
          .containsExactly(TYPE_FIELD, DATA_PROVIDER_FIELD)
        assertWithMessage("$environment ${watchedPath.sourcePathRegex} parameter type")
          .that(params.fieldsMap.getValue(TYPE_FIELD).stringValue)
          .isEqualTo(DATA_AVAILABILITY_SYNC_PARAMS_TYPE_URL)
        val dataProvider = params.fieldsMap.getValue(DATA_PROVIDER_FIELD).stringValue
        assertWithMessage("$environment runtime config for $dataProvider")
          .that(runtimeConfigsByDataProvider)
          .containsKey(dataProvider)

        val selectedConfig =
          ConfigLoader.buildDataAvailabilitySyncConfig(
            JsonFormat.printer().print(params),
            runtimeConfigs.configsList,
          )

        assertThat(selectedConfig).isEqualTo(runtimeConfigsByDataProvider.getValue(dataProvider))
        assertWithMessage("$environment Secure Computation connection for $dataProvider")
          .that(selectedConfig.hasSecureComputationControlPlaneConnection())
          .isTrue()
        assertThat(selectedConfig.secureComputationControlPlaneConnection.certFilePath)
          .endsWith("/edpa_tee_app_tls.pem")
        assertThat(selectedConfig.secureComputationControlPlaneConnection.privateKeyFilePath)
          .endsWith("/edpa_tee_app_tls.key")
        assertThat(selectedConfig.secureComputationControlPlaneConnection.certCollectionFilePath)
          .endsWith("/secure_computation_root.pem")
        assertThat(selectedConfig.secureComputationControlPlaneConnection)
          .isNotEqualTo(selectedConfig.impressionMetadataStorageConnection)
      }

      for (config in runtimeConfigs.configsList) {
        val bucket = config.dataAvailabilityStorage.gcs.bucketName
        val externalDoneUri =
          "gs://$bucket/${config.edpImpressionPath}/model-line/test-model-line/2000-01-01/done"
        val createRoutes =
          dataAvailabilityWatchedPaths.filter { Regex(it.sourcePathRegex).matches(externalDoneUri) }
        assertWithMessage("$environment create route for ${config.dataProvider}")
          .that(createRoutes)
          .hasSize(1)
        assertThat(
            createRoutes
              .single()
              .httpEndpointSink
              .appParams
              .fieldsMap
              .getValue(DATA_PROVIDER_FIELD)
              .stringValue
          )
          .isEqualTo(config.dataProvider)
        assertThat(createRoutes.single().httpEndpointSink.endpointUri)
          .endsWith("/data-availability-sync")

        val vidConfig =
          dispatcherConfigs.configsList.singleOrNull { it.dataProvider == config.dataProvider }
            ?: continue
        assertWithMessage("$environment internal path for ${config.dataProvider}")
          .that(config.vidLabeledImpressionPath)
          .isNotEmpty()
        val internalDoneUri =
          "gs://$bucket/${config.vidLabeledImpressionPath}/model-line/test-model-line/2000-01-01/done"
        assertWithMessage("$environment excludes internal create events for ${config.dataProvider}")
          .that(
            watcherConfig.watchedPathsList.none {
              Regex(it.sourcePathRegex).matches(internalDoneUri)
            }
          )
          .isTrue()
        val vidMonitorConfig =
          monitorConfigs.configsList.single { it.dataProvider == config.dataProvider }
        assertThat(vidConfig.edpImpressionPath).isEqualTo(config.vidLabeledImpressionPath)
        assertThat(vidMonitorConfig.edpImpressionPath).isEqualTo(config.vidLabeledImpressionPath)
        assertThat(vidConfig.vidLabeledImpressionsStorageParams.gcs.bucketName).isEqualTo(bucket)
        assertThat(vidMonitorConfig.vidLabeledImpressionsStorageParams.gcs.bucketName)
          .isEqualTo(bucket)

        for (path in listOf(config.edpImpressionPath, config.vidLabeledImpressionPath)) {
          val deletedUri = "gs://$bucket/$path/model-line/test-model-line/2000-01-01/impressions"
          val routes =
            deleteConfig.watchedPathsList.filter { Regex(it.sourcePathRegex).matches(deletedUri) }
          assertWithMessage("$environment delete route for $path").that(routes).hasSize(1)
          val route = routes.single()
          assertThat(route.identifier).isEqualTo("data-availability-cleanup")
          assertThat(route.httpEndpointSink.endpointUri).endsWith("/data-availability-cleanup")
          val cleanupConfig =
            ConfigLoader.buildDataAvailabilitySyncConfig(
              JsonFormat.printer().print(route.httpEndpointSink.appParams),
              runtimeConfigs.configsList,
            )
          assertThat(cleanupConfig.dataProvider).isEqualTo(config.dataProvider)
          assertThat(cleanupConfig.dataAvailabilityStorage.gcs.bucketName).isEqualTo(bucket)
          assertThat(
              deleteConfig.watchedPathsList.none {
                Regex(it.sourcePathRegex)
                  .matches(
                    "gs://unconfigured-test-bucket/$path/model-line/test-model-line/2000-01-01/impressions"
                  )
              }
            )
            .isTrue()
          val pathMonitors =
            availabilityMonitorConfigs.configsList.filter { it.edpImpressionPath == path }
          assertWithMessage("$environment availability monitor for $path")
            .that(pathMonitors)
            .hasSize(1)
          assertThat(pathMonitors.single().storage.gcs.bucketName).isEqualTo(bucket)
        }
      }
    }
  }

  private fun resolveRunfile(relativePath: String): Path =
    checkNotNull(RUNFILES.getRuntimePath(Paths.get("wfa_measurement_system", relativePath))) {
      "Runfile not found: $relativePath"
    }

  companion object {
    private const val DATA_AVAILABILITY_IDENTIFIER = "data-availability"
    private const val DATA_PROVIDER_FIELD = "dataProvider"
    private const val TYPE_FIELD = "@type"
    private const val DATA_AVAILABILITY_SYNC_PARAMS_TYPE_URL =
      "type.googleapis.com/wfa.measurement.edpaggregator.v1alpha.DataAvailabilitySyncParams"
    private val ENVIRONMENTS = listOf("dev", "head", "qa")
    private val RUNFILES = Runfiles.preload().withSourceRepository("")
    private val TYPE_REGISTRY =
      TypeRegistry.newBuilder()
        .add(ResultsFulfillerParams.getDescriptor())
        .add(EventGroupSyncParams.getDescriptor())
        .build()
  }
}
