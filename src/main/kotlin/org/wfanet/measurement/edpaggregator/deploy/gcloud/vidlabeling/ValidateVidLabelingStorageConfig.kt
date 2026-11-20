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

import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.DataAvailabilitySyncConfigs
import org.wfanet.measurement.config.edpaggregator.VidLabelingConfigs
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import picocli.CommandLine

/** Validates labeled-output storage routing before deployment. */
@CommandLine.Command(name = "ValidateVidLabelingStorageConfig")
class ValidateVidLabelingStorageConfig : Runnable {
  @CommandLine.Option(names = ["--vid-labeling-config"], required = true)
  private lateinit var vidLabelingConfigFile: File

  @CommandLine.Option(names = ["--vid-labeling-monitor-config"], required = true)
  private lateinit var vidLabelingMonitorConfigFile: File

  @CommandLine.Option(names = ["--data-availability-sync-config"], required = true)
  private lateinit var dataAvailabilitySyncConfigFile: File

  @CommandLine.Option(names = ["--data-watcher-config"], required = true)
  private lateinit var dataWatcherConfigFile: File

  override fun run() {
    val dispatcherConfigs =
      parseTextProto(vidLabelingConfigFile, VidLabelingConfigs.getDefaultInstance())
    val monitorConfigs =
      parseTextProto(vidLabelingMonitorConfigFile, VidLabelingConfigs.getDefaultInstance())
    val syncConfigs =
      parseTextProto(
        dataAvailabilitySyncConfigFile,
        DataAvailabilitySyncConfigs.getDefaultInstance(),
      )
    val typeRegistry = TypeRegistry.newBuilder().add(ResultsFulfillerParams.getDescriptor()).build()
    val watcherConfig =
      parseTextProto(dataWatcherConfigFile, DataWatcherConfig.getDefaultInstance(), typeRegistry)
    VidLabelingStorageConfigValidator.validate(
      dispatcherConfigs,
      monitorConfigs,
      syncConfigs,
      watcherConfig,
    )
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ValidateVidLabelingStorageConfig(), args)
  }
}
