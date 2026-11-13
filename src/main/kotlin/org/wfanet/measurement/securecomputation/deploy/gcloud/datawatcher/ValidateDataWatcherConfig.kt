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

package org.wfanet.measurement.securecomputation.deploy.gcloud.datawatcher

import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import org.wfanet.measurement.securecomputation.datawatcher.DataWatcherConfigValidator
import picocli.CommandLine

/** Validates the deployed DataWatcher configuration. */
@CommandLine.Command(name = "ValidateDataWatcherConfig")
class ValidateDataWatcherConfig : Runnable {
  @CommandLine.Option(
    names = ["--data-watcher-config"],
    description = ["Path to the deployed DataWatcher config textproto."],
    required = true,
  )
  private lateinit var dataWatcherConfigFile: File

  override fun run() {
    val typeRegistry = TypeRegistry.newBuilder().add(ResultsFulfillerParams.getDescriptor()).build()
    val dataWatcherConfig =
      parseTextProto(dataWatcherConfigFile, DataWatcherConfig.getDefaultInstance(), typeRegistry)
    DataWatcherConfigValidator.validate(dataWatcherConfig)
  }

  companion object {
    @JvmStatic fun main(args: Array<String>) = commandLineMain(ValidateDataWatcherConfig(), args)
  }
}
