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

package org.wfanet.measurement.edpaggregator.deploy.gcloud.requisitionfetcher

import com.google.protobuf.TypeRegistry
import java.io.File
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.edpaggregator.RequisitionFetcherConfig
import org.wfanet.measurement.config.securecomputation.DataWatcherConfig
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams
import picocli.CommandLine

/** Validates the deployed RequisitionFetcher and DataWatcher configs before direct dispatch. */
@CommandLine.Command(name = "ValidateRequisitionFetcherConfig")
class ValidateRequisitionFetcherConfig : Runnable {
  @CommandLine.Option(
    names = ["--requisition-fetcher-config"],
    description = ["Path to the RequisitionFetcher config textproto."],
    required = true,
  )
  private lateinit var requisitionFetcherConfigFile: File

  @CommandLine.Option(
    names = ["--data-watcher-config"],
    description = ["Path to the deployed DataWatcher config textproto."],
    required = true,
  )
  private lateinit var dataWatcherConfigFile: File

  @CommandLine.Option(
    names = ["--control-plane-target"],
    description = ["Secure Computation public API target."],
    required = true,
  )
  private lateinit var controlPlaneTarget: String

  override fun run() {
    val requisitionFetcherConfig =
      parseTextProto(requisitionFetcherConfigFile, RequisitionFetcherConfig.getDefaultInstance())
    val typeRegistry = TypeRegistry.newBuilder().add(ResultsFulfillerParams.getDescriptor()).build()
    val dataWatcherConfig =
      parseTextProto(dataWatcherConfigFile, DataWatcherConfig.getDefaultInstance(), typeRegistry)
    RequisitionFetcherConfigValidator.validate(
      requisitionFetcherConfig,
      controlPlaneTarget,
      dataWatcherConfig,
    )
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(ValidateRequisitionFetcherConfig(), args)
  }
}
