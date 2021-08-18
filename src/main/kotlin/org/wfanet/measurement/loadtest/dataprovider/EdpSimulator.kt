// Copyright 2021 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.loadtest.dataprovider

import java.io.File
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.anysketch.SketchConfig
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildPlaintextChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.loadtest.KingdomPublicApiFlags
import org.wfanet.measurement.loadtest.RequisitionFulfillmentServiceFlags
import org.wfanet.measurement.storage.StorageClient
import picocli.CommandLine

/** Converts a kingdom internal Requisition to system Api Requisition. */
abstract class EdpSimulator : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  protected fun run(storageClient: StorageClient) {
    //  override fun run() {
    val throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.throttlerMinimumInterval)

    val workflow =
      RequisitionFulfillmentWorkflow(
        flags.externalDataProviderId,
        flags.sketchConfig,
        flags.requisitionsStub,
        flags.requisitionFulfillmentStub,
        storageClient,
      )

    runBlocking { throttler.loopOnReady { workflow.execute() } }
  }

  protected class Flags {

    @CommandLine.Option(names = ["--external-data-provider-id"], required = true)
    lateinit var externalDataProviderId: String
      private set

    @CommandLine.Option(names = ["--throttler-minimum-interval"], defaultValue = "1s")
    lateinit var throttlerMinimumInterval: Duration
      private set

    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Mixin
    lateinit var kingdomPublicApiFlags: KingdomPublicApiFlags
      private set

    val requisitionsStub by lazy {
      RequisitionsCoroutineStub(
        buildPlaintextChannel(kingdomPublicApiFlags.target)
          .withVerboseLogging(debugVerboseGrpcClientLogging)
      )
    }

    @CommandLine.Mixin
    lateinit var requisitionFulfillmentServiceFlags: RequisitionFulfillmentServiceFlags
      private set

    val requisitionFulfillmentStub by lazy {
      RequisitionFulfillmentCoroutineStub(
        buildPlaintextChannel(requisitionFulfillmentServiceFlags.target)
          .withVerboseLogging(debugVerboseGrpcClientLogging)
      )
    }

    @CommandLine.Option(
      names = ["--sketch-config-file"],
      description = ["File path for SketchConfig proto message in text format."],
      defaultValue = "config/liquid_legions_sketch_config.textproto"
    )
    lateinit var sketchConfigFile: File
      private set

    val sketchConfig by lazy { parseTextProto(sketchConfigFile, SketchConfig.getDefaultInstance()) }

    @set:CommandLine.Option(
      names = ["--debug-verbose-grpc-client-logging"],
      description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
      defaultValue = "false"
    )
    var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
      private set
  }

  companion object {
    const val DAEMON_NAME = "EdpSimulator"
  }
}
