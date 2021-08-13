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
import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.anysketch.SketchConfig
import org.wfanet.measurement.api.v2alpha.RequisitionFulfillmentGrpcKt.RequisitionFulfillmentCoroutineStub
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildPlaintextChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import picocli.CommandLine

abstract class EdpSimulator : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: Flags
    private set

  private fun run(@CommandLine.Mixin flags: Flags) {}

  protected class Flags {

    @CommandLine.Option(names = ["--combined-public-keys-service-target"], required = true)
    lateinit var combinedPublicKeysServiceTarget: String
      private set

    @CommandLine.Option(names = ["--sketch-configs-service-target"], required = true)
    lateinit var sketchConfigsServiceTarget: String
      private set

    @CommandLine.Option(names = ["--external-data-provider-id"], required = true)
    lateinit var externalDataProviderId: String
      private set

    @CommandLine.Option(names = ["--throttler-minimum-interval"], defaultValue = "1s")
    lateinit var throttlerMinimumInterval: Duration
      private set

    @CommandLine.Mixin
    lateinit var server: CommonServer.Flags
      private set

    @CommandLine.Option(
      names = ["--computations-service-target"],
      description =
        ["gRPC target (authority string or URI) for Duchy internal Computations service."],
      required = true
    )
    lateinit var computationsServiceTarget: String
      private set

    @CommandLine.Option(
      names = ["--system-requisitions-service-target"],
      description =
        ["gRPC target (authority string or URI) for Requisitions service in the system API."],
      required = true
    )
    lateinit var systemRequisitionsServiceTarget: String
      private set

    val requisitionsStub by lazy {
      RequisitionsCoroutineStub(
        buildPlaintextChannel(systemRequisitionsServiceTarget)
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

    @CommandLine.Option(names = ["--requisition-fulfillment-service-target"], required = true)
    lateinit var requisitionFulfillmentServiceTarget: String
      private set

    val requisitionFulfillmentStub by lazy {
      RequisitionFulfillmentCoroutineStub(
        buildPlaintextChannel(requisitionFulfillmentServiceTarget)
          .withVerboseLogging(debugVerboseGrpcClientLogging)
      )
    }

    @set:CommandLine.Option(
      names = ["--debug-verbose-grpc-client-logging"],
      description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
      defaultValue = "false"
    )
    var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
      private set
  }

  companion object {
    const val SERVICE_NAME = "DataProvider"
  }
}
