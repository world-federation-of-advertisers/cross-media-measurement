// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.service.v1alpha.publisherdata

import io.grpc.ManagedChannelBuilder
import kotlin.properties.Delegates
import org.wfanet.measurement.api.v1alpha.DataProviderRegistrationGrpcKt.DataProviderRegistrationCoroutineStub
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import picocli.CommandLine

private class Flags {
  @set:CommandLine.Option(
    names = ["--port", "-p"],
    description = ["TCP port for gRPC server."],
    defaultValue = "8080"
  )
  var port by Delegates.notNull<Int>()
    private set

  @CommandLine.Option(
    names = ["--server-name"],
    description = ["Name of the gRPC server for logging purposes."],
    defaultValue = "PublisherDataServer"
  )
  lateinit var nameForLogging: String
    private set

  @CommandLine.Option(
    names = ["--metric-values-service-target"],
    description = ["gRPC target (authority string or URI) for MetricValues service."],
    required = true
  )
  lateinit var metricValuesServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--requisition-service-target"],
    description = ["gRPC target (authority string or URI) for Requisition service."],
    required = true
  )
  lateinit var requisitionServiceTarget: String
    private set

  @CommandLine.Option(
    names = ["--registration-service-target"],
    description = ["gRPC target (authority string or URI) for DataProviderRegistration service."],
    required = true
  )
  lateinit var registrationServiceTarget: String
    private set
}

@CommandLine.Command(
  name = "PublisherDataServer",
  description = ["Run server daemon for PublisherData service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  val metricValuesClient = MetricValuesCoroutineStub(
    ManagedChannelBuilder.forTarget(flags.metricValuesServiceTarget).build()
  )
  val requisitionClient = RequisitionCoroutineStub(
    ManagedChannelBuilder.forTarget(flags.requisitionServiceTarget).build()
  )
  val registrationClient = DataProviderRegistrationCoroutineStub(
    ManagedChannelBuilder.forTarget(flags.registrationServiceTarget).build()
  )

  CommonServer(
    flags.nameForLogging,
    flags.port,
    PublisherDataService(metricValuesClient, requisitionClient, registrationClient)
  ).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
