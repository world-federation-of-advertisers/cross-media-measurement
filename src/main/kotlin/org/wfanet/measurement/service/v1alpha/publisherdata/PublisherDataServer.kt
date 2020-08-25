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

import io.grpc.ManagedChannel
import io.grpc.ManagedChannelBuilder
import kotlin.properties.Delegates
import org.wfanet.measurement.api.v1alpha.DataProviderRegistrationGrpcKt.DataProviderRegistrationCoroutineStub
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.CommonServer
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import picocli.CommandLine

private class Flags {
  // TODO: extract to a common flag mixin to share with Herald
  @set:CommandLine.Option(
    names = ["--duchy-name"],
    description = ["Stable unique Duchy identifier."],
    required = true
  )
  var duchyName: String by Delegates.notNull()
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
private fun run(
  @CommandLine.Mixin flags: Flags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags
) {
  val metricValuesClient = MetricValuesCoroutineStub(makeChannel(flags.metricValuesServiceTarget))

  val requisitionClient =
    RequisitionCoroutineStub(makeChannel(flags.requisitionServiceTarget))
      .withDuchyId(flags.duchyName)

  val registrationClient =
    DataProviderRegistrationCoroutineStub(makeChannel(flags.registrationServiceTarget))

  val service = PublisherDataService(metricValuesClient, requisitionClient, registrationClient)

  CommonServer
    .fromFlags(commonServerFlags, "PublisherDataServer", service)
    .start()
    .blockUntilShutdown()
}

private fun makeChannel(target: String): ManagedChannel =
  ManagedChannelBuilder.forTarget(target).build()

fun main(args: Array<String>) = commandLineMain(::run, args)
