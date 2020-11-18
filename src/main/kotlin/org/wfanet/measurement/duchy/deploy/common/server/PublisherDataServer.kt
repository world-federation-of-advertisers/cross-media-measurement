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

package org.wfanet.measurement.duchy.deploy.common.server

import org.wfanet.measurement.api.v1alpha.DataProviderRegistrationGrpcKt.DataProviderRegistrationCoroutineStub
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildChannel
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.duchy.DuchyPublicKeys
import org.wfanet.measurement.duchy.deploy.common.CommonDuchyFlags
import org.wfanet.measurement.duchy.service.api.v1alpha.PublisherDataService
import org.wfanet.measurement.internal.duchy.MetricValuesGrpcKt.MetricValuesCoroutineStub
import org.wfanet.measurement.system.v1alpha.RequisitionGrpcKt.RequisitionCoroutineStub as SystemRequisitionCoroutineStub
import picocli.CommandLine

private const val SERVICE_NAME = "PublisherData"
private const val SERVER_NAME = "${SERVICE_NAME}Server"

private class Flags {
  @CommandLine.Mixin
  lateinit var duchy: CommonDuchyFlags
    private set

  @CommandLine.Mixin
  lateinit var duchyPublicKeys: DuchyPublicKeys.Flags
    private set

  @CommandLine.Mixin
  lateinit var server: CommonServer.Flags
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
    names = ["--system-requisition-service-target"],
    description = [
      "gRPC target (authority string or URI) for Requisition service in the system API."
    ],
    required = true
  )
  lateinit var systemRequisitionServiceTarget: String
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
  name = SERVER_NAME,
  description = ["Server daemon for $SERVICE_NAME service."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(@CommandLine.Mixin flags: Flags) {
  val metricValuesClient = MetricValuesCoroutineStub(buildChannel(flags.metricValuesServiceTarget))
  val requisitionClient =
    RequisitionCoroutineStub(buildChannel(flags.requisitionServiceTarget))
      .withDuchyId(flags.duchy.duchyName)
  val systemRequisitionClient =
    SystemRequisitionCoroutineStub(buildChannel(flags.systemRequisitionServiceTarget))
      .withDuchyId(flags.duchy.duchyName)
  val registrationClient =
    DataProviderRegistrationCoroutineStub(buildChannel(flags.registrationServiceTarget))

  val service = PublisherDataService(
    metricValuesClient,
    requisitionClient,
    systemRequisitionClient,
    registrationClient,
    DuchyPublicKeys.fromFlags(flags.duchyPublicKeys)
  )

  CommonServer
    .fromFlags(flags.server, SERVER_NAME, service)
    .start()
    .blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
