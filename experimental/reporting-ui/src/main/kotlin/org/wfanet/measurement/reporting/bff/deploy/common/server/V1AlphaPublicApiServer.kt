// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.bff.deploy.common.server

import io.grpc.Channel
import io.grpc.ServerServiceDefinition
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.reporting.bff.service.api.v1alpha.ReportsService
import org.wfanet.measurement.reporting.bff.v1alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub
import org.wfanet.measurement.reporting.bff.v1alpha.ReportsGrpcKt.ReportsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt as HaloEventGroupsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt as HaloReportsGrpcKt
import picocli.CommandLine

private const val SERVER_NAME = "V1AlphaPublicUiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Ui server daemon for Reporting v1alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile
    )
  val channel: Channel =
    buildMutualTlsChannel(
        reportingApiServerFlags.reportingApiFlags.target,
        clientCerts,
        reportingApiServerFlags.reportingApiFlags.certHost
      )
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val services: List<ServerServiceDefinition> =
    listOf(
      ReportsService(HaloReportsGrpcKt.ReportsCoroutineStub(channel)).bindService()
      EventGroupsService(HaloEventGroupsGrpcKt.EventGroupsCoroutineStub(channel)).bindService()
    )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
