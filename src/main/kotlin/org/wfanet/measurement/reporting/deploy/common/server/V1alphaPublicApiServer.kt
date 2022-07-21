// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.common.server

import io.grpc.ServerServiceDefinition
import java.io.File
import org.wfanet.measurement.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.TextprotoFilePrincipalLookup
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.common.identity.TextprotoFileApiKeyLookup
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetsService
import picocli.CommandLine

private const val SERVER_NAME = "V1alphaPublicApiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Reporting v1alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin v1alphaFlags: V1alphaFlags
) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile
    )
  val channel =
    buildMutualTlsChannel(
        reportingApiServerFlags.internalApiFlags.target,
        clientCerts,
        reportingApiServerFlags.internalApiFlags.certHost
      )
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val principalLookup =
    TextprotoFilePrincipalLookup(v1alphaFlags.authorityKeyIdentifierToPrincipalMapFile)

  //val apiKeyLookup =
  //  TextprotoFileApiKeyLookup(v1alphaFlags.measurementConsumerToApiKeyMapFile)

  val services: List<ServerServiceDefinition> =
    listOf(
      ReportingSetsService(InternalReportingSetsCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)

/** Flags specific to the V1alpha API version. */
private class V1alphaFlags {
  @CommandLine.Option(
    names = ["--authority-key-identifier-to-principal-map-file"],
    description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
    required = true,
  )
  lateinit var authorityKeyIdentifierToPrincipalMapFile: File
    private set

  @CommandLine.Option(
    names = ["--measurement-consumer-to-api-key-map-file"],
    description = ["File path to a MeasurementConsumerToApiKeyMap textproto"],
    required = true,
  )
  lateinit var measurementConsumerToApiKeyMapFile: File
    private set
}
