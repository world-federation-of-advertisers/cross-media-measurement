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

// import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as
// KingdomCertificatesCoroutineStub
// import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as
// KingdomMeasurementsCoroutineStub
// import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as
// InternalMeasurementsCoroutineStub
// import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as
// InternalReportsCoroutineStub
// import org.wfanet.measurement.reporting.service.api.v1alpha.EncryptionKeyPairStore
// import org.wfanet.measurement.reporting.service.api.v1alpha.ReportsService
// import
// org.wfanet.measurement.reporting.service.api.v1alpha.withMeasurementConsumerServerInterceptor
import io.grpc.Channel
import io.grpc.ServerServiceDefinition
import java.io.File
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as KingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.TextprotoFilePrincipalLookup
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.deploy.common.EncryptionKeyPairMap
import org.wfanet.measurement.reporting.deploy.common.KingdomApiFlags
import org.wfanet.measurement.reporting.service.api.v1alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v1alpha.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v1alpha.TextprotoFileMeasurementConsumerConfigLookup
import picocli.CommandLine

private const val SERVER_NAME = "V1AlphaPublicApiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Reporting v1alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin kingdomApiFlags: KingdomApiFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin v1alphaFlags: V1alphaFlags,
  @CommandLine.Mixin encryptionKeyPairMap: EncryptionKeyPairMap,
) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile
    )
  val channel: Channel =
    buildMutualTlsChannel(
        reportingApiServerFlags.internalApiFlags.target,
        clientCerts,
        reportingApiServerFlags.internalApiFlags.certHost
      )
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val kingdomChannel: Channel =
    buildMutualTlsChannel(kingdomApiFlags.target, clientCerts, kingdomApiFlags.target)
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val principalLookup =
    TextprotoFilePrincipalLookup(v1alphaFlags.authorityKeyIdentifierToPrincipalMapFile)

  val configLookup =
    TextprotoFileMeasurementConsumerConfigLookup(v1alphaFlags.measurementConsumerConfigFile)

  // TODO(@tristanvuong2021): update when #639 and #640 are merged in
  val services: List<ServerServiceDefinition> =
    listOf(
      EventGroupsService(
          KingdomEventGroupsCoroutineStub(kingdomChannel),
          KingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomChannel),
          InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs)
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      ReportingSetsService(InternalReportingSetsCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      /*
      ReportsService(
        InternalReportsCoroutineStub(channel),
        InternalMeasurementsCoroutineStub(channel),
        KingdomMeasurementsCoroutineStub(kingdomChannel),
        KingdomCertificatesCoroutineStub(kingdomChannel),
        InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
        null,
        null
      )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withMeasurementConsumerServerInterceptor(configLookup),
       */
      )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)

/** Flags specific to the V1Alpha API version. */
private class V1alphaFlags {
  @CommandLine.Option(
    names = ["--authority-key-identifier-to-principal-map-file"],
    description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
    required = true,
  )
  lateinit var authorityKeyIdentifierToPrincipalMapFile: File
    private set

  @CommandLine.Option(
    names = ["--measurement-consumer-config-file"],
    description = ["File path to a MeasurementConsumerConfig textproto"],
    required = true,
  )
  lateinit var measurementConsumerConfigFile: File
    private set
}
