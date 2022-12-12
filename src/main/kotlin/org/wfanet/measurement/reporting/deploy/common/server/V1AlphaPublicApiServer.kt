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

import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.ServerServiceDefinition
import java.io.File
import java.security.SecureRandom
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as KingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as KingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as KingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as KingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as KingdomMeasurementsCoroutineStub
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.api.memoizing
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.reporting.deploy.common.EncryptionKeyPairMap
import org.wfanet.measurement.reporting.deploy.common.KingdomApiFlags
import org.wfanet.measurement.reporting.service.api.v1alpha.AkidPrincipalLookup
import org.wfanet.measurement.reporting.service.api.v1alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v1alpha.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingPrincipal
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportsService
import org.wfanet.measurement.reporting.service.api.v1alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
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
  @CommandLine.Mixin v1AlphaFlags: V1AlphaFlags,
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

  val principalLookup: PrincipalLookup<ReportingPrincipal, ByteString> =
    AkidPrincipalLookup(
        v1AlphaFlags.authorityKeyIdentifierToPrincipalMapFile,
        v1AlphaFlags.measurementConsumerConfigFile
      )
      .memoizing()

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
      ReportsService(
          InternalReportsCoroutineStub(channel),
          InternalReportingSetsCoroutineStub(channel),
          InternalMeasurementsCoroutineStub(channel),
          KingdomDataProvidersCoroutineStub(kingdomChannel),
          KingdomMeasurementConsumersCoroutineStub(kingdomChannel),
          KingdomMeasurementsCoroutineStub(kingdomChannel),
          KingdomCertificatesCoroutineStub(kingdomChannel),
          InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
          SecureRandom(),
          v1AlphaFlags.signingPrivateKeyStoreDir,
          commonServerFlags.tlsFlags.signingCerts.trustedCertificates
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
    )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)

/** Flags specific to the V1Alpha API version. */
private class V1AlphaFlags {
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

  @CommandLine.Option(
    names = ["--signing-private-key-store-dir"],
    description = ["File path to the signing private key store directory"],
    required = true,
  )
  lateinit var signingPrivateKeyStoreDir: File
    private set
}
