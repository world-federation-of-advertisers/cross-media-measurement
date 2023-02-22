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

package org.wfanet.measurement.kingdom.deploy.common.server

import io.grpc.ServerServiceDefinition
import java.io.File
import org.wfanet.measurement.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub as InternalApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as InternalEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub as InternalPublicKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.service.api.v2alpha.AccountsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ApiKeysService
import org.wfanet.measurement.kingdom.service.api.v2alpha.CertificatesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementConsumersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.PublicKeysService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.withAccountAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.api.v2alpha.withApiKeyAuthenticationServerInterceptor
import picocli.CommandLine

private const val SERVER_NAME = "V2alphaPublicApiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Kingdom v2alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin llv2ProtocolConfigFlags: Llv2ProtocolConfigFlags,
  @CommandLine.Mixin v2alphaFlags: V2alphaFlags,
  @CommandLine.Mixin duchyInfoFlags: DuchyInfoFlags,
) {
  Llv2ProtocolConfig.initializeFromFlags(llv2ProtocolConfigFlags)
  DuchyInfo.initializeFromFlags(duchyInfoFlags)

  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile
    )
  val channel =
    buildMutualTlsChannel(
        kingdomApiServerFlags.internalApiFlags.target,
        clientCerts,
        kingdomApiServerFlags.internalApiFlags.certHost
      )
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)
      .withDefaultDeadline(kingdomApiServerFlags.internalApiFlags.defaultDeadlineDuration)

  val principalLookup = AkidPrincipalLookup(v2alphaFlags.authorityKeyIdentifierToPrincipalMapFile)

  val internalAccountsCoroutineStub = InternalAccountsCoroutineStub(channel)
  val internalApiKeysCoroutineStub = InternalApiKeysCoroutineStub(channel)
  val internalExchangeStepsCoroutineStub = InternalExchangeStepsCoroutineStub(channel)

  // TODO: do we need something similar to .withDuchyIdentities() for EDP and MC?
  val services: List<ServerServiceDefinition> =
    listOf(
      AccountsService(internalAccountsCoroutineStub, v2alphaFlags.redirectUri)
        .withAccountAuthenticationServerInterceptor(
          internalAccountsCoroutineStub,
          v2alphaFlags.redirectUri
        ),
      ApiKeysService(InternalApiKeysCoroutineStub(channel))
        .withAccountAuthenticationServerInterceptor(
          internalAccountsCoroutineStub,
          v2alphaFlags.redirectUri
        ),
      CertificatesService(InternalCertificatesCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      DataProvidersService(InternalDataProvidersCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      EventGroupsService(InternalEventGroupsCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      EventGroupMetadataDescriptorsService(
          InternalEventGroupMetadataDescriptorsCoroutineStub(channel)
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      ExchangeStepAttemptsService(
          InternalExchangeStepAttemptsCoroutineStub(channel),
          internalExchangeStepsCoroutineStub
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      ExchangeStepsService(internalExchangeStepsCoroutineStub)
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      MeasurementsService(
          InternalMeasurementsCoroutineStub(channel),
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      MeasurementConsumersService(InternalMeasurementConsumersCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withAccountAuthenticationServerInterceptor(
          internalAccountsCoroutineStub,
          v2alphaFlags.redirectUri
        )
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      PublicKeysService(InternalPublicKeysCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub),
      RequisitionsService(InternalRequisitionsCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup)
        .withApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub)
    )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
}

fun main(args: Array<String>) = commandLineMain(::run, args)

/** Flags specific to the V2alpha API version. */
private class V2alphaFlags {
  @CommandLine.Option(
    names = ["--authority-key-identifier-to-principal-map-file"],
    description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
    required = true,
  )
  lateinit var authorityKeyIdentifierToPrincipalMapFile: File
    private set

  @CommandLine.Option(
    names = ["--open-id-redirect-uri"],
    description = ["The redirect uri for OpenID Provider responses."],
    required = true
  )
  lateinit var redirectUri: String
    private set
}
