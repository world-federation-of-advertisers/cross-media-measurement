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

@file:OptIn(ExperimentalStdlibApi::class) // For `HexFormat`.

package org.wfanet.measurement.kingdom.deploy.common.server

import io.grpc.ServerServiceDefinition
import java.io.File
import kotlin.properties.Delegates
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.asCoroutineDispatcher
import org.wfanet.measurement.api.v2alpha.AccountPrincipal
import org.wfanet.measurement.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.api.v2alpha.ContextKeys
import org.wfanet.measurement.api.v2alpha.DataProviderPrincipal
import org.wfanet.measurement.api.v2alpha.DuchyPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerPrincipal
import org.wfanet.measurement.api.v2alpha.MeasurementPrincipal
import org.wfanet.measurement.api.v2alpha.ModelProviderPrincipal
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.common.api.grpc.AkidPrincipalServerInterceptor
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.ApiChangeMetricsInterceptor
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.RateLimiterProvider
import org.wfanet.measurement.common.grpc.RateLimitingServerInterceptor
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withInterceptors
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.AuthorityKeyServerInterceptor
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.DuchyInfoFlags
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.RateLimitConfig
import org.wfanet.measurement.config.RateLimitConfigKt
import org.wfanet.measurement.config.rateLimitConfig
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub as InternalApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as InternalEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub as InternalExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub as InternalModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelOutagesGrpcKt.ModelOutagesCoroutineStub as InternalModelOutagesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub as InternalModelProviderCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub as InternalModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub as InternalModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelShardsGrpcKt.ModelShardsCoroutineStub as InternalModelShardsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineStub as InternalModelSuitesCoroutineStub
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineStub as InternalPopulationsCoroutineStub
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub as InternalPublicKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub as InternalRecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.HmssProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.Llv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.RoLlv2ProtocolConfigFlags
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfig
import org.wfanet.measurement.kingdom.deploy.common.TrusTeeProtocolConfigFlags
import org.wfanet.measurement.kingdom.service.api.v2alpha.AccountAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.api.v2alpha.AccountsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ApiKeyAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.api.v2alpha.ApiKeysService
import org.wfanet.measurement.kingdom.service.api.v2alpha.CertificatesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementConsumersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelLinesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelOutagesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelReleasesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelRolloutsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelShardsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelSuitesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.PopulationsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.PublicKeysService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import picocli.CommandLine

private const val SERVER_NAME = "V2alphaPublicApiServer"

private val KEY_ID_FORMAT = HexFormat {
  upperCase = true
  bytes.byteSeparator = ":"
}

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Kingdom v2alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin serviceFlags: ServiceFlags,
  @CommandLine.Mixin llv2ProtocolConfigFlags: Llv2ProtocolConfigFlags,
  @CommandLine.Mixin roLlv2ProtocolConfigFlags: RoLlv2ProtocolConfigFlags,
  @CommandLine.Mixin hmssProtocolConfigFlags: HmssProtocolConfigFlags,
  @CommandLine.Mixin v2alphaFlags: V2alphaFlags,
  @CommandLine.Mixin trusteeProtocolConfigFlags: TrusTeeProtocolConfigFlags,
  @CommandLine.Mixin duchyInfoFlags: DuchyInfoFlags,
) {
  Llv2ProtocolConfig.initializeFromFlags(llv2ProtocolConfigFlags)
  RoLlv2ProtocolConfig.initializeFromFlags(roLlv2ProtocolConfigFlags)
  HmssProtocolConfig.initializeFromFlags(hmssProtocolConfigFlags)
  TrusTeeProtocolConfig.initializeFromFlags(trusteeProtocolConfigFlags)
  DuchyInfo.initializeFromFlags(duchyInfoFlags)

  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile,
    )
  val channel =
    buildMutualTlsChannel(
        kingdomApiServerFlags.internalApiFlags.target,
        clientCerts,
        kingdomApiServerFlags.internalApiFlags.certHost,
      )
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)
      .withDefaultDeadline(kingdomApiServerFlags.internalApiFlags.defaultDeadlineDuration)

  val internalAccountsCoroutineStub = InternalAccountsCoroutineStub(channel)
  val internalApiKeysCoroutineStub = InternalApiKeysCoroutineStub(channel)
  val internalRecurringExchangesCoroutineStub = InternalRecurringExchangesCoroutineStub(channel)
  val internalExchangeStepsCoroutineStub = InternalExchangeStepsCoroutineStub(channel)
  val internalDataProvidersStub = InternalDataProvidersCoroutineStub(channel)

  val accountInterceptor =
    AccountAuthenticationServerInterceptor(internalAccountsCoroutineStub, v2alphaFlags.redirectUri)
  val akidInterceptor = AuthorityKeyServerInterceptor()
  val akidPrincipalInterceptor =
    AkidPrincipalServerInterceptor(
      ContextKeys.PRINCIPAL_CONTEXT_KEY,
      AuthorityKeyServerInterceptor.CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY,
      AkidPrincipalLookup(v2alphaFlags.authorityKeyIdentifierToPrincipalMapFile),
    )
  val apiKeyPrincipalInterceptor =
    ApiKeyAuthenticationServerInterceptor(internalApiKeysCoroutineStub)
  val rateLimitingInterceptor =
    RateLimitingServerInterceptor(
      RateLimiterProvider(v2alphaFlags.rateLimitConfig) { context ->
        AuthorityKeyServerInterceptor.CLIENT_AUTHORITY_KEY_IDENTIFIER_CONTEXT_KEY.get(context)
          ?.toByteArray()
          ?.toHexString(KEY_ID_FORMAT)
      }::getRateLimiter
    )
  val apiChangeMetricsInterceptor = ApiChangeMetricsInterceptor { context ->
    when (val principal: MeasurementPrincipal? = ContextKeys.PRINCIPAL_CONTEXT_KEY.get(context)) {
      is DuchyPrincipal,
      is ModelProviderPrincipal,
      is DataProviderPrincipal -> principal.resourceKey.toName()

      is AccountPrincipal,
      is MeasurementConsumerPrincipal -> "measurementConsumers"

      null -> null
    }
  }

  val serviceDispatcher: CoroutineDispatcher = serviceFlags.executor.asCoroutineDispatcher()
  val services: List<ServerServiceDefinition> =
    listOf(
      AccountsService(internalAccountsCoroutineStub, v2alphaFlags.redirectUri, serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          accountInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ApiKeysService(InternalApiKeysCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          accountInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      CertificatesService(InternalCertificatesCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      DataProvidersService(internalDataProvidersStub, serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      EventGroupsService(InternalEventGroupsCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      EventGroupMetadataDescriptorsService(
          InternalEventGroupMetadataDescriptorsCoroutineStub(channel),
          serviceDispatcher,
        )
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      MeasurementsService(
          InternalMeasurementsCoroutineStub(channel),
          internalDataProvidersStub,
          v2alphaFlags.directNoiseMechanisms,
          reachOnlyLlV2Enabled = v2alphaFlags.reachOnlyLlV2Enabled,
          hmssEnabled = v2alphaFlags.hmssEnabled,
          hmssEnabledMeasurementConsumers = v2alphaFlags.hmssEnabledMeasurementConsumers,
          trusTeeEnabled = v2alphaFlags.trusTeeEnabled,
          trusTeeEnabledMeasurementConsumers = v2alphaFlags.trusTeeEnabledMeasurementConsumers,
          coroutineContext = serviceDispatcher,
        )
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      MeasurementConsumersService(
          InternalMeasurementConsumersCoroutineStub(channel),
          serviceDispatcher,
        )
        .withInterceptors(
          apiChangeMetricsInterceptor,
          accountInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      PublicKeysService(InternalPublicKeysCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      RequisitionsService(InternalRequisitionsCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ExchangesService(
          internalRecurringExchangesCoroutineStub,
          InternalExchangesCoroutineStub(channel),
          serviceDispatcher,
        )
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ExchangeStepsService(
          internalRecurringExchangesCoroutineStub,
          internalExchangeStepsCoroutineStub,
          serviceDispatcher,
        )
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ExchangeStepAttemptsService(
          InternalExchangeStepAttemptsCoroutineStub(channel),
          internalExchangeStepsCoroutineStub,
          serviceDispatcher,
        )
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelProvidersService(InternalModelProviderCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelLinesService(InternalModelLinesCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          apiKeyPrincipalInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelShardsService(InternalModelShardsCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelSuitesService(InternalModelSuitesCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelReleasesService(InternalModelReleasesCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelOutagesService(InternalModelOutagesCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      ModelRolloutsService(InternalModelRolloutsCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
      PopulationsService(InternalPopulationsCoroutineStub(channel), serviceDispatcher)
        .withInterceptors(
          apiChangeMetricsInterceptor,
          akidPrincipalInterceptor,
          rateLimitingInterceptor,
          akidInterceptor,
        ),
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
    required = true,
  )
  lateinit var redirectUri: String
    private set

  @CommandLine.Option(
    names = ["--rate-limit-config-file"],
    description =
      [
        "File path to RateLimitConfig protobuf message in text format.",
        "The principal identifier is the authority key identifier of the client certificate in " +
          "the format used by openssl-x509.",
      ],
    required = false,
  )
  private lateinit var rateLimitConfigFile: File

  val rateLimitConfig: RateLimitConfig by lazy {
    if (::rateLimitConfigFile.isInitialized) {
      parseTextProto(rateLimitConfigFile, RateLimitConfig.getDefaultInstance())
    } else {
      DEFAULT_RATE_LIMIT_CONFIG
    }
  }

  lateinit var directNoiseMechanisms: List<NoiseMechanism>
    private set

  @CommandLine.Spec
  lateinit var spec: CommandLine.Model.CommandSpec // injected by picocli
    private set

  @set:CommandLine.Option(
    names = ["--enable-ro-llv2-protocol"],
    description = ["Whether to enable the Reach-Only Liquid Legions v2 protocol"],
    negatable = true,
    required = false,
    defaultValue = "false",
  )
  var reachOnlyLlV2Enabled by Delegates.notNull<Boolean>()
    private set

  @set:CommandLine.Option(
    names = ["--enable-hmss"],
    description = ["whether to enable the Honest Majority Share Shuffle protocol"],
    negatable = true,
    required = false,
    defaultValue = "false",
  )
  var hmssEnabled by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--hmss-enabled-measurement-consumers"],
    description =
      [
        "MeasurementConsumer names who force to enable HMSS protocol" +
          " regardless the --enable-hmss flag."
      ],
    required = false,
    defaultValue = "",
  )
  lateinit var hmssEnabledMeasurementConsumers: List<String>
    private set

  @set:CommandLine.Option(
    names = ["--enable-trustee"],
    description = ["whether to enable the TrusTEE protocol"],
    negatable = true,
    required = false,
    defaultValue = "false",
  )
  var trusTeeEnabled by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--trustee-enabled-measurement-consumers"],
    description =
      [
        "MeasurementConsumer names who force to enable TrusTEE protocol" +
          " regardless the --enable-trustee flag."
      ],
    required = false,
    defaultValue = "",
  )
  lateinit var trusTeeEnabledMeasurementConsumers: List<String>
    private set

  @CommandLine.Option(
    names = ["--direct-noise-mechanism"],
    description =
      [
        "Noise mechanisms that can be used in direct computation. It can be specified multiple " +
          "times."
      ],
    required = true,
  )
  fun setDirectNoiseMechanisms(noiseMechanisms: List<NoiseMechanism>) {
    for (noiseMechanism in noiseMechanisms) {
      when (noiseMechanism) {
        NoiseMechanism.NONE,
        NoiseMechanism.CONTINUOUS_LAPLACE,
        NoiseMechanism.CONTINUOUS_GAUSSIAN -> {}
        NoiseMechanism.GEOMETRIC,
        // TODO(@riemanli): support DISCRETE_GAUSSIAN after having a clear definition of it.
        NoiseMechanism.DISCRETE_GAUSSIAN -> {
          throw CommandLine.ParameterException(
            spec.commandLine(),
            String.format(
              "Invalid noise mechanism $noiseMechanism for option '--direct-noise-mechanism'. " +
                "Discrete mechanisms are not supported for direct computations."
            ),
          )
        }
        NoiseMechanism.NOISE_MECHANISM_UNSPECIFIED,
        NoiseMechanism.UNRECOGNIZED -> {
          throw CommandLine.ParameterException(
            spec.commandLine(),
            String.format(
              "Invalid noise mechanism $noiseMechanism for option '--direct-noise-mechanism'."
            ),
          )
        }
      }
    }
    directNoiseMechanisms = noiseMechanisms
  }

  companion object {
    private val DEFAULT_RATE_LIMIT_CONFIG = rateLimitConfig {
      rateLimit =
        RateLimitConfigKt.rateLimit {
          defaultRateLimit =
            RateLimitConfigKt.methodRateLimit {
              maximumRequestCount = -1 // Unlimited.
              averageRequestRate = 1.0
            }
        }
    }
  }
}
