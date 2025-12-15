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

package org.wfanet.measurement.integration.common

import com.google.protobuf.Descriptors
import io.grpc.Channel
import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.identity.testing.withMetadataDuchyIdentities
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.duchy.herald.Herald
import org.wfanet.measurement.internal.kingdom.AccountsGrpcKt.AccountsCoroutineStub as InternalAccountsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ApiKeysGrpcKt.ApiKeysCoroutineStub as InternalApiKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as InternalEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangesGrpcKt.ExchangesCoroutineStub as InternalExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineStub as InternalMeasurementLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelLinesGrpcKt.ModelLinesCoroutineStub as InternalModelLinesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelReleasesGrpcKt.ModelReleasesCoroutineStub as InternalModelReleasesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelRolloutsGrpcKt.ModelRolloutsCoroutineStub as InternalModelRolloutsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ModelSuitesGrpcKt.ModelSuitesCoroutineStub as InternalModelSuitesCoroutineStub
import org.wfanet.measurement.internal.kingdom.PopulationsGrpcKt.PopulationsCoroutineStub as InternalPopulationsCoroutineStub
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub as InternalPublicKeysCoroutineStub
import org.wfanet.measurement.internal.kingdom.RecurringExchangesGrpcKt.RecurringExchangesCoroutineStub as InternalRecurringExchangesCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.toList
import org.wfanet.measurement.kingdom.service.api.v2alpha.AccountsService
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
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelReleasesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelRolloutsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ModelSuitesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.PopulationsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.PublicKeysService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.withAccountAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.api.v2alpha.withApiKeyAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationLogEntriesService as SystemComputationLogEntriesService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationParticipantsService as SystemComputationParticipantsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationsService as SystemComputationsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.RequisitionsService as SystemRequisitionsService
import org.wfanet.measurement.loadtest.panelmatchresourcesetup.PanelMatchResourceSetup

/** TestRule that starts and stops all Kingdom gRPC services. */
class InProcessKingdom(
  dataServicesProvider: () -> DataServices,
  /** The open id client redirect uri when creating the authentication uri. */
  private val redirectUri: String,
  val verboseGrpcLogging: Boolean = true,
) : TestRule {
  private val kingdomDataServices by lazy { dataServicesProvider() }

  val knownEventGroupMetadataTypes: Iterable<Descriptors.FileDescriptor>
    get() = kingdomDataServices.knownEventGroupMetadataTypes

  private val internalApiChannel by lazy { internalDataServer.channel }
  private val internalApiKeysClient by lazy { InternalApiKeysCoroutineStub(internalApiChannel) }
  private val internalMeasurementsClient by lazy {
    InternalMeasurementsCoroutineStub(internalApiChannel)
  }
  private val internalPublicKeysClient by lazy {
    InternalPublicKeysCoroutineStub(internalApiChannel)
  }
  private val internalMeasurementLogEntriesClient by lazy {
    InternalMeasurementLogEntriesCoroutineStub(internalApiChannel)
  }
  private val internalComputationParticipantsClient by lazy {
    InternalComputationParticipantsCoroutineStub(internalApiChannel)
  }
  private val internalRequisitionsClient by lazy {
    InternalRequisitionsCoroutineStub(internalApiChannel)
  }
  private val internalMeasurementConsumersClient by lazy {
    InternalMeasurementConsumersCoroutineStub(internalApiChannel)
  }
  private val internalEventGroupsClient by lazy {
    InternalEventGroupsCoroutineStub(internalApiChannel)
  }
  private val internalEventGroupMetadataDescriptorsClient by lazy {
    InternalEventGroupMetadataDescriptorsCoroutineStub(internalApiChannel)
  }
  private val internalExchangeStepAttemptsClient by lazy {
    InternalExchangeStepAttemptsCoroutineStub(internalApiChannel)
  }
  private val internalExchangeStepsClient by lazy {
    InternalExchangeStepsCoroutineStub(internalApiChannel)
  }
  private val internalExchangesClient by lazy { InternalExchangesCoroutineStub(internalApiChannel) }
  private val internalRecurringExchangesClient by lazy {
    InternalRecurringExchangesCoroutineStub(internalApiChannel)
  }

  private val internalDataServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's internal Data services")
      kingdomDataServices.buildDataServices().toList().forEach { addService(it) }
    }
  private val systemApiServer =
    GrpcTestServerRule(
      logAllRequests = verboseGrpcLogging,
      defaultServiceConfig = Herald.SERVICE_CONFIG,
    ) {
      logger.info("Building Kingdom's system API services")
      listOf(
          SystemComputationsService(internalMeasurementsClient),
          SystemComputationLogEntriesService(internalMeasurementLogEntriesClient),
          SystemComputationParticipantsService(internalComputationParticipantsClient),
          SystemRequisitionsService(internalRequisitionsClient),
        )
        .forEach { addService(it.withMetadataDuchyIdentities()) }
    }
  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's public API services")

      listOf(
          ApiKeysService(internalApiKeysClient)
            .withAccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri),
          CertificatesService(internalCertificatesClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          DataProvidersService(internalDataProvidersClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          EventGroupsService(internalEventGroupsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          EventGroupMetadataDescriptorsService(internalEventGroupMetadataDescriptorsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          MeasurementsService(
              internalMeasurementsClient,
              internalDataProvidersClient,
              MEASUREMENT_NOISE_MECHANISMS,
              reachOnlyLlV2Enabled = true,
              hmssEnabled = true,
            )
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          PublicKeysService(internalPublicKeysClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          RequisitionsService(internalRequisitionsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          ModelRolloutsService(internalModelRolloutsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          ModelReleasesService(internalModelReleasesClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          ModelSuitesService(internalModelSuitesClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          ModelLinesService(internalModelLinesClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          PopulationsService(internalPopulationsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          AccountsService(internalAccountsClient, redirectUri)
            .withAccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri),
          MeasurementConsumersService(internalMeasurementConsumersClient)
            .withMetadataPrincipalIdentities()
            .withAccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri)
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          ExchangesService(internalRecurringExchangesClient, internalExchangesClient)
            .withMetadataPrincipalIdentities(),
          ExchangeStepsService(internalRecurringExchangesClient, internalExchangeStepsClient)
            .withMetadataPrincipalIdentities(),
          ExchangeStepAttemptsService(
              internalExchangeStepAttemptsClient,
              internalExchangeStepsClient,
            )
            .withMetadataPrincipalIdentities(),
        )
        .forEach { addService(it) }
    }

  /** Provides a gRPC channel to the Kingdom's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  /** Provides a gRPC channel to the Kingdom's system API. */
  val systemApiChannel: Channel
    get() = systemApiServer.channel

  /** [PanelMatchResourceSetup] instance with the Kingdom's internal API. */
  val panelMatchResourceSetup: PanelMatchResourceSetup
    get() = PanelMatchResourceSetup(internalApiChannel)

  /** Provides access to Account and DataProvider creation in place of the Kingdom's operator. */
  val internalAccountsClient by lazy { InternalAccountsCoroutineStub(internalApiChannel) }
  val internalDataProvidersClient by lazy { InternalDataProvidersCoroutineStub(internalApiChannel) }

  /** Provides access to Duchy Certificate creation without having multiple Duchy clients. */
  val internalCertificatesClient by lazy { InternalCertificatesCoroutineStub(internalApiChannel) }

  /** Provides access to ModelProvider creation. */
  val internalModelProvidersClient by lazy {
    org.wfanet.measurement.internal.kingdom.ModelProvidersGrpcKt.ModelProvidersCoroutineStub(
      internalApiChannel
    )
  }

  /** Provides access to ModelRollout creation. */
  private val internalModelRolloutsClient by lazy {
    InternalModelRolloutsCoroutineStub(internalApiChannel)
  }

  /** Provides access to ModelRelease creation. */
  private val internalModelReleasesClient by lazy {
    InternalModelReleasesCoroutineStub(internalApiChannel)
  }

  /** Provides access to ModelSuite creation. */
  val internalModelSuitesClient by lazy { InternalModelSuitesCoroutineStub(internalApiChannel) }

  /** Provides access to ModelLine creation. */
  val internalModelLinesClient by lazy { InternalModelLinesCoroutineStub(internalApiChannel) }

  /** Provides access to Population creation. */
  val internalPopulationsClient by lazy { InternalPopulationsCoroutineStub(internalApiChannel) }

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(internalDataServer, systemApiServer, publicApiServer)
      .apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private val MEASUREMENT_NOISE_MECHANISMS: List<ProtocolConfig.NoiseMechanism> =
      listOf(
        ProtocolConfig.NoiseMechanism.NONE,
        ProtocolConfig.NoiseMechanism.CONTINUOUS_LAPLACE,
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
  }
}
