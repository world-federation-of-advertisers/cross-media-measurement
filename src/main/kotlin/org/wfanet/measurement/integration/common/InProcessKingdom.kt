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

import io.grpc.Channel
import java.util.concurrent.TimeUnit
import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.identity.testing.withMetadataDuchyIdentities
import org.wfanet.measurement.common.testing.chainRulesSequentially
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
import org.wfanet.measurement.internal.kingdom.PublicKeysGrpcKt.PublicKeysCoroutineStub as InternalPublicKeysCoroutineStub
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
import org.wfanet.measurement.kingdom.service.api.v2alpha.PublicKeysService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.withAccountAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.api.v2alpha.withApiKeyAuthenticationServerInterceptor
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationLogEntriesService as systemComputationLogEntriesService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationParticipantsService as systemComputationParticipantsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationsService as systemComputationsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.RequisitionsService as systemRequisitionsService

/** TestRule that starts and stops all Kingdom gRPC services. */
class InProcessKingdom(
  dataServicesProvider: () -> DataServices,
  val verboseGrpcLogging: Boolean = true,
  /** The open id client redirect uri when creating the authentication uri. */
  private val redirectUri: String,
) : TestRule {
  private val kingdomDataServices by lazy { dataServicesProvider() }

  private val internalApiChannel by lazy {
    internalDataServer.channel.withDefaultDeadline(
      DEFAULT_INTERNAL_DEADLINE_MILLIS,
      TimeUnit.MILLISECONDS
    )
  }
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

  private val internalDataServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's internal Data services")
      kingdomDataServices.buildDataServices().toList().forEach { addService(it) }
    }
  private val systemApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's system API services")
      listOf(
          systemComputationsService(internalMeasurementsClient),
          systemComputationLogEntriesService(internalMeasurementLogEntriesClient),
          systemComputationParticipantsService(internalComputationParticipantsClient),
          systemRequisitionsService(internalRequisitionsClient)
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
          MeasurementsService(internalMeasurementsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          PublicKeysService(internalPublicKeysClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          RequisitionsService(internalRequisitionsClient)
            .withMetadataPrincipalIdentities()
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient),
          AccountsService(internalAccountsClient, redirectUri)
            .withAccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri),
          MeasurementConsumersService(internalMeasurementConsumersClient)
            .withMetadataPrincipalIdentities()
            .withAccountAuthenticationServerInterceptor(internalAccountsClient, redirectUri)
            .withApiKeyAuthenticationServerInterceptor(internalApiKeysClient)
        )
        .forEach { addService(it) }

      listOf(
          ExchangeStepAttemptsService(
            internalExchangeStepAttemptsClient,
            internalExchangeStepsClient
          ),
          ExchangeStepsService(internalExchangeStepsClient),
          ExchangesService(internalExchangesClient)
        )
        .forEach { addService(it.withMetadataPrincipalIdentities()) }
    }

  /** Provides a gRPC channel to the Kingdom's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  /** Provides a gRPC channel to the Kingdom's system API. */
  val systemApiChannel: Channel
    get() = systemApiServer.channel

  /** Provides access to Account and DataProvider creation in place of the Kingdom's operator. */
  val internalAccountsClient by lazy { InternalAccountsCoroutineStub(internalApiChannel) }
  val internalDataProvidersClient by lazy { InternalDataProvidersCoroutineStub(internalApiChannel) }

  /** Provides access to Duchy Certificate creation without having multiple Duchy clients. */
  val internalCertificatesClient by lazy { InternalCertificatesCoroutineStub(internalApiChannel) }

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(internalDataServer, systemApiServer, publicApiServer)
      .apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    /** Default deadline for RPCs to internal server in milliseconds. */
    private const val DEFAULT_INTERNAL_DEADLINE_MILLIS = 30_000L
  }
}
