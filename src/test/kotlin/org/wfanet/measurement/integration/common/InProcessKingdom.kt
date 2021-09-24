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
import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.identity.testing.withMetadataDuchyIdentities
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.internal.kingdom.CertificatesGrpcKt.CertificatesCoroutineStub as InternalCertificatesCoroutineStub
import org.wfanet.measurement.internal.kingdom.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as InternalComputationParticipantsCoroutineStub
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt.DataProvidersCoroutineStub as InternalDataProvidersCoroutineStub
import org.wfanet.measurement.internal.kingdom.EventGroupsGrpcKt.EventGroupsCoroutineStub as InternalEventGroupsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepAttemptsGrpcKt.ExchangeStepAttemptsCoroutineStub as InternalExchangeStepAttemptsCoroutineStub
import org.wfanet.measurement.internal.kingdom.ExchangeStepsGrpcKt.ExchangeStepsCoroutineStub as InternalExchangeStepsCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementLogEntriesGrpcKt.MeasurementLogEntriesCoroutineStub as InternalMeasurementLogEntriesCoroutineStub
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.kingdom.deploy.common.service.DataServices
import org.wfanet.measurement.kingdom.deploy.common.service.toList
import org.wfanet.measurement.kingdom.service.api.v2alpha.CertificatesService
import org.wfanet.measurement.kingdom.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepAttemptsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.ExchangeStepsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementConsumersService
import org.wfanet.measurement.kingdom.service.api.v2alpha.MeasurementsService
import org.wfanet.measurement.kingdom.service.api.v2alpha.RequisitionsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationLogEntriesService as systemComputationLogEntriesService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationParticipantsService as systemComputationParticipantsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.ComputationsService as systemComputationsService
import org.wfanet.measurement.kingdom.service.system.v1alpha.RequisitionsService as systemRequisitionsService

/** TestRule that starts and stops all Kingdom gRPC services. */
class InProcessKingdom(
  dataServicesProvider: () -> DataServices,
  val verboseGrpcLogging: Boolean = true,
) : TestRule {
  private val kingdomDataServices by lazy { dataServicesProvider() }

  private val internalApiChannel by lazy { internalDataServer.channel }
  private val internalMeasurementsClient by lazy {
    InternalMeasurementsCoroutineStub(internalApiChannel)
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
  private val internalDataProvidersClient by lazy {
    InternalDataProvidersCoroutineStub(internalApiChannel)
  }
  private val internalEventGroupsClient by lazy {
    InternalEventGroupsCoroutineStub(internalApiChannel)
  }
  private val internalCertificatesClient by lazy {
    InternalCertificatesCoroutineStub(internalApiChannel)
  }
  private val internalExchangeStepAttemptsClient by lazy {
    InternalExchangeStepAttemptsCoroutineStub(internalApiChannel)
  }
  private val internalExchangeStepsClient by lazy {
    InternalExchangeStepsCoroutineStub(internalApiChannel)
  }

  private val internalDataServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's internal Data services")
      kingdomDataServices.buildDataServices().toList().forEach {
        addService(it.withVerboseLogging(verboseGrpcLogging))
      }
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
        .forEach {
          addService(it.withMetadataDuchyIdentities().withVerboseLogging(verboseGrpcLogging))
        }
    }
  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Kingdom's public API services")
      listOf(
        CertificatesService(internalCertificatesClient),
        DataProvidersService(internalDataProvidersClient),
        EventGroupsService(internalEventGroupsClient),
        ExchangeStepAttemptsService(
          internalExchangeStepAttemptsClient,
          internalExchangeStepsClient
        ),
        ExchangeStepsService(internalExchangeStepsClient),
        MeasurementsService(internalMeasurementsClient),
        MeasurementConsumersService(internalMeasurementConsumersClient),
        RequisitionsService(internalRequisitionsClient)
      )
        .forEach { addService(it.withVerboseLogging(verboseGrpcLogging)) }
    }

  /** Provides a gRPC channel to the Kingdom's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  /** Provides a gRPC channel to the Kingdom's system API. */
  val systemApiChannel: Channel
    get() = systemApiServer.channel

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(internalDataServer, systemApiServer, publicApiServer)
      .apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
