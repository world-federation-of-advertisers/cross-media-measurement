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

package org.wfanet.measurement.integration.common.reporting

import io.grpc.Channel
import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicKingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicKingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.testing.withMetadataPrincipalIdentities
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.reporting.deploy.common.service.DataServices
import org.wfanet.measurement.reporting.deploy.common.service.toList
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetsService

/** TestRule that starts and stops all Reporting Server gRPC services. */
class InProcessReportingServer(
  kingdomPublicApiChannel: Channel,
  dataServicesProvider: () -> DataServices,
  private val verboseGrpcLogging: Boolean = true
) : TestRule {
  private val publicKingdomMeasurementsCoroutineStub by lazy {
    PublicKingdomMeasurementsCoroutineStub(kingdomPublicApiChannel)
  }

  private val publicKingdomCertificatesCoroutineStub by lazy {
    PublicKingdomCertificatesCoroutineStub(kingdomPublicApiChannel)
  }

  private val reportingServerDataServices by lazy { dataServicesProvider() }

  private val internalApiChannel by lazy { internalDataServer.channel }
  private val internalMeasurementsClient by lazy {
    InternalMeasurementsCoroutineStub(internalApiChannel)
  }
  private val internalReportingSetsClient by lazy {
    InternalReportingSetsCoroutineStub(internalApiChannel)
  }
  private val internalReportsClient by lazy { InternalReportsCoroutineStub(internalApiChannel) }

  private val internalDataServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Reporting Server's internal Data services")
      reportingServerDataServices.buildDataServices().toList().forEach {
        addService(it.withVerboseLogging(verboseGrpcLogging))
      }
    }

  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Reporting Server's public API services")

      listOf(
          ReportingSetsService(internalReportingSetsClient).withMetadataPrincipalIdentities() // ,
          // ReportsService(internalReportsClient, internalMeasurementsClient,
          // publicKingdomMeasurementsCoroutineStub, publicKingdomCertificatesCoroutineStub)
          )
        .forEach { addService(it.withVerboseLogging(verboseGrpcLogging)) }
    }

  /** Provides a gRPC channel to the Reporting Server's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  override fun apply(statement: Statement, description: Description): Statement {
    return chainRulesSequentially(internalDataServer, publicApiServer).apply(statement, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
  }
}
