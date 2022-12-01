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

import com.google.protobuf.ByteString
import io.grpc.Channel
import java.io.File
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.util.logging.Logger
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicKingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicKingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as PublicKingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicKingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicKingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicKingdomMeasurementsCoroutineStub
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.integration.common.reporting.identity.withMetadataPrincipalIdentities
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer.Companion.toList
import org.wfanet.measurement.reporting.service.api.v1alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v1alpha.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportsService

/** TestRule that starts and stops all Reporting Server gRPC services. */
class InProcessReportingServer(
  private val reportingServerDataServices: ReportingDataServer.Services,
  private val publicKingdomChannel: Channel,
  private val encryptionKeyPairConfig: EncryptionKeyPairConfig,
  private val signingPrivateKeyDir: File,
  private val measurementConsumerConfig: MeasurementConsumerConfig,
  trustedCertificates: Map<ByteString, X509Certificate>,
  private val verboseGrpcLogging: Boolean = true,
) : TestRule {
  private val publicKingdomMeasurementConsumersClient by lazy {
    PublicKingdomMeasurementConsumersCoroutineStub(publicKingdomChannel)
  }
  private val publicKingdomMeasurementsClient by lazy {
    PublicKingdomMeasurementsCoroutineStub(publicKingdomChannel)
  }
  private val publicKingdomCertificatesClient by lazy {
    PublicKingdomCertificatesCoroutineStub(publicKingdomChannel)
  }
  private val publicKingdomDataProvidersClient by lazy {
    PublicKingdomDataProvidersCoroutineStub(publicKingdomChannel)
  }
  private val publicKingdomEventGroupsClient by lazy {
    PublicKingdomEventGroupsCoroutineStub(publicKingdomChannel)
  }
  private val publicKingdomEventGroupMetadataDescriptorsClient by lazy {
    PublicKingdomEventGroupMetadataDescriptorsCoroutineStub(publicKingdomChannel)
  }

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
      reportingServerDataServices.toList().forEach {
        addService(it.withVerboseLogging(verboseGrpcLogging))
      }
    }

  private val publicApiServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Reporting Server's public API services")

      val encryptionKeyPairStore =
        InMemoryEncryptionKeyPairStore(
          encryptionKeyPairConfig.principalKeyPairsList.associateBy(
            { it.principal },
            {
              it.keyPairsList.map { keyPair ->
                Pair(
                  signingPrivateKeyDir.resolve(keyPair.publicKeyFile).readByteString(),
                  loadPrivateKey(signingPrivateKeyDir.resolve(keyPair.privateKeyFile))
                )
              }
            }
          )
        )

      listOf(
          EventGroupsService(
              publicKingdomEventGroupsClient,
              publicKingdomEventGroupMetadataDescriptorsClient,
              encryptionKeyPairStore
            )
            .withMetadataPrincipalIdentities(measurementConsumerConfig),
          ReportingSetsService(internalReportingSetsClient)
            .withMetadataPrincipalIdentities(measurementConsumerConfig),
          ReportsService(
              internalReportsClient,
              internalReportingSetsClient,
              internalMeasurementsClient,
              publicKingdomDataProvidersClient,
              publicKingdomMeasurementConsumersClient,
              publicKingdomMeasurementsClient,
              publicKingdomCertificatesClient,
              encryptionKeyPairStore,
              SecureRandom(),
              signingPrivateKeyDir,
              trustedCertificates
            )
            .withMetadataPrincipalIdentities(measurementConsumerConfig)
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
