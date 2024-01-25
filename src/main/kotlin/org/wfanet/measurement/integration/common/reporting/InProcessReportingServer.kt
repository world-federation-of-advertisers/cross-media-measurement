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
import java.time.Duration
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicKingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicKingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as PublicKingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicKingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicKingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicKingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementSpecConfig
import org.wfanet.measurement.integration.common.reporting.identity.withMetadataPrincipalIdentities
import org.wfanet.measurement.internal.reporting.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer
import org.wfanet.measurement.reporting.deploy.common.server.ReportingDataServer.Companion.toList
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v1alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v1alpha.ReportsService
import org.wfanet.measurement.reporting.v1alpha.EventGroup

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
                  loadPrivateKey(signingPrivateKeyDir.resolve(keyPair.privateKeyFile)),
                )
              }
            },
          )
        )

      val celEnvCacheProvider =
        CelEnvCacheProvider(
          publicKingdomEventGroupMetadataDescriptorsClient.withAuthenticationKey(
            measurementConsumerConfig.apiKey
          ),
          EventGroup.getDescriptor(),
          Duration.ofSeconds(5),
          Dispatchers.Default,
        )

      listOf(
          EventGroupsService(
              publicKingdomEventGroupsClient,
              encryptionKeyPairStore,
              celEnvCacheProvider,
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
              trustedCertificates,
              MEASUREMENT_SPEC_CONFIG,
            )
            .withMetadataPrincipalIdentities(measurementConsumerConfig),
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

    private val MEASUREMENT_SPEC_CONFIG = measurementSpecConfig {
      reachSingleDataProvider =
        MeasurementSpecConfigKt.reachSingleDataProvider {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.000207
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
      reach =
        MeasurementSpecConfigKt.reach {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.0007444
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              randomStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart {
                  width = 256
                  numVidBuckets = 300
                }
            }
        }
      reachAndFrequencySingleDataProvider =
        MeasurementSpecConfigKt.reachAndFrequencySingleDataProvider {
          reachPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.000207
              delta = 1e-15
            }
          frequencyPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.004728
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
      reachAndFrequency =
        MeasurementSpecConfigKt.reachAndFrequency {
          reachPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.0007444
              delta = 1e-15
            }
          frequencyPrivacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.014638
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              randomStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.randomStart {
                  width = 256
                  numVidBuckets = 300
                }
            }
        }
      impression =
        MeasurementSpecConfigKt.impression {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.003592
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
      duration =
        MeasurementSpecConfigKt.duration {
          privacyParams =
            MeasurementSpecConfigKt.differentialPrivacyParams {
              epsilon = 0.007418
              delta = 1e-15
            }
          vidSamplingInterval =
            MeasurementSpecConfigKt.vidSamplingInterval {
              fixedStart =
                MeasurementSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                  start = 0f
                  width = 1f
                }
            }
        }
    }
  }
}
