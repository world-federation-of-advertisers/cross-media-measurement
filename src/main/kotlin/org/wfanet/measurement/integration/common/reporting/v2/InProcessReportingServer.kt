/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common.reporting.v2

import com.google.protobuf.ByteString
import com.google.protobuf.Descriptors
import com.google.protobuf.util.Durations
import io.grpc.Channel
import io.grpc.Status
import io.grpc.StatusException
import java.io.File
import java.security.SecureRandom
import java.security.cert.X509Certificate
import java.time.Duration
import java.util.logging.Logger
import kotlin.random.asKotlinRandom
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicKingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicKingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as PublicKingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicKingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicKingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicKingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.grpc.testing.GrpcTestServerRule
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.readByteString
import org.wfanet.measurement.common.testing.CloseableResource
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.config.AuthorityKeyToPrincipalMap
import org.wfanet.measurement.config.reporting.EncryptionKeyPairConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.config.reporting.MetricSpecConfigKt
import org.wfanet.measurement.config.reporting.measurementConsumerConfigs
import org.wfanet.measurement.config.reporting.metricSpecConfig
import org.wfanet.measurement.integration.common.AccessServicesFactory
import org.wfanet.measurement.integration.common.InProcessAccess
import org.wfanet.measurement.integration.common.PERMISSIONS_CONFIG
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.reporting.deploy.v2.common.server.AbstractInternalReportingServer.Companion.toList
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetadataPrincipalServerInterceptor.Companion.withMetadataPrincipalIdentities
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.validate
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub as PublicMetricsCoroutineStub

/** TestRule that starts and stops all Reporting Server gRPC services. */
class InProcessReportingServer(
  private val internalReportingServerServices: Services,
  private val accessServicesFactory: AccessServicesFactory,
  kingdomPublicApiChannel: Channel,
  private val encryptionKeyPairConfig: EncryptionKeyPairConfig,
  private val signingPrivateKeyDir: File,
  private val measurementConsumerConfig: MeasurementConsumerConfig,
  private val trustedCertificates: Map<ByteString, X509Certificate>,
  private val knownEventGroupMetadataTypes: Iterable<Descriptors.FileDescriptor>,
  private val verboseGrpcLogging: Boolean = true,
) : TestRule {
  private val publicKingdomMeasurementConsumersClient =
    PublicKingdomMeasurementConsumersCoroutineStub(kingdomPublicApiChannel)
  private val publicKingdomMeasurementsClient =
    PublicKingdomMeasurementsCoroutineStub(kingdomPublicApiChannel)
  private val publicKingdomCertificatesClient =
    PublicKingdomCertificatesCoroutineStub(kingdomPublicApiChannel)
  private val publicKingdomDataProvidersClient =
    PublicKingdomDataProvidersCoroutineStub(kingdomPublicApiChannel)
  private val publicKingdomEventGroupMetadataDescriptorsClient =
    PublicKingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomPublicApiChannel)
  private val publicKingdomEventGroupsClient =
    PublicKingdomEventGroupsCoroutineStub(kingdomPublicApiChannel)

  private val internalApiChannel
    get() = internalReportingServer.channel

  private val internalMeasurementConsumersClient by lazy {
    InternalMeasurementConsumersCoroutineStub(internalApiChannel)
  }
  private val internalMeasurementsClient by lazy {
    InternalMeasurementsCoroutineStub(internalApiChannel)
  }
  private val internalMetricCalculationSpecsClient by lazy {
    InternalMetricCalculationSpecsCoroutineStub(internalApiChannel)
  }
  private val internalMetricsClient by lazy { InternalMetricsCoroutineStub(internalApiChannel) }
  private val internalReportingSetsClient by lazy {
    InternalReportingSetsCoroutineStub(internalApiChannel)
  }
  private val internalReportsClient by lazy { InternalReportsCoroutineStub(internalApiChannel) }

  private val internalReportingServer =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      logger.info("Building Reporting Server's internal Data services")
      internalReportingServerServices.toList().forEach {
        addService(it.withVerboseLogging(verboseGrpcLogging))
      }
    }

  private lateinit var publicApiServer: GrpcTestServerRule

  lateinit var metricSpecConfig: MetricSpecConfig

  private val access =
    InProcessAccess(verboseGrpcLogging) {
      val tlsClientMapping =
        TlsClientPrincipalMapping(AuthorityKeyToPrincipalMap.getDefaultInstance())
      val permissionMapping = PermissionMapping(PERMISSIONS_CONFIG)
      accessServicesFactory.create(permissionMapping, tlsClientMapping)
    }

  private val celEnvCacheProvider =
    object :
      CloseableResource<CelEnvCacheProvider>({
        CelEnvCacheProvider(
          publicKingdomEventGroupMetadataDescriptorsClient.withAuthenticationKey(
            measurementConsumerConfig.apiKey
          ),
          EventGroup.getDescriptor(),
          Duration.ofSeconds(5),
          knownEventGroupMetadataTypes,
        )
      }) {
      val value: CelEnvCacheProvider
        get() = resource
    }

  private fun createPublicApiTestServerRule(): GrpcTestServerRule =
    GrpcTestServerRule(logAllRequests = verboseGrpcLogging) {
      runBlocking {
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

        val measurementConsumerName =
          MeasurementConsumerKey(
            MeasurementConsumerCertificateKey.fromName(
                measurementConsumerConfig.signingCertificateName
              )!!
              .measurementConsumerId
          )
        val measurementConsumerConfigs = measurementConsumerConfigs {
          configs[measurementConsumerName.toName()] = measurementConsumerConfig
        }

        try {
          internalMeasurementConsumersClient.createMeasurementConsumer(
            measurementConsumer {
              cmmsMeasurementConsumerId = measurementConsumerName.measurementConsumerId
            }
          )
        } catch (e: StatusException) {
          when (e.status) {
            Status.ALREADY_EXISTS -> {}
            else -> {
              throw e
            }
          }
        }

        METRIC_SPEC_CONFIG.validate()
        metricSpecConfig = METRIC_SPEC_CONFIG

        listOf(
            DataProvidersService(publicKingdomDataProvidersClient)
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
            EventGroupMetadataDescriptorsService(publicKingdomEventGroupMetadataDescriptorsClient)
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
            EventGroupsService(
                publicKingdomEventGroupsClient,
                encryptionKeyPairStore,
                celEnvCacheProvider.value,
              )
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
            MetricCalculationSpecsService(
                internalMetricCalculationSpecsClient,
                METRIC_SPEC_CONFIG,
                SecureRandom().asKotlinRandom(),
              )
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
            MetricsService(
                METRIC_SPEC_CONFIG,
                internalReportingSetsClient,
                internalMetricsClient,
                VariancesImpl,
                internalMeasurementsClient,
                publicKingdomDataProvidersClient,
                publicKingdomMeasurementsClient,
                publicKingdomCertificatesClient,
                publicKingdomMeasurementConsumersClient,
                encryptionKeyPairStore,
                SecureRandom().asKotlinRandom(),
                signingPrivateKeyDir,
                trustedCertificates,
                defaultVidModelLine = "",
                measurementConsumerModelLines = mapOf(),
                certificateCacheExpirationDuration = Duration.ofMinutes(60),
                dataProviderCacheExpirationDuration = Duration.ofMinutes(60),
                keyReaderContext = Dispatchers.IO,
                cacheLoaderContext = Dispatchers.Default,
              )
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
            ReportingSetsService(internalReportingSetsClient)
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
            ReportsService(
                internalReportsClient,
                internalMetricCalculationSpecsClient,
                PublicMetricsCoroutineStub(this@GrpcTestServerRule.channel),
                METRIC_SPEC_CONFIG,
                SecureRandom().asKotlinRandom(),
              )
              .withMetadataPrincipalIdentities(measurementConsumerConfigs),
          )
          .forEach { addService(it.withVerboseLogging(verboseGrpcLogging)) }
      }
    }

  /** Provides a gRPC channel to the Reporting Server's public API. */
  val publicApiChannel: Channel
    get() = publicApiServer.channel

  /** gRPC [Channel] for Access API. */
  val accessChannel: Channel
    get() = access.channel

  override fun apply(base: Statement, description: Description): Statement {
    publicApiServer = createPublicApiTestServerRule()
    return chainRulesSequentially(
        internalReportingServer,
        accessServicesFactory,
        access,
        celEnvCacheProvider,
        publicApiServer,
      )
      .apply(base, description)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val NUMBER_VID_BUCKETS = 300
    private val METRIC_SPEC_CONFIG = metricSpecConfig {
      reachParams =
        MetricSpecConfigKt.reachParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = 0.0041
                  delta = 1e-12
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 0.0f
                      width = 3.0f / NUMBER_VID_BUCKETS
                    }
                }
            }
          singleDataProviderParams = multipleDataProviderParams
        }

      reachAndFrequencyParams =
        MetricSpecConfigKt.reachAndFrequencyParams {
          multipleDataProviderParams =
            MetricSpecConfigKt.reachAndFrequencySamplingAndPrivacyParams {
              reachPrivacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = 0.0033
                  delta = 1e-12
                }
              frequencyPrivacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = 0.0033
                  delta = 1e-12
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 48.0f / NUMBER_VID_BUCKETS
                      width = 5.0f / NUMBER_VID_BUCKETS
                    }
                }
            }
          singleDataProviderParams = multipleDataProviderParams
          maximumFrequency = 10
        }

      impressionCountParams =
        MetricSpecConfigKt.impressionCountParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = 0.0011
                  delta = 1e-12
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 143.0f / NUMBER_VID_BUCKETS
                      width = 62.0f / NUMBER_VID_BUCKETS
                    }
                }
            }
          maximumFrequencyPerUser = 60
        }

      watchDurationParams =
        MetricSpecConfigKt.watchDurationParams {
          params =
            MetricSpecConfigKt.samplingAndPrivacyParams {
              privacyParams =
                MetricSpecConfigKt.differentialPrivacyParams {
                  epsilon = 0.001
                  delta = 1e-12
                }
              vidSamplingInterval =
                MetricSpecConfigKt.vidSamplingInterval {
                  fixedStart =
                    MetricSpecConfigKt.VidSamplingIntervalKt.fixedStart {
                      start = 205.0f / NUMBER_VID_BUCKETS
                      width = 95.0f / NUMBER_VID_BUCKETS
                    }
                }
            }
          maximumWatchDurationPerUser = Durations.fromSeconds(4000)
        }

      populationCountParams = MetricSpecConfig.PopulationCountParams.getDefaultInstance()
    }
  }
}
