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
import io.netty.handler.ssl.ClientAuth
import java.io.File
import java.nio.file.Paths
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
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.withTrustedPrincipalAuthentication
import org.wfanet.measurement.access.common.TlsClientPrincipalMapping
import org.wfanet.measurement.access.service.internal.PermissionMapping
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as PublicKingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as PublicKingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as PublicKingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as PublicKingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventMessageDescriptor
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerCertificateKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as PublicKingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as PublicKingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub as PublicKingdomModelLinesCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.tink.loadPrivateKey
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
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
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub as InternalImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.reporting.deploy.v2.common.server.AbstractInternalReportingServer.Companion.toList
import org.wfanet.measurement.reporting.deploy.v2.common.service.Services
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ImpressionQualificationFiltersService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.validate
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub as PublicMetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub as PublicReportsCoroutineStub

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
  private val eventDescriptor: Descriptors.Descriptor,
  // May be empty
  private val defaultModelLineName: String,
  private val populationDataProviderName: String,
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
  private val publicKingdomModelLinesClient =
    PublicKingdomModelLinesCoroutineStub(kingdomPublicApiChannel)

  private val internalApiChannel
    get() = buildMutualTlsChannel("localhost:${internalReportingServer.port}", SIGNING_CERTS)

  private val internalMeasurementConsumersClient by lazy {
    InternalMeasurementConsumersCoroutineStub(internalApiChannel)
  }
  private val internalMeasurementsClient by lazy {
    InternalMeasurementsCoroutineStub(internalApiChannel)
  }
  val internalMetricCalculationSpecsClient by lazy {
    InternalMetricCalculationSpecsCoroutineStub(internalApiChannel)
  }
  private val internalMetricsClient by lazy { InternalMetricsCoroutineStub(internalApiChannel) }
  private val internalReportingSetsClient by lazy {
    InternalReportingSetsCoroutineStub(internalApiChannel)
  }
  private val internalReportsClient by lazy { InternalReportsCoroutineStub(internalApiChannel) }

  val internalBasicReportsClient by lazy { InternalBasicReportsCoroutineStub(internalApiChannel) }

  private val internalImpressionQualificationFiltersClient by lazy {
    InternalImpressionQualificationFiltersCoroutineStub(internalApiChannel)
  }

  val internalReportResultsClient by lazy { ReportResultsCoroutineStub(internalApiChannel) }

  private lateinit var _internalReportingServer: CommonServer

  // An server that isn't inProcess is required for a Python process.
  private val internalReportingServerRule = TestRule { base, _ ->
    object : Statement() {
      override fun evaluate() {
        _internalReportingServer =
          CommonServer.fromParameters(
              verboseGrpcLogging = true,
              certs = SIGNING_CERTS,
              clientAuth = ClientAuth.OPTIONAL,
              nameForLogging = INTERNAL_REPORTING_SERVER_NAME,
              services =
                internalReportingServerServices.toList().map {
                  it.withVerboseLogging(verboseGrpcLogging)
                },
            )
            .start()
        base.evaluate()
      }
    }
  }

  val internalReportingServer: CommonServer
    get() = _internalReportingServer

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

        val authorization = Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(accessChannel))

        METRIC_SPEC_CONFIG.validate()
        metricSpecConfig = METRIC_SPEC_CONFIG

        listOf(
            DataProvidersService(
                publicKingdomDataProvidersClient,
                authorization,
                measurementConsumerConfig.apiKey,
              )
              .withTrustedPrincipalAuthentication(),
            EventGroupMetadataDescriptorsService(
                publicKingdomEventGroupMetadataDescriptorsClient,
                authorization,
                measurementConsumerConfig.apiKey,
              )
              .withTrustedPrincipalAuthentication(),
            EventGroupsService(
                publicKingdomEventGroupsClient,
                authorization,
                celEnvCacheProvider.value,
                measurementConsumerConfigs,
                encryptionKeyPairStore,
              )
              .withTrustedPrincipalAuthentication(),
            MetricCalculationSpecsService(
                internalMetricCalculationSpecsClient,
                publicKingdomModelLinesClient,
                METRIC_SPEC_CONFIG,
                authorization,
                SecureRandom().asKotlinRandom(),
                measurementConsumerConfigs,
              )
              .withTrustedPrincipalAuthentication(),
            MetricsService(
                METRIC_SPEC_CONFIG,
                measurementConsumerConfigs,
                internalReportingSetsClient,
                internalMetricsClient,
                VariancesImpl,
                internalMeasurementsClient,
                publicKingdomDataProvidersClient,
                publicKingdomMeasurementsClient,
                publicKingdomCertificatesClient,
                publicKingdomMeasurementConsumersClient,
                publicKingdomModelLinesClient,
                authorization,
                encryptionKeyPairStore,
                SecureRandom().asKotlinRandom(),
                SECRETS_DIR,
                trustedCertificates,
                defaultVidModelLine = defaultModelLineName,
                measurementConsumerModelLines = emptyMap(),
                certificateCacheExpirationDuration = Duration.ofMinutes(60),
                dataProviderCacheExpirationDuration = Duration.ofMinutes(60),
                keyReaderContext = Dispatchers.IO,
                cacheLoaderContext = Dispatchers.Default,
                populationDataProvider = populationDataProviderName,
              )
              .withTrustedPrincipalAuthentication(),
            ReportingSetsService(internalReportingSetsClient, authorization)
              .withTrustedPrincipalAuthentication(),
            ReportsService(
                internalReportsClient,
                internalMetricCalculationSpecsClient,
                PublicMetricsCoroutineStub(this@GrpcTestServerRule.channel),
                METRIC_SPEC_CONFIG,
                authorization,
                SecureRandom().asKotlinRandom(),
              )
              .withTrustedPrincipalAuthentication(),
            BasicReportsService(
                internalBasicReportsClient,
                internalImpressionQualificationFiltersClient,
                internalReportingSetsClient,
                internalMetricCalculationSpecsClient,
                PublicReportsCoroutineStub(this@GrpcTestServerRule.channel),
                publicKingdomModelLinesClient,
                EventMessageDescriptor(eventDescriptor),
                METRIC_SPEC_CONFIG,
                SecureRandom().asKotlinRandom(),
                authorization,
                measurementConsumerConfigs,
                emptyList(),
              )
              .withTrustedPrincipalAuthentication(),
            ImpressionQualificationFiltersService(
                internalImpressionQualificationFiltersClient,
                authorization,
              )
              .withTrustedPrincipalAuthentication(),
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
    return object : Statement() {
      override fun evaluate() {
        try {
          publicApiServer = createPublicApiTestServerRule()
          chainRulesSequentially(
              internalReportingServerRule,
              accessServicesFactory,
              access,
              celEnvCacheProvider,
              publicApiServer,
            )
            .apply(base, description)
            .evaluate()
        } finally {
          internalApiChannel.shutdownNow()
          internalReportingServer.shutdown()
        }
      }
    }
  }

  companion object {
    private const val INTERNAL_REPORTING_SERVER_NAME = "internal-reporting-server"

    private val SECRETS_DIR: File =
      getRuntimePath(
          Paths.get("wfa_measurement_system", "src", "main", "k8s", "testing", "secretfiles")
        )!!
        .toFile()

    private val ALL_ROOT_CERTS_FILE: File = SECRETS_DIR.resolve("all_root_certs.pem")
    private val REPORTING_TLS_CERT_FILE: File = SECRETS_DIR.resolve("reporting_tls.pem")
    private val REPORTING_TLS_KEY_FILE: File = SECRETS_DIR.resolve("reporting_tls.key")

    private val SIGNING_CERTS =
      SigningCerts.fromPemFiles(
        REPORTING_TLS_CERT_FILE,
        REPORTING_TLS_KEY_FILE,
        ALL_ROOT_CERTS_FILE,
      )

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
