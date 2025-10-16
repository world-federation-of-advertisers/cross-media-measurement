/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.k8s

import com.google.crypto.tink.InsecureSecretKeyAccess
import com.google.crypto.tink.TinkProtoKeysetFormat
import com.google.protobuf.util.JsonFormat
import io.grpc.Channel
import io.grpc.ManagedChannel
import java.nio.file.Paths
import java.security.KeyPair
import java.security.cert.X509Certificate
import java.time.ZoneOffset
import java.util.UUID
import java.util.logging.Logger
import okhttp3.HttpUrl
import okhttp3.HttpUrl.Companion.toHttpUrlOrNull
import okhttp3.OkHttpClient
import okhttp3.tls.HandshakeCertificates
import okhttp3.tls.HeldCertificate
import okhttp3.tls.decodeCertificatePem
import org.jetbrains.annotations.Blocking
import org.junit.ClassRule
import org.junit.rules.TemporaryFolder
import org.junit.rules.TestRule
import org.junit.runner.Description
import org.junit.runners.model.Statement
import org.measurement.integration.k8s.testing.CorrectnessTestConfig
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.ListModelReleasesRequestKt
import org.wfanet.measurement.api.v2alpha.ListModelRolloutsRequestKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt
import org.wfanet.measurement.api.v2alpha.ModelLine
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpc
import org.wfanet.measurement.api.v2alpha.ModelRelease
import org.wfanet.measurement.api.v2alpha.ModelReleaseKey
import org.wfanet.measurement.api.v2alpha.ModelReleasesGrpc
import org.wfanet.measurement.api.v2alpha.ModelRollout
import org.wfanet.measurement.api.v2alpha.ModelRolloutKey
import org.wfanet.measurement.api.v2alpha.ModelRolloutsGrpc
import org.wfanet.measurement.api.v2alpha.ModelSuite
import org.wfanet.measurement.api.v2alpha.ModelSuitesGrpc
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.createModelReleaseRequest
import org.wfanet.measurement.api.v2alpha.createModelSuiteRequest
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.getModelLineRequest
import org.wfanet.measurement.api.v2alpha.listModelReleasesRequest
import org.wfanet.measurement.api.v2alpha.listModelRolloutsRequest
import org.wfanet.measurement.api.v2alpha.modelRelease
import org.wfanet.measurement.api.v2alpha.modelSuite
import org.wfanet.measurement.common.api.ResourceKey
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.BearerTokenCallCredentials
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.testing.OpenIdProvider
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.testing.chainRulesSequentially
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.config.access.OpenIdProvidersConfig
import org.wfanet.measurement.integration.common.SyntheticGenerationSpecs
import org.wfanet.measurement.loadtest.measurementconsumer.EventQueryMeasurementConsumerSimulator
import org.wfanet.measurement.loadtest.measurementconsumer.MeasurementConsumerData
import org.wfanet.measurement.loadtest.reporting.ReportingUserSimulator
import org.wfanet.measurement.reporting.v2alpha.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt

/**
 * Test for correctness of an existing CMMS on Kubernetes with EDP simulators.
 *
 * The computation composition is using ACDP by assumption.
 *
 * This currently assumes that the CMMS instance is using the certificates and keys from this Bazel
 * workspace. It also assumes that there is a Reporting system connected to the CMMS.
 */
class SyntheticGeneratorCorrectnessTest : AbstractCorrectnessTest(measurementSystem) {
  private class RunningMeasurementSystem : MeasurementSystem(), TestRule {
    override val syntheticPopulationSpec: SyntheticPopulationSpec =
      SyntheticGenerationSpecs.SYNTHETIC_POPULATION_SPEC_LARGE
    override val syntheticEventGroupSpecs: List<SyntheticEventGroupSpec> =
      SyntheticGenerationSpecs.SYNTHETIC_DATA_SPECS_LARGE_2M
    override val populationDataProviderName: String
      get() = TEST_CONFIG.populationDataProvider

    override val runId: String by lazy { UUID.randomUUID().toString() }

    private lateinit var _testHarness: EventQueryMeasurementConsumerSimulator
    private lateinit var _reportingTestHarness: ReportingUserSimulator
    private lateinit var _modelLine: ModelLine

    override val testHarness: EventQueryMeasurementConsumerSimulator
      get() = _testHarness

    override val reportingTestHarness: ReportingUserSimulator
      get() = _reportingTestHarness

    override val modelLine: ModelLine
      get() = _modelLine

    private val channels = mutableListOf<ManagedChannel>()

    override fun apply(base: Statement, description: Description): Statement {
      return object : Statement() {
        override fun evaluate() {
          try {
            val pdpKingdomPublicApiChannel = buildKingdomPublicApiChannel(PDP_SIGNING_CERTS)
            val mpKingdomPublicApiChannel = buildKingdomPublicApiChannel(MP_SIGNING_CERTS)
            _modelLine = ensureModelLine(pdpKingdomPublicApiChannel, mpKingdomPublicApiChannel)

            _testHarness = createTestHarness()
            _reportingTestHarness = createReportingTestHarness()
            base.evaluate()
          } finally {
            shutDownChannels()
          }
        }
      }
    }

    @Blocking
    private fun ensureModelLine(
      pdpKingdomPublicApiChannel: Channel,
      mpKingdomPublicApiChannel: Channel,
    ): ModelLine {
      val population = ensurePopulation(pdpKingdomPublicApiChannel)

      val modelReleasesStub = ModelReleasesGrpc.newBlockingStub(mpKingdomPublicApiChannel)
      val modelSuitesStub = ModelSuitesGrpc.newBlockingStub(mpKingdomPublicApiChannel)

      // Find ModelReleases for the Population.
      val modelReleases: List<ModelRelease> =
        modelReleasesStub
          .listModelReleases(
            listModelReleasesRequest {
              parent = "${TEST_CONFIG.modelProvider}/modelSuites/-"
              filter = ListModelReleasesRequestKt.filter { populationIn += population.name }
            }
          )
          .modelReleasesList

      if (modelReleases.isEmpty()) {
        val modelSuite: ModelSuite =
          modelSuitesStub.createModelSuite(
            createModelSuiteRequest {
              parent = TEST_CONFIG.modelProvider
              modelSuite = modelSuite { displayName = "K8s test" }
            }
          )

        val modelRelease: ModelRelease =
          modelReleasesStub.createModelRelease(
            createModelReleaseRequest {
              parent = modelSuite.name
              modelRelease = modelRelease { this.population = population.name }
            }
          )

        return createModelLine(mpKingdomPublicApiChannel, modelSuite.name, modelRelease.name).also {
          logger.info { "Created ${it.name} for ${population.name}" }
        }
      }

      // If there's a ModelRelease for the Population, we expect a ModelLine to exist that
      // references it.
      val modelRelease = modelReleases.first()
      val modelReleaseKey = checkNotNull(ModelReleaseKey.fromName(modelRelease.name))
      val modelSuiteName = modelReleaseKey.parentKey.toName()
      val modelRolloutsStub = ModelRolloutsGrpc.newBlockingStub(mpKingdomPublicApiChannel)
      val modelRollouts: List<ModelRollout> =
        modelRolloutsStub
          .listModelRollouts(
            listModelRolloutsRequest {
              parent = "$modelSuiteName/modelLines/${ResourceKey.WILDCARD_ID}"
              filter = ListModelRolloutsRequestKt.filter { modelReleaseIn += modelRelease.name }
            }
          )
          .modelRolloutsList
      if (modelRollouts.isEmpty()) {
        throw Exception("Unable to find ModelLine for Population")
      }

      val modelRolloutKey = checkNotNull(ModelRolloutKey.fromName(modelRollouts.first().name))
      val modelLinesStub = ModelLinesGrpc.newBlockingStub(mpKingdomPublicApiChannel)
      val modelLine: ModelLine =
        modelLinesStub.getModelLine(
          getModelLineRequest { name = modelRolloutKey.parentKey.toName() }
        )
      if (
        modelLine.activeStartTime.toInstant() >
          getMinModelLineStartDate().atStartOfDay(ZoneOffset.UTC).toInstant()
      ) {
        throw Exception("Unable to find appropriately active ModelLine for Population")
      }

      logger.info { "Found ${modelLine.name} for ${population.name}" }
      return modelLine
    }

    private fun createTestHarness(): EventQueryMeasurementConsumerSimulator {
      val publicApiChannel = buildKingdomPublicApiChannel(MEASUREMENT_CONSUMER_SIGNING_CERTS)
      val measurementConsumerData =
        MeasurementConsumerData(
          TEST_CONFIG.measurementConsumer,
          MC_SIGNING_KEY,
          MC_ENCRYPTION_PRIVATE_KEY,
          TEST_CONFIG.apiAuthenticationKey,
        )

      return EventQueryMeasurementConsumerSimulator(
        measurementConsumerData,
        OUTPUT_DP_PARAMS,
        DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        EventGroupsGrpcKt.EventGroupsCoroutineStub(publicApiChannel),
        MeasurementsGrpcKt.MeasurementsCoroutineStub(publicApiChannel),
        MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(publicApiChannel),
        CertificatesGrpcKt.CertificatesCoroutineStub(publicApiChannel),
        MEASUREMENT_CONSUMER_SIGNING_CERTS.trustedCertificates,
        buildEventQuery(TEST_CONFIG.eventDataProvidersList),
        ProtocolConfig.NoiseMechanism.CONTINUOUS_GAUSSIAN,
      )
    }

    private fun createReportingTestHarness(): ReportingUserSimulator {
      val publicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.reportingPublicApiTarget,
            REPORTING_SIGNING_CERTS,
            TEST_CONFIG.reportingPublicApiCertHost,
          )
          .also { channels.add(it) }

      val accessPublicApiChannel =
        buildMutualTlsChannel(
            TEST_CONFIG.accessPublicApiTarget,
            ACCESS_SIGNING_CERTS,
            TEST_CONFIG.accessPublicApiCertHost,
          )
          .also { channels.add(it) }

      val secretFiles = getRuntimePath(SECRET_FILES_PATH)
      val reportingRootCert = secretFiles.resolve("reporting_root.pem").toFile()
      val cert = secretFiles.resolve("mc_tls.pem").toFile()
      val key = secretFiles.resolve("mc_tls.key").toFile()

      val clientCertificate: X509Certificate = cert.readText().decodeCertificatePem()
      val keyAlgorithm = clientCertificate.publicKey.algorithm
      val certificates =
        HandshakeCertificates.Builder()
          .addTrustedCertificate(reportingRootCert.readText().decodeCertificatePem())
          .heldCertificate(
            HeldCertificate(
              KeyPair(clientCertificate.publicKey, readPrivateKey(key, keyAlgorithm)),
              clientCertificate,
            )
          )
          .build()

      val okHttpReportingClient =
        OkHttpClient.Builder()
          .sslSocketFactory(certificates.sslSocketFactory(), certificates.trustManager)
          .build()

      val openIdProvidersConfigBuilder = OpenIdProvidersConfig.newBuilder()
      JsonFormat.parser()
        .ignoringUnknownFields()
        .merge(OPEN_ID_PROVIDERS_CONFIG_JSON_FILE.readText(), openIdProvidersConfigBuilder)
      val openIdProvidersConfig = openIdProvidersConfigBuilder.build()

      val principal =
        createAccessPrincipal(
          TEST_CONFIG.measurementConsumer,
          accessPublicApiChannel,
          openIdProvidersConfig.providerConfigByIssuerMap.keys.first(),
        )

      val getAccessToken = {
        OpenIdProvider(
          principal.user.issuer,
          TinkProtoKeysetFormat.parseKeyset(
            OPEN_ID_PROVIDERS_TINK_FILE.readBytes(),
            InsecureSecretKeyAccess.get(),
          ),
        )
          .generateCredentials(
            audience = TEST_CONFIG.reportingTokenAudience,
            subject = principal.user.subject,
            scopes =
              setOf(
                "reporting.basicReports.create",
                "reporting.reports.create",
                "reporting.metrics.create",
                "reporting.basicReports.get",
              ),
          ).token
      }

      val reportingServiceUrl: HttpUrl = TEST_CONFIG.reportingServiceEndpoint.toHttpUrlOrNull()
        ?: throw IllegalArgumentException("Invalid reporting service endpoint")

      return ReportingUserSimulator(
        measurementConsumerName = TEST_CONFIG.measurementConsumer,
        dataProvidersClient = DataProvidersGrpcKt.DataProvidersCoroutineStub(publicApiChannel),
        eventGroupsClient =
          org.wfanet.measurement.reporting.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub(
            publicApiChannel
          ),
        reportingSetsClient = ReportingSetsGrpcKt.ReportingSetsCoroutineStub(publicApiChannel),
        metricCalculationSpecsClient =
          MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub(publicApiChannel),
        reportsClient = ReportsGrpcKt.ReportsCoroutineStub(publicApiChannel),
        okHttpReportingClient = okHttpReportingClient,
        reportingGatewayScheme = reportingServiceUrl.scheme,
        reportingGatewayHost = reportingServiceUrl.host,
        reportingGatewayPort = reportingServiceUrl.port,
        getReportingAccessToken = getAccessToken,
      )
    }

    private fun buildKingdomPublicApiChannel(clientCerts: SigningCerts): Channel {
      return buildMutualTlsChannel(
          TEST_CONFIG.kingdomPublicApiTarget,
          clientCerts,
          TEST_CONFIG.kingdomPublicApiCertHost.ifEmpty { null },
        )
        .also { channels.add(it) }
    }

    private fun shutDownChannels() {
      for (channel in channels) {
        channel.shutdown()
      }
    }
  }

  companion object {
    private val logger = Logger.getLogger(this::class.java.enclosingClass.name)

    private val CONFIG_PATH =
      Paths.get("src", "test", "kotlin", "org", "wfanet", "measurement", "integration", "k8s")
    private const val TEST_CONFIG_NAME = "correctness_test_config.textproto"

    private val TEST_CONFIG: CorrectnessTestConfig by lazy {
      val configFile = getRuntimePath(CONFIG_PATH.resolve(TEST_CONFIG_NAME)).toFile()
      parseTextProto(configFile, CorrectnessTestConfig.getDefaultInstance())
    }

    private val tempDir = TemporaryFolder()
    private val measurementSystem = RunningMeasurementSystem()

    @ClassRule @JvmField val chainedRule = chainRulesSequentially(tempDir, measurementSystem)
  }
}
