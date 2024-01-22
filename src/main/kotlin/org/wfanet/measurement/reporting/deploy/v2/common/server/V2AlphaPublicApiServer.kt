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

package org.wfanet.measurement.reporting.deploy.v2.common.server

import com.google.protobuf.ByteString
import io.grpc.Channel
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.io.File
import java.security.SecureRandom
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as KingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as KingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as KingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as KingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as KingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.api.memoizing
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as InternalMeasurementConsumersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub as InternalReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub as InternalReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.reporting.deploy.v2.common.EncryptionKeyPairMap
import org.wfanet.measurement.reporting.deploy.v2.common.InProcessServersMethods.startInProcessServerWithService
import org.wfanet.measurement.reporting.deploy.v2.common.KingdomApiFlags
import org.wfanet.measurement.reporting.deploy.v2.common.ReportingApiServerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.V2AlphaFlags
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v2alpha.AkidPrincipalLookup
import org.wfanet.measurement.reporting.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetadataPrincipalServerInterceptor.Companion.withMetadataPrincipalIdentities
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleIterationsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportSchedulesService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingPrincipal
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import picocli.CommandLine

private const val SERVER_NAME = "V2AlphaPublicApiServer"

@CommandLine.Command(
  name = SERVER_NAME,
  description = ["Server daemon for Reporting v2alpha public API services."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin kingdomApiFlags: KingdomApiFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin v2AlphaFlags: V2AlphaFlags,
  @CommandLine.Mixin v2AlphaPublicServerFlags: V2AlphaPublicServerFlags,
  @CommandLine.Mixin encryptionKeyPairMap: EncryptionKeyPairMap,
) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile,
    )
  val channel: Channel =
    buildMutualTlsChannel(
        reportingApiServerFlags.internalApiFlags.target,
        clientCerts,
        reportingApiServerFlags.internalApiFlags.certHost,
      )
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val kingdomChannel: Channel =
    buildMutualTlsChannel(
        target = kingdomApiFlags.target,
        clientCerts = clientCerts,
        hostName = kingdomApiFlags.certHost,
      )
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val principalLookup: PrincipalLookup<ReportingPrincipal, ByteString> =
    AkidPrincipalLookup(
        v2AlphaPublicServerFlags.authorityKeyIdentifierToPrincipalMapFile,
        v2AlphaFlags.measurementConsumerConfigFile,
      )
      .memoizing()

  val measurementConsumerConfigs =
    parseTextProto(
      v2AlphaFlags.measurementConsumerConfigFile,
      MeasurementConsumerConfigs.getDefaultInstance(),
    )

  val internalMeasurementConsumersCoroutineStub = InternalMeasurementConsumersCoroutineStub(channel)
  runBlocking {
    measurementConsumerConfigs.configsMap.keys.forEach {
      val measurementConsumerKey =
        MeasurementConsumerKey.fromName(it)
          ?: throw IllegalArgumentException("measurement_consumer_config is invalid")
      try {
        internalMeasurementConsumersCoroutineStub.createMeasurementConsumer(
          measurementConsumer {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          }
        )
      } catch (e: StatusException) {
        when (e.status.code) {
          Status.Code.ALREADY_EXISTS -> {}
          else -> throw e
        }
      }
    }
  }

  val metricSpecConfig =
    parseTextProto(v2AlphaFlags.metricSpecConfigFile, MetricSpecConfig.getDefaultInstance())

  val apiKey = measurementConsumerConfigs.configsMap.values.first().apiKey
  val celEnvCacheProvider =
    CelEnvCacheProvider(
      KingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomChannel)
        .withAuthenticationKey(apiKey),
      EventGroup.getDescriptor(),
      reportingApiServerFlags.eventGroupMetadataDescriptorCacheDuration,
      Dispatchers.Default,
    )

  val metricsService =
    MetricsService(
      metricSpecConfig,
      InternalReportingSetsCoroutineStub(channel),
      InternalMetricsCoroutineStub(channel),
      VariancesImpl,
      InternalMeasurementsCoroutineStub(channel),
      KingdomDataProvidersCoroutineStub(kingdomChannel),
      KingdomMeasurementsCoroutineStub(kingdomChannel),
      KingdomCertificatesCoroutineStub(kingdomChannel),
      KingdomMeasurementConsumersCoroutineStub(kingdomChannel),
      InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
      SecureRandom(),
      v2AlphaFlags.signingPrivateKeyStoreDir,
      commonServerFlags.tlsFlags.signingCerts.trustedCertificates,
      Dispatchers.IO,
    )

  val inProcessExecutorService: ExecutorService =
    ThreadPoolExecutor(
      1,
      commonServerFlags.threadPoolSize,
      60L,
      TimeUnit.SECONDS,
      LinkedBlockingQueue(),
    )

  val inProcessServerName = InProcessServerBuilder.generateName()
  val inProcessServer: Server =
    startInProcessServerWithService(
      inProcessServerName,
      commonServerFlags,
      metricsService.withMetadataPrincipalIdentities(measurementConsumerConfigs),
      inProcessExecutorService,
    )
  val inProcessChannel =
    InProcessChannelBuilder.forName(inProcessServerName)
      .directExecutor()
      .build()
      .withShutdownTimeout(Duration.ofSeconds(30))

  val services: List<ServerServiceDefinition> =
    listOf(
      DataProvidersService(KingdomDataProvidersCoroutineStub(kingdomChannel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      EventGroupMetadataDescriptorsService(
          KingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomChannel)
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      EventGroupsService(
          KingdomEventGroupsCoroutineStub(kingdomChannel),
          InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
          celEnvCacheProvider,
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      metricsService.withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      ReportingSetsService(InternalReportingSetsCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      ReportsService(
          InternalReportsCoroutineStub(channel),
          InternalMetricCalculationSpecsCoroutineStub(channel),
          MetricsCoroutineStub(inProcessChannel),
          metricSpecConfig,
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      ReportSchedulesService(
          InternalReportSchedulesCoroutineStub(channel),
          InternalReportingSetsCoroutineStub(channel),
          KingdomDataProvidersCoroutineStub(kingdomChannel),
          KingdomEventGroupsCoroutineStub(kingdomChannel),
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      ReportScheduleIterationsService(InternalReportScheduleIterationsCoroutineStub(channel))
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      MetricCalculationSpecsService(
          InternalMetricCalculationSpecsCoroutineStub(channel),
          metricSpecConfig,
        )
        .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
    )
  CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
  inProcessChannel.shutdown()
  inProcessServer.shutdown()
  inProcessExecutorService.shutdown()
  inProcessServer.awaitTermination()
  inProcessExecutorService.awaitTermination(30, TimeUnit.SECONDS)
}

fun main(args: Array<String>) = commandLineMain(::run, args)

private class V2AlphaPublicServerFlags {
  @CommandLine.Option(
    names = ["--authority-key-identifier-to-principal-map-file"],
    description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
    required = true,
  )
  lateinit var authorityKeyIdentifierToPrincipalMapFile: File
    private set
}
