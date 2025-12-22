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

package org.wfanet.measurement.reporting.deploy.v2.common.job

import io.grpc.Channel
import io.grpc.Server
import io.grpc.inprocess.InProcessChannelBuilder
import io.grpc.inprocess.InProcessServerBuilder
import java.security.SecureRandom
import java.time.Duration
import kotlin.random.asKotlinRandom
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.withTrustedPrincipalAuthentication
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as KingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as KingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as KingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as KingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub as KingdomModelLinesCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.InProcessServersMethods.startInProcessServerWithService
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub as InternalReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub as InternalReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.reporting.deploy.v2.common.EncryptionKeyPairMap
import org.wfanet.measurement.reporting.deploy.v2.common.KingdomApiFlags
import org.wfanet.measurement.reporting.deploy.v2.common.ReportingApiServerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.V2AlphaFlags
import org.wfanet.measurement.reporting.job.ReportSchedulingJob
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleInfoServerInterceptor.Companion.withReportScheduleInfoInterceptor
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportsService
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
import picocli.CommandLine

@CommandLine.Command(
  name = "ReportSchedulingJobExecutor",
  description = ["Process for Reporting V2Alpha Report Scheduling."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin kingdomApiFlags: KingdomApiFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin v2AlphaFlags: V2AlphaFlags,
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
      .withShutdownTimeout(Duration.ofSeconds(5))
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val kingdomChannel: Channel =
    buildMutualTlsChannel(
        target = kingdomApiFlags.target,
        clientCerts = clientCerts,
        hostName = kingdomApiFlags.certHost,
      )
      .withShutdownTimeout(Duration.ofSeconds(5))
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)

  val accessChannel: Channel =
    buildMutualTlsChannel(
        reportingApiServerFlags.accessApiTarget,
        clientCerts,
        reportingApiServerFlags.accessApiCertHost,
      )
      .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)
  val authorization = Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(accessChannel))

  val measurementConsumerConfigs =
    parseTextProto(
      v2AlphaFlags.measurementConsumerConfigFile,
      MeasurementConsumerConfigs.getDefaultInstance(),
    )

  val metricSpecConfig =
    parseTextProto(v2AlphaFlags.metricSpecConfigFile, MetricSpecConfig.getDefaultInstance())

  val metricsService =
    MetricsService(
        metricSpecConfig,
        measurementConsumerConfigs,
        InternalReportingSetsCoroutineStub(channel),
        InternalMetricsCoroutineStub(channel),
        VariancesImpl,
        InternalMeasurementsCoroutineStub(channel),
        KingdomDataProvidersCoroutineStub(kingdomChannel),
        KingdomMeasurementsCoroutineStub(kingdomChannel),
        KingdomCertificatesCoroutineStub(kingdomChannel),
        KingdomMeasurementConsumersCoroutineStub(kingdomChannel),
        KingdomModelLinesCoroutineStub(kingdomChannel),
        authorization,
        InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
        SecureRandom().asKotlinRandom(),
        v2AlphaFlags.signingPrivateKeyStoreDir,
        commonServerFlags.tlsFlags.signingCerts.trustedCertificates,
        defaultVidModelLine = "",
        measurementConsumerModelLines = emptyMap(),
        certificateCacheExpirationDuration = Duration.ofMinutes(60),
        dataProviderCacheExpirationDuration = Duration.ofMinutes(60),
        keyReaderContext = Dispatchers.IO,
        cacheLoaderContext = Dispatchers.Default,
        populationDataProvider = reportingApiServerFlags.populationDataProvider,
      )
      .withTrustedPrincipalAuthentication()

  val inProcessMetricsServerName = InProcessServerBuilder.generateName()
  val inProcessMetricsServer: Server =
    startInProcessServerWithService(inProcessMetricsServerName, commonServerFlags, metricsService)
  val inProcessMetricsChannel =
    InProcessChannelBuilder.forName(inProcessMetricsServerName)
      .directExecutor()
      .build()
      .withShutdownTimeout(Duration.ofSeconds(5))

  val reportsService =
    ReportsService(
        InternalReportsCoroutineStub(channel),
        InternalMetricCalculationSpecsCoroutineStub(channel),
        MetricsCoroutineStub(inProcessMetricsChannel),
        metricSpecConfig,
        authorization,
        SecureRandom().asKotlinRandom(),
      )
      .withTrustedPrincipalAuthentication()
      .withReportScheduleInfoInterceptor()

  val inProcessReportsServerName = InProcessServerBuilder.generateName()
  val inProcessReportsServer: Server =
    startInProcessServerWithService(inProcessReportsServerName, commonServerFlags, reportsService)
  val inProcessReportsChannel =
    InProcessChannelBuilder.forName(inProcessReportsServerName)
      .directExecutor()
      .build()
      .withShutdownTimeout(Duration.ofSeconds(5))

  val reportSchedulingJob =
    ReportSchedulingJob(
      measurementConsumerConfigs,
      KingdomDataProvidersCoroutineStub(kingdomChannel),
      KingdomEventGroupsCoroutineStub(kingdomChannel),
      InternalReportingSetsCoroutineStub(channel),
      InternalReportScheduleIterationsCoroutineStub(channel),
      InternalReportSchedulesCoroutineStub(channel),
      ReportsCoroutineStub(inProcessReportsChannel),
    )

  runBlocking { reportSchedulingJob.execute() }
  inProcessMetricsChannel.shutdown()
  inProcessReportsChannel.shutdown()
  inProcessMetricsServer.shutdown()
  inProcessReportsServer.shutdown()
  inProcessMetricsServer.awaitTermination()
  inProcessReportsServer.awaitTermination()
}

fun main(args: Array<String>) = commandLineMain(::run, args)
