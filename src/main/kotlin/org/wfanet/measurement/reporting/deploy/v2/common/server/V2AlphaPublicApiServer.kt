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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import io.grpc.Channel
import io.grpc.Server
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusException
import io.grpc.inprocess.InProcessChannelBuilder
import java.io.File
import java.security.SecureRandom
import java.time.Duration
import java.util.concurrent.ExecutorService
import java.util.concurrent.LinkedBlockingQueue
import java.util.concurrent.ThreadPoolExecutor
import java.util.concurrent.TimeUnit
import kotlin.random.asKotlinRandom
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.access.client.v1alpha.Authorization
import org.wfanet.measurement.access.client.v1alpha.PrincipalAuthInterceptor
import org.wfanet.measurement.access.client.v1alpha.TrustedPrincipalAuthInterceptor
import org.wfanet.measurement.access.v1alpha.PermissionsGrpcKt
import org.wfanet.measurement.access.v1alpha.PrincipalsGrpcKt
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt.CertificatesCoroutineStub as KingdomCertificatesCoroutineStub
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt.DataProvidersCoroutineStub as KingdomDataProvidersCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt.EventGroupMetadataDescriptorsCoroutineStub as KingdomEventGroupMetadataDescriptorsCoroutineStub
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt.EventGroupsCoroutineStub as KingdomEventGroupsCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as KingdomMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as KingdomMeasurementsCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.instrumented
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.access.OpenIdProvidersConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
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
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleIterationsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportSchedulesService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.validate
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import picocli.CommandLine

private object V2AlphaPublicApiServer {
  private const val SERVER_NAME = "V2AlphaPublicApiServer"
  private const val IN_PROCESS_SERVER_NAME = "$SERVER_NAME-in-process"

  @CommandLine.Command(
    name = SERVER_NAME,
    description = ["Server daemon for Reporting v2alpha public API services."],
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
  )
  fun run(
    @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
    @CommandLine.Mixin kingdomApiFlags: KingdomApiFlags,
    @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
    @CommandLine.Mixin v2AlphaFlags: V2AlphaFlags,
    @CommandLine.Mixin v2AlphaPublicServerFlags: V2AlphaPublicServerFlags,
    @CommandLine.Mixin encryptionKeyPairMap: EncryptionKeyPairMap,
    @CommandLine.Option(
      names = ["--system-measurement-consumer"],
      description =
        [
          "Resource name of the CMMS MeasurementConsumer for the Reporting system.",
          "Defaults to an arbitrary MeasurementConsumer.",
          "This is used for CMMS API calls where the MeasurementConsumer is not specified.",
        ],
      required = false,
      defaultValue = CommandLine.Option.NULL_VALUE,
    )
    systemMeasurementConsumerName: String?,
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

    val accessChannel: Channel =
      buildMutualTlsChannel(
          reportingApiServerFlags.accessApiTarget,
          clientCerts,
          reportingApiServerFlags.accessApiCertHost,
        )
        .withVerboseLogging(reportingApiServerFlags.debugVerboseGrpcClientLogging)
    val authorization = Authorization(PermissionsGrpcKt.PermissionsCoroutineStub(accessChannel))

    // TODO(@SanjayVas): Load this from command-line option.
    val openIdProvidersConfig = OpenIdProvidersConfig.getDefaultInstance()
    val principalAuthInterceptor =
      PrincipalAuthInterceptor(
        openIdProvidersConfig,
        PrincipalsGrpcKt.PrincipalsCoroutineStub(accessChannel),
        true,
      )

    val measurementConsumerConfigs =
      parseTextProto(
        v2AlphaFlags.measurementConsumerConfigFile,
        MeasurementConsumerConfigs.getDefaultInstance(),
      )
    val systemMeasurementConsumerConfig: MeasurementConsumerConfig =
      if (systemMeasurementConsumerName == null) {
        measurementConsumerConfigs.configsMap.values.first()
      } else {
        measurementConsumerConfigs.configsMap.getValue(systemMeasurementConsumerName)
      }

    val internalMeasurementConsumersCoroutineStub =
      InternalMeasurementConsumersCoroutineStub(channel)
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
    metricSpecConfig.validate()

    val apiKey = measurementConsumerConfigs.configsMap.values.first().apiKey
    val celEnvCacheProvider =
      CelEnvCacheProvider(
        KingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomChannel)
          .withAuthenticationKey(apiKey),
        EventGroup.getDescriptor(),
        reportingApiServerFlags.eventGroupMetadataDescriptorCacheDuration,
        v2AlphaPublicServerFlags.knownEventGroupMetadataTypes,
        Dispatchers.Default,
      )

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
        authorization,
        InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
        SecureRandom().asKotlinRandom(),
        v2AlphaFlags.signingPrivateKeyStoreDir,
        commonServerFlags.tlsFlags.signingCerts.trustedCertificates,
        reportingApiServerFlags.defaultVidModelLine,
        reportingApiServerFlags.measurementConsumerModelLines,
        certificateCacheExpirationDuration =
          v2AlphaPublicServerFlags.certificateCacheExpirationDuration,
        dataProviderCacheExpirationDuration =
          v2AlphaPublicServerFlags.dataProviderCacheExpirationDuration,
        Dispatchers.IO,
        Dispatchers.Default,
      )

    val inProcessExecutorService: ExecutorService =
      ThreadPoolExecutor(
          1,
          commonServerFlags.threadPoolSize,
          60L,
          TimeUnit.SECONDS,
          LinkedBlockingQueue(),
        )
        .instrumented(IN_PROCESS_SERVER_NAME)

    val inProcessServer: Server =
      startInProcessServerWithService(
        IN_PROCESS_SERVER_NAME,
        commonServerFlags,
        metricsService.withInterceptor(TrustedPrincipalAuthInterceptor),
        inProcessExecutorService,
      )
    val inProcessChannel =
      InProcessChannelBuilder.forName(IN_PROCESS_SERVER_NAME)
        .directExecutor()
        .build()
        .withShutdownTimeout(Duration.ofSeconds(30))

    val services: List<ServerServiceDefinition> = buildList {
      if (v2AlphaPublicServerFlags.initNewServices) {
        // TODO(tristanvuong2021): change this once access PRs merged
        add(
          BasicReportsService(
            InternalBasicReportsCoroutineStub(channel),
            authorization,
          )
            .withInterceptor(principalAuthInterceptor)
        )
      }

      add(
        DataProvidersService(
            KingdomDataProvidersCoroutineStub(kingdomChannel),
            authorization,
            systemMeasurementConsumerConfig.apiKey,
          )
          .withInterceptor(principalAuthInterceptor)
      )
      add(
        EventGroupMetadataDescriptorsService(
            KingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomChannel),
            authorization,
            systemMeasurementConsumerConfig.apiKey,
          )
          .withInterceptor(principalAuthInterceptor),
      )
      add(
        EventGroupsService(
            KingdomEventGroupsCoroutineStub(kingdomChannel),
            authorization,
            celEnvCacheProvider,
            measurementConsumerConfigs,
            InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
          )
          .withInterceptor(principalAuthInterceptor),
      )
      add(metricsService.withInterceptor(principalAuthInterceptor))
      add(
         ReportingSetsService(InternalReportingSetsCoroutineStub(channel), authorization)
         .withInterceptor(principalAuthInterceptor)
      )
      add(
        ReportsService(
            InternalReportsCoroutineStub(channel),
            InternalMetricCalculationSpecsCoroutineStub(channel),
            MetricsCoroutineStub(inProcessChannel),
            metricSpecConfig,
            authorization,
            SecureRandom().asKotlinRandom(),
          )
          .withInterceptor(principalAuthInterceptor))
      add(ReportSchedulesService(
          InternalReportSchedulesCoroutineStub(channel),
          InternalReportingSetsCoroutineStub(channel),
          KingdomDataProvidersCoroutineStub(kingdomChannel),
          KingdomEventGroupsCoroutineStub(kingdomChannel),
          authorization,
          measurementConsumerConfigs,
        )
        .withInterceptor(principalAuthInterceptor))
      add(ReportScheduleIterationsService(
          InternalReportScheduleIterationsCoroutineStub(channel),
          authorization,
        ).withInterceptor(principalAuthInterceptor)
      )
      add(
        MetricCalculationSpecsService(
            InternalMetricCalculationSpecsCoroutineStub(channel),
            metricSpecConfig,
            authorization,
            SecureRandom().asKotlinRandom(),
          )
          .withInterceptor(principalAuthInterceptor)
      )
    }

    CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
    inProcessChannel.shutdown()
    inProcessServer.shutdown()
    inProcessExecutorService.shutdown()
    inProcessServer.awaitTermination()
    inProcessExecutorService.awaitTermination(30, TimeUnit.SECONDS)
  }

  class V2AlphaPublicServerFlags {
    @CommandLine.Option(
      names = ["--init-new-services"],
      description = ["Initialize the new Phase 1 Service if set to true."],
      required = false,
    )
    var initNewServices: Boolean = false

    @CommandLine.Option(
      names = ["--authority-key-identifier-to-principal-map-file"],
      description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
      required = true,
    )
    lateinit var authorityKeyIdentifierToPrincipalMapFile: File
      private set

    @CommandLine.Option(
      names = ["--certificate-cache-expiration-duration"],
      description = ["Duration to mark cache entries as expired in format 1d1h1m1s1ms1ns"],
      required = true,
    )
    lateinit var certificateCacheExpirationDuration: Duration
      private set

    @CommandLine.Option(
      names = ["--data-provider-cache-expiration-duration"],
      description = ["Duration to mark cache entries as expired in format 1d1h1m1s1ms1ns"],
      required = true,
    )
    lateinit var dataProviderCacheExpirationDuration: Duration
      private set

    @CommandLine.Option(
      names = ["--known-event-group-metadata-type"],
      description =
        [
          "File path to FileDescriptorSet containing known EventGroup metadata types.",
          "This is in addition to standard protobuf well-known types.",
          "Can be specified multiple times.",
        ],
      required = false,
      defaultValue = "",
    )
    private fun setKnownEventGroupMetadataTypes(fileDescriptorSetFiles: List<File>) {
      val fileDescriptorSets =
        fileDescriptorSetFiles.map { file ->
          file.inputStream().use { input -> DescriptorProtos.FileDescriptorSet.parseFrom(input) }
        }
      knownEventGroupMetadataTypes = ProtoReflection.buildFileDescriptors(fileDescriptorSets)
    }

    lateinit var knownEventGroupMetadataTypes: List<Descriptors.FileDescriptor>
      private set
  }
}

fun main(args: Array<String>) = commandLineMain(V2AlphaPublicApiServer::run, args)
