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
import com.google.protobuf.util.JsonFormat
import io.grpc.Channel
import io.grpc.ServerServiceDefinition
import io.grpc.Status
import io.grpc.StatusRuntimeException
import io.grpc.inprocess.InProcessChannelBuilder
import java.io.File
import java.security.SecureRandom
import java.time.Duration
import kotlin.random.asKotlinRandom
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.asCoroutineDispatcher
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
import org.wfanet.measurement.api.v2alpha.ModelLinesGrpcKt.ModelLinesCoroutineStub as KingdomModelLinesCoroutineStub
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.InProcessServersMethods.startInProcessServerWithService
import org.wfanet.measurement.common.grpc.ServiceFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withInterceptor
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.access.OpenIdProvidersConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFilter as InternalImpressionQualificationFilter
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpc as InternalImpressionQualificationFiltersGrpc
import org.wfanet.measurement.internal.reporting.v2.ImpressionQualificationFiltersGrpcKt.ImpressionQualificationFiltersCoroutineStub as InternalImpressionQualificationFiltersCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpc as InternalMeasurementConsumersGrpc
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt.MeasurementsCoroutineStub as InternalMeasurementsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub as InternalMetricCalculationSpecsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineStub as InternalMetricsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineStub as InternalReportScheduleIterationsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineStub as InternalReportSchedulesCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt.ReportsCoroutineStub as InternalReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.getImpressionQualificationFilterRequest
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.reporting.deploy.v2.common.EncryptionKeyPairMap
import org.wfanet.measurement.reporting.deploy.v2.common.EventMessageFlags
import org.wfanet.measurement.reporting.deploy.v2.common.KingdomApiFlags
import org.wfanet.measurement.reporting.deploy.v2.common.ReportingApiServerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.V2AlphaFlags
import org.wfanet.measurement.reporting.service.api.CelEnvCacheProvider
import org.wfanet.measurement.reporting.service.api.InMemoryEncryptionKeyPairStore
import org.wfanet.measurement.reporting.service.api.v2alpha.BasicReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.DataProvidersService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupMetadataDescriptorsService
import org.wfanet.measurement.reporting.service.api.v2alpha.EventGroupsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ImpressionQualificationFilterKey
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricCalculationSpecsService
import org.wfanet.measurement.reporting.service.api.v2alpha.MetricsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ModelLinesService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportScheduleIterationsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportSchedulesService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportingSetsService
import org.wfanet.measurement.reporting.service.api.v2alpha.ReportsService
import org.wfanet.measurement.reporting.service.api.v2alpha.validate
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub
import org.wfanet.measurement.reporting.v2alpha.ReportsGrpcKt.ReportsCoroutineStub
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
    @CommandLine.Mixin serviceFlags: ServiceFlags,
    @CommandLine.Mixin v2AlphaFlags: V2AlphaFlags,
    @CommandLine.Mixin v2AlphaPublicServerFlags: V2AlphaPublicServerFlags,
    @CommandLine.Mixin encryptionKeyPairMap: EncryptionKeyPairMap,
    @CommandLine.Mixin eventMessageFlags: EventMessageFlags,
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

    val openIdProvidersConfig =
      v2AlphaPublicServerFlags.openIdProvidersConfigFile.bufferedReader().use { reader ->
        OpenIdProvidersConfig.newBuilder().also { JsonFormat.parser().merge(reader, it) }.build()
      }
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

    val internalMeasurementConsumersBlockingStub =
      InternalMeasurementConsumersGrpc.newBlockingStub(channel)

    measurementConsumerConfigs.configsMap.keys.forEach {
      val measurementConsumerKey =
        MeasurementConsumerKey.fromName(it)
          ?: throw IllegalArgumentException("measurement_consumer_config is invalid")
      try {
        internalMeasurementConsumersBlockingStub.createMeasurementConsumer(
          measurementConsumer {
            cmmsMeasurementConsumerId = measurementConsumerKey.measurementConsumerId
          }
        )
      } catch (e: StatusRuntimeException) {
        when (e.status.code) {
          Status.Code.ALREADY_EXISTS -> {}
          else -> throw e
        }
      }
    }

    val internalImpressionQualificationFiltersBlockingStub =
      InternalImpressionQualificationFiltersGrpc.newBlockingStub(channel)

    val baseImpressionQualificationFilters: List<InternalImpressionQualificationFilter> =
      buildList {
        reportingApiServerFlags.baseImpressionQualificationFilters.forEach {
          try {
            add(
              internalImpressionQualificationFiltersBlockingStub.getImpressionQualificationFilter(
                getImpressionQualificationFilterRequest {
                  externalImpressionQualificationFilterId =
                    ImpressionQualificationFilterKey.fromName(it)?.impressionQualificationFilterId
                      ?: throw IllegalArgumentException(
                        "$it in base_impression_qualification_filters is an invalid resource name"
                      )
                }
              )
            )
          } catch (e: StatusRuntimeException) {
            when (e.status.code) {
              Status.Code.NOT_FOUND -> {
                throw IllegalArgumentException(
                  "$it in base_impression_qualification_filters is not found"
                )
              }

              else -> throw e
            }
          }
        }
      }

    val metricSpecConfig =
      parseTextProto(v2AlphaFlags.metricSpecConfigFile, MetricSpecConfig.getDefaultInstance())
    metricSpecConfig.validate()

    val basicReportMetricSpecConfig =
      if (v2AlphaFlags.basicReportMetricSpecConfigFile == null) {
        metricSpecConfig
      } else {
        parseTextProto(
          v2AlphaFlags.basicReportMetricSpecConfigFile!!,
          MetricSpecConfig.getDefaultInstance(),
        )
      }
    basicReportMetricSpecConfig.validate()

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

    val serviceDispatcher: CoroutineDispatcher = serviceFlags.executor.asCoroutineDispatcher()

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
        reportingApiServerFlags.defaultVidModelLine,
        reportingApiServerFlags.measurementConsumerModelLines,
        certificateCacheExpirationDuration =
          v2AlphaPublicServerFlags.certificateCacheExpirationDuration,
        dataProviderCacheExpirationDuration =
          v2AlphaPublicServerFlags.dataProviderCacheExpirationDuration,
        keyReaderContext = Dispatchers.IO,
        cacheLoaderContext = Dispatchers.Default,
        coroutineContext = serviceDispatcher,
        populationDataProvider = reportingApiServerFlags.populationDataProvider,
      )

    startInProcessServerWithService(
      "$IN_PROCESS_SERVER_NAME-metrics",
      commonServerFlags,
      metricsService.withInterceptor(TrustedPrincipalAuthInterceptor),
    )

    val inProcessMetricsChannel =
      InProcessChannelBuilder.forName("$IN_PROCESS_SERVER_NAME-metrics")
        .directExecutor()
        .build()
        .withShutdownTimeout(Duration.ofSeconds(30))

    val reportsService =
      ReportsService(
        InternalReportsCoroutineStub(channel),
        InternalMetricCalculationSpecsCoroutineStub(channel),
        MetricsCoroutineStub(inProcessMetricsChannel),
        metricSpecConfig,
        authorization,
        SecureRandom().asKotlinRandom(),
        reportingApiServerFlags.allowSamplingIntervalWrapping,
        serviceDispatcher,
      )

    startInProcessServerWithService(
      "$IN_PROCESS_SERVER_NAME-reports",
      commonServerFlags,
      reportsService.withInterceptor(TrustedPrincipalAuthInterceptor),
    )

    val inProcessReportsChannel =
      InProcessChannelBuilder.forName("$IN_PROCESS_SERVER_NAME-reports")
        .directExecutor()
        .build()
        .withShutdownTimeout(Duration.ofSeconds(30))

    val services: List<ServerServiceDefinition> =
      listOf(
        DataProvidersService(
            KingdomDataProvidersCoroutineStub(kingdomChannel),
            authorization,
            systemMeasurementConsumerConfig.apiKey,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        EventGroupMetadataDescriptorsService(
            KingdomEventGroupMetadataDescriptorsCoroutineStub(kingdomChannel),
            authorization,
            systemMeasurementConsumerConfig.apiKey,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        EventGroupsService(
            KingdomEventGroupsCoroutineStub(kingdomChannel),
            authorization,
            celEnvCacheProvider,
            measurementConsumerConfigs,
            InMemoryEncryptionKeyPairStore(encryptionKeyPairMap.keyPairs),
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        metricsService.withInterceptor(principalAuthInterceptor),
        ReportingSetsService(
            InternalReportingSetsCoroutineStub(channel),
            authorization,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        reportsService.withInterceptor(principalAuthInterceptor),
        ReportSchedulesService(
            InternalReportSchedulesCoroutineStub(channel),
            InternalReportingSetsCoroutineStub(channel),
            KingdomDataProvidersCoroutineStub(kingdomChannel),
            KingdomEventGroupsCoroutineStub(kingdomChannel),
            authorization,
            measurementConsumerConfigs,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        ReportScheduleIterationsService(
            InternalReportScheduleIterationsCoroutineStub(channel),
            authorization,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        MetricCalculationSpecsService(
            InternalMetricCalculationSpecsCoroutineStub(channel),
            KingdomModelLinesCoroutineStub(kingdomChannel),
            metricSpecConfig,
            authorization,
            SecureRandom().asKotlinRandom(),
            measurementConsumerConfigs,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        BasicReportsService(
            InternalBasicReportsCoroutineStub(channel),
            InternalImpressionQualificationFiltersCoroutineStub(channel),
            InternalReportingSetsCoroutineStub(channel),
            InternalMetricCalculationSpecsCoroutineStub(channel),
            ReportsCoroutineStub(inProcessReportsChannel),
            KingdomModelLinesCoroutineStub(kingdomChannel),
            eventMessageFlags.eventDescriptor,
            basicReportMetricSpecConfig,
            SecureRandom().asKotlinRandom(),
            authorization,
            measurementConsumerConfigs,
            baseImpressionQualificationFilters.map { it.externalImpressionQualificationFilterId },
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
        ModelLinesService(
            KingdomModelLinesCoroutineStub(kingdomChannel),
            authorization,
            systemMeasurementConsumerConfig.apiKey,
            serviceDispatcher,
          )
          .withInterceptor(principalAuthInterceptor),
      )

    CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()
  }

  class V2AlphaPublicServerFlags {
    @CommandLine.Option(
      names = ["--authority-key-identifier-to-principal-map-file"],
      description = ["File path to a AuthorityKeyToPrincipalMap textproto"],
      required = true,
    )
    lateinit var authorityKeyIdentifierToPrincipalMapFile: File
      private set

    @CommandLine.Option(
      names = ["--open-id-providers-config-file"],
      description = ["File path to OpenIdProvidersConfig in ProtoJSON format"],
      required = true,
    )
    lateinit var openIdProvidersConfigFile: File
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
