/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.securecomputation.deploy.common.server

import com.google.protobuf.ByteString
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
import org.wfanet.measurement.api.v2alpha.CertificatesGrpcKt
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupMetadataDescriptorsGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.api.withAuthenticationKey
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.api.PrincipalLookup
import org.wfanet.measurement.common.api.memoizing
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.instrumented
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.config.reporting.MetricSpecConfig
import org.wfanet.measurement.internal.reporting.v2.MeasurementConsumersGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.measurementConsumer
import org.wfanet.measurement.measurementconsumer.stats.VariancesImpl
import org.wfanet.measurement.reporting.deploy.v2.common.EncryptionKeyPairMap
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
import org.wfanet.measurement.reporting.service.api.v2alpha.validate
import org.wfanet.measurement.reporting.service.api.v2alpha.withPrincipalsFromX509AuthorityKeyIdentifiers
import org.wfanet.measurement.reporting.v2alpha.EventGroup
import org.wfanet.measurement.securecomputation.deploy.common.ControlPlaneApiServerFlags
import picocli.CommandLine

private object  V1AlphaPublicApiServer {

  private const val SERVER_NAME = "V1AlphaPublicApiServer"

  @CommandLine.Command(
    name = SERVER_NAME,
    description = ["Server daemon for ControlPlane v1alpha public API services."],
    mixinStandardHelpOptions = true,
    showDefaultValues = true,
  )
  fun run(
    @CommandLine.Mixin controlPlaneApiServerFlags: ControlPlaneApiServerFlags,
    @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
    @CommandLine.Option(
      names = ["--access-api-target"],
      description = ["gRPC target of the Access public API server"],
      required = true,
    )
    accessApiTarget: String,
    @CommandLine.Option(
      names = ["--access-api-cert-host"],
      description =
      [
        "Expected hostname (DNS-ID) in the Access public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --access-api-target.",
      ],
      required = false,
      defaultValue = CommandLine.Option.NULL_VALUE,
    )
    accessApiCertHost: String?,
  ) {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = commonServerFlags.tlsFlags.certFile,
        privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile,
      )
    val channel: Channel =
      buildMutualTlsChannel(
        controlPlaneApiServerFlags.internalApiFlags.target,
        clientCerts,
        controlPlaneApiServerFlags.internalApiFlags.certHost,
      )
        .withVerboseLogging(controlPlaneApiServerFlags.debugVerboseGrpcClientLogging)

    val services: List<ServerServiceDefinition> =
      listOf(
        ReportingSetsService(ReportingSetsGrpcKt.ReportingSetsCoroutineStub(channel))
          .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
        ReportsService(
          ReportsGrpcKt.ReportsCoroutineStub(channel),
          MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineStub(channel),
          org.wfanet.measurement.reporting.v2alpha.MetricsGrpcKt.MetricsCoroutineStub(inProcessChannel),
          metricSpecConfig,
          SecureRandom().asKotlinRandom(),
        )
          .withPrincipalsFromX509AuthorityKeyIdentifiers(principalLookup),
      )
    CommonServer.fromFlags(commonServerFlags, SERVER_NAME, services).start().blockUntilShutdown()

  }

fun main(args: Array<String>) = commandLineMain(V1AlphaPublicApiServer::run, args)

