// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.daemon.mill.liquidlegionsv2

import com.google.protobuf.ByteString
import io.opentelemetry.api.GlobalOpenTelemetry
import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.Aggregation
import io.opentelemetry.sdk.metrics.InstrumentSelector
import io.opentelemetry.sdk.metrics.InstrumentType
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.View
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import java.time.Clock
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.crypto.SigningKeyHandle
import org.wfanet.measurement.common.crypto.readCertificate
import org.wfanet.measurement.common.crypto.readPrivateKey
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.identity.DuchyInfo
import org.wfanet.measurement.common.identity.withDuchyId
import org.wfanet.measurement.common.throttler.MinimumIntervalThrottler
import org.wfanet.measurement.duchy.daemon.mill.Certificate
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.LiquidLegionsV2Mill
import org.wfanet.measurement.duchy.daemon.mill.liquidlegionsv2.crypto.JniLiquidLegionsV2Encryption
import org.wfanet.measurement.duchy.db.computation.ComputationDataClients
import org.wfanet.measurement.internal.duchy.ComputationStatsGrpcKt.ComputationStatsCoroutineStub
import org.wfanet.measurement.internal.duchy.ComputationsGrpcKt.ComputationsCoroutineStub
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.measurement.system.v1alpha.ComputationControlGrpcKt.ComputationControlCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationLogEntriesGrpcKt.ComputationLogEntriesCoroutineStub as SystemComputationLogEntriesCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationParticipantsGrpcKt.ComputationParticipantsCoroutineStub as SystemComputationParticipantsCoroutineStub
import org.wfanet.measurement.system.v1alpha.ComputationsGrpcKt.ComputationsCoroutineStub as SystemComputationsCoroutineStub
import picocli.CommandLine

private const val OTEL_EXPORTER_OTLP_ENDPOINT = "OTEL_EXPORTER_OTLP_ENDPOINT"
private const val OTEL_SERVICE_NAME = "OTEL_SERVICE_NAME"

abstract class LiquidLegionsV2MillDaemon : Runnable {
  @CommandLine.Mixin
  protected lateinit var flags: LiquidLegionsV2MillFlags
    private set

  protected fun run(storageClient: StorageClient) {
    DuchyInfo.initializeFromFlags(flags.duchyInfoFlags)
    val duchyName = flags.duchy.duchyName

    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = flags.tlsFlags.certFile,
        privateKeyFile = flags.tlsFlags.privateKeyFile,
        trustedCertCollectionFile = flags.tlsFlags.certCollectionFile
      )

    val computationsServiceChannel =
      buildMutualTlsChannel(
          flags.computationsServiceFlags.target,
          clientCerts,
          flags.computationsServiceFlags.certHost
        )
        .withShutdownTimeout(flags.channelShutdownTimeout)
    val dataClients =
      ComputationDataClients(
        ComputationsCoroutineStub(computationsServiceChannel).withDuchyId(duchyName),
        storageClient
      )

    val computationControlClientMap =
      DuchyInfo.entries
        .filterKeys { it != duchyName }
        .mapValues { (duchyId, entry) ->
          ComputationControlCoroutineStub(
              buildMutualTlsChannel(
                  flags.computationControlServiceTargets.getValue(duchyId),
                  clientCerts,
                  entry.computationControlServiceCertHost
                )
                .withShutdownTimeout(flags.channelShutdownTimeout)
            )
            .withDuchyId(duchyName)
        }

    val systemApiChannel =
      buildMutualTlsChannel(flags.systemApiFlags.target, clientCerts, flags.systemApiFlags.certHost)
        .withShutdownTimeout(flags.channelShutdownTimeout)

    val systemComputationsClient =
      SystemComputationsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationParticipantsClient =
      SystemComputationParticipantsCoroutineStub(systemApiChannel).withDuchyId(duchyName)
    val systemComputationLogEntriesClient =
      SystemComputationLogEntriesCoroutineStub(systemApiChannel).withDuchyId(duchyName)

    val computationStatsClient = ComputationStatsCoroutineStub(computationsServiceChannel)

    val csX509Certificate =
      flags.csCertificateDerFile.inputStream().use { input -> readCertificate(input) }
    val csCertificate = Certificate(flags.csCertificateName, csX509Certificate)
    // TODO: Read from a KMS-encrypted store instead.
    val csSigningKey =
      SigningKeyHandle(
        csX509Certificate,
        flags.csPrivateKeyDerFile.inputStream().use { input ->
          readPrivateKey(ByteString.readFrom(input), csX509Certificate.publicKey.algorithm)
        }
      )

    // This will be the name of the pod when deployed to Kubernetes. Note that the millId is
    // included in mill logs to help debugging.
    val millId = System.getenv("HOSTNAME")

    val otlpEndpoint: String? = System.getenv(OTEL_EXPORTER_OTLP_ENDPOINT)
    val otelServiceName: String? = System.getenv(OTEL_SERVICE_NAME)
    val openTelemetry: OpenTelemetry =
      if (otlpEndpoint == null || otelServiceName == null) {
        GlobalOpenTelemetry.get()
      } else {
        val resource: Resource =
          Resource.getDefault()
            .merge(Resource.create(Attributes.of(ResourceAttributes.SERVICE_NAME, otelServiceName)))
        val meterProvider =
          SdkMeterProvider.builder()
            .setResource(resource)
            .registerMetricReader(
              PeriodicMetricReader.builder(
                  OtlpGrpcMetricExporter.builder()
                    .setTimeout(Duration.ofSeconds(30L))
                    .setEndpoint(otlpEndpoint)
                    .build()
                )
                .build()
            )
            .registerView(
              InstrumentSelector.builder().setType(InstrumentType.HISTOGRAM).build(),
              View.builder()
                .setAggregation(
                  Aggregation.explicitBucketHistogram(
                    listOf(0.5, 1.0, 15.0, 30.0, 60.0, 120.0, 180.0, 240.0, 300.0, 600.0, 1200.0)
                  )
                )
                .build()
            )
            .build()
        OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build()
      }

    val mill =
      LiquidLegionsV2Mill(
        millId = millId,
        duchyId = flags.duchy.duchyName,
        signingKey = csSigningKey,
        consentSignalCert = csCertificate,
        dataClients = dataClients,
        systemComputationParticipantsClient = systemComputationParticipantsClient,
        systemComputationsClient = systemComputationsClient,
        systemComputationLogEntriesClient = systemComputationLogEntriesClient,
        computationStatsClient = computationStatsClient,
        workerStubs = computationControlClientMap,
        cryptoWorker = JniLiquidLegionsV2Encryption(),
        throttler = MinimumIntervalThrottler(Clock.systemUTC(), flags.pollingInterval),
        requestChunkSizeBytes = flags.requestChunkSizeBytes,
        openTelemetry = openTelemetry
      )

    runBlocking { mill.continuallyProcessComputationQueue() }
  }
}
