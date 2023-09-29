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

package org.wfanet.measurement.kingdom.deploy.common.job

import io.opentelemetry.api.OpenTelemetry
import io.opentelemetry.api.common.Attributes
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter
import io.opentelemetry.sdk.OpenTelemetrySdk
import io.opentelemetry.sdk.metrics.SdkMeterProvider
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader
import io.opentelemetry.sdk.resources.Resource
import io.opentelemetry.semconv.resource.attributes.ResourceAttributes
import java.time.Clock
import java.time.Duration
import kotlin.properties.Delegates
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineStub
import org.wfanet.measurement.kingdom.batch.CompletedMeasurementsDeletion
import org.wfanet.measurement.kingdom.deploy.common.server.KingdomApiServerFlags
import picocli.CommandLine

@CommandLine.Command(
  name = "CompletedMeasurementsDeletionJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true
)
class CompletedMeasurementsDeletionJob private constructor() : Runnable {
  @CommandLine.Mixin private lateinit var tlsFlags: TlsFlags

  @CommandLine.Mixin private lateinit var kingdomApiServerFlags: KingdomApiServerFlags

  @set:CommandLine.Option(
    names = ["--max-to-delete-per-rpc"],
    defaultValue = "25",
    description =
      [
        "The maximum number of measurements to delete in a single call to the API.",
        "This flag can be adjusted to ensure the RPC does not timeout."
      ],
  )
  private var maxToDeletePerRpc by Delegates.notNull<Int>()

  @CommandLine.Option(
    names = ["--time-to-live"],
    defaultValue = "100d",
    description =
      [
        "Time to live (TTL) for a completed Measurement since latest update time. After " +
          "completion, a Measurement won't live longer than this duration."
      ],
  )
  private lateinit var measurementsTimeToLive: Duration

  @CommandLine.Option(
    names = ["--otel-exporter-otlp-endpoint"],
    description = ["Endpoint for OpenTelemetry Collector."],
    required = true,
  )
  private lateinit var otelExporterOtlpEndpoint: String

  @CommandLine.Option(
    names = ["--otel-service-name"],
    description = ["Service name to label cronjob metrics with."],
    required = true,
  )
  private lateinit var otelServiceName: String

  @set:CommandLine.Option(
    names = ["--dry-run"],
    description =
      [
        "Option to print the Measurements that would be deleted instead of performing the deletion."
      ],
    required = false,
    defaultValue = "false"
  )
  private var dryRun by Delegates.notNull<Boolean>()

  override fun run() {
    val clientCerts =
      SigningCerts.fromPemFiles(
        certificateFile = tlsFlags.certFile,
        privateKeyFile = tlsFlags.privateKeyFile,
        trustedCertCollectionFile = tlsFlags.certCollectionFile
      )

    val channel =
      buildMutualTlsChannel(
          kingdomApiServerFlags.internalApiFlags.target,
          clientCerts,
          kingdomApiServerFlags.internalApiFlags.certHost
        )
        .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)
        .withDefaultDeadline(kingdomApiServerFlags.internalApiFlags.defaultDeadlineDuration)

    val internalMeasurementsClient = MeasurementsCoroutineStub(channel)

    val otlpEndpoint = otelExporterOtlpEndpoint
    val otelServiceName = otelServiceName
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
            .setInterval(Duration.ofSeconds(60L))
            .build()
        )
        .build()
    val openTelemetry: OpenTelemetry =
      OpenTelemetrySdk.builder().setMeterProvider(meterProvider).build()

    val completedMeasurementsDeletion =
      CompletedMeasurementsDeletion(
        internalMeasurementsClient,
        maxToDeletePerRpc,
        measurementsTimeToLive,
        dryRun,
        Clock.systemUTC(),
        openTelemetry
      )
    completedMeasurementsDeletion.run()
  }

  companion object {
    @JvmStatic
    fun main(args: Array<String>) = commandLineMain(CompletedMeasurementsDeletionJob(), args)
  }
}
