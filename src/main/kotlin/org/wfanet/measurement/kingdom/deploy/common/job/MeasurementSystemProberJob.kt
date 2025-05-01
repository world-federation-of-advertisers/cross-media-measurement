/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import java.io.File
import java.time.Duration
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.DataProvidersGrpcKt
import org.wfanet.measurement.api.v2alpha.EventGroupsGrpcKt
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.kingdom.batch.MeasurementSystemProber
import picocli.CommandLine.Command
import picocli.CommandLine.Mixin
import picocli.CommandLine.Option

private class MeasurementSystemProberFlags {
  @Option(
    names = ["--measurement-consumer"],
    description = ["API resource name of the MeasurementConsumer"],
    required = true,
  )
  lateinit var measurementConsumer: String
    private set

  @Option(
    names = ["--private-key-der-file"],
    description = ["Private key for MeasurementConsumer"],
    required = true,
  )
  lateinit var privateKeyDerFile: File
    private set

  @Option(
    names = ["--api-key"],
    description = ["API authentication key for the MeasurementConsumer"],
    required = true,
  )
  lateinit var apiAuthenticationKey: String
    private set

  @Mixin
  lateinit var tlsFlags: TlsFlags
    private set

  @Option(
    names = ["--kingdom-public-api-target"],
    description = ["gRPC target (authority) of the Kingdom public API server"],
    required = true,
  )
  lateinit var target: String
    private set

  @Option(
    names = ["--kingdom-public-api-cert-host"],
    description =
      [
        "Expected hostname (DNS-ID) in the Kingdom public API server's TLS certificate.",
        "This overrides derivation of the TLS DNS-ID from --kingdom-public-api-target.",
      ],
    required = false,
  )
  var certHost: String? = null
    private set

  @set:Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set

  @Option(
    names = ["--data-provider"],
    description = ["Data provider API resource name (can be specified multiple times)"],
    required = true,
    arity = "1..*",
  )
  lateinit var dataProvider: List<String>
    private set

  @Option(
    names = ["--measurement-lookback-duration"],
    description =
      [
        "Subtracted from the current time, specifies the start time for the interval of event data collection"
      ],
    required = true,
    defaultValue = "1d",
  )
  lateinit var measurementLookbackDuration: Duration
    private set

  @Option(
    names = ["--duration-between-measurements"],
    description =
      [
        "Added to the update time of the most recently completed measurement, determines whether enough time has elapsed to request a new measurement"
      ],
    required = true,
    defaultValue = "1d",
  )
  lateinit var durationBetweenMeasurement: Duration
    private set

  @Option(
    names = ["--measurement-update-lookback-duration"],
    description =
      [
        "Subtracted from the current time to get the window for checking recent updated measurements"
      ],
    required = false,
    defaultValue = "2h",
  )
  lateinit var measurementUpdateLookbackDuration: Duration
    private set
}

@Command(
  name = "MeasurementSystemProberJob",
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(@Mixin flags: MeasurementSystemProberFlags) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = flags.tlsFlags.certFile,
      privateKeyFile = flags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = flags.tlsFlags.certCollectionFile,
    )

  val channel =
    buildMutualTlsChannel(flags.target, clientCerts, flags.certHost)
      .withVerboseLogging(flags.debugVerboseGrpcClientLogging)

  val measurementsService =
    org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub(channel)
  val measurementConsumersService =
    MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub(channel)
  val dataProvidersService = DataProvidersGrpcKt.DataProvidersCoroutineStub(channel)
  val eventGroupsService = EventGroupsGrpcKt.EventGroupsCoroutineStub(channel)
  val requisitionsService = RequisitionsGrpcKt.RequisitionsCoroutineStub(channel)

  val measurementSystemProber =
    MeasurementSystemProber(
      flags.measurementConsumer,
      flags.dataProvider,
      flags.apiAuthenticationKey,
      flags.privateKeyDerFile,
      flags.measurementLookbackDuration,
      flags.durationBetweenMeasurement,
      flags.measurementUpdateLookbackDuration,
      measurementConsumersService,
      measurementsService,
      dataProvidersService,
      eventGroupsService,
      requisitionsService,
    )
  runBlocking { measurementSystemProber.run() }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
