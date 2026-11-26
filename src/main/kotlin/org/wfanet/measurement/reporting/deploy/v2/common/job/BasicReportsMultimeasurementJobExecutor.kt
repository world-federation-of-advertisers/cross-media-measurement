/*
 * Copyright 2026 The Cross-Media Measurement Authors
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
import java.time.Duration
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.v2alpha.MeasurementConsumersGrpcKt.MeasurementConsumersCoroutineStub as CmmsMeasurementConsumersCoroutineStub
import org.wfanet.measurement.api.v2alpha.MeasurementsGrpcKt.MeasurementsCoroutineStub as CmmsMeasurementsCoroutineStub
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withShutdownTimeout
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportResultsGrpcKt.ReportResultsCoroutineStub
import org.wfanet.measurement.reporting.deploy.v2.common.KingdomApiFlags
import org.wfanet.measurement.reporting.deploy.v2.common.ReportingApiServerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.V2AlphaFlags
import org.wfanet.measurement.reporting.job.BasicReportsMeasurementsJob
import org.wfanet.measurement.reporting.job.BasicReportsMultimeasurementResultsJob
import picocli.CommandLine

@CommandLine.Command(
  name = "BasicReportsMultimeasurementJobExecutor",
  description =
    [
      "Creates the CMMS Measurements of multimeasurement BasicReports, then writes the results of " +
        "those whose Measurements have completed."
    ],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin kingdomApiFlags: KingdomApiFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin v2AlphaFlags: V2AlphaFlags,
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

  val measurementConsumerConfigs =
    parseTextProto(
      v2AlphaFlags.measurementConsumerConfigFile,
      MeasurementConsumerConfigs.getDefaultInstance(),
    )
  val internalBasicReportsStub = InternalBasicReportsCoroutineStub(channel)
  val cmmsMeasurementsStub = CmmsMeasurementsCoroutineStub(kingdomChannel)

  runBlocking {
    BasicReportsMeasurementsJob(
        measurementConsumerConfigs,
        internalBasicReportsStub,
        CmmsMeasurementConsumersCoroutineStub(kingdomChannel),
        cmmsMeasurementsStub,
      )
      .execute()
    BasicReportsMultimeasurementResultsJob(
        measurementConsumerConfigs,
        internalBasicReportsStub,
        ReportResultsCoroutineStub(channel),
        cmmsMeasurementsStub,
      )
      .execute()
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
