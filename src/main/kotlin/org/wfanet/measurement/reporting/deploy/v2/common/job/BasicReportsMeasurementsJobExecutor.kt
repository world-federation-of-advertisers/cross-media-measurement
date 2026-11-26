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
import java.io.File
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
import org.wfanet.measurement.config.reporting.ImpressionQualificationFilterConfig
import org.wfanet.measurement.config.reporting.MeasurementConsumerConfigs
import org.wfanet.measurement.internal.reporting.v2.BasicReportsGrpcKt.BasicReportsCoroutineStub as InternalBasicReportsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.reporting.deploy.v2.common.EventMessageFlags
import org.wfanet.measurement.reporting.deploy.v2.common.KingdomApiFlags
import org.wfanet.measurement.reporting.deploy.v2.common.ReportingApiServerFlags
import org.wfanet.measurement.reporting.deploy.v2.common.V2AlphaFlags
import org.wfanet.measurement.reporting.job.BasicReportsMeasurementsJob
import org.wfanet.measurement.reporting.service.api.v2alpha.MultimeasurementCompiler
import picocli.CommandLine

class BasicReportsMeasurementsJobFlags {
  @CommandLine.Option(
    names = ["--impression-qualification-filter-config-file"],
    description =
      ["Path to the ImpressionQualificationFilterConfig textproto of the internal API server."],
    required = true,
  )
  lateinit var impressionQualificationFilterConfigFile: File
    private set

  @CommandLine.Option(
    names = ["--emit-cel-null-guards-for-nested-members"],
    description =
      [
        "Whether generated CEL filters guard fields nested inside a `oneof` member of an event " +
          "template with a `!= null` check. Must match the v2alpha public API server."
      ],
    required = false,
  )
  var emitCelNullGuardsForNestedMembers: Boolean = false
    private set
}

@CommandLine.Command(
  name = "BasicReportsMeasurementsJobExecutor",
  description = ["Creates the CMMS Measurements of multimeasurement BasicReports."],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin reportingApiServerFlags: ReportingApiServerFlags,
  @CommandLine.Mixin kingdomApiFlags: KingdomApiFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin v2AlphaFlags: V2AlphaFlags,
  @CommandLine.Mixin eventMessageFlags: EventMessageFlags,
  @CommandLine.Mixin basicReportsMeasurementsJobFlags: BasicReportsMeasurementsJobFlags,
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
  val impressionQualificationFilterConfig =
    parseTextProto(
      basicReportsMeasurementsJobFlags.impressionQualificationFilterConfigFile,
      ImpressionQualificationFilterConfig.getDefaultInstance(),
    )

  val job =
    BasicReportsMeasurementsJob(
      measurementConsumerConfigs,
      InternalBasicReportsCoroutineStub(channel),
      InternalReportingSetsCoroutineStub(channel),
      CmmsMeasurementConsumersCoroutineStub(kingdomChannel),
      CmmsMeasurementsCoroutineStub(kingdomChannel),
      MultimeasurementCompiler(
        eventMessageFlags.eventDescriptor.eventTemplateFieldsByPath,
        basicReportsMeasurementsJobFlags.emitCelNullGuardsForNestedMembers,
      ),
      impressionQualificationFilterConfig.impressionQualificationFiltersList.associate {
        it.externalImpressionQualificationFilterId to it.impressionQualificationFilterId
      },
    )

  runBlocking { job.execute() }
}

fun main(args: Array<String>) = commandLineMain(::run, args)
