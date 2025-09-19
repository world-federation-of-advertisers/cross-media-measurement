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

package org.wfanet.measurement.kingdom.deploy.gcloud.job

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import kotlin.properties.Delegates
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.TlsFlags
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt
import org.wfanet.measurement.kingdom.deploy.common.InternalApiFlags
import picocli.CommandLine

@CommandLine.Command(
  name = "OperationalMetricsJobExecutor",
  description =
    [
      "Process for reading data for Metrics from the Kingdom Internal Server and writing it to BigQuery."
    ],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin internalApiFlags: InternalApiFlags,
  @CommandLine.Mixin tlsFlags: TlsFlags,
  @CommandLine.Mixin operationalMetricsFlags: OperationalMetricsFlags,
) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = tlsFlags.certFile,
      privateKeyFile = tlsFlags.privateKeyFile,
      trustedCertCollectionFile = tlsFlags.certCollectionFile,
    )

  val channel =
    buildMutualTlsChannel(internalApiFlags.target, clientCerts, internalApiFlags.certHost)
      .withVerboseLogging(operationalMetricsFlags.debugVerboseGrpcClientLogging)
      .withDefaultDeadline(internalApiFlags.defaultDeadlineDuration)

  val measurementsClient = MeasurementsGrpcKt.MeasurementsCoroutineStub(channel)
  val requisitionsClient = RequisitionsGrpcKt.RequisitionsCoroutineStub(channel)

  val bigQuery: BigQuery =
    BigQueryOptions.newBuilder()
      .apply { setProjectId(operationalMetricsFlags.bigQueryProject) }
      .build()
      .service

  runBlocking {
    val projectId = operationalMetricsFlags.bigQueryProject
    val datasetId = operationalMetricsFlags.bigQueryDataSet
    val measurementsTableId = operationalMetricsFlags.measurementsTable
    val requisitionsTableId = operationalMetricsFlags.requisitionsTable
    val computationParticipantStagesTableId =
      operationalMetricsFlags.computationParticipantStagesTable
    val latestMeasurementReadTableId = operationalMetricsFlags.latestMeasurementReadTable
    val latestRequisitionReadTableId = operationalMetricsFlags.latestRequisitionReadTable
    val latestComputationReadTableId = operationalMetricsFlags.latestComputationReadTable

    BigQueryWriteClient.create().use { bigQueryWriteClient ->
      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          requisitionsClient = requisitionsClient,
          bigQuery = bigQuery,
          bigQueryWriteClient = bigQueryWriteClient,
          projectId = projectId,
          datasetId = datasetId,
          latestMeasurementReadTableId = latestMeasurementReadTableId,
          latestRequisitionReadTableId = latestRequisitionReadTableId,
          latestComputationReadTableId = latestComputationReadTableId,
          measurementsTableId = measurementsTableId,
          requisitionsTableId = requisitionsTableId,
          computationParticipantStagesTableId = computationParticipantStagesTableId,
          batchSize = operationalMetricsFlags.batchSize,
        )

      operationalMetricsExport.execute()
    }
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)

class OperationalMetricsFlags {
  @set:CommandLine.Option(
    names = ["--debug-verbose-grpc-client-logging"],
    description = ["Enables full gRPC request and response logging for outgoing gRPCs"],
    defaultValue = "false",
  )
  var debugVerboseGrpcClientLogging by Delegates.notNull<Boolean>()
    private set

  @CommandLine.Option(
    names = ["--bigquery-project"],
    description = ["BigQuery Project ID"],
    required = true,
  )
  lateinit var bigQueryProject: String
    private set

  @CommandLine.Option(
    names = ["--bigquery-dataset"],
    description = ["BigQuery Dataset ID"],
    required = true,
  )
  lateinit var bigQueryDataSet: String
    private set

  @CommandLine.Option(
    names = ["--measurements-table"],
    description = ["Measurements table ID"],
    required = true,
  )
  lateinit var measurementsTable: String
    private set

  @CommandLine.Option(
    names = ["--requisitions-table"],
    description = ["Requisitions table ID"],
    required = true,
  )
  lateinit var requisitionsTable: String
    private set

  @CommandLine.Option(
    names = ["--computation-participant-stages-table"],
    description = ["Computation Participant Stages table ID"],
    required = true,
  )
  lateinit var computationParticipantStagesTable: String
    private set

  @CommandLine.Option(
    names = ["--latest-measurement-read-table"],
    description = ["Latest Measurement Read table ID"],
    required = true,
  )
  lateinit var latestMeasurementReadTable: String
    private set

  @CommandLine.Option(
    names = ["--latest-requisition-read-table"],
    description = ["Latest Requisition Read table ID"],
    required = true,
  )
  lateinit var latestRequisitionReadTable: String
    private set

  @CommandLine.Option(
    names = ["--latest-computation-read-table"],
    description = ["Latest Computation Read table ID"],
    required = true,
  )
  lateinit var latestComputationReadTable: String
    private set

  @CommandLine.Option(names = ["--batch-size"], description = ["Number of rows inserted at once."])
  var batchSize: Int = 1000
    private set
}
