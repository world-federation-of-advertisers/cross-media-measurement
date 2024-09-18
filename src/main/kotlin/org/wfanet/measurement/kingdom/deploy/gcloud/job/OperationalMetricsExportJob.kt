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
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.common.crypto.SigningCerts
import org.wfanet.measurement.common.grpc.CommonServer
import org.wfanet.measurement.common.grpc.buildMutualTlsChannel
import org.wfanet.measurement.common.grpc.withDefaultDeadline
import org.wfanet.measurement.common.grpc.withVerboseLogging
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.kingdom.deploy.common.server.KingdomApiServerFlags
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
  @CommandLine.Mixin kingdomApiServerFlags: KingdomApiServerFlags,
  @CommandLine.Mixin commonServerFlags: CommonServer.Flags,
  @CommandLine.Mixin operationalMetricsFlags: OperationalMetricsFlags,
) {
  val clientCerts =
    SigningCerts.fromPemFiles(
      certificateFile = commonServerFlags.tlsFlags.certFile,
      privateKeyFile = commonServerFlags.tlsFlags.privateKeyFile,
      trustedCertCollectionFile = commonServerFlags.tlsFlags.certCollectionFile,
    )

  val channel =
    buildMutualTlsChannel(
        kingdomApiServerFlags.internalApiFlags.target,
        clientCerts,
        kingdomApiServerFlags.internalApiFlags.certHost,
      )
      .withVerboseLogging(kingdomApiServerFlags.debugVerboseGrpcClientLogging)
      .withDefaultDeadline(kingdomApiServerFlags.internalApiFlags.defaultDeadlineDuration)

  val measurementsClient = MeasurementsGrpcKt.MeasurementsCoroutineStub(channel)

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
    val latestMeasurementReadTableId = operationalMetricsFlags.latestMeasurementReadTable

    BigQueryWriteClient.create().use { bigQueryWriteClient ->
      val operationalMetricsExport =
        OperationalMetricsExport(
          measurementsClient = measurementsClient,
          bigQuery = bigQuery,
          bigQueryWriteClient = bigQueryWriteClient,
          projectId = projectId,
          datasetId = datasetId,
          latestMeasurementReadTableId = latestMeasurementReadTableId,
          measurementsTableId = measurementsTableId,
          requisitionsTableId = requisitionsTableId,
        )

      operationalMetricsExport.execute()
    }
  }
}

fun main(args: Array<String>) = commandLineMain(::run, args)

class OperationalMetricsFlags {
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
    names = ["--latest-measurement-read-table"],
    description = ["Latest Measurement Read table ID"],
    required = true,
  )
  lateinit var latestMeasurementReadTable: String
    private set
}
