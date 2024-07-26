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
import com.google.cloud.bigquery.storage.v1.ProtoSchema
import com.google.cloud.bigquery.storage.v1.TableName
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.common.commandLineMain
import org.wfanet.measurement.gcloud.spanner.SpannerFlags
import org.wfanet.measurement.gcloud.spanner.usingSpanner
import org.wfanet.measurement.internal.kingdom.ComputationParticipantData
import org.wfanet.measurement.internal.kingdom.LatestMeasurementRead
import org.wfanet.measurement.internal.kingdom.MeasurementData
import org.wfanet.measurement.internal.kingdom.RequisitionData
import org.wfanet.measurement.kingdom.deploy.common.DuchyIds
import org.wfanet.measurement.kingdom.deploy.common.DuchyIdsFlags
import org.wfanet.measurement.kingdom.job.OperationalMetricsJob
import picocli.CommandLine

@CommandLine.Command(
  name = "OperationalMetricsJobExecutor",
  description =
    [
      "Process for reading data for Metrics from the Kingdom Spanner Database and writing it to BigQuery."
    ],
  mixinStandardHelpOptions = true,
  showDefaultValues = true,
)
private fun run(
  @CommandLine.Mixin spannerFlags: SpannerFlags,
  @CommandLine.Mixin operationalMetricsFlags: OperationalMetricsFlags,
  @CommandLine.Mixin duchyIdsFlags: DuchyIdsFlags,
) {
  DuchyIds.initializeFromFlags(duchyIdsFlags)

  val bigQuery: BigQuery =
    BigQueryOptions.newBuilder()
      .apply { setProjectId(operationalMetricsFlags.bigQueryProject) }
      .build()
      .service

  runBlocking {
    spannerFlags.usingSpanner { spanner ->
      val spannerClient = spanner.databaseClient

      val projectId = operationalMetricsFlags.bigQueryProject
      val datasetId = operationalMetricsFlags.bigQueryDataSet
      val measurementsTableId = operationalMetricsFlags.measurementsTable
      val requisitionsTableId = operationalMetricsFlags.requisitionsTable
      val computationParticipantsTableId = operationalMetricsFlags.computationParticipantsTable
      val latestMeasurementReadTableId = operationalMetricsFlags.latestMeasurementReadTable

      BigQueryWriteClient.create().use { bigQueryWriteClient ->
        val measurementsTableName: TableName = TableName.of(projectId, datasetId, measurementsTableId)
        val requisitionsTableName: TableName = TableName.of(projectId, datasetId, requisitionsTableId)
        val computationParticipantsTableName: TableName =
          TableName.of(projectId, datasetId, computationParticipantsTableId)
        val latestMeasurementReadTableName: TableName =
          TableName.of(projectId, datasetId, latestMeasurementReadTableId)

        val measurementsDataWriter =
          OperationalMetricsJob.DataWriterImplementation(
            measurementsTableName,
            bigQueryWriteClient,
            ProtoSchema.newBuilder()
              .setProtoDescriptor(MeasurementData.getDescriptor().toProto())
              .build(),
          )
        val requisitionsDataWriter =
          OperationalMetricsJob.DataWriterImplementation(
            requisitionsTableName,
            bigQueryWriteClient,
            ProtoSchema.newBuilder()
              .setProtoDescriptor(RequisitionData.getDescriptor().toProto())
              .build(),
          )
        val computationParticipantsDataWriter =
          OperationalMetricsJob.DataWriterImplementation(
            computationParticipantsTableName,
            bigQueryWriteClient,
            ProtoSchema.newBuilder()
              .setProtoDescriptor(ComputationParticipantData.getDescriptor().toProto())
              .build(),
          )
        val latestMeasurementReadDataWriter =
          OperationalMetricsJob.DataWriterImplementation(
            latestMeasurementReadTableName,
            bigQueryWriteClient,
            ProtoSchema.newBuilder()
              .setProtoDescriptor(LatestMeasurementRead.getDescriptor().toProto())
              .build(),
          )

        val operationalMetricsJob =
          OperationalMetricsJob(
            spannerClient = spannerClient,
            bigQuery = bigQuery,
            datasetId= datasetId,
            latestMeasurementReadTableId = latestMeasurementReadTableId,
            measurementsDataWriter = measurementsDataWriter,
            requisitionsDataWriter = requisitionsDataWriter,
            computationParticipantsDataWriter = computationParticipantsDataWriter,
            latestMeasurementReadDataWriter = latestMeasurementReadDataWriter,
          )

        try {
          measurementsDataWriter.init()
          requisitionsDataWriter.init()
          computationParticipantsDataWriter.init()
          latestMeasurementReadDataWriter.init()
          operationalMetricsJob.execute()
        } finally {
          measurementsDataWriter.close()
          requisitionsDataWriter.close()
          computationParticipantsDataWriter.close()
          latestMeasurementReadDataWriter.close()
        }
      }
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
    names = ["--computation-participants-table"],
    description = ["Computation Participants table ID"],
    required = true,
  )
  lateinit var computationParticipantsTable: String
    private set

  @CommandLine.Option(
    names = ["--latest-measurement-read-table"],
    description = ["Latest Measurement Read table ID"],
    required = true,
  )
  lateinit var latestMeasurementReadTable: String
    private set
}
