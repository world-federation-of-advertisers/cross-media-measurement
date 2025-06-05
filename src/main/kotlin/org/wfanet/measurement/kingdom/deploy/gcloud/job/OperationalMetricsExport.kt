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
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.cloud.bigquery.storage.v1.ProtoSchema
import com.google.cloud.bigquery.storage.v1.ProtoSchemaConverter
import com.google.cloud.bigquery.storage.v1.StreamWriter
import com.google.protobuf.ByteString
import com.google.protobuf.Timestamp
import com.google.protobuf.util.Timestamps
import com.google.rpc.Code
import io.grpc.StatusException
import java.time.Duration
import java.util.concurrent.ExecutionException
import java.util.logging.Logger
import kotlinx.coroutines.flow.catch
import kotlinx.coroutines.flow.toList
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.DuchyMeasurementLogEntry
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequestKt
import org.wfanet.measurement.internal.kingdom.bigquerytables.ComputationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.LatestComputationReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.LatestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.LatestRequisitionReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementType
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.RequisitionsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.computationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.copy
import org.wfanet.measurement.internal.kingdom.bigquerytables.latestComputationReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.latestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.latestRequisitionReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.measurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.requisitionsTableRow
import org.wfanet.measurement.internal.kingdom.computationKey
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.streamRequisitionsRequest

class OperationalMetricsExport(
  private val measurementsClient: MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val requisitionsClient: RequisitionsGrpcKt.RequisitionsCoroutineStub,
  private val bigQuery: BigQuery,
  private val bigQueryWriteClient: BigQueryWriteClient,
  private val projectId: String,
  private val datasetId: String,
  private val latestMeasurementReadTableId: String,
  private val latestRequisitionReadTableId: String,
  private val latestComputationReadTableId: String,
  private val measurementsTableId: String,
  private val requisitionsTableId: String,
  private val computationParticipantStagesTableId: String,
  private val streamWriterFactory: StreamWriterFactory = StreamWriterFactoryImpl(),
  private val batchSize: Int = DEFAULT_BATCH_SIZE,
) {
  suspend fun execute() {
    exportMeasurements()
    exportRequisitions()
    exportComputationParticipants()
  }

  private suspend fun exportMeasurements() {
    val query =
      """
    SELECT update_time
    FROM `$datasetId.$latestMeasurementReadTableId`
    ORDER BY update_time DESC
    LIMIT 1
    """
        .trimIndent()

    val queryJobConfiguration: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query).build()

    val results = bigQuery.query(queryJobConfiguration).iterateAll()
    logger.info("Retrieved latest measurement read info from BigQuery")

    val latestMeasurementReadFromPreviousJob: FieldValueList? = results.firstOrNull()

    var streamMeasurementsRequest = streamMeasurementsRequest {
      measurementView = Measurement.View.DEFAULT
      limit = batchSize
      filter =
        StreamMeasurementsRequestKt.filter {
          states += Measurement.State.SUCCEEDED
          states += Measurement.State.FAILED
          if (latestMeasurementReadFromPreviousJob != null) {
            updatedAfter = Timestamps.fromNanos(
              latestMeasurementReadFromPreviousJob.get("update_time").longValue
            )
          }
        }
    }

    DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = measurementsTableId,
        client = bigQueryWriteClient,
        protoSchema = ProtoSchemaConverter.convert(MeasurementsTableRow.getDescriptor()),
        streamWriterFactory = streamWriterFactory,
      )
      .use { measurementsDataWriter ->
        DataWriter(
            projectId = projectId,
            datasetId = datasetId,
            tableId = latestMeasurementReadTableId,
            client = bigQueryWriteClient,
            protoSchema =
              ProtoSchemaConverter.convert(LatestMeasurementReadTableRow.getDescriptor()),
            streamWriterFactory = streamWriterFactory,
          )
          .use { latestMeasurementReadDataWriter ->
            val measurementsProtoRowsList = mutableListOf<ByteString>()
            // Previous UpdateTime that was different than latestUpdateTime
            var prevUpdateTime: Timestamp = Timestamp.getDefaultInstance()
            var latestUpdateTime: Timestamp? = null

            while (true) {
              val measurementsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()

              val measurementsList: List<Measurement> =
                measurementsClient
                  .streamMeasurements(streamMeasurementsRequest)
                  .catch { e ->
                    if (e is StatusException) {
                      logger.warning("Failed to retrieved Measurements")
                      throw e
                    }
                  }.toList()

              if (measurementsList.isEmpty()) {
                break
              }

              logger.info("Measurements read from the Kingdom Internal Server")

              if (latestUpdateTime == null) {
                latestUpdateTime = measurementsList[0].updateTime
              }

              for (measurement in measurementsList) {
                if (Timestamps.compare(latestUpdateTime, measurement.updateTime) != 0) {
                  measurementsProtoRowsBuilder.addAllSerializedRows(measurementsProtoRowsList)
                  measurementsProtoRowsList.clear()
                  prevUpdateTime = latestUpdateTime!!
                  latestUpdateTime = measurement.updateTime
                }

                val measurementType =
                  getMeasurementType(
                    measurement.details.measurementSpec,
                    measurement.details.apiVersion,
                  )

                val measurementConsumerId =
                  externalIdToApiId(measurement.externalMeasurementConsumerId)
                val measurementId = externalIdToApiId(measurement.externalMeasurementId)

                val measurementCompletionDurationSeconds =
                  Duration.between(
                    measurement.createTime.toInstant(),
                    measurement.updateTime.toInstant(),
                  )
                    .seconds

                val measurementState =
                  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
                  when (measurement.state) {
                    // StreamMeasurements filter only returns SUCCEEDED and FAILED
                    // Measurements.
                    Measurement.State.PENDING_REQUISITION_PARAMS,
                    Measurement.State.PENDING_REQUISITION_FULFILLMENT,
                    Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
                    Measurement.State.PENDING_COMPUTATION,
                    Measurement.State.STATE_UNSPECIFIED,
                    Measurement.State.CANCELLED,
                    Measurement.State.UNRECOGNIZED -> MeasurementsTableRow.State.UNRECOGNIZED
                    Measurement.State.SUCCEEDED -> MeasurementsTableRow.State.SUCCEEDED
                    Measurement.State.FAILED -> MeasurementsTableRow.State.FAILED
                  }

                val measurementsTableRow =
                  measurementsTableRow {
                    this.measurementConsumerId = measurementConsumerId
                    this.measurementId = measurementId
                    isDirect = measurement.details.protocolConfig.hasDirect()
                    this.measurementType = measurementType
                    state = measurementState
                    createTime = measurement.createTime
                    updateTime = measurement.updateTime
                    completionDurationSeconds = measurementCompletionDurationSeconds
                    completionDurationSecondsSquared =
                      measurementCompletionDurationSeconds *
                        measurementCompletionDurationSeconds
                  }
                    .toByteString()

                measurementsProtoRowsList.add(measurementsTableRow)
              }

              if (measurementsProtoRowsBuilder.serializedRowsCount > 0) {
                measurementsDataWriter.appendRows(measurementsProtoRowsBuilder.build())
                logger.info("Measurement Metrics written to BigQuery")

                val latestMeasurementReadTableRow = latestMeasurementReadTableRow {
                  updateTime = Timestamps.toNanos(prevUpdateTime)
                }

                latestMeasurementReadDataWriter.appendRows(
                  ProtoRows.newBuilder()
                    .addSerializedRows(latestMeasurementReadTableRow.toByteString())
                    .build()
                )
              }

              if (measurementsList.size == batchSize) {
                val lastMeasurement = measurementsList[measurementsList.lastIndex]
                streamMeasurementsRequest =
                  streamMeasurementsRequest.copy {
                    filter =
                      filter.copy {
                        after =
                          StreamMeasurementsRequestKt.FilterKt.after {
                            updateTime = latestUpdateTime!!
                            measurement = measurementKey {
                              externalMeasurementConsumerId =
                                lastMeasurement.externalMeasurementConsumerId
                              externalMeasurementId = lastMeasurement.externalMeasurementId
                            }
                          }
                      }
                  }
              } else break
            }

            if (measurementsProtoRowsList.isNotEmpty()) {
              val measurementsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
              measurementsProtoRowsBuilder.addAllSerializedRows(measurementsProtoRowsList)
              measurementsDataWriter.appendRows(measurementsProtoRowsBuilder.build())
              logger.info("Measurement Metrics written to BigQuery")

              val latestMeasurementReadTableRow = latestMeasurementReadTableRow {
                updateTime = Timestamps.toNanos(latestUpdateTime)
              }

              latestMeasurementReadDataWriter.appendRows(
                ProtoRows.newBuilder()
                  .addSerializedRows(latestMeasurementReadTableRow.toByteString())
                  .build()
              )
            }
          }
      }
  }

  private suspend fun exportRequisitions() {
    val query =
      """
    SELECT update_time
    FROM `$datasetId.$latestRequisitionReadTableId`
    ORDER BY update_time DESC
    LIMIT 1
    """
        .trimIndent()

    val queryJobConfiguration: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query).build()

    val results = bigQuery.query(queryJobConfiguration).iterateAll()
    logger.info("Retrieved latest requisition read info from BigQuery")

    val latestRequisitionReadFromPreviousJob: FieldValueList? = results.firstOrNull()

    var streamRequisitionsRequest = streamRequisitionsRequest {
      limit = batchSize
      filter =
        StreamRequisitionsRequestKt.filter {
          states += Requisition.State.FULFILLED
          states += Requisition.State.REFUSED
          if (latestRequisitionReadFromPreviousJob != null) {
            updatedAfter = Timestamps.fromNanos(
              latestRequisitionReadFromPreviousJob.get("update_time").longValue
            )
          }
        }
    }

    DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = requisitionsTableId,
        client = bigQueryWriteClient,
        protoSchema = ProtoSchemaConverter.convert(RequisitionsTableRow.getDescriptor()),
        streamWriterFactory = streamWriterFactory,
      )
      .use { requisitionsDataWriter ->
        DataWriter(
            projectId = projectId,
            datasetId = datasetId,
            tableId = latestRequisitionReadTableId,
            client = bigQueryWriteClient,
            protoSchema =
              ProtoSchemaConverter.convert(LatestRequisitionReadTableRow.getDescriptor()),
            streamWriterFactory = streamWriterFactory,
          )
          .use { latestRequisitionReadDataWriter ->
            val requisitionsProtoRowsList = mutableListOf<ByteString>()
            // Previous UpdateTime that was different than latestUpdateTime
            var prevUpdateTime: Timestamp = Timestamp.getDefaultInstance()
            var latestUpdateTime: Timestamp? = null
            while (true) {
              val requisitionsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()

              val requisitionsList: List<Requisition> =
                requisitionsClient
                .streamRequisitions(streamRequisitionsRequest)
                .catch { e ->
                  if (e is StatusException) {
                    logger.warning("Failed to retrieved Requisitions")
                    throw e
                  }
                }.toList()

              if (requisitionsList.isEmpty()) {
                break
              }

              logger.info("Requisitions read from the Kingdom Internal Server")

              if (latestUpdateTime == null) {
                latestUpdateTime = requisitionsList[0].updateTime
              }

              for (requisition in requisitionsList) {
                if (Timestamps.compare(latestUpdateTime, requisition.updateTime) != 0) {
                  requisitionsProtoRowsBuilder.addAllSerializedRows(requisitionsProtoRowsList)
                  requisitionsProtoRowsList.clear()
                  prevUpdateTime = latestUpdateTime!!
                  latestUpdateTime = requisition.updateTime
                }

                val measurementType =
                  getMeasurementType(
                    requisition.parentMeasurement.measurementSpec,
                    requisition.parentMeasurement.apiVersion,
                  )

                val measurementConsumerId =
                  externalIdToApiId(requisition.externalMeasurementConsumerId)
                val measurementId = externalIdToApiId(requisition.externalMeasurementId)

                val requisitionState =
                  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
                  when (requisition.state) {
                    Requisition.State.STATE_UNSPECIFIED,
                    Requisition.State.UNRECOGNIZED,
                    Requisition.State.PENDING_PARAMS,
                    Requisition.State.WITHDRAWN,
                    Requisition.State.UNFULFILLED -> RequisitionsTableRow.State.UNRECOGNIZED
                    Requisition.State.FULFILLED -> RequisitionsTableRow.State.FULFILLED
                    Requisition.State.REFUSED -> RequisitionsTableRow.State.REFUSED
                  }

                val requisitionCompletionDurationSeconds =
                  Duration.between(
                    requisition.parentMeasurement.createTime.toInstant(),
                    requisition.updateTime.toInstant(),
                  )
                    .seconds

                val requisitionsTableRow =
                  requisitionsTableRow {
                    this.measurementConsumerId = measurementConsumerId
                    this.measurementId = measurementId
                    requisitionId = externalIdToApiId(requisition.externalRequisitionId)
                    dataProviderId = externalIdToApiId(requisition.externalDataProviderId)
                    isDirect = requisition.parentMeasurement.protocolConfig.hasDirect()
                    this.measurementType = measurementType
                    state = requisitionState
                    createTime = requisition.parentMeasurement.createTime
                    updateTime = requisition.updateTime
                    completionDurationSeconds = requisitionCompletionDurationSeconds
                    completionDurationSecondsSquared =
                      requisitionCompletionDurationSeconds *
                        requisitionCompletionDurationSeconds
                  }
                    .toByteString()

                requisitionsProtoRowsList.add(requisitionsTableRow)
              }

              if (requisitionsProtoRowsBuilder.serializedRowsCount > 0) {
                requisitionsDataWriter.appendRows(requisitionsProtoRowsBuilder.build())
                logger.info("Requisition Metrics written to BigQuery")

                val latestRequisitionReadTableRow = latestRequisitionReadTableRow {
                  updateTime = Timestamps.toNanos(prevUpdateTime)
                }

                latestRequisitionReadDataWriter.appendRows(
                  ProtoRows.newBuilder()
                    .addSerializedRows(latestRequisitionReadTableRow.toByteString())
                    .build()
                )
              }

              if (requisitionsList.size == batchSize) {
                val lastRequisition = requisitionsList[requisitionsList.lastIndex]
                streamRequisitionsRequest =
                  streamRequisitionsRequest.copy {
                    filter =
                      filter.copy {
                        after =
                          StreamRequisitionsRequestKt.FilterKt.after {
                            updateTime = latestUpdateTime!!
                            externalDataProviderId = lastRequisition.externalDataProviderId
                            externalRequisitionId = lastRequisition.externalRequisitionId
                          }
                      }
                  }
              } else break
            }

            if (requisitionsProtoRowsList.isNotEmpty()) {
              val requisitionsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
              requisitionsProtoRowsBuilder.addAllSerializedRows(requisitionsProtoRowsList)
              requisitionsDataWriter.appendRows(requisitionsProtoRowsBuilder.build())
              logger.info("Requisition Metrics written to BigQuery")

              val latestRequisitionReadTableRow = latestRequisitionReadTableRow {
                updateTime = Timestamps.toNanos(latestUpdateTime)
              }

              latestRequisitionReadDataWriter.appendRows(
                ProtoRows.newBuilder()
                  .addSerializedRows(latestRequisitionReadTableRow.toByteString())
                  .build()
              )
            }
          }
      }
  }

  private suspend fun exportComputationParticipants() {
    val query =
      """
    SELECT update_time
    FROM `$datasetId.$latestComputationReadTableId`
    ORDER BY update_time DESC
    LIMIT 1
    """
        .trimIndent()

    val queryJobConfiguration: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query).build()

    val results = bigQuery.query(queryJobConfiguration).iterateAll()
    logger.info("Retrieved latest computation read info from BigQuery")

    val latestComputationReadFromPreviousJob: FieldValueList? = results.firstOrNull()

    var streamComputationsRequest = streamMeasurementsRequest {
      measurementView = Measurement.View.COMPUTATION_STATS
      limit = batchSize
      filter =
        StreamMeasurementsRequestKt.filter {
          states += Measurement.State.SUCCEEDED
          states += Measurement.State.FAILED
          if (latestComputationReadFromPreviousJob != null) {
            updatedAfter = Timestamps.fromNanos(
              latestComputationReadFromPreviousJob.get("update_time").longValue
            )
          }
        }
    }

    DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = computationParticipantStagesTableId,
        client = bigQueryWriteClient,
        protoSchema =
          ProtoSchemaConverter.convert(ComputationParticipantStagesTableRow.getDescriptor()),
        streamWriterFactory = streamWriterFactory,
      )
      .use { computationParticipantStagesDataWriter ->
        DataWriter(
            projectId = projectId,
            datasetId = datasetId,
            tableId = latestComputationReadTableId,
            client = bigQueryWriteClient,
            protoSchema =
              ProtoSchemaConverter.convert(LatestComputationReadTableRow.getDescriptor()),
            streamWriterFactory = streamWriterFactory,
          )
          .use { latestComputationReadDataWriter ->
              val computationParticipantStagesProtoRowsList = mutableListOf<ByteString>()
              // Previous UpdateTime that was different than latestUpdateTime
              var prevUpdateTime: Timestamp = Timestamp.getDefaultInstance()
              var latestUpdateTime: Timestamp? = null

              while (true) {
                val computationParticipantStagesProtoRowsBuilder: ProtoRows.Builder =
                  ProtoRows.newBuilder()
                val computationsList: List<Measurement> =
                  measurementsClient
                    .streamMeasurements(streamComputationsRequest)
                    .catch { e ->
                      if (e is StatusException) {
                        logger.warning("Failed to retrieved Computations")
                        throw e
                      }
                    }.toList()

                if (computationsList.isEmpty()) {
                  break
                }

                logger.info("Computations read from the Kingdom Internal Server")

                if (latestUpdateTime == null) {
                  latestUpdateTime = computationsList[0].updateTime
                }

                for (measurement in computationsList) {
                  if (Timestamps.compare(latestUpdateTime, measurement.updateTime) != 0) {
                    computationParticipantStagesProtoRowsBuilder.addAllSerializedRows(
                      computationParticipantStagesProtoRowsList
                    )
                    computationParticipantStagesProtoRowsList.clear()
                    prevUpdateTime = latestUpdateTime!!
                    latestUpdateTime = measurement.updateTime
                  }

                  if (measurement.externalComputationId != 0L) {
                    val measurementType =
                      getMeasurementType(
                        measurement.details.measurementSpec,
                        measurement.details.apiVersion,
                      )

                    val measurementConsumerId =
                      externalIdToApiId(measurement.externalMeasurementConsumerId)
                    val measurementId = externalIdToApiId(measurement.externalMeasurementId)
                    val computationId = externalIdToApiId(measurement.externalComputationId)

                    val baseComputationParticipantStagesTableRow =
                      computationParticipantStagesTableRow {
                        this.measurementConsumerId = measurementConsumerId
                        this.measurementId = measurementId
                        this.computationId = computationId
                        this.measurementType = measurementType
                      }

                    // Map of ExternalDuchyId to log entries.
                    val logEntriesMap: Map<String, MutableList<DuchyMeasurementLogEntry>> =
                      buildMap {
                        for (logEntry in measurement.logEntriesList) {
                          val logEntries = getOrPut(logEntry.externalDuchyId) { mutableListOf() }
                          logEntries.add(logEntry)
                        }
                      }

                    for (computationParticipant in measurement.computationParticipantsList) {
                      val sortedStageLogEntries =
                        logEntriesMap[computationParticipant.externalDuchyId]?.sortedBy {
                          it.details.stageAttempt.stage
                        } ?: emptyList()

                      if (sortedStageLogEntries.isEmpty()) {
                        continue
                      }

                      sortedStageLogEntries.zipWithNext { logEntry, nextLogEntry ->
                        if (logEntry.details.stageAttempt.stageName.isNotBlank()) {
                          computationParticipantStagesProtoRowsList.add(
                            baseComputationParticipantStagesTableRow
                              .copy {
                                duchyId = computationParticipant.externalDuchyId
                                result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
                                stageName = logEntry.details.stageAttempt.stageName
                                stageStartTime = logEntry.details.stageAttempt.stageStartTime
                                completionDurationSeconds =
                                  Duration.between(
                                      logEntry.details.stageAttempt.stageStartTime.toInstant(),
                                      nextLogEntry.details.stageAttempt.stageStartTime.toInstant(),
                                    )
                                    .seconds
                                completionDurationSecondsSquared =
                                  completionDurationSeconds * completionDurationSeconds
                              }
                              .toByteString()
                          )
                        }
                      }

                      val logEntry = sortedStageLogEntries.last()
                      if (logEntry.details.stageAttempt.stageName.isBlank()) {
                        continue
                      }

                      val computationParticipantStagesProtoRow: ByteString? =
                        when (measurement.state) {
                          Measurement.State.SUCCEEDED ->
                            baseComputationParticipantStagesTableRow
                              .copy {
                                duchyId = computationParticipant.externalDuchyId
                                result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
                                stageName = logEntry.details.stageAttempt.stageName
                                stageStartTime = logEntry.details.stageAttempt.stageStartTime
                                completionDurationSeconds =
                                  Duration.between(
                                    logEntry.details.stageAttempt.stageStartTime.toInstant(),
                                    measurement.updateTime.toInstant(),
                                  )
                                    .seconds
                                completionDurationSecondsSquared =
                                  completionDurationSeconds * completionDurationSeconds
                              }
                              .toByteString()
                          Measurement.State.FAILED ->
                            baseComputationParticipantStagesTableRow
                              .copy {
                                duchyId = computationParticipant.externalDuchyId
                                result = ComputationParticipantStagesTableRow.Result.FAILED
                                stageName = logEntry.details.stageAttempt.stageName
                                stageStartTime = logEntry.details.stageAttempt.stageStartTime
                                completionDurationSeconds =
                                  Duration.between(
                                    logEntry.details.stageAttempt.stageStartTime.toInstant(),
                                    measurement.updateTime.toInstant(),
                                  )
                                    .seconds
                                completionDurationSecondsSquared =
                                  completionDurationSeconds * completionDurationSeconds
                              }
                              .toByteString()
                          else -> null
                        }

                      if (computationParticipantStagesProtoRow != null) {
                        computationParticipantStagesProtoRowsList.add(
                          computationParticipantStagesProtoRow
                        )
                      }
                    }
                  }

                  if (computationParticipantStagesProtoRowsBuilder.serializedRowsCount > 0) {
                    computationParticipantStagesDataWriter.appendRows(
                      computationParticipantStagesProtoRowsBuilder.build()
                    )
                    logger.info("Computation Participant Stage Metrics written to BigQuery")

                    val latestComputationReadTableRow = latestComputationReadTableRow {
                      updateTime = Timestamps.toNanos(prevUpdateTime)
                    }

                    latestComputationReadDataWriter.appendRows(
                      ProtoRows.newBuilder()
                        .addSerializedRows(latestComputationReadTableRow.toByteString())
                        .build()
                    )
                  }
                }

                if (computationsList.size == batchSize) {
                  val lastComputation = computationsList[computationsList.lastIndex]
                  streamComputationsRequest =
                    streamComputationsRequest.copy {
                      filter =
                        filter.copy {
                          after =
                            StreamMeasurementsRequestKt.FilterKt.after {
                              updateTime = latestUpdateTime!!
                              computation = computationKey {
                                externalComputationId =
                                  lastComputation.externalComputationId
                              }
                            }
                        }
                    }
                } else break
              }

              if (computationParticipantStagesProtoRowsList.isNotEmpty()) {
                val computationParticipantStagesProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
                computationParticipantStagesProtoRowsBuilder.addAllSerializedRows(computationParticipantStagesProtoRowsList)
                computationParticipantStagesDataWriter.appendRows(computationParticipantStagesProtoRowsBuilder.build())
                logger.info("Computation Participant Stage Metrics written to BigQuery")

                val latestComputationReadTableRow = latestMeasurementReadTableRow {
                  updateTime = Timestamps.toNanos(latestUpdateTime)
                }

                latestComputationReadDataWriter.appendRows(
                  ProtoRows.newBuilder()
                    .addSerializedRows(latestComputationReadTableRow.toByteString())
                    .build()
                )
              }
          }
      }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val DEFAULT_BATCH_SIZE = 1000

    private fun getMeasurementType(
      measurementSpecByteString: ByteString,
      apiVersion: String,
    ): MeasurementType {
      require(Version.fromString(apiVersion) == Version.V2_ALPHA)

      val measurementSpec = MeasurementSpec.parseFrom(measurementSpecByteString)

      val measurementType =
        @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
        when (measurementSpec.measurementTypeCase) {
          MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
            MeasurementType.REACH_AND_FREQUENCY
          MeasurementSpec.MeasurementTypeCase.IMPRESSION -> MeasurementType.IMPRESSION
          MeasurementSpec.MeasurementTypeCase.DURATION -> MeasurementType.DURATION
          MeasurementSpec.MeasurementTypeCase.REACH -> MeasurementType.REACH
          MeasurementSpec.MeasurementTypeCase.POPULATION -> MeasurementType.POPULATION
          MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
            MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
        }

      return measurementType
    }
  }

  private class DataWriter(
    private val projectId: String,
    private val datasetId: String,
    private val tableId: String,
    private val client: BigQueryWriteClient,
    private val protoSchema: ProtoSchema,
    private val streamWriterFactory: StreamWriterFactory,
  ) : AutoCloseable {
    private var streamWriter: StreamWriter =
      streamWriterFactory.create(projectId, datasetId, tableId, client, protoSchema)
    private var recreateCount: Int = 0

    override fun close() {
      streamWriter.close()
    }

    /**
     * Writes data to the stream.
     *
     * @param protoRows protos representing the rows to write.
     * @throws IllegalStateException if append fails and error is not retriable or too many retry
     *   attempts have been made
     */
    @Blocking
    fun appendRows(protoRows: ProtoRows) {
      logger.info("Begin writing to stream ${streamWriter.streamName}")
      for (i in 1..RETRY_COUNT) {
        if (streamWriter.isClosed) {
          if (!streamWriter.isUserClosed && recreateCount < MAX_RECREATE_COUNT) {
            logger.info("Recreating stream writer")
            streamWriter =
              streamWriterFactory.create(projectId, datasetId, tableId, client, protoSchema)
            recreateCount++
          } else {
            throw IllegalStateException("Unable to recreate stream writer")
          }
        }

        try {
          val response = streamWriter.append(protoRows).get()
          if (response.hasError()) {
            logger.warning("Write response error: ${response.error}")
            if (response.error.code != Code.INTERNAL.number) {
              throw IllegalStateException("Cannot retry failed append.")
            } else if (i == RETRY_COUNT) {
              throw IllegalStateException("Too many retries.")
            }
          } else {
            logger.info("End writing to stream ${streamWriter.streamName}")
            break
          }
        } catch (e: AppendSerializationError) {
          logger.warning("Logging serialization errors")
          for (value in e.rowIndexToErrorMessage.values) {
            logger.warning(value)
          }
          throw e
        } catch (e: ExecutionException) {
          if (e.cause is AppendSerializationError) {
            logger.warning("Logging serialization errors")
            for (value in (e.cause as AppendSerializationError).rowIndexToErrorMessage.values) {
              logger.warning(value)
            }
          }
          throw e
        }
      }
    }

    companion object {
      private const val MAX_RECREATE_COUNT = 3
      private const val RETRY_COUNT = 3
    }
  }
}
