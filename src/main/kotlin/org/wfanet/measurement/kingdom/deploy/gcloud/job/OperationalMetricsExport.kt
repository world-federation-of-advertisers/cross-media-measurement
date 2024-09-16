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
import com.google.protobuf.Timestamp
import com.google.protobuf.any
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import com.google.rpc.Code
import java.util.logging.Logger
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.jetbrains.annotations.Blocking
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.bigquerytables.ComputationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.LatestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementState
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementType
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.RequisitionsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.computationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.latestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.measurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.requisitionsTableRow
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

class OperationalMetricsExport(
  private val measurementsClient: MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val bigQuery: BigQuery,
  private val bigQueryWriteClient: BigQueryWriteClient,
  private val projectId: String,
  private val datasetId: String,
  private val latestMeasurementReadTableId: String,
  private val measurementsTableId: String,
  private val requisitionsTableId: String,
  private val computationParticipantStagesTableId: String,
  private val streamWriterFactory: StreamWriterFactory = StreamWriterFactoryImpl(),
) {
  suspend fun execute() {
    var measurementsQueryResponseSize: Int

    val query =
      """
    SELECT update_time, external_measurement_consumer_id, external_measurement_id
    FROM `$datasetId.$latestMeasurementReadTableId`
    ORDER BY update_time DESC, external_measurement_consumer_id DESC, external_measurement_id DESC
    LIMIT 1
    """
        .trimIndent()

    val queryJobConfiguration: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query).build()

    val results = bigQuery.query(queryJobConfiguration).iterateAll()
    logger.info("Retrieved latest measurement read info from BigQuery")

    val latestMeasurementReadFromPreviousJob: FieldValueList? = results.firstOrNull()

    var streamMeasurementsRequest = streamMeasurementsRequest {
      measurementView = Measurement.View.FULL
      limit = BATCH_SIZE
      filter =
        StreamMeasurementsRequestKt.filter {
          states += Measurement.State.SUCCEEDED
          states += Measurement.State.FAILED
          if (latestMeasurementReadFromPreviousJob != null) {
            after =
              StreamMeasurementsRequestKt.FilterKt.after {
                updateTime =
                  Timestamps.fromNanos(
                    latestMeasurementReadFromPreviousJob.get("update_time").longValue
                  )
                measurement = measurementKey {
                  externalMeasurementConsumerId =
                    latestMeasurementReadFromPreviousJob
                      .get("external_measurement_consumer_id")
                      .longValue
                  externalMeasurementId =
                    latestMeasurementReadFromPreviousJob.get("external_measurement_id").longValue
                }
              }
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
            tableId = requisitionsTableId,
            client = bigQueryWriteClient,
            protoSchema = ProtoSchemaConverter.convert(RequisitionsTableRow.getDescriptor()),
            streamWriterFactory = streamWriterFactory,
          )
          .use { requisitionsDataWriter ->
            DataWriter(
                projectId = projectId,
                datasetId = datasetId,
                tableId = computationParticipantStagesTableId,
                client = bigQueryWriteClient,
                protoSchema =
                  ProtoSchema.newBuilder()
                    .setProtoDescriptor(
                      ComputationParticipantStagesTableRow.getDescriptor().toProto()
                    )
                    .build(),
                streamWriterFactory = streamWriterFactory,
              )
              .use { computationParticipantStagesDataWriter ->
                DataWriter(
                    projectId = projectId,
                    datasetId = datasetId,
                    tableId = latestMeasurementReadTableId,
                    client = bigQueryWriteClient,
                    protoSchema =
                      ProtoSchema.newBuilder()
                        .setProtoDescriptor(LatestMeasurementReadTableRow.getDescriptor().toProto())
                        .build(),
                    streamWriterFactory = streamWriterFactory,
                  )
                  .use { latestMeasurementReadDataWriter ->
                    do {
                      measurementsQueryResponseSize = 0

                      val measurementsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
                      val requisitionsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
                      val computationParticipantStagesProtoRowsBuilder: ProtoRows.Builder =
                        ProtoRows.newBuilder()
                      var latestUpdateTime: Timestamp = Timestamp.getDefaultInstance()

                      measurementsClient.streamMeasurements(streamMeasurementsRequest).collect {
                        measurement ->
                        measurementsQueryResponseSize++
                        latestUpdateTime = measurement.updateTime

                        val measurementSpec = signedMessage {
                          setMessage(
                            any {
                              value = measurement.details.measurementSpec
                              typeUrl =
                                when (measurement.details.apiVersion) {
                                  Version.V2_ALPHA.toString() ->
                                    ProtoReflection.getTypeUrl(MeasurementSpec.getDescriptor())
                                  else ->
                                    ProtoReflection.getTypeUrl(MeasurementSpec.getDescriptor())
                                }
                            }
                          )
                          signature = measurement.details.measurementSpecSignature
                          signatureAlgorithmOid =
                            measurement.details.measurementSpecSignatureAlgorithmOid
                        }
                        val measurementTypeCase =
                          measurementSpec.unpack<MeasurementSpec>().measurementTypeCase
                        val measurementType =
                          @Suppress(
                            "WHEN_ENUM_CAN_BE_NULL_IN_JAVA"
                          ) // Proto enum fields are never null.
                          when (measurementTypeCase) {
                            MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
                              MeasurementType.REACH_AND_FREQUENCY
                            MeasurementSpec.MeasurementTypeCase.IMPRESSION ->
                              MeasurementType.IMPRESSION
                            MeasurementSpec.MeasurementTypeCase.DURATION -> MeasurementType.DURATION
                            MeasurementSpec.MeasurementTypeCase.REACH -> MeasurementType.REACH
                            MeasurementSpec.MeasurementTypeCase.POPULATION ->
                              MeasurementType.POPULATION
                            MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
                              MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
                          }

                        val measurementConsumerId =
                          externalIdToApiId(measurement.externalMeasurementConsumerId)
                        val measurementId = externalIdToApiId(measurement.externalMeasurementId)

                        val measurementCompletionDurationSeconds =
                          Durations.toSeconds(
                            Timestamps.between(measurement.createTime, measurement.updateTime)
                          )

                        val measurementState =
                          @Suppress(
                            "WHEN_ENUM_CAN_BE_NULL_IN_JAVA"
                          ) // Proto enum fields are never null.
                          when (measurement.state) {
                            // StreamMeasurements filter only returns SUCCEEDED and FAILED
                            // Measurements.
                            Measurement.State.PENDING_REQUISITION_PARAMS,
                            Measurement.State.PENDING_REQUISITION_FULFILLMENT,
                            Measurement.State.PENDING_PARTICIPANT_CONFIRMATION,
                            Measurement.State.PENDING_COMPUTATION,
                            Measurement.State.STATE_UNSPECIFIED,
                            Measurement.State.CANCELLED,
                            Measurement.State.UNRECOGNIZED -> MeasurementState.UNRECOGNIZED
                            Measurement.State.SUCCEEDED -> MeasurementState.SUCCEEDED
                            Measurement.State.FAILED -> MeasurementState.FAILED
                          }

                        measurementsProtoRowsBuilder.addSerializedRows(
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
                        )

                        for (requisition in measurement.requisitionsList) {
                          val requisitionState =
                            @Suppress(
                              "WHEN_ENUM_CAN_BE_NULL_IN_JAVA"
                            ) // Proto enum fields are never null.
                            when (requisition.state) {
                              Requisition.State.STATE_UNSPECIFIED,
                              Requisition.State.UNRECOGNIZED,
                              Requisition.State.PENDING_PARAMS,
                              Requisition.State.WITHDRAWN,
                              Requisition.State.UNFULFILLED -> continue
                              Requisition.State.FULFILLED -> RequisitionsTableRow.State.FULFILLED
                              Requisition.State.REFUSED -> RequisitionsTableRow.State.REFUSED
                            }

                          val requisitionCompletionDurationSeconds =
                            Durations.toSeconds(
                              Timestamps.between(measurement.createTime, requisition.updateTime)
                            )

                          requisitionsProtoRowsBuilder.addSerializedRows(
                            requisitionsTableRow {
                                this.measurementConsumerId = measurementConsumerId
                                this.measurementId = measurementId
                                requisitionId = externalIdToApiId(requisition.externalRequisitionId)
                                dataProviderId =
                                  externalIdToApiId(requisition.externalDataProviderId)
                                isDirect = measurement.details.protocolConfig.hasDirect()
                                this.measurementType = measurementType
                                state = requisitionState
                                createTime = measurement.createTime
                                updateTime = requisition.updateTime
                                completionDurationSeconds = requisitionCompletionDurationSeconds
                                completionDurationSecondsSquared =
                                  requisitionCompletionDurationSeconds *
                                    requisitionCompletionDurationSeconds
                              }
                              .toByteString()
                          )
                        }

                        for (computationParticipant in measurement.computationParticipantsList) {
                          var prevTimestamp: Timestamp = measurement.createTime
                          for (successLogEntry in
                            computationParticipant.successLogEntriesList.sortedBy {
                              it.logEntry.createTime.toInstant()
                            }) {
                            val completionDurationSeconds =
                              Durations.toSeconds(
                                Timestamps.between(
                                  prevTimestamp,
                                  successLogEntry.logEntry.createTime,
                                )
                              )

                            computationParticipantStagesProtoRowsBuilder.addSerializedRows(
                              computationParticipantStagesTableRow {
                                  this.measurementConsumerId = measurementConsumerId
                                  this.measurementId = measurementId
                                  computationId =
                                    externalIdToApiId(measurement.externalComputationId)
                                  duchyId = computationParticipant.externalDuchyId
                                  this.measurementType = measurementType
                                  this.measurementState = measurementState
                                  result = ComputationParticipantStagesTableRow.Result.SUCCEEDED
                                  stage = successLogEntry.details.stageAttempt.stageName
                                  logEntryCreateTime = successLogEntry.logEntry.createTime
                                  this.completionDurationSeconds = completionDurationSeconds
                                  completionDurationSecondsSquared =
                                    completionDurationSeconds * completionDurationSeconds
                                }
                                .toByteString()
                            )

                            prevTimestamp = successLogEntry.logEntry.createTime
                          }

                          if (
                            computationParticipant.hasFailureLogEntry() &&
                              measurementState == MeasurementState.FAILED
                          ) {
                            val completionDurationSeconds =
                              Durations.toSeconds(
                                Timestamps.between(
                                  prevTimestamp,
                                  computationParticipant.failureLogEntry.logEntry.createTime,
                                )
                              )

                            computationParticipantStagesProtoRowsBuilder.addSerializedRows(
                              computationParticipantStagesTableRow {
                                  this.measurementConsumerId = measurementConsumerId
                                  this.measurementId = measurementId
                                  computationId =
                                    externalIdToApiId(measurement.externalComputationId)
                                  duchyId = computationParticipant.externalDuchyId
                                  this.measurementType = measurementType
                                  this.measurementState = measurementState
                                  result = ComputationParticipantStagesTableRow.Result.FAILED
                                  stage =
                                    computationParticipant.failureLogEntry.details.stageAttempt
                                      .stageName
                                  logEntryCreateTime =
                                    computationParticipant.failureLogEntry.logEntry.createTime
                                  this.completionDurationSeconds = completionDurationSeconds
                                  completionDurationSecondsSquared =
                                    completionDurationSeconds * completionDurationSeconds
                                }
                                .toByteString()
                            )
                          }
                        }
                      }

                      logger.info("Measurements read from the Kingdom Internal Server")

                      if (measurementsProtoRowsBuilder.serializedRowsCount > 0) {
                        coroutineScope {
                          launch {
                            measurementsDataWriter.appendRows(measurementsProtoRowsBuilder.build())
                          }
                          if (requisitionsProtoRowsBuilder.serializedRowsCount > 0) {
                            launch {
                              requisitionsDataWriter.appendRows(
                                requisitionsProtoRowsBuilder.build()
                              )
                            }
                          }
                          if (
                            computationParticipantStagesProtoRowsBuilder.serializedRowsCount > 0
                          ) {
                            launch {
                              computationParticipantStagesDataWriter.appendRows(
                                computationParticipantStagesProtoRowsBuilder.build()
                              )
                            }
                          }
                        }
                      } else {
                        logger.info("No more Measurements to process")
                        break
                      }

                      logger.info("Metrics written to BigQuery")

                      val lastMeasurement =
                        MeasurementsTableRow.parseFrom(
                          measurementsProtoRowsBuilder.serializedRowsList.last()
                        )
                      val latestMeasurementReadTableRow = latestMeasurementReadTableRow {
                        updateTime = Timestamps.toNanos(latestUpdateTime)
                        externalMeasurementConsumerId =
                          apiIdToExternalId(lastMeasurement.measurementConsumerId)
                        externalMeasurementId = apiIdToExternalId(lastMeasurement.measurementId)
                      }

                      latestMeasurementReadDataWriter.appendRows(
                        ProtoRows.newBuilder()
                          .addSerializedRows(latestMeasurementReadTableRow.toByteString())
                          .build()
                      )

                      streamMeasurementsRequest =
                        streamMeasurementsRequest.copy {
                          filter =
                            filter.copy {
                              after =
                                StreamMeasurementsRequestKt.FilterKt.after {
                                  updateTime = latestUpdateTime
                                  measurement = measurementKey {
                                    externalMeasurementConsumerId =
                                      latestMeasurementReadTableRow.externalMeasurementConsumerId
                                    externalMeasurementId =
                                      latestMeasurementReadTableRow.externalMeasurementId
                                  }
                                }
                            }
                        }
                    } while (measurementsQueryResponseSize == BATCH_SIZE)
                  }
              }
          }
      }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)
    private const val BATCH_SIZE = 3000
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
          for (value in e.rowIndexToErrorMessage.values) {
            logger.warning(value)
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
