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
import java.util.concurrent.TimeUnit
import java.util.concurrent.TimeoutException
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
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementType
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.RequisitionsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.computationParticipantStagesTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.copy
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
    val updateTime = getLatestUpdateTime(measurementsTableId, "update_time_nanoseconds")
    logger.info("Retrieved latest measurement read info from BigQuery")

    val streamMeasurementsRequest = streamMeasurementsRequest {
      measurementView = Measurement.View.DEFAULT
      limit = batchSize
      filter =
        StreamMeasurementsRequestKt.filter {
          states += Measurement.State.SUCCEEDED
          states += Measurement.State.FAILED
          updatedAfter = updateTime
        }
    }

    val measurementsList: List<Measurement> =
      measurementsClient
        .streamMeasurements(streamMeasurementsRequest)
        .catch { e ->
          if (e is StatusException) {
            logger.warning("Failed to retrieved Measurements")
            throw e
          }
        }
        .toList()

    val readMeasurements: (suspend (Measurement) -> List<Measurement>) =
      { measurement: Measurement ->
        measurementsClient
          .streamMeasurements(
            streamMeasurementsRequest.copy {
              filter =
                filter.copy {
                  after =
                    StreamMeasurementsRequestKt.FilterKt.after {
                      this.updateTime = measurement.updateTime
                      this.measurement = measurementKey {
                        externalMeasurementConsumerId = measurement.externalMeasurementConsumerId
                        externalMeasurementId = measurement.externalMeasurementId
                      }
                    }
                }
            }
          )
          .catch { e ->
            if (e is StatusException) {
              logger.warning("Failed to retrieved Measurements")
              throw e
            }
          }
          .toList()
      }

    val processMeasurement: ((Measurement) -> List<ByteString>) = { measurement: Measurement ->
      val protoRowsList = mutableListOf<ByteString>()

      val measurementType =
        getMeasurementType(measurement.details.measurementSpec, measurement.details.apiVersion)

      val measurementConsumerId = externalIdToApiId(measurement.externalMeasurementConsumerId)
      val measurementId = externalIdToApiId(measurement.externalMeasurementId)

      val measurementCompletionDurationSeconds =
        Duration.between(measurement.createTime.toInstant(), measurement.updateTime.toInstant())
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
            this.updateTime = measurement.updateTime
            completionDurationSeconds = measurementCompletionDurationSeconds
            completionDurationSecondsSquared =
              measurementCompletionDurationSeconds * measurementCompletionDurationSeconds
            updateTimeNanoseconds = Timestamps.toNanos(measurement.updateTime)
          }
          .toByteString()

      protoRowsList.add(measurementsTableRow)
      protoRowsList
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
        appendData(
          dataWriter = measurementsDataWriter,
          firstBatch = measurementsList,
          readBatch = readMeasurements,
          retrieveUpdateTime = { measurement -> measurement.updateTime },
          processBatchItem = processMeasurement,
          successfulBatchReadMessage = "Measurements read from the Kingdom Internal Server",
          successfulAppendMessage = "Measurement Metrics written to BigQuery",
        )
      }
  }

  private suspend fun exportRequisitions() {
    val updateTime = getLatestUpdateTime(requisitionsTableId, "update_time_nanoseconds")
    logger.info("Retrieved latest requisition read info from BigQuery")

    val streamRequisitionsRequest = streamRequisitionsRequest {
      limit = batchSize
      filter =
        StreamRequisitionsRequestKt.filter {
          states += Requisition.State.FULFILLED
          states += Requisition.State.REFUSED
          updatedAfter = updateTime
        }
    }

    val requisitionsList: List<Requisition> =
      requisitionsClient
        .streamRequisitions(streamRequisitionsRequest)
        .catch { e ->
          if (e is StatusException) {
            logger.warning("Failed to retrieved Requisitions")
            throw e
          }
        }
        .toList()

    val readRequisitions: (suspend (Requisition) -> List<Requisition>) =
      { requisition: Requisition ->
        requisitionsClient
          .streamRequisitions(
            streamRequisitionsRequest.copy {
              filter =
                filter.copy {
                  after =
                    StreamRequisitionsRequestKt.FilterKt.after {
                      this.updateTime = requisition.updateTime
                      externalDataProviderId = requisition.externalDataProviderId
                      externalRequisitionId = requisition.externalRequisitionId
                    }
                }
            }
          )
          .catch { e ->
            if (e is StatusException) {
              logger.warning("Failed to retrieved Requisitions")
              throw e
            }
          }
          .toList()
      }

    val processRequisition: ((Requisition) -> List<ByteString>) = { requisition: Requisition ->
      val protoRowsList = mutableListOf<ByteString>()

      val measurementType =
        getMeasurementType(
          requisition.parentMeasurement.measurementSpec,
          requisition.parentMeasurement.apiVersion,
        )

      val measurementConsumerId = externalIdToApiId(requisition.externalMeasurementConsumerId)
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
            buildLabel = requisition.details.fulfillmentContext.buildLabel
            warnings += requisition.details.fulfillmentContext.warningsList
            createTime = requisition.parentMeasurement.createTime
            this.updateTime = requisition.updateTime
            completionDurationSeconds = requisitionCompletionDurationSeconds
            completionDurationSecondsSquared =
              requisitionCompletionDurationSeconds * requisitionCompletionDurationSeconds
            updateTimeNanoseconds = Timestamps.toNanos(requisition.updateTime)
          }
          .toByteString()

      protoRowsList.add(requisitionsTableRow)
      protoRowsList
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
        appendData(
          dataWriter = requisitionsDataWriter,
          firstBatch = requisitionsList,
          readBatch = readRequisitions,
          retrieveUpdateTime = { requisition -> requisition.updateTime },
          processBatchItem = processRequisition,
          successfulBatchReadMessage = "Requisitions read from the Kingdom Internal Server",
          successfulAppendMessage = "Requisition Metrics written to BigQuery",
        )
      }
  }

  private suspend fun exportComputationParticipants() {
    val updateTime =
      getLatestUpdateTime(
        computationParticipantStagesTableId,
        "computation_update_time_nanoseconds",
      )
    logger.info("Retrieved latest computation read info from BigQuery")

    val streamComputationsRequest = streamMeasurementsRequest {
      measurementView = Measurement.View.COMPUTATION_STATS
      limit = batchSize
      filter =
        StreamMeasurementsRequestKt.filter {
          states += Measurement.State.SUCCEEDED
          states += Measurement.State.FAILED
          updatedAfter = updateTime
        }
    }

    val computationsList: List<Measurement> =
      measurementsClient
        .streamMeasurements(streamComputationsRequest)
        .catch { e ->
          if (e is StatusException) {
            logger.warning("Failed to retrieved Computations")
            throw e
          }
        }
        .toList()

    val readComputations: (suspend (Measurement) -> List<Measurement>) =
      { measurement: Measurement ->
        measurementsClient
          .streamMeasurements(
            streamComputationsRequest.copy {
              filter =
                filter.copy {
                  after =
                    StreamMeasurementsRequestKt.FilterKt.after {
                      this.updateTime = measurement.updateTime
                      computation = computationKey {
                        externalComputationId = measurement.externalComputationId
                      }
                    }
                }
            }
          )
          .catch { e ->
            if (e is StatusException) {
              logger.warning("Failed to retrieved Computations")
              throw e
            }
          }
          .toList()
      }

    val processComputation: ((Measurement) -> List<ByteString>) = { measurement: Measurement ->
      val protoRowsList = mutableListOf<ByteString>()
      if (measurement.externalComputationId != 0L) {
        val measurementType =
          getMeasurementType(measurement.details.measurementSpec, measurement.details.apiVersion)

        val measurementConsumerId = externalIdToApiId(measurement.externalMeasurementConsumerId)
        val measurementId = externalIdToApiId(measurement.externalMeasurementId)
        val computationId = externalIdToApiId(measurement.externalComputationId)

        val baseComputationParticipantStagesTableRow = computationParticipantStagesTableRow {
          this.measurementConsumerId = measurementConsumerId
          this.measurementId = measurementId
          this.computationId = computationId
          this.measurementType = measurementType
        }

        // Map of ExternalDuchyId to log entries.
        val logEntriesMap: Map<String, MutableList<DuchyMeasurementLogEntry>> = buildMap {
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
              protoRowsList.add(
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
                    computationUpdateTimeNanoseconds = Timestamps.toNanos(measurement.updateTime)
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
                    computationUpdateTimeNanoseconds = Timestamps.toNanos(measurement.updateTime)
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
                    computationUpdateTimeNanoseconds = Timestamps.toNanos(measurement.updateTime)
                  }
                  .toByteString()
              else -> null
            }

          if (computationParticipantStagesProtoRow != null) {
            protoRowsList.add(computationParticipantStagesProtoRow)
          }
        }
      }
      protoRowsList
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
        appendData(
          dataWriter = computationParticipantStagesDataWriter,
          firstBatch = computationsList,
          readBatch = readComputations,
          retrieveUpdateTime = { measurement -> measurement.updateTime },
          processBatchItem = processComputation,
          successfulBatchReadMessage = "Computations read from the Kingdom Internal Server",
          successfulAppendMessage = "Computation Participant Stage Metrics written to BigQuery",
        )
      }
  }

  private fun getLatestUpdateTime(tableId: String, columnName: String): Timestamp {
    val query =
      """
    SELECT MAX($columnName) as $columnName
    FROM `$datasetId.$tableId`
    """
        .trimIndent()

    val queryJobConfiguration: QueryJobConfiguration =
      QueryJobConfiguration.newBuilder(query).build()

    val results = bigQuery.query(queryJobConfiguration).iterateAll()

    val latestUpdateTime: FieldValueList? = results.firstOrNull()

    return if (latestUpdateTime != null) {
      val readValue = latestUpdateTime.get(columnName)
      if (!readValue.isNull) {
        Timestamps.fromNanos(readValue.longValue)
      } else {
        Timestamp.getDefaultInstance()
      }
    } else {
      Timestamp.getDefaultInstance()
    }
  }

  /** This is under the assumption that batches are retrieved based on updateTime. */
  private suspend fun <T> appendData(
    dataWriter: DataWriter,
    firstBatch: List<T>,
    readBatch: (suspend (T) -> List<T>),
    retrieveUpdateTime: (T) -> Timestamp,
    processBatchItem: (T) -> List<ByteString>,
    successfulBatchReadMessage: String,
    successfulAppendMessage: String,
  ) {
    // This list keeps track of items with the same updateTime that haven't been appended yet.
    val protoRowsList = mutableListOf<ByteString>()
    var latestUpdateTime: Timestamp? = null

    var batch: List<T> = firstBatch

    if (batch.isEmpty()) {
      return
    }

    do {
      val protoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()

      if (latestUpdateTime == null) {
        latestUpdateTime = retrieveUpdateTime(batch[0])
      }

      for (item in batch) {
        // Only when it is guaranteed that no more items with the same updateTime exist will the
        // items with the same updateTime be setup for appending.
        if (Timestamps.compare(latestUpdateTime, retrieveUpdateTime(item)) != 0) {
          protoRowsBuilder.addAllSerializedRows(protoRowsList)
          protoRowsList.clear()
          latestUpdateTime = retrieveUpdateTime(item)
        }

        val protoRowByteStrings: List<ByteString> = processBatchItem(item)
        protoRowsList.addAll(protoRowByteStrings)
      }

      if (protoRowsBuilder.serializedRowsCount > 0) {
        dataWriter.appendRows(protoRowsBuilder.build())
        logger.info(successfulAppendMessage)
      }

      if (batch.size == batchSize) {
        batch = readBatch(batch[batch.lastIndex])
        logger.info(successfulBatchReadMessage)
      } else {
        batch = emptyList()
      }
    } while (batch.isNotEmpty())

    // The check above only works when there are more items to process. If there are no more items
    // to process, then the elements in this list can be appended.
    if (protoRowsList.isNotEmpty()) {
      val protoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      protoRowsBuilder.addAllSerializedRows(protoRowsList)
      dataWriter.appendRows(protoRowsBuilder.build())
      logger.info(successfulAppendMessage)
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
          val response = streamWriter.append(protoRows).get(30L, TimeUnit.SECONDS)
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
        } catch (e: TimeoutException) {
          if (i == RETRY_COUNT) {
            throw IllegalStateException("Too many retries.")
          }
        }
      }
    }

    companion object {
      private const val MAX_RECREATE_COUNT = 3
      private const val RETRY_COUNT = 3
    }
  }
}
