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

import com.google.api.gax.core.FixedExecutorProvider
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.Exceptions.AppendSerializationError
import com.google.cloud.bigquery.storage.v1.ProtoRows
import com.google.cloud.bigquery.storage.v1.ProtoSchema
import com.google.cloud.bigquery.storage.v1.StreamWriter
import com.google.cloud.bigquery.storage.v1.TableName
import com.google.protobuf.Timestamp
import com.google.protobuf.any
import com.google.protobuf.util.Durations
import com.google.protobuf.util.Timestamps
import com.google.rpc.Code
import java.util.concurrent.Executors
import java.util.logging.Logger
import kotlinx.coroutines.CoroutineScope
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.runBlocking
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.internal.kingdom.ComputationParticipant
import org.wfanet.measurement.internal.kingdom.DataProvider
import org.wfanet.measurement.internal.kingdom.DataProvidersGrpcKt
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequest
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.bigquerytables.ComputationParticipantsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.LatestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementType
import org.wfanet.measurement.internal.kingdom.bigquerytables.MeasurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.RequisitionsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.computationParticipantsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.latestMeasurementReadTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.measurementsTableRow
import org.wfanet.measurement.internal.kingdom.bigquerytables.requisitionsTableRow
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.getDataProviderRequest
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

class OperationalMetricsExport(
  private val measurementsClient: MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val dataProvidersClient: DataProvidersGrpcKt.DataProvidersCoroutineStub,
  private val bigQuery: BigQuery,
  private val bigQueryWriteClient: BigQueryWriteClient,
  private val projectId: String,
  private val datasetId: String,
  private val latestMeasurementReadTableId: String,
  private val measurementsTableId: String,
  private val requisitionsTableId: String,
  private val computationParticipantsTableId: String,
  private val createStreamWriterFunc:
    (
      projectId: String,
      datasetId: String,
      tableId: String,
      client: BigQueryWriteClient,
      protoSchema: ProtoSchema,
    ) -> StreamWriter =
    this::createStreamWriter,
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
      measurementView = Measurement.View.COMPUTATION
      orderBy = StreamMeasurementsRequest.OrderBy.MEASUREMENT
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

    val measurementsDataWriter =
      DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = measurementsTableId,
        client = bigQueryWriteClient,
        protoSchema =
          ProtoSchema.newBuilder()
            .setProtoDescriptor(MeasurementsTableRow.getDescriptor().toProto())
            .build(),
        createStreamWriterFunc = createStreamWriterFunc,
      )
    val requisitionsDataWriter =
      DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = requisitionsTableId,
        client = bigQueryWriteClient,
        protoSchema =
          ProtoSchema.newBuilder()
            .setProtoDescriptor(RequisitionsTableRow.getDescriptor().toProto())
            .build(),
        createStreamWriterFunc = createStreamWriterFunc,
      )
    val computationParticipantsDataWriter =
      DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = computationParticipantsTableId,
        client = bigQueryWriteClient,
        protoSchema =
          ProtoSchema.newBuilder()
            .setProtoDescriptor(ComputationParticipantsTableRow.getDescriptor().toProto())
            .build(),
        createStreamWriterFunc = createStreamWriterFunc,
      )
    val latestMeasurementReadDataWriter =
      DataWriter(
        projectId = projectId,
        datasetId = datasetId,
        tableId = latestMeasurementReadTableId,
        client = bigQueryWriteClient,
        protoSchema =
          ProtoSchema.newBuilder()
            .setProtoDescriptor(LatestMeasurementReadTableRow.getDescriptor().toProto())
            .build(),
        createStreamWriterFunc = createStreamWriterFunc,
      )

    // Map of externalDataProviderId to DataProvider
    val dataProvidersMap: HashMap<Long, DataProvider> = hashMapOf()

    do {
      measurementsQueryResponseSize = 0

      val measurementsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      val requisitionsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      val computationParticipantsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      var latestUpdateTime: Timestamp = Timestamp.getDefaultInstance()

      measurementsClient.streamMeasurements(streamMeasurementsRequest).collect { measurement ->
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
                  else -> ProtoReflection.getTypeUrl(MeasurementSpec.getDescriptor())
                }
            }
          )
          signature = measurement.details.measurementSpecSignature
          signatureAlgorithmOid = measurement.details.measurementSpecSignatureAlgorithmOid
        }
        val measurementTypeCase = measurementSpec.unpack<MeasurementSpec>().measurementTypeCase
        val measurementType =
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
          when (measurementTypeCase) {
            MeasurementSpec.MeasurementTypeCase.REACH_AND_FREQUENCY ->
              MeasurementType.REACH_AND_FREQUENCY
            MeasurementSpec.MeasurementTypeCase.IMPRESSION -> MeasurementType.IMPRESSION
            MeasurementSpec.MeasurementTypeCase.DURATION -> MeasurementType.DURATION
            MeasurementSpec.MeasurementTypeCase.REACH -> MeasurementType.REACH
            MeasurementSpec.MeasurementTypeCase.POPULATION -> MeasurementType.POPULATION
            MeasurementSpec.MeasurementTypeCase.MEASUREMENTTYPE_NOT_SET ->
              MeasurementType.MEASUREMENT_TYPE_UNSPECIFIED
          }

        val measurementConsumerId = externalIdToApiId(measurement.externalMeasurementConsumerId)
        val measurementId = externalIdToApiId(measurement.externalMeasurementId)
        val measurementCreateTimeMicros = Timestamps.toMicros(measurement.createTime)

        val measurementCompletionDurationSeconds =
          Durations.toSeconds(Timestamps.between(measurement.createTime, measurement.updateTime))

        val measurementState =
          @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
          when (measurement.state) {
            // StreamMeasurements filter only returns SUCCEEDED and FAILED Measurements.
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

        measurementsProtoRowsBuilder.addSerializedRows(
          measurementsTableRow {
              this.measurementConsumerId = measurementConsumerId
              this.measurementId = measurementId
              isDirect = measurement.details.protocolConfig.hasDirect()
              this.measurementType = measurementType
              state = measurementState
              createTime = measurementCreateTimeMicros
              updateTime = Timestamps.toMicros(measurement.updateTime)
              completionDurationSeconds = measurementCompletionDurationSeconds
              completionDurationSecondsSquared =
                measurementCompletionDurationSeconds * measurementCompletionDurationSeconds
            }
            .toByteString()
        )

        for (requisition in measurement.requisitionsList) {
          val requisitionCompletionDurationSeconds =
            Durations.toSeconds(Timestamps.between(measurement.createTime, requisition.updateTime))

          val requisitionState =
            @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
            when (requisition.state) {
              Requisition.State.STATE_UNSPECIFIED -> RequisitionsTableRow.State.STATE_UNSPECIFIED
              Requisition.State.UNRECOGNIZED -> RequisitionsTableRow.State.UNRECOGNIZED
              Requisition.State.PENDING_PARAMS,
              Requisition.State.UNFULFILLED -> RequisitionsTableRow.State.UNFULFILLED
              Requisition.State.FULFILLED -> RequisitionsTableRow.State.FULFILLED
              Requisition.State.REFUSED -> RequisitionsTableRow.State.REFUSED
            }

          val value = dataProvidersMap[requisition.externalDataProviderId]
          val dataProvider =
            if (value != null) {
              value
            } else {
              val response = runBlocking {
                dataProvidersClient.getDataProvider(
                  getDataProviderRequest {
                    externalDataProviderId = requisition.externalDataProviderId
                  }
                )
              }
              dataProvidersMap[requisition.externalDataProviderId] = response
              response
            }

          requisitionsProtoRowsBuilder.addSerializedRows(
            requisitionsTableRow {
                this.measurementConsumerId = measurementConsumerId
                this.measurementId = measurementId
                requisitionId = externalIdToApiId(requisition.externalRequisitionId)
                dataProviderId = externalIdToApiId(requisition.externalDataProviderId)
                isDirect = measurement.details.protocolConfig.hasDirect()
                this.measurementType = measurementType
                state = requisitionState
                createTime = measurementCreateTimeMicros
                updateTime = Timestamps.toMicros(requisition.updateTime)
                completionDurationSeconds = requisitionCompletionDurationSeconds
                completionDurationSecondsSquared =
                  requisitionCompletionDurationSeconds * requisitionCompletionDurationSeconds
                dataProviderDisplayName = dataProvider.details.displayName
              }
              .toByteString()
          )
        }

        if (measurement.externalComputationId != 0L) {
          for (computationParticipant in measurement.computationParticipantsList) {
            val computationParticipantCompletionDurationSeconds =
              Durations.toSeconds(
                Timestamps.between(measurement.createTime, computationParticipant.updateTime)
              )

            val computationParticipantProtocol =
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (computationParticipant.details.protocolCase) {
                ComputationParticipant.Details.ProtocolCase.LIQUID_LEGIONS_V2 ->
                  ComputationParticipantsTableRow.Protocol.LIQUID_LEGIONS_V2
                ComputationParticipant.Details.ProtocolCase.REACH_ONLY_LIQUID_LEGIONS_V2 ->
                  ComputationParticipantsTableRow.Protocol.REACH_ONLY_LIQUID_LEGIONS_V2
                ComputationParticipant.Details.ProtocolCase.HONEST_MAJORITY_SHARE_SHUFFLE ->
                  ComputationParticipantsTableRow.Protocol.HONEST_MAJORITY_SHARE_SHUFFLE
                ComputationParticipant.Details.ProtocolCase.PROTOCOL_NOT_SET ->
                  ComputationParticipantsTableRow.Protocol.PROTOCOL_UNSPECIFIED
              }

            val computationParticipantState =
              @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
              when (computationParticipant.state) {
                ComputationParticipant.State.STATE_UNSPECIFIED ->
                  ComputationParticipantsTableRow.State.STATE_UNSPECIFIED
                ComputationParticipant.State.UNRECOGNIZED ->
                  ComputationParticipantsTableRow.State.UNRECOGNIZED
                ComputationParticipant.State.CREATED ->
                  ComputationParticipantsTableRow.State.CREATED
                ComputationParticipant.State.REQUISITION_PARAMS_SET ->
                  ComputationParticipantsTableRow.State.REQUISITION_PARAMS_SET
                ComputationParticipant.State.READY -> ComputationParticipantsTableRow.State.READY
                ComputationParticipant.State.FAILED -> ComputationParticipantsTableRow.State.FAILED
              }

            computationParticipantsProtoRowsBuilder.addSerializedRows(
              computationParticipantsTableRow {
                  this.measurementConsumerId = measurementConsumerId
                  this.measurementId = measurementId
                  computationId = externalIdToApiId(measurement.externalComputationId)
                  duchyId = computationParticipant.externalDuchyId
                  protocol = computationParticipantProtocol
                  this.measurementType = measurementType
                  state = computationParticipantState
                  createTime = measurementCreateTimeMicros
                  updateTime = Timestamps.toMicros(computationParticipant.updateTime)
                  completionDurationSeconds = computationParticipantCompletionDurationSeconds
                  completionDurationSecondsSquared =
                    computationParticipantCompletionDurationSeconds *
                      computationParticipantCompletionDurationSeconds
                }
                .toByteString()
            )
          }
        }
      }

      logger.info("Measurements read from the Kingdom Internal Server")

      val deferredResults: MutableList<Deferred<Unit>> = mutableListOf()
      if (measurementsProtoRowsBuilder.serializedRowsCount > 0) {
        coroutineScope {
          deferredResults.add(
            measurementsDataWriter.asyncAppendRows(this, measurementsProtoRowsBuilder.build())
          )
          deferredResults.add(
            requisitionsDataWriter.asyncAppendRows(this, requisitionsProtoRowsBuilder.build())
          )
          if (computationParticipantsProtoRowsBuilder.serializedRowsCount > 0) {
            deferredResults.add(
              computationParticipantsDataWriter.asyncAppendRows(
                this,
                computationParticipantsProtoRowsBuilder.build(),
              )
            )
          }
        }
        deferredResults.awaitAll()
      } else {
        logger.info("No more Measurements to process")
        break
      }

      logger.info("Metrics written to BigQuery")

      val lastMeasurement =
        MeasurementsTableRow.parseFrom(measurementsProtoRowsBuilder.serializedRowsList.last())
      val latestMeasurementReadTableRow = latestMeasurementReadTableRow {
        updateTime = Timestamps.toNanos(latestUpdateTime)
        externalMeasurementConsumerId = apiIdToExternalId(lastMeasurement.measurementConsumerId)
        externalMeasurementId = apiIdToExternalId(lastMeasurement.measurementId)
      }
      coroutineScope {
        latestMeasurementReadDataWriter
          .asyncAppendRows(
            this,
            ProtoRows.newBuilder()
              .addSerializedRows(latestMeasurementReadTableRow.toByteString())
              .build(),
          )
          .await()
      }

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
                    externalMeasurementId = latestMeasurementReadTableRow.externalMeasurementId
                  }
                }
            }
        }
    } while (measurementsQueryResponseSize == BATCH_SIZE)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val BATCH_SIZE = 1000

    private const val DEFAULT_STREAM_PATH = "/streams/_default"

    private fun createStreamWriter(
      projectId: String,
      datasetId: String,
      tableId: String,
      client: BigQueryWriteClient,
      protoSchema: ProtoSchema,
    ): StreamWriter {
      val tableName = TableName.of(projectId, datasetId, tableId)
      return StreamWriter.newBuilder(tableName.toString() + DEFAULT_STREAM_PATH, client)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(1)))
        .setEnableConnectionPool(true)
        .setDefaultMissingValueInterpretation(
          AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE
        )
        .setWriterSchema(protoSchema)
        .build()
    }
  }

  private class DataWriter(
    private val projectId: String,
    private val datasetId: String,
    private val tableId: String,
    private val client: BigQueryWriteClient,
    private val protoSchema: ProtoSchema,
    private val createStreamWriterFunc:
      (
        projectId: String,
        datasetId: String,
        tableId: String,
        client: BigQueryWriteClient,
        protoSchema: ProtoSchema,
      ) -> StreamWriter,
  ) : AutoCloseable {
    private var streamWriter: StreamWriter =
      createStreamWriterFunc(projectId, datasetId, tableId, client, protoSchema)
    private var recreateCount: Int = 0

    override fun close() {
      streamWriter.close()
    }

    /**
     * Writes data to the stream.
     *
     * @param protoRows protos representing the rows to write.
     * @returns ApiFuture containing the write response.
     * @throws IllegalStateException if append fails and error is not retriable or too many retry
     *   attempts have been made
     */
    fun asyncAppendRows(scope: CoroutineScope, protoRows: ProtoRows): Deferred<Unit> {
      return scope.async {
        logger.info("Begin writing to stream ${streamWriter.streamName}")
        for (i in 1..RETRY_COUNT) {
          if (streamWriter.isClosed) {
            if (!streamWriter.isUserClosed && recreateCount < MAX_RECREATE_COUNT) {
              logger.info("Recreating stream writer")
              streamWriter =
                createStreamWriterFunc(projectId, datasetId, tableId, client, protoSchema)
              recreateCount++
            } else {
              throw IllegalStateException("Unable to recreate stream writer")
            }
          }

          try {
            val response = streamWriter.append(protoRows).await()
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
    }

    companion object {
      private const val MAX_RECREATE_COUNT = 3
      private const val RETRY_COUNT = 3
    }
  }
}
