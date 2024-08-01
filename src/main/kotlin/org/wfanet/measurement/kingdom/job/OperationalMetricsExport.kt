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

package org.wfanet.measurement.kingdom.job

import com.google.api.gax.core.FixedExecutorProvider
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings
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
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.MeasurementData
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.computationParticipantData
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.latestMeasurementRead
import org.wfanet.measurement.internal.kingdom.measurementData
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.internal.kingdom.requisitionData
import org.wfanet.measurement.internal.kingdom.streamMeasurementsRequest

class OperationalMetricsExport(
  private val measurementsClient: MeasurementsGrpcKt.MeasurementsCoroutineStub,
  private val bigQuery: BigQuery,
  private val datasetId: String,
  private val latestMeasurementReadTableId: String,
  private val measurementsDataWriter: DataWriter,
  private val requisitionsDataWriter: DataWriter,
  private val computationParticipantsDataWriter: DataWriter,
  private val latestMeasurementReadDataWriter: DataWriter,
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
      orderByMeasurementView = Measurement.View.DEFAULT
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

    do {
      measurementsQueryResponseSize = 0

      val measurementsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      val requisitionsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      val computationParticipantsProtoRowsBuilder: ProtoRows.Builder = ProtoRows.newBuilder()
      var latestUpdateTime: Timestamp = Timestamp.getDefaultInstance()

      try {
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

          val measurementUpdateTimeMinusCreateTime =
            Durations.toMillis(Timestamps.between(measurement.createTime, measurement.updateTime))

          val measurementConsumerId = externalIdToApiId(measurement.externalMeasurementConsumerId)
          val measurementId = externalIdToApiId(measurement.externalMeasurementId)

          measurementsProtoRowsBuilder.addSerializedRows(
            measurementData {
                this.measurementConsumerId = measurementConsumerId
                this.measurementId = measurementId
                isDirect = measurement.details.protocolConfig.hasDirect()
                measurementType = measurementTypeCase.name
                state = measurement.state.name
                createTime = Timestamps.toMicros(measurement.createTime)
                updateTime = Timestamps.toMicros(measurement.updateTime)
                updateTimeMinusCreateTime = measurementUpdateTimeMinusCreateTime
                updateTimeMinusCreateTimeSquared =
                  measurementUpdateTimeMinusCreateTime * measurementUpdateTimeMinusCreateTime
              }
              .toByteString()
          )

          for (requisition in measurement.requisitionsList) {
            val requisitionUpdateTimeMinusCreateTime =
              Durations.toMillis(Timestamps.between(measurement.createTime, requisition.updateTime))

            val state =
              when (requisition.state) {
                Requisition.State.PENDING_PARAMS -> Requisition.State.UNFULFILLED.name
                else -> requisition.state.name
              }

            requisitionsProtoRowsBuilder.addSerializedRows(
              requisitionData {
                  this.measurementConsumerId = measurementConsumerId
                  this.measurementId = measurementId
                  requisitionId = externalIdToApiId(requisition.externalRequisitionId)
                  dataProviderId = externalIdToApiId(requisition.externalDataProviderId)
                  isDirect = measurement.details.protocolConfig.hasDirect()
                  measurementType = measurementTypeCase.name
                  this.state = state
                  createTime = Timestamps.toMicros(measurement.createTime)
                  updateTime = Timestamps.toMicros(requisition.updateTime)
                  updateTimeMinusCreateTime = requisitionUpdateTimeMinusCreateTime
                  updateTimeMinusCreateTimeSquared =
                    requisitionUpdateTimeMinusCreateTime * requisitionUpdateTimeMinusCreateTime
                }
                .toByteString()
            )
          }

          if (measurement.externalComputationId != 0L) {
            for (computationParticipant in measurement.computationParticipantsList) {
              val computationParticipantUpdateTimeMinusCreateTime =
                Durations.toMillis(
                  Timestamps.between(measurement.createTime, computationParticipant.updateTime)
                )

              computationParticipantsProtoRowsBuilder.addSerializedRows(
                computationParticipantData {
                    this.measurementConsumerId = measurementConsumerId
                    this.measurementId = measurementId
                    computationId = externalIdToApiId(measurement.externalComputationId)
                    duchyId = computationParticipant.externalDuchyId
                    protocol = computationParticipant.details.protocolCase.name
                    measurementType = measurementTypeCase.name
                    state = computationParticipant.state.name
                    createTime = Timestamps.toMicros(measurement.createTime)
                    updateTime = Timestamps.toMicros(computationParticipant.updateTime)
                    updateTimeMinusCreateTime = computationParticipantUpdateTimeMinusCreateTime
                    updateTimeMinusCreateTimeSquared =
                      computationParticipantUpdateTimeMinusCreateTime *
                        computationParticipantUpdateTimeMinusCreateTime
                  }
                  .toByteString()
              )
            }
          }
        }
      } catch (e: Exception) {
        logger.warning("Reading or processing Measurements failed.")
        throw e
      }

      logger.info("Measurements read from Spanner")

      val deferredResults: MutableList<Deferred<Unit>> = mutableListOf()
      if (measurementsProtoRowsBuilder.serializedRowsCount > 0) {
        deferredResults.add(measurementsDataWriter.appendRows(measurementsProtoRowsBuilder.build()))
        deferredResults.add(requisitionsDataWriter.appendRows(requisitionsProtoRowsBuilder.build()))
        if (computationParticipantsProtoRowsBuilder.serializedRowsCount > 0) {
          deferredResults.add(
            computationParticipantsDataWriter.appendRows(
              computationParticipantsProtoRowsBuilder.build()
            )
          )
        }
        deferredResults.awaitAll()
      } else {
        logger.info("No more Measurements to process")
        break
      }

      logger.info("Metrics written to BigQuery")

      val lastMeasurement =
        MeasurementData.parseFrom(measurementsProtoRowsBuilder.serializedRowsList.last())
      val latestMeasurementRead = latestMeasurementRead {
        updateTime = Timestamps.toNanos(latestUpdateTime)
        externalMeasurementConsumerId = apiIdToExternalId(lastMeasurement.measurementConsumerId)
        externalMeasurementId = apiIdToExternalId(lastMeasurement.measurementId)
      }
      latestMeasurementReadDataWriter
        .appendRows(
          ProtoRows.newBuilder().addSerializedRows(latestMeasurementRead.toByteString()).build()
        )
        .await()

      streamMeasurementsRequest =
        streamMeasurementsRequest.copy {
          filter =
            filter.copy {
              after =
                StreamMeasurementsRequestKt.FilterKt.after {
                  updateTime = latestUpdateTime
                  measurement = measurementKey {
                    externalMeasurementConsumerId =
                      latestMeasurementRead.externalMeasurementConsumerId
                    externalMeasurementId = latestMeasurementRead.externalMeasurementId
                  }
                }
            }
        }
    } while (measurementsQueryResponseSize == BATCH_SIZE)
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val BATCH_SIZE = 1000
  }

  abstract class DataWriter {
    abstract suspend fun appendRows(protoRows: ProtoRows): Deferred<Unit>
  }

  class DataWriterImplementation(
    private val projectId: String,
    private val datasetId: String,
    private val tableId: String,
    private val client: BigQueryWriteClient,
    private val protoSchema: ProtoSchema,
  ) : DataWriter() {
    private lateinit var streamWriter: StreamWriter
    private var recreateCount: Int = 0

    fun init() {
      streamWriter = createStreamWriter()
    }

    fun close() {
      if (this::streamWriter.isInitialized) {
        streamWriter.close()
      }
    }

    private fun createStreamWriter(): StreamWriter {
      val tableName = TableName.of(projectId, datasetId, tableId)
      return StreamWriter.newBuilder(tableName.toString() + DEFAULT_STREAM_PATH, client)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(1)))
        .setChannelProvider(BigQueryWriteSettings.defaultGrpcTransportProviderBuilder().build())
        .setEnableConnectionPool(true)
        .setDefaultMissingValueInterpretation(
          AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE
        )
        .setWriterSchema(protoSchema)
        .build()
    }

    /**
     * Writes data to the stream.
     *
     * @param protoRows protos representing the rows to write.
     * @returns ApiFuture containing the write response.
     * @throws IllegalStateException if append fails and is no longer retriable
     */
    override suspend fun appendRows(protoRows: ProtoRows): Deferred<Unit> {
      return coroutineScope {
        async {
          logger.info("Begin writing to stream ${streamWriter.streamName}")
          for (i in 1..RETRY_COUNT) {
            if (streamWriter.isClosed) {
              if (!streamWriter.isUserClosed && recreateCount < MAX_RECREATE_COUNT) {
                logger.info("Recreating stream writer")
                streamWriter = createStreamWriter()
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
                  throw IllegalStateException("Cannot retry append.")
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
            } catch (e: Exception) {
              logger.warning("Append attempt failed: ${e.printStackTrace()}")
              if (i == RETRY_COUNT) {
                throw IllegalStateException("Too many retries.")
              }
            }
          }
        }
      }
    }

    companion object {
      private const val MAX_RECREATE_COUNT = 3
      private const val RETRY_COUNT = 3
      private const val DEFAULT_STREAM_PATH = "/streams/_default"
    }
  }
}
