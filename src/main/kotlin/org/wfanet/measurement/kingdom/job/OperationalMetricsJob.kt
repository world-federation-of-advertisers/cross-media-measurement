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
import com.google.api.gax.retrying.RetrySettings
import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.BigQueryError
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.Job
import com.google.cloud.bigquery.JobId
import com.google.cloud.bigquery.JobInfo
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.storage.v1.AppendRowsRequest
import com.google.cloud.bigquery.storage.v1.BigQueryWriteClient
import com.google.cloud.bigquery.storage.v1.BigQueryWriteSettings
import com.google.cloud.bigquery.storage.v1.JsonStreamWriter
import com.google.cloud.bigquery.storage.v1.TableName
import com.google.protobuf.Timestamp
import com.google.protobuf.any
import com.google.protobuf.util.Timestamps
import com.google.rpc.Code
import java.util.UUID
import java.util.concurrent.Executors
import java.util.logging.Logger
import kotlinx.coroutines.Deferred
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.coroutineScope
import org.json.JSONArray
import org.json.JSONObject
import org.threeten.bp.Duration
import org.wfanet.measurement.api.Version
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.setMessage
import org.wfanet.measurement.api.v2alpha.signedMessage
import org.wfanet.measurement.api.v2alpha.unpack
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.gcloud.common.await
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.Measurement
import org.wfanet.measurement.internal.kingdom.StreamMeasurementsRequestKt
import org.wfanet.measurement.internal.kingdom.copy
import org.wfanet.measurement.internal.kingdom.measurementKey
import org.wfanet.measurement.kingdom.deploy.gcloud.spanner.queries.StreamMeasurements

class OperationalMetricsJob(
  private val spannerClient: AsyncDatabaseClient,
  private val bigQuery: BigQuery,
  private val bigQueryWriteClient: BigQueryWriteClient,
  private val projectName: String,
  private val datasetName: String,
  private val measurementsTableName: String,
  private val requisitionsTableName: String,
  private val computationParticipantsTableName: String,
  private val latestMeasurementReadTableName: String,
) {
  suspend fun execute() {
    val measurementsTable: TableName = TableName.of(projectName, datasetName, measurementsTableName)
    val requisitionsTable: TableName = TableName.of(projectName, datasetName, requisitionsTableName)
    val computationParticipantsTable: TableName =
      TableName.of(projectName, datasetName, computationParticipantsTableName)
    val latestMeasurementReadTable: TableName =
      TableName.of(projectName, datasetName, latestMeasurementReadTableName)

    val measurementsDataWriter = JsonDataWriter(measurementsTable, bigQueryWriteClient)
    val requisitionsDataWriter = JsonDataWriter(requisitionsTable, bigQueryWriteClient)
    val computationParticipantsDataWriter =
      JsonDataWriter(computationParticipantsTable, bigQueryWriteClient)
    val latestMeasurementReadDataWriter =
      JsonDataWriter(latestMeasurementReadTable, bigQueryWriteClient)

    var measurementsQueryResponseSize: Int

    try {
      val query =
        """
      SELECT update_time, external_measurement_consumer_id, external_measurement_id
      FROM `$datasetName.$latestMeasurementReadTableName`
      ORDER BY update_time DESC, external_measurement_consumer_id DESC, external_measurement_id DESC
      LIMIT 1
      """
          .trimIndent()

      val queryJobConfiguration: QueryJobConfiguration =
        QueryJobConfiguration.newBuilder(query).build()
      bigQuery.query(queryJobConfiguration)
      val jobId: JobId = JobId.of(UUID.randomUUID().toString())
      val queryJob: Job =
        bigQuery.create(JobInfo.newBuilder(queryJobConfiguration).setJobId(jobId).build())
      logger.info("Connected to BigQuery Successfully.")

      val resultJob: Job = queryJob.waitFor() ?: error("Query job no longer exists")
      val queryError: BigQueryError? = resultJob.status.error
      if (queryError != null) {
        error("Query job failed with $queryError")
      }

      val lastMeasurementRead: FieldValueList = resultJob.getQueryResults().iterateAll().first()
      var filter =
        StreamMeasurementsRequestKt.filter {
          states += Measurement.State.SUCCEEDED
          states += Measurement.State.FAILED
          states += Measurement.State.CANCELLED
          after =
            StreamMeasurementsRequestKt.FilterKt.after {
              updateTime = lastMeasurementRead.get("update_time").timestampInstant.toProtoTime()
              measurement = measurementKey {
                externalMeasurementConsumerId =
                  lastMeasurementRead.get("external_measurement_consumer_id").longValue
                externalMeasurementId = lastMeasurementRead.get("external_measurement_id").longValue
              }
            }
        }

      do {
        measurementsQueryResponseSize = 0

        val measurementsJSONArray = JSONArray()
        val requisitionsJSONArray = JSONArray()
        val computationParticipantsJSONArray = JSONArray()

        try {
          StreamMeasurements(
              Measurement.View.COMPUTATION,
              filter,
              BATCH_SIZE,
              Measurement.View.DEFAULT,
            )
            .execute(spannerClient.singleUse())
            .collect { result ->
              measurementsQueryResponseSize++
              val measurement = result.measurement

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
              val measurementTypeCase =
                measurementSpec.unpack<MeasurementSpec>().measurementTypeCase

              val measurementUpdateTimeMinusCreateTime =
                Timestamps.between(measurement.createTime, measurement.updateTime).seconds

              val measurementJSONObject = JSONObject()
              measurementJSONObject.put(
                "external_measurement_consumer_id",
                measurement.externalMeasurementConsumerId,
              )
              measurementJSONObject.put(
                "external_measurement_id",
                measurement.externalMeasurementId,
              )
              measurementJSONObject.put("is_direct", measurement.details.protocolConfig.hasDirect())
              measurementJSONObject.put("measurement_type", measurementTypeCase.name)
              measurementJSONObject.put("state", measurement.state.name)
              measurementJSONObject.put("create_time", measurement.createTime.toJson())
              measurementJSONObject.put("update_time", measurement.updateTime.toJson())
              measurementJSONObject.put(
                "update_time_minus_create_time",
                measurementUpdateTimeMinusCreateTime,
              )
              measurementJSONObject.put(
                "update_time_minus_create_time_squared",
                measurementUpdateTimeMinusCreateTime * measurementUpdateTimeMinusCreateTime,
              )

              measurementsJSONArray.put(measurementJSONObject)

              for (requisition in measurement.requisitionsList) {
                val requisitionUpdateTimeMinusCreateTime =
                  Timestamps.between(measurement.createTime, requisition.updateTime).seconds

                val requisitionJSONObject = JSONObject()
                requisitionJSONObject.put(
                  "external_measurement_id",
                  measurement.externalMeasurementId,
                )
                requisitionJSONObject.put(
                  "external_requisition_id",
                  requisition.externalRequisitionId,
                )
                requisitionJSONObject.put(
                  "external_data_provider_id",
                  requisition.externalDataProviderId,
                )
                requisitionJSONObject.put(
                  "is_direct",
                  measurement.details.protocolConfig.hasDirect(),
                )
                requisitionJSONObject.put("measurement_type", measurementTypeCase.name)
                requisitionJSONObject.put("state", requisition.state.name)
                requisitionJSONObject.put("create_time", measurement.createTime.toJson())
                requisitionJSONObject.put("update_time", requisition.updateTime.toJson())
                requisitionJSONObject.put(
                  "update_time_minus_create_time",
                  requisitionUpdateTimeMinusCreateTime,
                )
                requisitionJSONObject.put(
                  "update_time_minus_create_time_squared",
                  requisitionUpdateTimeMinusCreateTime * requisitionUpdateTimeMinusCreateTime,
                )

                requisitionsJSONArray.put(requisitionJSONObject)
              }

              if (measurement.externalComputationId != 0L) {
                for (computationParticipant in measurement.computationParticipantsList) {
                  val computationParticipantUpdateTimeMinusCreateTime =
                    Timestamps.between(measurement.createTime, computationParticipant.updateTime)
                      .seconds

                  val computationParticipantJSONObject = JSONObject()
                  computationParticipantJSONObject.put(
                    "external_measurement_id",
                    measurement.externalMeasurementId,
                  )
                  computationParticipantJSONObject.put(
                    "external_computation_id",
                    measurement.externalComputationId,
                  )
                  computationParticipantJSONObject.put(
                    "external_duchy_id",
                    computationParticipant.externalDuchyId,
                  )
                  computationParticipantJSONObject.put(
                    "protocol_config",
                    measurement.details.duchyProtocolConfig.protocolCase.name,
                  )
                  computationParticipantJSONObject.put("measurement_type", measurementTypeCase.name)
                  computationParticipantJSONObject.put("state", computationParticipant.state.name)
                  computationParticipantJSONObject.put(
                    "create_time",
                    measurement.createTime.toJson(),
                  )
                  computationParticipantJSONObject.put(
                    "update_time",
                    computationParticipant.updateTime.toJson(),
                  )
                  computationParticipantJSONObject.put(
                    "update_time_minus_create_time",
                    computationParticipantUpdateTimeMinusCreateTime,
                  )
                  computationParticipantJSONObject.put(
                    "update_time_minus_create_time_squared",
                    computationParticipantUpdateTimeMinusCreateTime *
                      computationParticipantUpdateTimeMinusCreateTime,
                  )

                  computationParticipantsJSONArray.put(computationParticipantJSONObject)
                }
              }
            }
        } catch (e: Exception) {
          logger.warning("Reading or processing Measurements failed.")
          logger.warning(e.message)
          break
        }

        val deferredResults: MutableList<Deferred<Unit>> = mutableListOf()
        deferredResults.add(measurementsDataWriter.appendRows(measurementsJSONArray))
        deferredResults.add(requisitionsDataWriter.appendRows(requisitionsJSONArray))
        if (!computationParticipantsJSONArray.isEmpty) {
          deferredResults.add(
            computationParticipantsDataWriter.appendRows(computationParticipantsJSONArray)
          )
        }
        deferredResults.awaitAll()

        val lastMeasurement: JSONObject =
          measurementsJSONArray.getJSONObject(measurementsJSONArray.length() - 1)
        latestMeasurementReadDataWriter.appendRows(JSONArray().put(lastMeasurement)).await()

        filter =
          filter.copy {
            after =
              after.copy {
                updateTime =
                  Timestamp.parseFrom(
                    lastMeasurement.get("update_time").toString().encodeToByteArray()
                  )
                measurement = measurementKey {
                  externalMeasurementConsumerId =
                    lastMeasurement.get("external_measurement_consumer_id").toString().toLong()
                  externalMeasurementId =
                    lastMeasurement.get("external_measurement_id").toString().toLong()
                }
              }
          }
      } while (measurementsQueryResponseSize == BATCH_SIZE)
    } finally {
      measurementsDataWriter.close()
      requisitionsDataWriter.close()
      computationParticipantsDataWriter.close()
    }
  }

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val BATCH_SIZE = 1000
  }

  private class JsonDataWriter(
    private val parent: TableName,
    private val client: BigQueryWriteClient,
  ) {
    private var streamWriter: JsonStreamWriter = createStreamWriter()
    private var recreateCount: Int = 0

    fun close() {
      streamWriter.close()
    }

    private fun createStreamWriter(): JsonStreamWriter {
      return JsonStreamWriter.newBuilder(parent.toString(), client)
        .setExecutorProvider(FixedExecutorProvider.create(Executors.newScheduledThreadPool(1)))
        .setChannelProvider(
          BigQueryWriteSettings.defaultGrpcTransportProviderBuilder()
            .setKeepAliveTime(Duration.ofMinutes(1))
            .setKeepAliveTimeout(Duration.ofMinutes(1))
            .setKeepAliveWithoutCalls(true)
            .build()
        )
        .setEnableConnectionPool(true)
        .setDefaultMissingValueInterpretation(
          AppendRowsRequest.MissingValueInterpretation.DEFAULT_VALUE
        )
        .setIgnoreUnknownFields(true)
        .setRetrySettings(
          RetrySettings.newBuilder()
            .setInitialRetryDelay(Duration.ofMillis(500))
            .setRetryDelayMultiplier(1.1)
            .setMaxAttempts(5)
            .setMaxRetryDelay(Duration.ofMinutes(1))
            .build()
        )
        .build()
    }

    /**
     * Writes data to the stream.
     *
     * @param jsonArray array of JSON objects representing the rows to write.
     * @returns ApiFuture containing the write response.
     * @throws IllegalStateException if append fails and is no longer retriable
     */
    suspend fun appendRows(jsonArray: JSONArray): Deferred<Unit> {
      return coroutineScope {
        async {
          for (i in 1..RETRY_COUNT) {
            if (
              !streamWriter.isUserClosed &&
                streamWriter.isClosed &&
                recreateCount < MAX_RECREATE_COUNT
            ) {
              streamWriter = createStreamWriter()
              recreateCount++
            } else {
              throw IllegalStateException("Stream writer has failed too many times.")
            }

            try {
              val response = streamWriter.append(jsonArray).await()
              if (response.hasError()) {
                if (response.error.code != Code.INTERNAL.number) {
                  throw IllegalStateException("Cannot retry append.")
                } else if (i == RETRY_COUNT) {
                  throw IllegalStateException("Too many retries.")
                }
              } else {
                break
              }
            } catch (e: Exception) {
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
    }
  }
}
