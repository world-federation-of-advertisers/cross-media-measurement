// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.panelmatch.client.exchangetasks

import com.google.cloud.bigquery.BigQuery
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableId
import com.google.protobuf.ByteString
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import org.wfanet.measurement.common.toProtoDate
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.BigQueryServiceFactory
import org.wfanet.panelmatch.client.authorizedview.BigQueryStreamingStatus
import org.wfanet.panelmatch.client.authorizedview.BigQueryStreamingStatus.DataType
import org.wfanet.panelmatch.client.authorizedview.bigQueryStreamingStatus
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.common.Fingerprinters.sha256
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.toByteString
import org.wfanet.panelmatch.common.toBase64

/**
 * Unified BigQuery streaming task that writes either JoinKeyAndId or EncryptedMatchedEvent protos
 * to BigQuery using the streaming API. The task relies on BigQuery's built-in features for
 * reliability:
 * - Insert IDs for deduplication
 * - BigQuery client library's automatic retry for transient errors
 * - Small batch sizes (5000 rows) to minimize impact of failures
 *
 * Use the factory methods [forJoinKeys] or [forEncryptedEvents] to create instances.
 */
class WriteToBigQueryTask
internal constructor(
  private val projectId: String,
  private val datasetId: String,
  private val tableId: String,
  private val exchangeDate: LocalDate,
  private val keyColumnName: String,
  private val dataColumnName: String? = null,
  private val dateColumnName: String,
  private val bigQueryServiceFactory: BigQueryServiceFactory,
  private val process: suspend WriteToBigQueryTask.(StorageClient.Blob) -> BigQueryStreamingStatus,
) : ExchangeTask {

  private val bigQueryTableId: TableId by lazy { TableId.of(projectId, datasetId, tableId) }

  private val bigquery: BigQuery by lazy { bigQueryServiceFactory.getService(projectId) }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> {
    logger.info("Starting WriteToBigQueryTask for $projectId.$datasetId.$tableId")

    val inputBlob = requireNotNull(input[INPUT_LABEL]) { "Missing required input: $INPUT_LABEL" }

    val statusProto = process(inputBlob)

    logger.info(
      "BigQuery Streaming Complete: ${statusProto.status}, " +
        "Rows written: ${statusProto.statistics.rowsWritten}, " +
        "Rows failed: ${statusProto.statistics.rowsFailed}"
    )

    if (statusProto.statistics.rowsFailed > 0) {
      throw IllegalStateException(
        "Failed to stream ${statusProto.statistics.rowsFailed} rows " +
          "out of ${statusProto.statistics.rowsWritten + statusProto.statistics.rowsFailed} total"
      )
    }

    return mapOf(OUTPUT_LABEL to flowOf(statusProto.toByteString()))
  }

  /**
   * Process a JoinKeyAndIdCollection proto and write keys to BigQuery in batches.
   *
   * For the authorized view workflow, this hashes the G(Key) values to H(G(Key)) before writing to
   * BigQuery. This ensures the MP join keys match the format used by EDP in PreprocessEventsTask.
   */
  private suspend fun processJoinKeys(blob: StorageClient.Blob): BigQueryStreamingStatus {
    val stats = StreamingStats()
    val collection = JoinKeyAndIdCollection.parseFrom(blob.toByteString())

    val validJoinKeyAndIds =
      collection.joinKeyAndIdsList.filter {
        it.joinKey.key != ByteString.EMPTY && it.joinKeyIdentifier.id != ByteString.EMPTY
      }

    if (validJoinKeyAndIds.isEmpty()) {
      logger.warning("JoinKeyAndIdCollection is empty after filtering.")
      return stats.toStatusProto(bigQueryTableId, exchangeDate, DataType.JOIN_KEYS)
    }

    val joinKeys = validJoinKeyAndIds.map { it.joinKey.key }
    require(joinKeys.toSet().size == joinKeys.size) { "JoinKeys are not distinct after filtering" }

    logger.info("Processing ${validJoinKeyAndIds.size} valid keys from JoinKeyAndIdCollection")

    for (batch in validJoinKeyAndIds.chunked(BATCH_SIZE)) {
      val bigQueryRows =
        batch.map { joinKeyAndId ->
          val key = sha256(joinKeyAndId.joinKey.key).toBase64()
          val row = mapOf(keyColumnName to key, dateColumnName to exchangeDate.toIsoDateString())
          key to row
        }
      streamBatch(bigQueryRows, stats)
    }

    logger.info("Successfully hashed and wrote join keys as H(G(Key)) for authorized view")
    return stats.toStatusProto(bigQueryTableId, exchangeDate, DataType.JOIN_KEYS)
  }

  /** Process a stream of EncryptedMatchedEvent protos and write them to BigQuery. */
  private suspend fun processEncryptedEvents(blob: StorageClient.Blob): BigQueryStreamingStatus {
    requireNotNull(dataColumnName) { "dataColumnName must be set for encrypted events" }

    val stats = StreamingStats()

    // Parsing and filtering are lazy; blob is already in memory
    val encryptedEventsSequence =
      blob.toByteString().parseDelimitedMessages(encryptedMatchedEvent {}).asSequence().filter {
        event ->
        event.encryptedJoinKey != ByteString.EMPTY && event.encryptedEventData != ByteString.EMPTY
      }

    var eventCount = 0
    for (batch in encryptedEventsSequence.chunked(BATCH_SIZE)) {
      eventCount += batch.size
      val bigQueryRows =
        batch.map { encryptedEvent ->
          val key = encryptedEvent.encryptedJoinKey.toBase64()
          val row =
            mapOf(
              keyColumnName to key,
              dataColumnName to encryptedEvent.encryptedEventData.toBase64(),
              dateColumnName to exchangeDate.toIsoDateString(),
            )
          key to row
        }
      streamBatch(bigQueryRows, stats)
    }

    if (eventCount == 0) {
      logger.warning("No valid encrypted events found in input blob after filtering.")
    } else {
      logger.info("Processed $eventCount valid encrypted events")
    }
    return stats.toStatusProto(bigQueryTableId, exchangeDate, DataType.ENCRYPTED_EVENTS)
  }

  /**
   * Stream a batch of rows to BigQuery.
   *
   * Relies on BigQuery's built-in retry mechanism and insert ID deduplication. Partial failures are
   * tracked in stats but not retried manually.
   */
  private suspend fun streamBatch(
    batch: List<Pair<String, Map<String, Any>>>,
    stats: StreamingStats,
  ) {
    val request =
      InsertAllRequest.newBuilder(bigQueryTableId)
        .apply { batch.forEach { (insertId, row) -> addRow(insertId, row) } }
        .build()

    val response = bigquery.insertAll(request)

    if (response.hasErrors()) {
      val errorCount = response.insertErrors.size
      stats.failureCount += errorCount
      stats.successCount += batch.size - errorCount
      logger.warning("$errorCount rows failed in batch of ${batch.size}")
      response.insertErrors.forEach { (index, errorList) ->
        logger.fine("Row at index $index failed: $errorList")
      }
    } else {
      stats.successCount += batch.size
      logger.fine("Successfully inserted ${batch.size} rows")
    }
    stats.batchCount++
  }

  companion object {
    private const val INPUT_LABEL = "input"
    private const val OUTPUT_LABEL = "status"

    private const val BATCH_SIZE = 5000

    private val logger by loggerFor()

    const val DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN_NAME = "encrypted_join_key"
    const val DEFAULT_ENCRYPTED_EVENT_DATA_COLUMN_NAME = "encrypted_event_data"
    const val DEFAULT_ENCRYPTED_EXCHANGE_DATE_COLUMN_NAME = "exchange_date"

    /** Returns an [ExchangeTask] that writes JoinKeyAndIdCollection to BigQuery. */
    @JvmStatic
    fun forJoinKeys(
      projectId: String,
      datasetId: String,
      tableId: String,
      exchangeDate: LocalDate,
      bigQueryServiceFactory: BigQueryServiceFactory,
      keyColumnName: String = DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN_NAME,
      dateColumnName: String = DEFAULT_ENCRYPTED_EXCHANGE_DATE_COLUMN_NAME,
    ): ExchangeTask {
      return WriteToBigQueryTask(
        projectId = projectId,
        datasetId = datasetId,
        tableId = tableId,
        exchangeDate = exchangeDate,
        keyColumnName = keyColumnName,
        dataColumnName = null,
        dateColumnName = dateColumnName,
        bigQueryServiceFactory = bigQueryServiceFactory,
        process = WriteToBigQueryTask::processJoinKeys,
      )
    }

    /** Returns an [ExchangeTask] that writes EncryptedMatchedEvents to BigQuery. */
    @JvmStatic
    fun forEncryptedEvents(
      projectId: String,
      datasetId: String,
      tableId: String,
      exchangeDate: LocalDate,
      bigQueryServiceFactory: BigQueryServiceFactory,
      keyColumnName: String = DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN_NAME,
      dataColumnName: String = DEFAULT_ENCRYPTED_EVENT_DATA_COLUMN_NAME,
      dateColumnName: String = DEFAULT_ENCRYPTED_EXCHANGE_DATE_COLUMN_NAME,
    ): ExchangeTask {
      return WriteToBigQueryTask(
        projectId = projectId,
        datasetId = datasetId,
        tableId = tableId,
        exchangeDate = exchangeDate,
        keyColumnName = keyColumnName,
        dataColumnName = dataColumnName,
        dateColumnName = dateColumnName,
        bigQueryServiceFactory = bigQueryServiceFactory,
        process = WriteToBigQueryTask::processEncryptedEvents,
      )
    }

    private fun LocalDate.toIsoDateString(): String {
      return this.format(DateTimeFormatter.ISO_DATE)
    }
  }

  private data class StreamingStats(
    var successCount: Int = 0,
    var failureCount: Int = 0,
    var batchCount: Int = 0,
  ) {
    fun toProto(): BigQueryStreamingStatus.StreamingStatistics {
      return BigQueryStreamingStatus.StreamingStatistics.newBuilder()
        .apply {
          rowsWritten = this@StreamingStats.successCount.toLong()
          rowsFailed = this@StreamingStats.failureCount.toLong()
          batchesSent = this@StreamingStats.batchCount
        }
        .build()
    }

    fun toStatus(): BigQueryStreamingStatus.Status {
      return when {
        failureCount == 0 && successCount > 0 -> BigQueryStreamingStatus.Status.SUCCESS
        failureCount > 0 && successCount > 0 -> BigQueryStreamingStatus.Status.PARTIAL_SUCCESS
        successCount == 0 && failureCount > 0 -> BigQueryStreamingStatus.Status.FAILURE
        else -> BigQueryStreamingStatus.Status.STATUS_UNSPECIFIED
      }
    }

    fun toErrorMessage(): String? {
      return if (failureCount > 0) {
        "Failed to stream $failureCount rows out of ${successCount + failureCount} total rows"
      } else null
    }

    fun toStatusProto(
      tableId: TableId,
      exchangeDate: LocalDate,
      dataType: DataType,
    ): BigQueryStreamingStatus {
      return bigQueryStreamingStatus {
        tableInfo = tableId.toProtoTableInfo()
        this.exchangeDate = exchangeDate.toProtoDate()
        this.dataType = dataType
        statistics = this@StreamingStats.toProto()
        status = this@StreamingStats.toStatus()
        this@StreamingStats.toErrorMessage()?.let { errorMessage = it }
      }
    }

    companion object {
      private fun TableId.toProtoTableInfo(): BigQueryStreamingStatus.TableInfo {
        return BigQueryStreamingStatus.TableInfo.newBuilder()
          .apply {
            projectId = this@toProtoTableInfo.project
            datasetId = this@toProtoTableInfo.dataset
            tableId = this@toProtoTableInfo.table
          }
          .build()
      }
    }
  }
}
