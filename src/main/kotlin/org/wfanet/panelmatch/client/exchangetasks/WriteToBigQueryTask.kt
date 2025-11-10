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
import com.google.type.Date
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.Base64
import java.util.UUID
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.BigQueryServiceFactory
import org.wfanet.panelmatch.client.authorizedview.BigQueryStreamingStatus
import org.wfanet.panelmatch.client.authorizedview.BigQueryStreamingStatus.DataType
import org.wfanet.panelmatch.client.authorizedview.bigQueryStreamingStatus
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.toByteString

// Top-level private constants
private const val INPUT_LABEL = "input"
private const val OUTPUT_LABEL = "status"

// Batching configuration - optimized for < 1M events
private const val BATCH_SIZE = 5000

/**
 * Unified BigQuery streaming task that writes either JoinKeyAndId or EncryptedMatchedEvent protos
 * to BigQuery using the streaming API.
 *
 * This class follows the pattern from [DeterministicCommutativeCipherTask], using a process
 * function parameter to handle different input types while sharing all common BigQuery streaming
 * logic.
 *
 * The task relies on BigQuery's built-in features for reliability:
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
  private val dataColumnName: String? = null, // null for keys, present for events
  private val dateColumnName: String,
  private val bigQueryServiceFactory: BigQueryServiceFactory,
  private val process: suspend WriteToBigQueryTask.(StorageClient.Blob) -> BigQueryStreamingStatus,
) : ExchangeTask {

  /** Lazily initialized BigQuery TableId. */
  private val bigQueryTableId: TableId by lazy { TableId.of(projectId, datasetId, tableId) }

  /** Lazily initialized BigQuery service instance. */
  private val bigquery: BigQuery by lazy { bigQueryServiceFactory.getService(projectId) }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    withContext(Dispatchers.IO) {
      logger.info("Starting BigQueryStreamingTask for $projectId.$datasetId.$tableId")

      val inputBlob = requireNotNull(input[INPUT_LABEL]) { "Missing required input: $INPUT_LABEL" }

      val statusProto = process(inputBlob)

      // Log a human-readable summary
      logger.info(
        "BigQuery Streaming Complete: ${statusProto.status}, " +
          "Rows written: ${statusProto.statistics.rowsWritten}, " +
          "Rows failed: ${statusProto.statistics.rowsFailed}"
      )

      // Return the proto as the output
      mapOf(OUTPUT_LABEL to flowOf(statusProto.toByteString()))
    }

  /** Process a JoinKeyAndIdCollection proto and write keys to BigQuery in batches. */
  private suspend fun processJoinKeys(blob: StorageClient.Blob): BigQueryStreamingStatus {
    val stats = StreamingStats()

    // Read the entire blob as a JoinKeyAndIdCollection proto
    val collection = JoinKeyAndIdCollection.parseFrom(blob.toByteString().toByteArray())

    require(collection.joinKeyAndIdsList.isNotEmpty()) {
      "JoinKeyAndIdCollection must not be empty"
    }

    // Validate that all keys and identifiers are distinct
    val joinKeys = collection.joinKeyAndIdsList.map { it.joinKey.key }
    require(joinKeys.toSet().size == joinKeys.size) { "JoinKeys are not distinct" }

    val joinKeyIds = collection.joinKeyAndIdsList.map { it.joinKeyIdentifier.id }
    require(joinKeyIds.toSet().size == joinKeyIds.size) { "JoinKeyIdentifiers are not distinct" }

    logger.info("Processing ${collection.joinKeyAndIdsList.size} keys from JoinKeyAndIdCollection")

    val currentBatch = mutableListOf<Pair<String, Map<String, Any>>>()

    // Iterate through all keys in the collection
    for (joinKeyAndId in collection.joinKeyAndIdsList) {
      // Convert JoinKeyAndId to BigQuery row
      val row =
        mapOf(
          keyColumnName to
            Base64.getEncoder().encodeToString(joinKeyAndId.joinKey.key.toByteArray()),
          dateColumnName to exchangeDate.format(DateTimeFormatter.ISO_DATE),
        )

      val insertId = generateInsertId(row)
      currentBatch.add(insertId to row)

      // Check if batch is full
      if (currentBatch.size >= BATCH_SIZE) {
        streamBatch(currentBatch, stats)
        currentBatch.clear()
      }
    }

    // Flush any remaining rows
    if (currentBatch.isNotEmpty()) {
      streamBatch(currentBatch, stats)
    }

    // Build and return the final status proto
    return stats.toStatusProto(bigQueryTableId, exchangeDate, DataType.JOIN_KEYS)
  }

  /** Process a stream of EncryptedMatchedEvent protos and write them to BigQuery. */
  private suspend fun processEncryptedEvents(blob: StorageClient.Blob): BigQueryStreamingStatus {
    val stats = StreamingStats()

    // Read the entire blob and parse delimited EncryptedMatchedEvent protos
    val encryptedEvents = blob.toByteString().parseDelimitedMessages(encryptedMatchedEvent {})

    val currentBatch = mutableListOf<Pair<String, Map<String, Any>>>()
    var eventCount = 0

    for (encryptedEvent in encryptedEvents) {
      // Validate event has required fields
      require(encryptedEvent.encryptedJoinKey != ByteString.EMPTY) {
        "Encrypted join key must not be empty"
      }
      require(encryptedEvent.encryptedEventData != ByteString.EMPTY) {
        "Encrypted event data must not be empty"
      }

      eventCount++
      // Convert EncryptedMatchedEvent to BigQuery row
      val row =
        mapOf(
          keyColumnName to
            Base64.getEncoder().encodeToString(encryptedEvent.encryptedJoinKey.toByteArray()),
          dataColumnName!! to
            Base64.getEncoder().encodeToString(encryptedEvent.encryptedEventData.toByteArray()),
          dateColumnName to exchangeDate.format(DateTimeFormatter.ISO_DATE),
        )

      val insertId = generateInsertId(row)
      currentBatch.add(insertId to row)

      // Check if batch is full
      if (currentBatch.size >= BATCH_SIZE) {
        streamBatch(currentBatch, stats)
        currentBatch.clear()
      }
    }

    // Flush any remaining rows
    if (currentBatch.isNotEmpty()) {
      streamBatch(currentBatch, stats)
    }

    // Validate we processed at least one event
    require(eventCount > 0) { "No events found in input stream" }

    logger.info("Processed $eventCount encrypted events")

    // Build and return the final status proto
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

      // Log individual errors for debugging
      response.insertErrors.forEach { (index, errorList) ->
        logger.fine("Row at index $index failed: $errorList")
      }
    } else {
      stats.successCount += batch.size
      logger.fine("Successfully inserted ${batch.size} rows")
    }
    stats.batchCount++
  }

  /**
   * Generate a unique insert ID for deduplication. BigQuery uses this to prevent duplicate inserts.
   */
  private fun generateInsertId(row: Map<String, Any>): String {
    val contentHash = row.hashCode()
    return "${System.currentTimeMillis()}_${contentHash}_${UUID.randomUUID()}"
  }

  companion object {
    private val logger by loggerFor()

    const val DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN = "encrypted_join_key"
    const val DEFAULT_ENCRYPTED_EVENT_DATA_COLUMN = "encrypted_event_data"
    const val DEFAULT_ENCRYPTED_EXCHANGE_DATE_COLUMN = "exchange_date"

    /** Returns an [ExchangeTask] that writes JoinKeyAndIdCollection to BigQuery. */
    @JvmStatic
    fun forJoinKeys(
      projectId: String,
      datasetId: String,
      tableId: String,
      exchangeDate: LocalDate,
      bigQueryServiceFactory: BigQueryServiceFactory,
      keyColumnName: String = "encrypted_join_key",
      dateColumnName: String = "exchange_date",
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
      keyColumnName: String = "encrypted_join_key",
      dataColumnName: String = "encrypted_event_data",
      dateColumnName: String = "exchange_date",
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
  }
}

/** Convert LocalDate to proto Date. */
private fun LocalDate.toProtoDate(): Date {
  return Date.newBuilder()
    .apply {
      year = this@toProtoDate.year
      month = this@toProtoDate.monthValue
      day = this@toProtoDate.dayOfMonth
    }
    .build()
}

/** Convert TableId to proto TableInfo. */
private fun TableId.toProtoTableInfo(): BigQueryStreamingStatus.TableInfo {
  return BigQueryStreamingStatus.TableInfo.newBuilder()
    .apply {
      projectId = this@toProtoTableInfo.project
      datasetId = this@toProtoTableInfo.dataset
      tableId = this@toProtoTableInfo.table
    }
    .build()
}

/** Internal data class for tracking streaming statistics. */
internal data class StreamingStats(
  var successCount: Int = 0,
  var failureCount: Int = 0,
  var batchCount: Int = 0,
) {
  /** Convert to proto StreamingStatistics. */
  fun toProto(): BigQueryStreamingStatus.StreamingStatistics {
    return BigQueryStreamingStatus.StreamingStatistics.newBuilder()
      .apply {
        rowsWritten = this@StreamingStats.successCount.toLong()
        rowsFailed = this@StreamingStats.failureCount.toLong()
        batchesSent = this@StreamingStats.batchCount
      }
      .build()
  }

  /** Determine the overall status based on statistics. */
  fun toStatus(): BigQueryStreamingStatus.Status {
    return when {
      failureCount == 0 && successCount > 0 -> BigQueryStreamingStatus.Status.SUCCESS
      failureCount > 0 && successCount > 0 -> BigQueryStreamingStatus.Status.PARTIAL_SUCCESS
      successCount == 0 && failureCount > 0 -> BigQueryStreamingStatus.Status.FAILURE
      else -> BigQueryStreamingStatus.Status.STATUS_UNSPECIFIED
    }
  }

  /** Generate error message if there were failures. */
  fun toErrorMessage(): String? {
    return if (failureCount > 0) {
      "Failed to stream $failureCount rows out of ${successCount + failureCount} total rows"
    } else null
  }

  /** Convert to BigQueryStreamingStatus proto with the given data type. */
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
}
