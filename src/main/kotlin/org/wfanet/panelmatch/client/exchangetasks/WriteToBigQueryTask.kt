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
import com.google.cloud.bigquery.BigQueryError
import com.google.cloud.bigquery.BigQueryOptions
import com.google.cloud.bigquery.InsertAllRequest
import com.google.cloud.bigquery.TableId
import com.google.protobuf.ByteString
import java.util.Base64
import java.util.UUID
import java.util.logging.Logger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.delay
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.common.parseDelimitedMessages
import org.wfanet.panelmatch.common.storage.newInputStream
import org.wfanet.panelmatch.common.storage.toByteString

/** Statistics for streaming operation. */
internal data class StreamingStats(
  var successCount: Int = 0,
  var failureCount: Int = 0,
  var batchCount: Int = 0,
)

/**
 * Unified BigQuery streaming task that writes either JoinKeyAndId or EncryptedMatchedEvent protos
 * to BigQuery using the streaming API.
 *
 * This class follows the pattern from [DeterministicCommutativeCipherTask], using a process
 * function parameter to handle different input types while sharing all common BigQuery streaming
 * logic.
 *
 * Use the factory methods [forJoinKeys] or [forEncryptedEvents] to create instances.
 */
class WriteToBigQueryTask
internal constructor(
  private val projectId: String,
  private val datasetId: String,
  private val tableId: String,
  private val exchangeDate: String,
  private val keyColumnName: String,
  private val dataColumnName: String? = null, // null for keys, present for events
  private val dateColumnName: String,
  private val process: suspend WriteToBigQueryTask.(StorageClient.Blob) -> StreamingStats,
) : ExchangeTask {

  companion object {
    private val logger: Logger = Logger.getLogger(this::class.java.name)

    private const val INPUT_LABEL = "input"
    private const val OUTPUT_LABEL = "status"

    // Batching configuration - optimized for < 1M events
    private const val BATCH_SIZE = 5000

    // Retry configuration
    private const val MAX_RETRY_ATTEMPTS = 3
    private const val INITIAL_BACKOFF_MS = 100L
    private const val MAX_BACKOFF_MS = 32_000L

    /** Returns an [ExchangeTask] that writes JoinKeyAndIdCollection to BigQuery. */
    @JvmStatic
    fun forJoinKeys(
      projectId: String,
      datasetId: String,
      tableId: String,
      exchangeDate: String,
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
        process = WriteToBigQueryTask::processJoinKeys,
      )
    }

    /** Returns an [ExchangeTask] that writes EncryptedMatchedEvents to BigQuery. */
    @JvmStatic
    fun forEncryptedEvents(
      projectId: String,
      datasetId: String,
      tableId: String,
      exchangeDate: String,
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
        process = WriteToBigQueryTask::processEncryptedEvents,
      )
    }
  }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    withContext(Dispatchers.IO) {
      logger.info("Starting BigQueryStreamingTask for $projectId.$datasetId.$tableId")

      val inputBlob = requireNotNull(input[INPUT_LABEL]) { "Missing required input: $INPUT_LABEL" }

      val stats = process(inputBlob)

      val statusMessage = buildStatusMessage(stats)
      logger.info(statusMessage)
      mapOf(OUTPUT_LABEL to flowOf(ByteString.copyFromUtf8(statusMessage)))
    }

  /** Process a JoinKeyAndIdCollection proto and write keys to BigQuery in batches. */
  private suspend fun processJoinKeys(blob: StorageClient.Blob): StreamingStats {
    val stats = StreamingStats()
    val bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().service
    val tableIdObj = TableId.of(projectId, datasetId, tableId)

    withContext(Dispatchers.IO) {
      // Read the entire blob as a JoinKeyAndIdCollection proto
      val collectionBytes = blob.toByteString()
      val collection = JoinKeyAndIdCollection.parseFrom(collectionBytes.toByteArray())

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
            dateColumnName to exchangeDate,
          )

        val insertId = generateInsertId(row)
        currentBatch.add(insertId to row)

        // Check if batch is full
        if (currentBatch.size >= BATCH_SIZE) {
          streamBatch(bigquery, tableIdObj, currentBatch, stats)
          currentBatch.clear()
        }
      }

      // Flush any remaining rows
      if (currentBatch.isNotEmpty()) {
        streamBatch(bigquery, tableIdObj, currentBatch, stats)
      }
    }

    return stats
  }

  /** Process a stream of EncryptedMatchedEvent protos and write them to BigQuery. */
  private suspend fun processEncryptedEvents(blob: StorageClient.Blob): StreamingStats {
    val stats = StreamingStats()
    val bigquery = BigQueryOptions.newBuilder().setProjectId(projectId).build().service
    val tableIdObj = TableId.of(projectId, datasetId, tableId)

    withContext(Dispatchers.IO) {
      blob.newInputStream(this).use { inputStream ->
        // Parse stream of length-delimited EncryptedMatchedEvent protos
        val prototype = encryptedMatchedEvent {}
        val protoStream = inputStream.parseDelimitedMessages(prototype)

        val currentBatch = mutableListOf<Pair<String, Map<String, Any>>>()
        var eventCount = 0

        for (encryptedEvent in protoStream) {
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
              dateColumnName to exchangeDate,
            )

          val insertId = generateInsertId(row)
          currentBatch.add(insertId to row)

          // Check if batch is full
          if (currentBatch.size >= BATCH_SIZE) {
            streamBatch(bigquery, tableIdObj, currentBatch, stats)
            currentBatch.clear()
          }
        }

        // Flush any remaining rows
        if (currentBatch.isNotEmpty()) {
          streamBatch(bigquery, tableIdObj, currentBatch, stats)
        }

        // Validate we processed at least one event
        require(eventCount > 0) { "No events found in input stream" }

        logger.info("Processed $eventCount encrypted events")
      }
    }

    return stats
  }

  /** Stream a batch of rows to BigQuery with retry logic. */
  private suspend fun streamBatch(
    bigquery: BigQuery,
    tableId: TableId,
    batch: List<Pair<String, Map<String, Any>>>,
    stats: StreamingStats,
  ) {
    var attempt = 0
    var backoffMs = INITIAL_BACKOFF_MS
    var remainingRows = batch

    while (attempt < MAX_RETRY_ATTEMPTS && remainingRows.isNotEmpty()) {
      attempt++

      try {
        // Build insert request
        val requestBuilder = InsertAllRequest.newBuilder(tableId)
        remainingRows.forEach { (insertId, row) -> requestBuilder.addRow(insertId, row) }

        // Execute streaming insert
        val response = bigquery.insertAll(requestBuilder.build())

        if (!response.hasErrors()) {
          // Success - all rows inserted
          stats.successCount += remainingRows.size
          stats.batchCount++
          logger.fine("Streamed batch of ${remainingRows.size} rows successfully")
          return
        }

        // Handle partial failures
        val errors = response.insertErrors
        val retryableRows = mutableListOf<Pair<String, Map<String, Any>>>()

        errors.forEach { (index, errorList) ->
          val row = remainingRows[index.toInt()]
          if (isRetryable(errorList)) {
            retryableRows.add(row)
          } else {
            logger.warning("Permanent failure for row: $errorList")
            stats.failureCount++
          }
        }

        // Successfully inserted rows
        stats.successCount += remainingRows.size - errors.size

        if (retryableRows.isEmpty()) {
          stats.batchCount++
          return // No retryable errors
        }

        // Prepare for retry
        remainingRows = retryableRows
        if (attempt < MAX_RETRY_ATTEMPTS) {
          logger.info("Retrying ${retryableRows.size} rows after ${backoffMs}ms")
          delay(backoffMs)
          backoffMs = kotlin.math.min(backoffMs * 2, MAX_BACKOFF_MS)
        }
      } catch (e: Exception) {
        logger.severe("Error streaming batch: ${e.message}")
        if (attempt >= MAX_RETRY_ATTEMPTS) {
          stats.failureCount += remainingRows.size
          throw e
        }
        delay(backoffMs)
        backoffMs = kotlin.math.min(backoffMs * 2, MAX_BACKOFF_MS)
      }
    }

    // If we get here, we've exhausted retries
    if (remainingRows.isNotEmpty()) {
      stats.failureCount += remainingRows.size
      logger.severe(
        "Failed to insert ${remainingRows.size} rows after $MAX_RETRY_ATTEMPTS attempts"
      )
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

  /**
   * Determine if errors are retryable. See: https://cloud.google.com/bigquery/docs/error-messages
   */
  private fun isRetryable(errors: List<BigQueryError>): Boolean {
    return errors.any { error ->
      error.reason in
        setOf("backendError", "rateLimitExceeded", "internalError", "timeout", "quotaExceeded")
    }
  }

  /** Build status message from streaming statistics. */
  private fun buildStatusMessage(stats: StreamingStats): String {
    val tableType = if (dataColumnName == null) "Keys" else "Events"
    return buildString {
      appendLine("BigQuery Streaming Complete for $tableType Table")
      appendLine("Project: $projectId")
      appendLine("Dataset: $datasetId")
      appendLine("Table: $tableId")
      appendLine("Exchange Date: $exchangeDate")
      appendLine("Rows written: ${stats.successCount}")
      appendLine("Rows failed: ${stats.failureCount}")
      appendLine("Batches sent: ${stats.batchCount}")
      appendLine("Status: ${if (stats.failureCount == 0) "SUCCESS" else "PARTIAL_SUCCESS"}")
    }
  }
}