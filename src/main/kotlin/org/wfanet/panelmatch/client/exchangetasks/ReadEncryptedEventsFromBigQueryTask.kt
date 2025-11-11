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
import com.google.cloud.bigquery.FieldValueList
import com.google.cloud.bigquery.QueryJobConfiguration
import com.google.cloud.bigquery.Schema
import com.google.cloud.bigquery.StandardSQLTypeName.BYTES
import com.google.protobuf.ByteString
import java.time.LocalDate
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.withContext
import org.wfanet.measurement.storage.StorageClient
import org.wfanet.panelmatch.client.authorizedview.BigQueryServiceFactory
import org.wfanet.panelmatch.client.authorizedview.EncryptedMatchedEvent
import org.wfanet.panelmatch.client.authorizedview.encryptedMatchedEvent
import org.wfanet.panelmatch.common.loggerFor
import org.wfanet.panelmatch.common.toDelimitedByteString

/**
 * Specialized BigQuery reader for encrypted events from authorized views.
 *
 * This task reads from BigQuery authorized views and streams individual EncryptedMatchedEvent
 * protos using length-delimited encoding (not batched). It expects at least two columns (with
 * configurable names):
 * - Encrypted join key column (BYTES): Base64-encoded EDP-encrypted join key
 * - Encrypted event data column (BYTES): AES-GCM encrypted event data
 * - Exchange Date (DATE): Date of the current exchange The output is a stream of length-delimited
 *   EncryptedMatchedEvent protos, matching the pattern used in the private membership workflow for
 *   efficient streaming processing.
 */
class ReadEncryptedEventsFromBigQueryTask(
  private val projectId: String,
  private val datasetId: String,
  private val tableOrViewId: String,
  private val exchangeDate: LocalDate,
  private val bigQueryServiceFactory: BigQueryServiceFactory,
  private val keyColumnName: String = DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN_NAME,
  private val eventDataColumnName: String = DEFAULT_ENCRYPTED_EVENT_DATA_COLUMN_NAME,
  private val dateColumnName: String = DEFAULT_DATE_COLUMN_NAME,
) : ExchangeTask {

  private val bigQuery: BigQuery by lazy {
    logger.info("Getting BigQuery service for project $projectId from factory")
    bigQueryServiceFactory.getService(projectId)
  }

  override suspend fun execute(
    input: Map<String, StorageClient.Blob>
  ): Map<String, Flow<ByteString>> =
    withContext(Dispatchers.IO) {
      logger.info(
        "Starting ReadEncryptedEventsFromBigQueryTask for $projectId.$datasetId.$tableOrViewId"
      )

      val protoFlow = streamAsProto()
      mapOf(OUTPUT_LABEL to protoFlow)
    }

  override fun skipReadInput(): Boolean = true

  /**
   * Streams BigQuery data as individual length-delimited EncryptedMatchedEvent proto messages. Each
   * event is streamed immediately without batching for lower latency and memory usage.
   */
  private fun streamAsProto(): Flow<ByteString> = flow {
    try {
      val fullTableName = "`$projectId.$datasetId.$tableOrViewId`"
      val query = buildString {
        append("SELECT ")
        append("`$keyColumnName`, ")
        append("`$eventDataColumnName` ")
        append("FROM $fullTableName")
        append(" WHERE `$dateColumnName` = DATE('$exchangeDate')")
      }

      val queryConfig = QueryJobConfiguration.newBuilder(query).setUseLegacySql(false).build()

      val tableResult = bigQuery.query(queryConfig)

      // Validate schema has expected columns
      val schema = requireNotNull(tableResult.getSchema()) { "No schema returned from BigQuery" }

      try {
        validateSchema(schema)
      } catch (e: Exception) {
        logger.severe("Schema validation failed: ${e.message}")
        return@flow  // Return empty flow on validation failure
      }

      generateSequence(tableResult) { page -> if (page.hasNextPage()) page.getNextPage() else null }
        .flatMap { page -> page.values.asSequence() }
        .mapNotNull { row ->
          try {
            parseRowToEncryptedMatchedEvent(row)
          } catch (e: Exception) {
            logger.warning("Failed to parse row: ${e.message}")
            null  // Skip rows that fail to parse
          }
        }
        .forEach { event -> emit(event.toDelimitedByteString()) }
      logger.info(
        "Completed ReadEncryptedEventsFromBigQueryTask for $projectId.$datasetId.$tableOrViewId"
      )
    } catch (e: Exception) {
      logger.severe(
        "ReadEncryptedEventsFromBigQueryTask failed for $projectId.$datasetId.$tableOrViewId: ${e.message}\n${e.stackTraceToString()}"
      )
    }
  }

  /** Validates that the BigQuery schema has the expected columns. */
  private fun validateSchema(schema: Schema) {
    val fieldsByName = schema.fields.associateBy { it.name }

    // Check existence and type of encrypted join key column
    val joinKeyField =
      requireNotNull(fieldsByName[keyColumnName]) {
        "Missing required column '$keyColumnName' in BigQuery table/view"
      }

    require(joinKeyField.type.standardType == BYTES) {
      "Column '$keyColumnName' must be BYTES type, but was ${joinKeyField.type.standardType}"
    }

    // Check existence and type of encrypted event data column
    val eventDataField =
      requireNotNull(fieldsByName[eventDataColumnName]) {
        "Missing required column '$eventDataColumnName' in BigQuery table/view"
      }

    require(eventDataField.type.standardType == BYTES) {
      "Column '$eventDataColumnName' must be BYTES type, but was ${eventDataField.type.standardType}"
    }
  }

  /**
   * Parses a BigQuery row into an EncryptedMatchedEvent proto. Converts BigQuery's base64 string
   * columns to raw bytes for consistency.
   */
  private fun parseRowToEncryptedMatchedEvent(row: FieldValueList): EncryptedMatchedEvent {
    // Get encrypted join key (BYTES column)
    val encryptedJoinKeyBytes =
      requireNotNull(row.get(keyColumnName).getBytesValue()) {
        "Null value found in column '$keyColumnName'"
      }

    // Get encrypted event data (BYTES column)
    val encryptedEventData =
      requireNotNull(row.get(eventDataColumnName).getBytesValue()) {
        "Null value found in column '$eventDataColumnName'"
      }

    return encryptedMatchedEvent {
      this.encryptedJoinKey = ByteString.copyFrom(encryptedJoinKeyBytes)
      this.encryptedEventData = ByteString.copyFrom(encryptedEventData)
    }
  }

  companion object {
    private val logger by loggerFor()
    private const val OUTPUT_LABEL = "encrypted-events"
    private const val MAX_RETRIES = 3

    // Default column names (can be overridden via constructor)
    const val DEFAULT_ENCRYPTED_JOIN_KEY_COLUMN_NAME = "encrypted_join_key"
    const val DEFAULT_ENCRYPTED_EVENT_DATA_COLUMN_NAME = "encrypted_event_data"
    const val DEFAULT_DATE_COLUMN_NAME = "exchange_date"
  }
}
