/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.privacybudgetmanager.deploy.postgres

import java.sql.Connection
import java.sql.Date
import java.sql.Statement
import java.sql.Timestamp
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.privacybudgetmanager.Charges
import org.wfanet.measurement.privacybudgetmanager.Ledger
import org.wfanet.measurement.privacybudgetmanager.LedgerException
import org.wfanet.measurement.privacybudgetmanager.LedgerExceptionType
import org.wfanet.measurement.privacybudgetmanager.LedgerRowKey
import org.wfanet.measurement.privacybudgetmanager.Query
import org.wfanet.measurement.privacybudgetmanager.Slice
import org.wfanet.measurement.privacybudgetmanager.TransactionContext
import org.wfanet.measurement.privacybudgetmanager.charges
import org.wfanet.measurement.privacybudgetmanager.copy

private const val MAX_BATCH_INSERT = 1000
private const val MAX_BATCH_READ = 1000

/**
 * A [Ledger] implemented in Postgres compatible SQL.
 *
 * @param createConnection is a function that creates a postgres JDBC connection that will be owned
 *   by the ledger and should not be used outside it.
 * @param activeLandscape is the identifier of the landscape used by this ledger. All [Query]s and
 *   [PrivacyCharge]s need to target this landscape. The [PrivacyCharges] table should be in READY
 *   state for any operation to be carried out by this ledger.
 */
class PostgresLedger(createConnection: () -> Connection, val activeLandscapeId: String) : Ledger {
  private val connection = createConnection()

  init {
    connection.autoCommit = false
  }

  private var previousTransactionContext: PostgresTransactionContext? = null

  override fun startTransaction(): PostgresTransactionContext {
    val previousTransactionIsClosed = previousTransactionContext?.isClosed ?: true
    if (!previousTransactionIsClosed) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION)
    }
    if (connection.isClosed) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED)
    }
    connection.createStatement().use { statement: Statement ->
      statement.executeUpdate("begin transaction")
      return PostgresTransactionContext(connection, activeLandscapeId)
    }
  }
}

class PostgresTransactionContext(
  private val connection: Connection,
  private val activeLandscapeId: String,
) : TransactionContext {

  private val idCache = mutableMapOf<Pair<String, String>, Int>()

  private data class LedgerEntry(
    val edpId: String,
    val measurementConsumerId: String,
    val externalReferenceId: String,
    val isRefund: Boolean,
    val createTime: Timestamp,
  )

  enum class ChargesTableState {
    BACKFILLING,
    READY,
  }

  private var transactionHasEnded = false

  val isClosed: Boolean
    get() = transactionHasEnded

  private suspend fun getChargesTableState(): ChargesTableState {
    throwIfTransactionHasEnded()
    val query = "SELECT State FROM PrivacyChargesMetadata WHERE PrivacyLandscapeName = ?"
    connection.prepareStatement(query).use { preparedStatement ->
      preparedStatement.setString(1, activeLandscapeId)
      preparedStatement.executeQuery().use { resultSet ->
        if (resultSet.next()) {
          return ChargesTableState.valueOf(resultSet.getString("State"))
        } else {
          throw LedgerException(
            LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST,
            "landscapeid : $activeLandscapeId",
          )
        }
      }
    }
  }

  private suspend fun throwIfTransactionHasEnded() {
    if (transactionHasEnded) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT)
    }
  }

  private suspend fun checkTableState() {
    val chargesTableState = getChargesTableState()
    if (chargesTableState != ChargesTableState.READY) {
      throw LedgerException(LedgerExceptionType.TABLE_NOT_READY, "landscapeid : $activeLandscapeId")
    }
  }

  private suspend fun validateQueries(queries: List<Query>) {
    val invalidIds =
      queries
        .filterNot { it.privacyLandscapeIdentifier == activeLandscapeId }
        .map { it.privacyLandscapeIdentifier }
        .distinct()

    if (invalidIds.isNotEmpty()) {
      throw LedgerException(
        LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS,
        "Mismatched landscape IDs: ${invalidIds.joinToString(", ")}",
      )
    }
  }

  /**
   * Reads queries from the database in batches and returns the matching ones with their create
   * times.
   *
   * @param queries The list of queries to find in the ledger.
   * @return The list of queries that existed in the ledger, with createTime populated.
   *
   * TODO(uakyol): this function does not support refunds for now. Add this functionality.
   */
  override suspend fun readQueries(queries: List<Query>, maxBatchSize: Int): List<Query> {
    throwIfTransactionHasEnded()
    validateQueries(queries)
    checkTableState()
    return withContext(Dispatchers.IO) {
      val foundQueries = mutableListOf<Query>()
      if (queries.isEmpty()) {
        return@withContext emptyList()
      }

      // Read Queries in batches
      queries.chunked(MAX_BATCH_READ).forEach { queryBatch ->
        val batchSize = queryBatch.size

        // Get the query from the database with it's create time if it exists.
        val selectStatement =
          """
            SELECT ed.EventDataProviderName as EventDataProviderId, 
                   mc.MeasurementConsumerName as MeasurementConsumerId, 
                   er.LedgerEntryExternalReferenceName as ExternalReferenceId, 
                   le.IsRefund, 
                   le.CreateTime
            FROM LedgerEntries le
            INNER JOIN EventDataProviders ed ON le.EventDataProviderId = ed.id
            INNER JOIN MeasurementConsumers mc ON le.MeasurementConsumerId = mc.id
            INNER JOIN LedgerEntryExternalReferences er ON le.ExternalReferenceId = er.id
            WHERE (ed.EventDataProviderName, mc.MeasurementConsumerName, er.LedgerEntryExternalReferenceName, le.IsRefund) IN
            (${generateInClausePlaceholders(batchSize, clauseSize=4)})
            """

        connection.prepareStatement(selectStatement).use { preparedStatement ->

          // Construct the select sql with the parameters for each query.
          var parameterIndex = 1
          queryBatch.forEach { query ->
            preparedStatement.setString(
              parameterIndex++,
              query.queryIdentifiers.eventDataProviderId,
            )
            preparedStatement.setString(
              parameterIndex++,
              query.queryIdentifiers.measurementConsumerId,
            )
            preparedStatement.setString(
              parameterIndex++,
              query.queryIdentifiers.externalReferenceId,
            )
            preparedStatement.setBoolean(parameterIndex++, query.queryIdentifiers.isRefund)
          }

          preparedStatement.executeQuery().use { resultSet ->
            val ledgerEntries = mutableListOf<LedgerEntry>()

            // Iterate over the retrieved ledger entries from the ledger. Each corresponds to
            // a query in the input to this function.
            while (resultSet.next()) {
              ledgerEntries.add(
                LedgerEntry(
                  edpId = resultSet.getString("EventDataProviderId"),
                  measurementConsumerId = resultSet.getString("MeasurementConsumerId"),
                  externalReferenceId = resultSet.getString("ExternalReferenceId"),
                  isRefund = resultSet.getBoolean("IsRefund"),
                  createTime = resultSet.getTimestamp("CreateTime"),
                )
              )
            }

            // Match the ledger entries to the given queries. Update each query with
            // the createtime retrieved from the database. This operation is important.
            // See
            // /src/main/kotlin/org/wfanet/measurement/privacybudgetmanager/PrivacyBudgetManager.kt
            // for more detailed comments.
            ledgerEntries.forEach { ledgerEntry ->
              val matchingQuery =
                queryBatch.find { query ->
                  query.queryIdentifiers.eventDataProviderId == ledgerEntry.edpId &&
                    query.queryIdentifiers.measurementConsumerId ==
                      ledgerEntry.measurementConsumerId &&
                    query.queryIdentifiers.externalReferenceId == ledgerEntry.externalReferenceId &&
                    query.queryIdentifiers.isRefund == ledgerEntry.isRefund
                }
              checkNotNull(matchingQuery)
              val updatedQuery =
                matchingQuery.copy {
                  queryIdentifiers =
                    queryIdentifiers.copy {
                      createTime = ledgerEntry.createTime.toInstant().toProtoTime()
                    }
                }
              foundQueries.add(updatedQuery)
            }
          }
        }
      }
      return@withContext foundQueries
    }
  }

  override suspend fun readChargeRows(rowKeys: List<LedgerRowKey>, maxBatchSize: Int): Slice =
    withContext(Dispatchers.IO) {
      throwIfTransactionHasEnded()
      checkTableState()
      val slice = Slice()
      if (rowKeys.isEmpty()) {
        return@withContext slice
      }

      try {
        rowKeys.chunked(maxBatchSize).forEach { rowKeyBatch ->
          val batchSize = rowKeyBatch.size

          val selectStatement =
            """
          SELECT ed.EventDataProviderName,
                 mc.MeasurementConsumerName,
                 egr.EventGroupReferenceId,
                 pc.Date, 
                 pc.Charges
          FROM PrivacyCharges pc
          INNER JOIN EventDataProviders ed ON pc.EventDataProviderId = ed.id
          INNER JOIN MeasurementConsumers mc ON pc.MeasurementConsumerId = mc.id
          INNER JOIN EventGroupReferences egr ON pc.EventGroupReferenceId = egr.id
          WHERE (ed.EventDataProviderName, mc.MeasurementConsumerName, egr.EventGroupReferenceId, pc.Date) IN
          (${generateInClausePlaceholders(batchSize, clauseSize=4)})
        """
          connection.prepareStatement(selectStatement).use { preparedStatement ->
            var parameterIndex = 1
            rowKeyBatch.forEach { rowKey ->
              preparedStatement.setString(parameterIndex++, rowKey.eventDataProviderName)
              preparedStatement.setString(parameterIndex++, rowKey.measurementConsumerName)
              preparedStatement.setString(parameterIndex++, rowKey.eventGroupReferenceId)
              preparedStatement.setObject(parameterIndex++, java.sql.Date.valueOf(rowKey.date))
            }

            preparedStatement.executeQuery().use { resultSet ->
              while (resultSet.next()) {
                val eventDataProviderName = resultSet.getString("EventDataProviderName")
                val measurementConsumerName = resultSet.getString("MeasurementConsumerName")
                val eventGroupReferenceId = resultSet.getString("EventGroupReferenceId")
                val date = resultSet.getDate("Date").toLocalDate()
                val chargesBytes = resultSet.getBytes("Charges")

                val charges: Charges =
                  if (chargesBytes != null && chargesBytes.isNotEmpty()) {
                    Charges.parseFrom(chargesBytes)
                  } else {
                    charges {}
                  }

                val rowKey =
                  LedgerRowKey(
                    eventDataProviderName,
                    measurementConsumerName,
                    eventGroupReferenceId,
                    date,
                  )
                slice.merge(rowKey, charges)
              }
            }
          }
        }
      } catch (e: Exception) {
        throw Exception("Error reading charge rows: ${e.message}", e)
      }
      return@withContext slice
    }

  override suspend fun write(delta: Slice, queries: List<Query>, maxBatchSize: Int): List<Query> =
    withContext(Dispatchers.IO) {
      throwIfTransactionHasEnded()
      validateQueries(queries)
      checkTableState()

      val insertChargesStatement =
        """
      INSERT INTO PrivacyCharges (EventDataProviderId, MeasurementConsumerId, EventGroupReferenceId, Date, Charges)
      VALUES (?, ?, ?, ?, ?)
      ON CONFLICT (EventDataProviderId, MeasurementConsumerId, EventGroupReferenceId, Date) DO UPDATE
      SET Charges = EXCLUDED.Charges;
      """
      // The numnber of keys here are the number of unique rows a batch of requisitions target.
      // Assuming a report worth of reqisitions are packed, a high end reasonable scenario of the
      // union
      // of all the requisitions is that they target 90 days * 50 Event Groups * 1 MC * 1 EDP = 4500
      // rows.
      val keys = delta.getLedgerRowKeys()
      connection.prepareStatement(insertChargesStatement).use { preparedStatement ->
        keys.chunked(maxBatchSize).forEach { batchKeys ->
          batchKeys.forEach { key ->
            val charges = delta.get(key)!!

            // Look up integer IDs from dimension tables
            val edpIdInt = getOrInsertEdpId(key.eventDataProviderName)
            val measurementConsumerIdInt =
              getOrInsertMeasurementConsumerId(key.measurementConsumerName)
            val eventGroupReferenceIdInt =
              getOrInsertEventGroupReferenceId(key.eventGroupReferenceId)

            preparedStatement.setInt(1, edpIdInt)
            preparedStatement.setInt(2, measurementConsumerIdInt)
            preparedStatement.setInt(3, eventGroupReferenceIdInt)
            preparedStatement.setDate(4, Date.valueOf(key.date))
            preparedStatement.setBytes(5, charges.toByteString().toByteArray())
            preparedStatement.addBatch()
          }
        }
        preparedStatement.executeBatch()
        preparedStatement.clearBatch()
      }

      val insertLedgerEntriesStatement =
        """
      INSERT INTO LedgerEntries (EventDataProviderId, MeasurementConsumerId, ExternalReferenceId, IsRefund, CreateTime)
      VALUES (?, ?, ?, ?, NOW())
      """
      connection.prepareStatement(insertLedgerEntriesStatement).use { preparedStatement ->
        queries.chunked(maxBatchSize).forEach { batchQueries ->
          batchQueries.forEach { query ->
            val identifiers = query.queryIdentifiers

            // Look up integer IDs from dimension tables
            val edpIdInt = getOrInsertEdpId(identifiers.eventDataProviderId)
            val measurementConsumerIdInt =
              getOrInsertMeasurementConsumerId(identifiers.measurementConsumerId)
            val externalReferenceIdInt =
              getOrInsertExternalReferenceId(identifiers.externalReferenceId)

            preparedStatement.setInt(1, edpIdInt)
            preparedStatement.setInt(2, measurementConsumerIdInt)
            preparedStatement.setInt(3, externalReferenceIdInt)
            preparedStatement.setBoolean(4, identifiers.isRefund)
            preparedStatement.addBatch()
          }
        }
        preparedStatement.executeBatch()
        preparedStatement.clearBatch()
      }

      return@withContext readQueries(queries)
    }

  private suspend fun getOrInsertEdpId(eventDataProviderName: String) =
    getOrInsertResourceId("EventDataProviders", eventDataProviderName)

  private suspend fun getOrInsertMeasurementConsumerId(measurementConsumerName: String) =
    getOrInsertResourceId("MeasurementConsumers", measurementConsumerName)

  private suspend fun getOrInsertEventGroupReferenceId(eventGroupReferenceId: String) =
    getOrInsertResourceId("EventGroupReferences", eventGroupReferenceId)

  private suspend fun getOrInsertExternalReferenceId(externalReferenceName: String) =
    getOrInsertResourceId("LedgerEntryExternalReferences", externalReferenceName)

  private suspend fun getOrInsertResourceId(tableName: String, value: String): Int {
    throwIfTransactionHasEnded()
    val nameColumn = resourceTablesToNameCols.get(tableName)
    val key = Pair(tableName, value)
    return idCache.getOrPut(key) {
      val insertSql =
        """
        INSERT INTO $tableName ($nameColumn) VALUES (?)
        ON CONFLICT ($nameColumn) DO UPDATE SET $nameColumn = EXCLUDED.$nameColumn
        RETURNING id;
      """
          .trimIndent()

      connection.prepareStatement(insertSql).use { insertStatement ->
        insertStatement.setString(1, value)
        insertStatement.executeQuery().use { resultSet ->
          if (resultSet.next()) {
            return resultSet.getInt(1)
          } else {
            // This should ideally not happen with ON CONFLICT DO UPDATE RETURNING id
            // unless there's a different error during the operation.
            // However, we keep the exception for robustness.
            throw IllegalStateException(
              "Failed to retrieve or insert resource ID for '$value' in '$tableName.$nameColumn'."
            )
          }
        }
      }
    }
  }

  override suspend fun commit() {
    connection.commit()
    transactionHasEnded = true
  }

  override fun close() {
    connection.rollback()
    transactionHasEnded = true
  }

  private companion object {

    // Maps the resource table names for the to the name columns.
    val resourceTablesToNameCols =
      mapOf(
        "EventDataProviders" to "EventDataProviderName",
        "MeasurementConsumers" to "MeasurementConsumerName",
        "EventGroupReferences" to "EventGroupReferenceId",
        "LedgerEntryExternalReferences" to "LedgerEntryExternalReferenceName",
      )

    /**
     * Generates the placeholders for an IN clause to be used in a database query, based on the
     * specified batch size and the number of parameters per element in the IN clause.
     *
     * For example, if `batchSize` is 3 and `clauseSize` is 4, the generated string will be "(?, ?,
     * ?, ?), (?, ?, ?, ?), (?, ?, ?, ?)". This is suitable for an IN clause where each element
     * consists of 4 parameters (e.g., EdpId, MeasurementConsumerId, ExternalReferenceId, IsRefund).
     *
     * @param batchSize The number of sets of placeholders to generate (the number of elements in
     *   the IN clause).
     * @param clauseSize The number of question mark placeholders within each set (the number of
     *   parameters for each element in the IN clause).
     * @return A string containing the generated IN clause placeholders.
     */
    fun generateInClausePlaceholders(batchSize: Int, clauseSize: Int): String {
      val questionMarks = "(?" + ", ?".repeat(clauseSize - 1) + ")"
      return (1..batchSize).joinToString(separator = ", ") { questionMarks }
    }
  }
}
