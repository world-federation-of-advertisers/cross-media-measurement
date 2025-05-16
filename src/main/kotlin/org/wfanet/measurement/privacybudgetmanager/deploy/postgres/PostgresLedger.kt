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
import java.sql.PreparedStatement
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
private const val CHARGES_TABLE_READY_STATE: String = "READY"

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

  private data class LedgerEntry(
    val edpId: String,
    val measurementConsumerId: String,
    val externalReferenceId: String,
    val isRefund: Boolean,
    val createTime: Timestamp,
  )

  private var transactionHasEnded = false

  val isClosed: Boolean
    get() = transactionHasEnded

  private suspend fun getChargesTableState(): String {
    try {
      val query = "SELECT State FROM PrivacyChargesMetadata WHERE PrivacyLandscapeName = ?"
      connection.prepareStatement(query).use { preparedStatement ->
        preparedStatement.setString(1, activeLandscapeId)
        preparedStatement.executeQuery().use { resultSet ->
          if (resultSet.next()) {
            return resultSet.getString("State")
          } else {
            throw LedgerException(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
          }
        }
      }
    } catch (e: Exception) {
      throw e
    }
  }

  private suspend fun throwIfTransactionHasEnded() {
    if (transactionHasEnded) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT)
    }
  }

  private suspend fun checkTableState() {
    val chargesTableState = getChargesTableState()
    if (chargesTableState != CHARGES_TABLE_READY_STATE) {
      throw LedgerException(LedgerExceptionType.TABLE_NOT_READY)
    }
  }

  private suspend fun areValid(queries: List<Query>) {
    if (!queries.all { it.privacyLandscapeIdentifier == activeLandscapeId }) {
      throw LedgerException(LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS)
    }
  }

  /**
   * Reads queries from the database in batches and returns the matching ones with their create
   * times.
   *
   * @param queries The list of queries to find in the ledger.
   * @return The list of queries that existed in the ledger, with createTime populated.
   */
  override suspend fun readQueries(queries: List<Query>, maxBatchSize: Int): List<Query> {
    areValid(queries)
    checkTableState()
    return withContext(Dispatchers.IO) {
      val foundQueries = mutableListOf<Query>()
      if (queries.isEmpty()) {
        return@withContext emptyList()
      }

      try {
        queries.chunked(MAX_BATCH_READ).forEach { queryBatch ->
          val batchSize = queryBatch.size

          val selectStatement =
            """
            SELECT EdpId, MeasurementConsumerId, ExternalReferenceId, IsRefund, CreateTime
            FROM LedgerEntries
            WHERE (EdpId, MeasurementConsumerId, ExternalReferenceId, IsRefund) IN 
            (${generateInClausePlaceholders(batchSize, 4)})
            """

          connection.prepareStatement(selectStatement).use { preparedStatement ->
            setBatchQueryReadParameters(preparedStatement, queryBatch)
            preparedStatement.executeQuery().use { resultSet ->

              // TODO(uakyol) : optimize this by using a hashmap with keys as query identifiers
              val ledgerEntries = mutableListOf<LedgerEntry>()
              while (resultSet.next()) {
                ledgerEntries.add(
                  LedgerEntry(
                    edpId = resultSet.getString("EdpId"),
                    measurementConsumerId = resultSet.getString("MeasurementConsumerId"),
                    externalReferenceId = resultSet.getString("ExternalReferenceId"),
                    isRefund = resultSet.getBoolean("IsRefund"),
                    createTime = resultSet.getTimestamp("CreateTime"),
                  )
                )
              }

              // After getting all LedgerEntries for the batch, match them with the original queries
              ledgerEntries.forEach { ledgerEntry ->
                val matchingQuery =
                  queryBatch.find { query ->
                    query.queryIdentifiers.eventDataProviderId == ledgerEntry.edpId &&
                      query.queryIdentifiers.measurementConsumerId ==
                        ledgerEntry.measurementConsumerId &&
                      query.queryIdentifiers.externalReferenceId ==
                        ledgerEntry.externalReferenceId &&
                      query.queryIdentifiers.isRefund == ledgerEntry.isRefund
                  }
                if (matchingQuery != null) {
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
        }
      } catch (e: Exception) {
        throw e
      }
      return@withContext foundQueries
    }
  }

  override suspend fun readChargeRows(rowKeys: List<LedgerRowKey>, maxBatchSize: Int): Slice =
    withContext(Dispatchers.IO) {
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
            SELECT EdpId, MeasurementConsumerId, EventGroupReferenceId, Date, Charges
            FROM PrivacyCharges
            WHERE (EdpId, MeasurementConsumerId, EventGroupReferenceId, Date) IN
            (${generateInClausePlaceholders(batchSize, 4)})
          """
          connection.prepareStatement(selectStatement).use { preparedStatement ->
            setBatchChargeReadParameters(preparedStatement, rowKeyBatch)
            preparedStatement.executeQuery().use { resultSet ->
              while (resultSet.next()) {
                val edpId = resultSet.getString("EdpId")
                val measurementConsumerId = resultSet.getString("MeasurementConsumerId")
                val eventGroupReferenceId = resultSet.getString("EventGroupReferenceId")
                val date = resultSet.getDate("Date").toLocalDate()
                val chargesBytes = resultSet.getBytes("Charges")

                val charges: Charges =
                  if (chargesBytes != null && chargesBytes.isNotEmpty()) {
                    Charges.parseFrom(chargesBytes)
                  } else {
                    charges {}
                  }

                val rowKey = LedgerRowKey(edpId, measurementConsumerId, eventGroupReferenceId, date)
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

  override suspend fun write(delta: Slice, queries: List<Query>, maxBatchSize: Int): List<Query> {
    areValid(queries)
    checkTableState()

    val insertChargesStatement =
      """
      INSERT INTO PrivacyCharges (EdpId, MeasurementConsumerId, EventGroupReferenceId, Date, Charges)
      VALUES (?, ?, ?, ?, ?)
    """
    val keys = delta.getLedgerRowKeys()
    connection.prepareStatement(insertChargesStatement).use { preparedStatement ->
      keys.chunked(maxBatchSize).forEach { batchKeys ->
        batchKeys.forEach { key ->
          val charges = delta.get(key)!!

          preparedStatement.setString(1, key.edpId)
          preparedStatement.setString(2, key.measurementConsumerId)
          preparedStatement.setString(3, key.eventGroupReferenceId)
          preparedStatement.setDate(4, Date.valueOf(key.date))
          preparedStatement.setBytes(5, charges.toByteString().toByteArray())
          preparedStatement.addBatch()
        }
      }

      preparedStatement.executeBatch()
      preparedStatement.clearBatch()
    }

    // Unfortunately, RETURNING createTime is not supported, hence we re read the
    // queries with their create times after insert.
    // This approach has a much better perfromance since its 2 calls to the db rather
    // than making ~700 calls (queries per report)
    val insertLedgerEntriesStatement =
      """
            INSERT INTO LedgerEntries (EdpId, MeasurementConsumerId, ExternalReferenceId, IsRefund, CreateTime)
            VALUES (?, ?, ?, ?, NOW())
        """
    connection.prepareStatement(insertLedgerEntriesStatement).use { preparedStatement ->
      queries.chunked(maxBatchSize).forEach { batchQueries ->
        batchQueries.forEach { query ->
          val identifiers = query.queryIdentifiers
          preparedStatement.setString(1, identifiers.eventDataProviderId)
          preparedStatement.setString(2, identifiers.measurementConsumerId)
          preparedStatement.setString(3, identifiers.externalReferenceId)
          preparedStatement.setBoolean(4, identifiers.isRefund)
          preparedStatement.addBatch()
        }
      }
      preparedStatement.executeBatch()
      preparedStatement.clearBatch()
    }

    return readQueries(queries)
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

    /** Sets the parameters for the prepared statement for a batch of queries. */
    fun setBatchQueryReadParameters(preparedStatement: PreparedStatement, queryBatch: List<Query>) {
      var parameterIndex = 1
      for (query in queryBatch) {
        val identifiers = query.queryIdentifiers
        preparedStatement.setString(parameterIndex++, identifiers.eventDataProviderId)
        preparedStatement.setString(parameterIndex++, identifiers.measurementConsumerId)
        preparedStatement.setString(parameterIndex++, identifiers.externalReferenceId)
        preparedStatement.setBoolean(parameterIndex++, identifiers.isRefund)
      }
    }

    /** Sets the parameters for the prepared statement for a batch of ledger row keys. */
    fun setBatchChargeReadParameters(
      preparedStatement: PreparedStatement,
      rowKeys: List<LedgerRowKey>,
    ) {
      var parameterIndex = 1
      for (key in rowKeys) {
        preparedStatement.setString(parameterIndex++, key.edpId)
        preparedStatement.setString(parameterIndex++, key.measurementConsumerId)
        preparedStatement.setString(parameterIndex++, key.eventGroupReferenceId)
        preparedStatement.setDate(parameterIndex++, Date.valueOf(key.date))
      }
    }
  }
}
