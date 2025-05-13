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
import java.sql.Statement
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.privacybudgetmanager.Ledger
import org.wfanet.measurement.privacybudgetmanager.LedgerRowKey
import org.wfanet.measurement.privacybudgetmanager.Query
import org.wfanet.measurement.privacybudgetmanager.Slice
import org.wfanet.measurement.privacybudgetmanager.TransactionContext
import org.wfanet.measurement.privacybudgetmanager.LedgerException
import org.wfanet.measurement.privacybudgetmanager.LedgerExceptionType

private const val MAX_BATCH_INSERT = 1000
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
  private var transactionHasEnded = false

  val isClosed: Boolean
    get() = transactionHasEnded

  private suspend fun getChargesTableState(): String {
    try {
      val query = "SELECT State FROM PrivacyChargesMetadata WHERE PrivacyLandscapeName = ?"
      val preparedStatement = connection.prepareStatement(query)
      preparedStatement.setString(1, activeLandscapeId)
      val resultSet = preparedStatement.executeQuery()

      if (resultSet.next()) {
        return resultSet.getString("State")
      } else {
        throw LedgerException(LedgerExceptionType.TABLE_METADATA_DOESNT_EXIST)
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
    println("chargesTableStatechargesTableStatechargesTableState $chargesTableState")
    if(chargesTableState != CHARGES_TABLE_READY_STATE) {
      throw LedgerException(LedgerExceptionType.TABLE_NOT_READY)
    }
  }

  private suspend fun areValid(queries: List<Query>) {
    if(!queries.all { it.privacyLandscapeIdentifier == activeLandscapeId }) {
      throw LedgerException(LedgerExceptionType.INVALID_PRIVACY_LANDSCAPE_IDS)
    }
  }

  override suspend fun readQueries(queries: List<Query>): List<Query> {
    areValid(queries)
    checkTableState()
    return emptyList<Query>()
  }

  override suspend fun readChargeRows(rowKeys: List<LedgerRowKey>): Slice {
    checkTableState()
    return Slice()
  }

  override suspend fun write(delta: Slice, queries: List<Query>): List<Query> {
    areValid(queries)
    checkTableState()
    return emptyList<Query>()
  }

  override suspend fun commit() {
    connection.commit()
    transactionHasEnded = true
  }

  override fun close() {
    connection.rollback()
    transactionHasEnded = true
  }
}
