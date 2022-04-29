/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.postgres

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyCharge

/**
 * A [PrivacyBudgetLedgerBackingStore] implemented in Postgres compatible SQL.
 *
 * @param createConnection is a function that creates a postgres JDBC connection that will be owned
 * by the backing store and should not be used outside this backing store.
 */
class PostgresBackingStore(createConnection: () -> Connection) : PrivacyBudgetLedgerBackingStore {
  private val connection = createConnection()
  init {
    connection.autoCommit = false
  }
  private var previousTransactionContext: PostgresBackingStoreTransactionContext? = null

  override fun startTransaction(): PostgresBackingStoreTransactionContext {
    val previousTransactionIsClosed = previousTransactionContext?.isClosed ?: true
    if (!previousTransactionIsClosed) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION,
        emptyList(),
      )
    }
    if (connection.isClosed) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED,
        emptyList(),
      )
    }
    connection.createStatement().use { statement: Statement ->
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeUpdate("begin transaction")
      val getTransactionSql = "select nextval('LedgerEntriesTransactionIdSeq')"
      statement.executeQuery(getTransactionSql).use { rs: ResultSet ->
        rs.next()
        val transactionId = rs.getLong(1)
        previousTransactionContext =
          PostgresBackingStoreTransactionContext(transactionId, connection)
        return previousTransactionContext!!
      }
    }
  }

  override fun close() {
    connection.close()
  }
}

class PostgresBackingStoreTransactionContext(
  override val transactionId: Long,
  private val connection: Connection,
) : PrivacyBudgetLedgerTransactionContext {
  private var transactionHasEnded = false

  val isClosed: Boolean
    get() = transactionHasEnded

  private fun throwIfTransactionHasEnded(privacyBucketGroups: List<PrivacyBucketGroup>) {
    if (transactionHasEnded) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT,
        privacyBucketGroups,
      )
    }
  }

  override fun findIntersectingLedgerEntries(
    privacyBucketGroup: PrivacyBucketGroup
  ): List<PrivacyBudgetLedgerEntry> {
    throwIfTransactionHasEnded(listOf(privacyBucketGroup))
    assert(privacyBucketGroup.startingDate == privacyBucketGroup.endingDate)
    val selectBucketSql =
      """
        SELECT
          LedgerEntryId,
          TransactionId,
          Delta,
          Epsilon,
          RepetitionCount
        FROM LedgerEntries
        WHERE
          MeasurementConsumerId = ?
          AND Date = ?
          AND AgeGroup = CAST(? AS AgeGroup)
          AND Gender = CAST(? AS Gender)
          and VidStart = ?
      """.trimIndent()
    connection.prepareStatement(selectBucketSql).use { statement: PreparedStatement ->
      statement.setString(1, privacyBucketGroup.measurementConsumerId)
      statement.setObject(2, privacyBucketGroup.startingDate)
      statement.setString(3, privacyBucketGroup.ageGroup.string)
      statement.setString(4, privacyBucketGroup.gender.string)
      statement.setFloat(5, privacyBucketGroup.vidSampleStart)
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeQuery().use { rs: ResultSet ->
        val entries = ArrayList<PrivacyBudgetLedgerEntry>()
        while (rs.next()) {
          val rowId = rs.getLong("LedgerEntryId")
          val transactionId = rs.getLong("TransactionId")
          val delta = rs.getFloat("Delta")
          val epsilon = rs.getFloat("Epsilon")
          val repetitionCount = rs.getInt("RepetitionCount")
          val entry =
            PrivacyBudgetLedgerEntry(
              rowId,
              transactionId,
              privacyBucketGroup,
              PrivacyCharge(epsilon, delta),
              repetitionCount,
            )
          entries.add(entry)
        }
        return entries
      }
    }
  }

  override fun addLedgerEntry(
    privacyBucketGroup: PrivacyBucketGroup,
    privacyCharge: PrivacyCharge
  ) {
    throwIfTransactionHasEnded(listOf(privacyBucketGroup))
    val insertEntrySql =
      """
        INSERT into LedgerEntries (
          MeasurementConsumerId,
          TransactionId,
          Date,
          AgeGroup,
          Gender,
          VidStart,
          Delta,
          Epsilon,
          RepetitionCount
        ) VALUES (
          ?,
          ?,
          ?,
          CAST(? AS AgeGroup),
          CAST(? AS Gender),
          ?,
          ?,
          ?,
          ?)
      """.trimIndent()
    connection.prepareStatement(insertEntrySql).use { statement: PreparedStatement ->
      statement.setString(1, privacyBucketGroup.measurementConsumerId)
      statement.setLong(2, transactionId)
      statement.setObject(3, privacyBucketGroup.startingDate)
      statement.setString(4, privacyBucketGroup.ageGroup.string)
      statement.setString(5, privacyBucketGroup.gender.string)
      statement.setFloat(6, privacyBucketGroup.vidSampleStart)
      statement.setFloat(7, privacyCharge.delta)
      statement.setFloat(8, privacyCharge.epsilon)
      statement.setInt(9, 1) // RepetitionCount
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeUpdate()
    }
  }

  override fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry) {
    // TODO(@uakyol) Refactor this out as part of making the ledger immutable
    throwIfTransactionHasEnded(listOf(privacyBudgetLedgerEntry.privacyBucketGroup))
    val updateEntrySql =
      """
        UPDATE LedgerEntries
        SET
          MeasurementConsumerId = ?,
          TransactionId = ?,
          Date = ?,
          AgeGroup = CAST(? AS AgeGroup),
          Gender = CAST(? AS Gender),
          VidStart = ?,
          Delta = ?,
          Epsilon = ?,
          RepetitionCount = ?
        WHERE LedgerEntryId = ?
      """.trimIndent()
    val privacyBucketGroup = privacyBudgetLedgerEntry.privacyBucketGroup
    val privacyCharge = privacyBudgetLedgerEntry.privacyCharge
    connection.prepareStatement(updateEntrySql).use { statement: PreparedStatement ->
      statement.setString(1, privacyBucketGroup.measurementConsumerId)
      statement.setLong(2, transactionId)
      statement.setObject(3, privacyBucketGroup.startingDate)
      statement.setString(4, privacyBucketGroup.ageGroup.string)
      statement.setString(5, privacyBucketGroup.gender.string)
      statement.setFloat(6, privacyBucketGroup.vidSampleStart)
      statement.setFloat(7, privacyCharge.delta)
      statement.setFloat(8, privacyCharge.epsilon)
      statement.setInt(9, privacyBudgetLedgerEntry.repetitionCount)
      statement.setLong(10, privacyBudgetLedgerEntry.rowId)
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeUpdate()
    }
  }

  override fun mergePreviousTransaction(previousTransactionId: Long) {
    TODO("TODO(@uakyol) Implement this function as part of making the ledger immutable")
  }

  override fun undoPreviousTransaction(previousTransactionId: Long) {
    TODO("TODO(@uakyol) Implement this function as part of making the ledger immutable")
  }

  override fun commit() {
    connection.commit()
    transactionHasEnded = true
  }

  override fun close() {
    connection.rollback()
    transactionHasEnded = true
  }
}
