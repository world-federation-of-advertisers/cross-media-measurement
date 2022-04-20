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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement

/** A privacy budget ledger backing store, implemented in Postgres compatible SQL. */
class PostgresBackingStore(private val connection: Connection) : PrivacyBudgetLedgerBackingStore {
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
      statement.executeUpdate()
    }
  }

  override fun updateLedgerEntry(privacyBudgetLedgerEntry: PrivacyBudgetLedgerEntry) {
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
      statement.executeUpdate()
    }
  }

  override fun mergePreviousTransaction(previousTransactionId: Long) {
    require(previousTransactionId != 0L) { "Special transaction ID of zero cannot be merged" }
    throwIfTransactionHasEnded(emptyList())
    val mergeTransactionSql =
      """
        WITH
        transactionEntries AS (
          SELECT *
          FROM LedgerEntries
          WHERE
            TransactionId = ?
        ),
        entriesMergedIntoRepetitionCount AS (
          UPDATE LedgerEntries
          SET
            RepetitionCount =
              LedgerEntries.RepetitionCount + transactionEntries.RepetitionCount
          FROM transactionEntries
          WHERE
            LedgerEntries.TransactionId = 0
            AND LedgerEntries.MeasurementConsumerID = transactionEntries.MeasurementConsumerID
            AND LedgerEntries.Date = transactionEntries.Date
            AND LedgerEntries.AgeGroup = transactionEntries.AgeGroup
            AND LedgerEntries.Gender = transactionEntries.Gender
            AND LedgerEntries.VidStart = transactionEntries.VidStart
            AND LedgerEntries.Delta = transactionEntries.Delta
            AND LedgerEntries.Epsilon = transactionEntries.Epsilon
          RETURNING transactionEntries.LedgerEntryId
        )
        DELETE
        FROM LedgerEntries
        USING entriesMergedIntoRepetitionCount
        WHERE
          LedgerEntries.LedgerEntryId = entriesMergedIntoRepetitionCount.LedgerEntryId;

        UPDATE LedgerEntries
        SET TransactionId = 0
        WHERE
          TransactionId = ?;
      """.trimIndent()
    connection.prepareStatement(mergeTransactionSql).use { statement: PreparedStatement ->
      statement.setLong(1, previousTransactionId)
      statement.setLong(2, previousTransactionId)
      statement.executeUpdate()
    }
  }

  override fun undoPreviousTransaction(previousTransactionId: Long) {
    throwIfTransactionHasEnded(emptyList())
    require(previousTransactionId != 0L) { "Special transaction ID of zero cannot be undone" }
    val deleteTransactionSql =
      """
        DELETE FROM LedgerEntries
        WHERE
          TransactionId = ?
      """.trimIndent()
    connection.prepareStatement(deleteTransactionSql).use { statement: PreparedStatement ->
      statement.setLong(1, previousTransactionId)
      statement.executeUpdate()
    }
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
