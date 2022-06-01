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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Charge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

/**
 * A [PrivacyBudgetLedgerBackingStore] implemented in Postgres compatible SQL.
 *
 * @param createConnection is a function that creates a postgres JDBC connection that will be owned
 * by the backing store and should not be used outside this backing store.
 */
class PostgresBackingStore(createConnection: () -> Connection) : PrivacyBudgetLedgerBackingStore {
  // TODO(@duliomatos1) to redesign this to reduce connection lifetime, e.g. using a Connection for
  // a single transaction/operation and then closing it
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
      return PostgresBackingStoreTransactionContext(connection)
    }
  }

  override fun close() {
    connection.close()
  }
}

class PostgresBackingStoreTransactionContext(
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

  private fun getLastReference(measurementConsumerId: String, referenceId: String): Boolean? {
    val selectSql =
      """
        SELECT
          IsRefund,
          CreateTime
        FROM LedgerEntries
        WHERE
          MeasurementConsumerId = ? AND ReferenceId = ?
        Order by CreateTime DESC
        Limit 1
      """.trimIndent()
    connection.prepareStatement(selectSql).use { statement: PreparedStatement ->
      statement.setString(1, measurementConsumerId)
      statement.setString(2, referenceId)
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeQuery().use { rs: ResultSet ->
        if (rs.next()) {
          return rs.getBoolean("IsRefund")
        }
      }
    }
    return null
  }

  override fun hasLedgerEntry(reference: Reference): Boolean =
    getLastReference(reference.measurementConsumerId, reference.referenceId)
      ?.xor(reference.isRefund)
      ?: true

  override fun findIntersectingBalanceEntries(
    privacyBucketGroup: PrivacyBucketGroup
  ): List<PrivacyBudgetBalanceEntry> {
    throwIfTransactionHasEnded(listOf(privacyBucketGroup))
    assert(privacyBucketGroup.startingDate == privacyBucketGroup.endingDate)
    val selectBucketSql =
      """
        SELECT
          Delta,
          Epsilon,
          RepetitionCount
        FROM PrivacyBucketCharges
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
        val entries = ArrayList<PrivacyBudgetBalanceEntry>()
        while (rs.next()) {
          entries.add(
            PrivacyBudgetBalanceEntry(
              privacyBucketGroup,
              Charge(rs.getFloat("Epsilon"), rs.getFloat("Delta")),
              rs.getInt("RepetitionCount"),
            )
          )
        }
        return entries
      }
    }
  }

  private fun addBalanceEntry(
    privacyBucketGroup: PrivacyBucketGroup,
    privacyCharge: Charge,
    refundCharge: Boolean = false
  ) {
    throwIfTransactionHasEnded(listOf(privacyBucketGroup))
    val insertEntrySql =
      """
        INSERT into PrivacyBucketCharges (
          MeasurementConsumerId,
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
          CAST(? AS AgeGroup),
          CAST(? AS Gender),
          ?,
          ?,
          ?,
          1)
      ON CONFLICT (MeasurementConsumerId,
          Date,
          AgeGroup,
          Gender,
          VidStart,
          Delta,
          Epsilon)
      DO
         UPDATE SET RepetitionCount = ? + PrivacyBucketCharges.RepetitionCount;
      """.trimIndent()
    connection.prepareStatement(insertEntrySql).use { statement: PreparedStatement ->
      statement.setString(1, privacyBucketGroup.measurementConsumerId)
      statement.setObject(2, privacyBucketGroup.startingDate)
      statement.setString(3, privacyBucketGroup.ageGroup.string)
      statement.setString(4, privacyBucketGroup.gender.string)
      statement.setFloat(5, privacyBucketGroup.vidSampleStart)
      statement.setFloat(6, privacyCharge.delta)
      statement.setFloat(7, privacyCharge.epsilon)
      statement.setInt(8, if (refundCharge) -1 else 1) // update RepetitionCount
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeUpdate()
    }
  }

  private fun addLedgerEntry(privacyReference: Reference) {
    val insertEntrySql =
      """
        INSERT into LedgerEntries (
          MeasurementConsumerId,
          ReferenceId,
          IsRefund,
          CreateTime
        ) VALUES (
          ?,
          ?,
          ?,
          NOW())
      """.trimIndent()
    connection.prepareStatement(insertEntrySql).use { statement: PreparedStatement ->
      statement.setString(1, privacyReference.measurementConsumerId)
      statement.setString(2, privacyReference.referenceId)
      statement.setObject(3, privacyReference.isRefund)
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeUpdate()
    }
  }

  override fun addLedgerEntries(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    charges: Set<Charge>,
    reference: Reference
  ) {
    for (privacyBucketGroup in privacyBucketGroups) {
      for (charge in charges) {
        addBalanceEntry(privacyBucketGroup, charge, reference.isRefund)
      }
    }
    addLedgerEntry(reference)
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
