/*
 * Copyright 2022 The Cross-Media Measurement Authors
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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.deploy.common.postgres

import java.sql.Connection
import java.sql.PreparedStatement
import java.sql.ResultSet
import java.sql.Statement
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpCharge
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetAcdpBalanceEntry
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerBackingStore
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetLedgerTransactionContext
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Reference

private const val MAX_BATCH_INSERT = 1000

/**
 * A [PrivacyBudgetLedgerBackingStore] implemented in Postgres compatible SQL.
 *
 * @param createConnection is a function that creates a postgres JDBC connection that will be owned
 *   by the backing store and should not be used outside this backing store.
 *
 * TODO(@uakyol): Use R2DBC-based
 *   [org.wfanet.measurement.common.db.r2dbc.postgres.PostgresDatabaseClient] instead of JDBC.
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
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.NESTED_TRANSACTION)
    }
    if (connection.isClosed) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.BACKING_STORE_CLOSED)
    }
    connection.createStatement().use { statement: Statement ->
      statement.executeUpdate("begin transaction")
      return PostgresBackingStoreTransactionContext(connection)
    }
  }

  override fun close() {
    connection.close()
  }
}

class PostgresBackingStoreTransactionContext(private val connection: Connection) :
  PrivacyBudgetLedgerTransactionContext {
  private var transactionHasEnded = false

  val isClosed: Boolean
    get() = transactionHasEnded

  private fun throwIfTransactionHasEnded() {
    if (transactionHasEnded) {
      throw PrivacyBudgetManagerException(PrivacyBudgetManagerExceptionType.UPDATE_AFTER_COMMIT)
    }
  }

  private suspend fun getLastReference(
    measurementConsumerId: String,
    referenceId: String,
  ): Boolean? {
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
      """
        .trimIndent()
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

  override suspend fun hasLedgerEntry(reference: Reference): Boolean {
    val lastReference =
      getLastReference(reference.measurementConsumerId, reference.referenceId) ?: return false
    return reference.isRefund == lastReference
  }

  override suspend fun findAcdpBalanceEntry(
    privacyBucketGroup: PrivacyBucketGroup
  ): PrivacyBudgetAcdpBalanceEntry {
    throwIfTransactionHasEnded()
    assert(privacyBucketGroup.startingDate == privacyBucketGroup.endingDate)

    val selectBucketSql =
      """
        SELECT
          Rho,
          Theta
        FROM PrivacyBucketAcdpCharges
        WHERE
          MeasurementConsumerId = ?
          AND Date = ?
          AND AgeGroup = CAST(? AS AgeGroup)
          AND Gender = CAST(? AS Gender)
          AND VidStart = ?
      """
        .trimIndent()

    connection.prepareStatement(selectBucketSql).use { statement: PreparedStatement ->
      statement.setString(1, privacyBucketGroup.measurementConsumerId)
      statement.setObject(2, privacyBucketGroup.startingDate)
      statement.setString(3, privacyBucketGroup.ageGroup.string)
      statement.setString(4, privacyBucketGroup.gender.string)
      statement.setFloat(5, privacyBucketGroup.vidSampleStart)

      statement.executeQuery().use { rs: ResultSet ->
        var acdpBalanceEntry: PrivacyBudgetAcdpBalanceEntry =
          PrivacyBudgetAcdpBalanceEntry(privacyBucketGroup, AcdpCharge(0.0, 0.0))

        if (rs.next()) {
          acdpBalanceEntry =
            PrivacyBudgetAcdpBalanceEntry(
              privacyBucketGroup,
              AcdpCharge(rs.getDouble("Rho"), rs.getDouble("Theta")),
            )
        }

        return acdpBalanceEntry
      }
    }
  }

  private suspend fun addLedgerEntry(privacyReference: Reference) {
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
      """
        .trimIndent()
    connection.prepareStatement(insertEntrySql).use { statement: PreparedStatement ->
      statement.setString(1, privacyReference.measurementConsumerId)
      statement.setString(2, privacyReference.referenceId)
      statement.setObject(3, privacyReference.isRefund)
      // TODO(@duliomatos) Make the blocking IO run within a dispatcher using coroutines
      statement.executeUpdate()
    }
  }

  override suspend fun addAcdpLedgerEntries(
    privacyBucketGroups: Set<PrivacyBucketGroup>,
    acdpCharges: Set<AcdpCharge>,
    reference: Reference,
  ) {
    throwIfTransactionHasEnded()

    val insertEntrySql =
      """
        INSERT into PrivacyBucketAcdpCharges (
          MeasurementConsumerId,
          Date,
          AgeGroup,
          Gender,
          VidStart,
          Rho,
          Theta
        ) VALUES (
          ?,
          ?,
          CAST(? AS AgeGroup),
          CAST(? AS Gender),
          ?,
          ?,
          ?
          )
      ON CONFLICT (MeasurementConsumerId,
          Date,
          AgeGroup,
          Gender,
          VidStart)
      DO
         UPDATE SET Rho = ? + PrivacyBucketAcdpCharges.Rho, Theta = ? + PrivacyBucketAcdpCharges.Theta;
      """
        .trimIndent()

    val queryTotalAcdpCharge = getQueryTotalAcdpCharge(acdpCharges, reference.isRefund)

    val statement: PreparedStatement = connection.prepareStatement(insertEntrySql)

    privacyBucketGroups.forEachIndexed { index, queryBucketGroup ->
      statement.setString(1, queryBucketGroup.measurementConsumerId)
      statement.setObject(2, queryBucketGroup.startingDate)
      statement.setString(3, queryBucketGroup.ageGroup.string)
      statement.setString(4, queryBucketGroup.gender.string)
      statement.setFloat(5, queryBucketGroup.vidSampleStart)
      statement.setDouble(6, queryTotalAcdpCharge.rho)
      statement.setDouble(7, queryTotalAcdpCharge.theta)
      statement.setDouble(8, queryTotalAcdpCharge.rho)
      statement.setDouble(9, queryTotalAcdpCharge.theta)
      statement.addBatch()
      // execute every 1000 rows or fewer
      if (index % MAX_BATCH_INSERT == 0 || index == privacyBucketGroups.size - 1) {
        statement.executeBatch()
      }
    }

    addLedgerEntry(reference)
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
