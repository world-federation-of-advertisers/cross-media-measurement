// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.common.postgres.writers

import com.google.protobuf.Message
import io.r2dbc.spi.R2dbcDataIntegrityViolationException
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadWriteContext
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.service.internal.ComputationAlreadyExistsException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationTokenVersionMismatchException

/** Insert a new row into the Postgres Computations table. */
suspend fun PostgresWriter.TransactionScope.insertComputation(
  localId: Long,
  creationTime: Instant?,
  updateTime: Instant,
  globalId: String,
  protocol: Long? = null,
  stage: Long? = null,
  lockOwner: String? = null,
  lockExpirationTime: Instant? = null,
  details: Message? = null,
) {

  val insertComputationStatement =
    boundStatement(
      """
      INSERT INTO Computations
        (
          ComputationId,
          Protocol,
          ComputationStage,
          UpdateTime,
          GlobalComputationId,
          LockOwner,
          LockExpirationTime,
          ComputationDetails,
          ComputationDetailsJSON,
          CreationTime
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb, $10)
      """
    ) {
      bind("$1", localId)
      bind("$2", protocol)
      bind("$3", stage)
      bind("$4", updateTime)
      bind("$5", globalId)
      bind("$6", lockOwner)
      bind("$7", lockExpirationTime)
      bind("$8", details?.toByteArray())
      bind("$9", details?.toJson())
      bind("$10", creationTime)
    }

  try {
    transactionContext.executeStatement(insertComputationStatement)
  } catch (ex: R2dbcDataIntegrityViolationException) {
    if (ex.message?.contains("duplicate key") == true) {
      throw ComputationAlreadyExistsException(globalId)
    }
    throw ex
  }
}

/**
 * Updates a row in the Postgres Computations table.
 *
 * If an argument is null, its corresponding field in the database will not be updated.
 */
suspend fun PostgresWriter.TransactionScope.updateComputation(
  localId: Long,
  updateTime: Instant,
  stage: Long? = null,
  creationTime: Instant? = null,
  globalComputationId: String? = null,
  protocol: Long? = null,
  lockOwner: String? = null,
  lockExpirationTime: Instant? = null,
  details: Message? = null,
) {
  val sql =
    boundStatement(
      """
      UPDATE Computations SET
        CreationTime = COALESCE($1, CreationTime),
        UpdateTime = COALESCE($2, UpdateTime),
        GlobalComputationId = COALESCE($3, GlobalComputationId),
        Protocol = COALESCE($4, Protocol),
        LockOwner = COALESCE($5, LockOwner),
        LockExpirationTime = COALESCE($6, LockExpirationTime),
        ComputationDetails = COALESCE($7, ComputationDetails),
        ComputationDetailsJson = COALESCE($8::jsonb, ComputationDetailsJson),
        ComputationStage = COALESCE($9, ComputationStage)
      WHERE
        ComputationId = $10
      """
    ) {
      bind("$1", creationTime)
      bind("$2", updateTime)
      bind("$3", globalComputationId)
      bind("$4", protocol)
      bind("$5", lockOwner)
      bind("$6", lockExpirationTime)
      bind("$7", details?.toByteArray())
      bind("$8", details?.toJson())
      bind("$9", stage)
      bind("$10", localId)
    }

  transactionContext.executeStatement(sql)
}

/** Extends an existing lock to [lockExpirationTime], without changing the lock owner. */
suspend fun PostgresWriter.TransactionScope.extendComputationLock(
  localComputationId: Long,
  updateTime: Instant,
  lockExpirationTime: Instant,
) {
  val sql =
    boundStatement(
      """
    UPDATE Computations SET
      UpdateTime = $1,
      LockExpirationTime = $2
    WHERE
      ComputationId = $3;
    """
        .trimIndent()
    ) {
      bind("$1", updateTime)
      bind("$2", lockExpirationTime)
      bind("$3", localComputationId)
    }
  transactionContext.executeStatement(sql)
}

/** Release a lock by setting the owner and expiration to null. */
suspend fun PostgresWriter.TransactionScope.releaseComputationLock(
  localComputationId: Long,
  updateTime: Instant,
) {
  setLock(transactionContext, localComputationId, updateTime)
}

/**
 * Acquire a lock by setting the owner and expiration. If the owner is null, the computation will be
 * put onto the queue for next worker to claim.
 */
suspend fun PostgresWriter.TransactionScope.acquireComputationLock(
  localId: Long,
  updateTime: Instant,
  ownerId: String?,
  lockExpirationTime: Instant,
) {
  setLock(transactionContext, localId, updateTime, ownerId, lockExpirationTime)
}

private suspend fun setLock(
  readWriteContext: ReadWriteContext,
  localId: Long,
  updateTime: Instant,
  ownerId: String? = null,
  lockExpirationTime: Instant? = null,
) {
  val sql =
    boundStatement(
      """
    UPDATE Computations SET
      UpdateTime = $1,
      LockOwner = $2,
      LockExpirationTime = $3
    WHERE
      ComputationId = $4;
    """
        .trimIndent()
    ) {
      bind("$1", updateTime)
      bind("$2", ownerId)
      bind("$3", lockExpirationTime)
      bind("$4", localId)
    }
  readWriteContext.executeStatement(sql)
}

/**
 * Checks if the version of local computation matches with the version in database.
 *
 * The token edit version is used similarly to an `etag`. See https://google.aip.dev/154
 *
 * @throws ComputationNotFoundException if the Computation with [localId] is not found
 * @throws ComputationTokenVersionMismatchException if [editVersion] does not match
 */
suspend fun PostgresWriter.TransactionScope.checkComputationUnmodified(
  localId: Long,
  editVersion: Long,
) {
  val sql =
    boundStatement(
      """
          SELECT UpdateTime
          FROM Computations
          WHERE ComputationId = $1
        """
        .trimIndent()
    ) {
      bind("$1", localId)
    }
  val updateTime: Instant =
    transactionContext
      .executeQuery(sql)
      .consume { row -> row.get<Instant>("UpdateTime") }
      .firstOrNull() ?: throw ComputationNotFoundException(localId)
  val updateTimeMillis = updateTime.toEpochMilli()
  if (editVersion != updateTimeMillis) {
    val editVersionTime = Instant.ofEpochMilli(editVersion)
    val message =
      """
      Failed to update because of editVersion mismatch.
        Local computation's version: $editVersion ($editVersionTime)
        Computations table's version: $updateTimeMillis ($updateTime)
        Difference: ${Duration.between(editVersionTime, updateTime)}
      """
        .trimIndent()
    throw ComputationTokenVersionMismatchException(
      computationId = localId,
      version = updateTimeMillis,
      tokenVersion = editVersion,
      message = message,
    )
  }
}

/**
 * Deletes a computation by local identifier
 *
 * @param localId local identifier of a computation
 * @return number of rows deleted
 */
suspend fun PostgresWriter.TransactionScope.deleteComputationByLocalId(localId: Long): Long {
  val sql =
    boundStatement(
      """
        DELETE FROM Computations
        WHERE ComputationId = $1
      """
        .trimIndent()
    ) {
      bind("$1", localId)
    }
  return transactionContext.executeStatement(sql).numRowsUpdated
}

/**
 * Deletes a computation by local identifier
 *
 * @param globalId global identifier of a computation
 * @return number of rows deleted
 */
suspend fun PostgresWriter.TransactionScope.deleteComputationByGlobalId(globalId: String): Long {
  val sql =
    boundStatement(
      """
        DELETE FROM Computations
        WHERE GlobalComputationId = $1
      """
        .trimIndent()
    ) {
      bind("$1", globalId)
    }
  return transactionContext.executeStatement(sql).numRowsUpdated
}
