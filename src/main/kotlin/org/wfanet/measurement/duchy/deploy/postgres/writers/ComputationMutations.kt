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

package org.wfanet.measurement.duchy.deploy.postgres.writers

import com.google.protobuf.Message
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadWriteContext
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException

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
        ComputationId = COALESCE($9, ComputationId),
        ComputationStage = COALESCE($10, ComputationStage)
      WHERE
        ComputationId = $9
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
      bind("$9", localId)
      bind("$10", stage)
    }

  transactionContext.executeStatement(sql)
}

suspend fun PostgresWriter.TransactionScope.extendComputationLock(
  localComputationId: Long,
  updateTime: Instant,
  lockExpirationTime: Instant
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

suspend fun PostgresWriter.TransactionScope.releaseComputationLock(
  localComputationId: Long,
  updateTime: Instant
) {
  setLock(transactionContext, localComputationId, updateTime)
}

suspend fun PostgresWriter.TransactionScope.acquireComputationLock(
  localId: Long,
  updateTime: Instant,
  ownerId: String?,
  lockExpirationTime: Instant
) {
  setLock(transactionContext, localId, updateTime, ownerId, lockExpirationTime)
}

private suspend fun setLock(
  readWriteContext: ReadWriteContext,
  localId: Long,
  updateTime: Instant,
  ownerId: String? = null,
  lockExpirationTime: Instant? = null
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

suspend fun PostgresWriter.TransactionScope.updateComputationStage(
  localId: Long,
  stage: Long,
  nextAttempt: Long? = null,
  creationTime: Instant? = null,
  endTime: Instant? = null,
  previousStage: Long? = null,
  followingStage: Long? = null,
  details: Message? = null,
) {
  val sql =
    boundStatement(
      """
      UPDATE ComputationStages SET
        CreationTime = COALESCE($3, CreationTime),
        NextAttempt = COALESCE($4, NextAttempt),
        EndTime = COALESCE($5, EndTime),
        PreviousStage = COALESCE($6, PreviousStage),
        FollowingStage = COALESCE($7, FollowingStage),
        Details = COALESCE($8, Details),
        DetailsJSON = COALESCE($9::jsonb, DetailsJSON)
      WHERE
        ComputationId = $1
      AND
        ComputationStage = $2;
      """
    ) {
      bind("$1", localId)
      bind("$2", stage)
      bind("$3", creationTime)
      bind("$4", nextAttempt)
      bind("$5", endTime)
      bind("$6", previousStage)
      bind("$7", followingStage)
      bind("$8", details?.toByteArray())
      bind("$9", details?.toJson())
    }

  transactionContext.executeStatement(sql)
}

suspend fun PostgresWriter.TransactionScope.updateComputationStageAttempt(
  localId: Long,
  stage: Long,
  attempt: Long,
  beginTime: Instant? = null,
  endTime: Instant? = null,
  details: Message? = null
) {
  val sql =
    boundStatement(
      """
      UPDATE ComputationStageAttempts SET
        BeginTime = COALESCE($4, BeginTime),
        EndTime = COALESCE($5, EndTime),
        Details = COALESCE($6, Details),
        DetailsJSON = COALESCE($7::jsonb, DetailsJSON)
      WHERE
        ComputationId = $1
      AND
        ComputationStage = $2
      AND
        Attempt = $3
      """
    ) {
      bind("$1", localId)
      bind("$2", stage)
      bind("$3", attempt)
      bind("$4", beginTime)
      bind("$5", endTime)
      bind("$6", details?.toByteArray())
      bind("$7", details?.toJson())
    }

  transactionContext.executeStatement(sql)
}

suspend fun PostgresWriter.TransactionScope.insertComputationStage(
  localId: Long,
  stage: Long,
  nextAttempt: Long? = null,
  creationTime: Instant? = null,
  endTime: Instant? = null,
  previousStage: Long? = null,
  followingStage: Long? = null,
  details: Message? = null,
) {
  val insertComputationStageStatement =
    boundStatement(
      """
      INSERT INTO ComputationStages
        (
          ComputationId,
          ComputationStage,
          CreationTime,
          NextAttempt,
          EndTime,
          PreviousStage,
          FollowingStage,
          Details,
          DetailsJSON
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9::jsonb)
      """
    ) {
      bind("$1", localId)
      bind("$2", stage)
      bind("$3", creationTime)
      bind("$4", nextAttempt)
      bind("$5", endTime)
      bind("$6", previousStage)
      bind("$7", followingStage)
      bind("$8", details?.toByteArray())
      bind("$9", details?.toJson())
    }

  transactionContext.executeStatement(insertComputationStageStatement)
}

suspend fun PostgresWriter.TransactionScope.checkComputationUnmodified(
  localId: Long,
  editVersion: Long
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
      .firstOrNull()
      ?: throw ComputationNotFoundException(localId)
  val updateTimeMillis = updateTime.toEpochMilli()
  if (editVersion != updateTimeMillis) {
    val editVersionTime = Instant.ofEpochMilli(editVersion)
    error(
      """
          Failed to update because of editVersion mismatch.
            Token's editVersion: $editVersion ($editVersionTime)
            Computations table's UpdateTime: $updateTimeMillis ($updateTime)
            Difference: ${Duration.between(editVersionTime, updateTime)}
          """
        .trimIndent()
    )
  }
}
