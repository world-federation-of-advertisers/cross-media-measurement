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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStageDetailsHelper
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.service.internal.ComputationDetailsNotFoundException
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.duchy.service.internal.DuchyInternalException
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.duchy.copy

/**
 * [PostgresWriter] to claim one ready for processing task for an owner.
 *
 * @param protocol The protocol of the task to claim
 * @param ownerId The identifier of the worker process that will own the lock.
 * @return [String] a global computation id of work that was claimed.
 * @return null when no task was claimed.
 *
 * Throws a subclass of [DuchyInternalException] on [execute]:
 * * [ComputationNotFoundException] when computation could not be found
 * * [ComputationDetailsNotFoundException] when computation details could not be found
 */
class ClaimWork<ProtocolT, ComputationDT : Message, StageT, StageDT : Message>(
  private val protocol: ProtocolT,
  private val ownerId: String,
  private val lockDuration: Duration,
  computationTypeEnumHelper: ComputationTypeEnumHelper<ProtocolT>,
  computationProtocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
  computationProtocolStageDetailsHelper:
    ComputationProtocolStageDetailsHelper<ProtocolT, StageT, StageDT, ComputationDT>,
) :
  PostgresWriter<String?>(),
  ComputationTypeEnumHelper<ProtocolT> by computationTypeEnumHelper,
  ComputationProtocolStagesEnumHelper<ProtocolT, StageT> by computationProtocolStagesEnumHelper,
  ComputationProtocolStageDetailsHelper<
    ProtocolT, StageT, StageDT, ComputationDT
  > by computationProtocolStageDetailsHelper {

  private data class UnclaimedTaskQueryResult<StageT>(
    val computationId: Long,
    val globalId: String,
    val computationStage: StageT,
    val creationTime: Instant,
    val updateTime: Instant,
    val nextAttempt: Long
  )

  private fun buildUnclaimedTaskQueryResult(row: ResultRow): UnclaimedTaskQueryResult<StageT> =
    UnclaimedTaskQueryResult(
      row["ComputationId"],
      row["GlobalComputationId"],
      longValuesToComputationStageEnum(
        ComputationStageLongValues(row["Protocol"], row["ComputationStage"])
      ),
      row["CreationTime"],
      row["UpdateTime"],
      row["NextAttempt"]
    )

  private data class LockOwnerQueryResult(val lockOwner: String?, val updateTime: Instant)

  private fun buildLockOwnerQueryResult(row: ResultRow): LockOwnerQueryResult =
    LockOwnerQueryResult(lockOwner = row["LockOwner"], updateTime = row["UpdateTime"])

  override suspend fun TransactionScope.runTransaction(): String? {
    return listUnclaimedTasks(protocol, Instant.now())
      // First the possible tasks to claim are selected from the computations table, then for each
      // item in the list we try to claim the lock in a transaction which will only succeed if the
      // lock is still available. This pattern means only the item which is being updated
      // would need to be locked and not every possible computation that can be worked on.
      .filter { claim(it) }
      // If the value is null, no tasks were claimed.
      .firstOrNull()
      ?.globalId
  }

  private suspend fun TransactionScope.listUnclaimedTasks(
    protocol: ProtocolT,
    timestamp: Instant
  ): Flow<UnclaimedTaskQueryResult<StageT>> {
    val listUnclaimedTasksSql =
      boundStatement(
        """
      SELECT c.ComputationId,  c.GlobalComputationId,
             c.Protocol, c.ComputationStage, c.UpdateTime,
             c.CreationTime, cs.NextAttempt
      FROM Computations AS c
        JOIN ComputationStages AS cs
      ON c.Protocol = $1
        AND c.LockExpirationTime IS NOT NULL
        AND c.LockExpirationTime <= $2
      ORDER BY c.CreationTime ASC, c.LockExpirationTime ASC, c.UpdateTime ASC
      LIMIT 50;
      """
      ) {
        bind("$1", protocolEnumToLong(protocol))
        bind("$2", timestamp)
      }

    return transactionContext
      .executeQuery(listUnclaimedTasksSql)
      .consume(::buildUnclaimedTaskQueryResult)
  }

  /**
   * Tries to claim a specific computation for an owner, returning the result of the attempt. If a
   * lock is acquired a new row is written to the ComputationStageAttempts table.
   */
  private suspend fun TransactionScope.claim(
    unclaimedTask: UnclaimedTaskQueryResult<StageT>
  ): Boolean {
    val currentLockOwner = readLockOwner(unclaimedTask.computationId)
    // Verify that the row hasn't been updated since the previous, non-transactional read.
    // If it has been updated since that time the lock should not be acquired.
    if (currentLockOwner.updateTime != unclaimedTask.updateTime) return false

    val writeTime = Instant.now()
    setLock(unclaimedTask.computationId, ownerId, writeTime, lockDuration)

    insertComputationStageAttempt(
      unclaimedTask.computationId,
      unclaimedTask.computationStage,
      unclaimedTask.nextAttempt,
      beginTime = writeTime,
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )

    updateComputationStage(
      unclaimedTask.computationId,
      unclaimedTask.computationStage,
      nextAttempt = unclaimedTask.nextAttempt + 1
    )

    if (currentLockOwner.lockOwner != null) {
      // The current attempt is the one before the nextAttempt
      val currentAttempt = unclaimedTask.nextAttempt - 1
      val details =
        readComputationStageDetails(
          unclaimedTask.computationId,
          unclaimedTask.computationStage,
          currentAttempt
        )
      // If the computation was locked, but that lock was expired we need to finish off the
      // current attempt of the stage.
      updateComputationStageAttempt(
        localId = unclaimedTask.computationId,
        stage = unclaimedTask.computationStage,
        attempt = currentAttempt,
        endTime = writeTime,
        details =
          details.copy { reasonEnded = ComputationStageAttemptDetails.EndReason.LOCK_OVERWRITTEN }
      )
    }
    // The lock was acquired.
    return true
  }

  private suspend fun TransactionScope.readLockOwner(computationId: Long): LockOwnerQueryResult {
    val readLockOwnerSql =
      boundStatement(
        """
      SELECT LockOwner, UpdateTime
      FROM Computations
      WHERE
        ComputationId = $1;
      """
      ) {
        bind("$1", computationId)
      }
    return transactionContext
      .executeQuery(readLockOwnerSql)
      .consume(::buildLockOwnerQueryResult)
      .firstOrNull()
      ?: throw ComputationNotFoundException(computationId)
  }

  private suspend fun TransactionScope.insertComputationStageAttempt(
    localId: Long,
    stage: StageT,
    attempt: Long,
    beginTime: Instant,
    endTime: Instant? = null,
    details: ComputationStageAttemptDetails
  ) {
    val insertComputationStageAttemptSql =
      boundStatement(
        """
      INSERT INTO ComputationStageAttempts
        (
          ComputationId,
          ComputationStage,
          Attempt,
          BeginTime,
          EndTime,
          Details,
          DetailsJson
        )
      VALUES ($1, $2, $3, $4, $5, $6, $7::jsonb);
      """
      ) {
        bind("$1", localId)
        bind("$2", computationStageEnumToLongValues(stage).stage)
        bind("$3", attempt)
        bind("$4", beginTime)
        bind("$5", endTime)
        bind("$6", details.toByteArray())
        bind("$7", details.toJson())
      }

    transactionContext.executeStatement(insertComputationStageAttemptSql)
  }

  private suspend fun TransactionScope.updateComputation(
    localId: Long,
    updateTime: Instant,
    creationTime: Instant? = null,
    globalId: String? = null,
    protocol: ProtocolT? = null,
    stage: StageT? = null,
    lockOwner: String? = null,
    lockExpirationTime: Instant? = null,
    details: ComputationDT? = null
  ) {
    val updateComputationSql =
      boundStatement(
        """
      UPDATE Computations SET
        ComputationId = COALESCE($1, ComputationId),
        CreationTime = COALESCE($2, CreationTime),
        UpdateTime = COALESCE($3, UpdateTime),
        GlobalComputationId = COALESCE($4, GlobalComputationId),
        Protocol = COALESCE($5, Protocol),
        ComputationStage = COALESCE($6, ComputationStage),
        LockOwner = COALESCE($7, LockOwner),
        LockExpirationTime = COALESCE($8, LockExpirationTime),
        ComputationDetails = COALESCE($9, ComputationDetails),
        ComputationDetailsJson = COALESCE($10::jsonb, ComputationDetailsJson)
      WHERE
        ComputationId = $1;
      """
      ) {
        bind("$1", localId)
        bind("$2", creationTime)
        bind("$3", updateTime)
        bind("$4", globalId)
        bind("$5", protocol?.let { protocolEnumToLong(it) })
        bind("$6", stage?.let { computationStageEnumToLongValues(it).stage })
        bind("$7", lockOwner)
        bind("$8", lockExpirationTime)
        bind("$9", details?.toByteArray())
        bind("$10", details?.toJson())
      }

    transactionContext.executeStatement(updateComputationSql)
  }

  private suspend fun TransactionScope.updateComputationStage(
    localId: Long,
    stage: StageT,
    nextAttempt: Long? = null,
    creationTime: Instant? = null,
    endTime: Instant? = null,
    previousStage: StageT? = null,
    followingStage: StageT? = null,
    details: StageDT? = null
  ) {
    val updateComputationStageSql =
      boundStatement(
        """
      UPDATE ComputationStages SET
        ComputationId = COALESCE($1, ComputationId),
        ComputationStage = COALESCE($2, ComputationStage),
        CreationTime = COALESCE($3, CreationTime),
        NextAttempt = COALESCE($4, NextAttempt),
        EndTime = COALESCE($5, EndTime),
        PreviousStage = COALESCE($6, PreviousStage),
        FollowingStage = COALESCE($7, FollowingStage),
        Details = COALESCE($8, Details),
        DetailsJSON = COALESCE($9::jsonb, DetailsJSON)
      WHERE
        ComputationId = $1;
      """
      ) {
        bind("$1", localId)
        bind("$2", computationStageEnumToLongValues(stage).stage)
        bind("$3", creationTime)
        bind("$4", nextAttempt)
        bind("$5", endTime)
        bind("$6", previousStage?.let { computationStageEnumToLongValues(it).stage })
        bind("$7", followingStage?.let { computationStageEnumToLongValues(it).stage })
        bind("$8", details?.toByteArray())
        bind("$9", details?.toJson())
      }

    transactionContext.executeStatement(updateComputationStageSql)
  }

  private suspend fun TransactionScope.updateComputationStageAttempt(
    localId: Long,
    stage: StageT,
    attempt: Long,
    beginTime: Instant? = null,
    endTime: Instant? = null,
    details: ComputationStageAttemptDetails? = null
  ) {
    val updateComputationStageAttemptsSql =
      boundStatement(
        """
      UPDATE ComputationStageAttempts SET
        ComputationId = COALESCE($1, ComputationId),
        ComputationStage = COALESCE($2, ComputationStage),
        Attempt = COALESCE($3, Attempt),
        BeginTime = COALESCE($4, BeginTime),
        EndTime = COALESCE($5, EndTime),
        Details = COALESCE($6, Details),
        DetailsJSON = COALESCE($7::jsonb, DetailsJSON)
      WHERE
        ComputationId = $1;
      """
      ) {
        bind("$1", localId)
        bind("$2", computationStageEnumToLongValues(stage).stage)
        bind("$3", attempt)
        bind("$4", beginTime)
        bind("$5", endTime)
        bind("$6", details?.toByteArray())
        bind("$7", details?.toJson())
      }

    transactionContext.executeStatement(updateComputationStageAttemptsSql)
  }

  private suspend fun TransactionScope.setLock(
    computationId: Long,
    ownerId: String,
    writeTime: Instant,
    lockDuration: Duration
  ) {
    updateComputation(
      localId = computationId,
      updateTime = writeTime,
      lockOwner = ownerId,
      lockExpirationTime = writeTime.plus(lockDuration),
    )
  }

  private suspend fun TransactionScope.readComputationStageDetails(
    computationId: Long,
    stage: StageT,
    currentAttempt: Long
  ): ComputationStageAttemptDetails {
    val readComputationStageDetailsSql =
      boundStatement(
        """
      SELECT Details
      FROM ComputationStageAttempts
      WHERE
        ComputationId = $1,
        ComputationStage = $2,
        Attempt = $3
      """
      ) {
        bind("$1", computationId)
        bind("$2", computationStageEnumToLongValues(stage).stage)
        bind("$3", currentAttempt)
      }

    return transactionContext
      .executeQuery(readComputationStageDetailsSql)
      .consume { it.getProtoMessage("Details", ComputationStageAttemptDetails.parser()) }
      .firstOrNull()
      ?: throw ComputationDetailsNotFoundException(computationId, stage.toString(), currentAttempt)
  }
}
