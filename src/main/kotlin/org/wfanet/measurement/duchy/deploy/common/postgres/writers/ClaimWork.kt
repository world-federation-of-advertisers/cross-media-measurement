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

import java.time.Clock
import java.time.Duration
import java.time.Instant
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.filter
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.duchy.db.computation.ComputationProtocolStagesEnumHelper
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.db.computation.ComputationTypeEnumHelper
import org.wfanet.measurement.duchy.deploy.common.postgres.readers.ComputationStageAttemptReader
import org.wfanet.measurement.duchy.service.internal.ComputationNotFoundException
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails
import org.wfanet.measurement.internal.duchy.copy

/**
 * [PostgresWriter] to claim one ready for processing task for an owner.
 *
 * @param protocol The protocol of the task to claim
 * @param ownerId The identifier of the worker process that will own the lock.
 * @param lockDuration The [Duration] that a worker holds the computation lock.
 * @param clock See [Clock].
 * @param computationTypeEnumHelper See [ComputationTypeEnumHelper].
 * @param protocolStagesEnumHelper See [ComputationProtocolStagesEnumHelper].
 * @return [String] a global computation id of work that was claimed.
 * @return null when no task was claimed.
 *
 * Throws following exceptions on [execute]:
 * * [ComputationNotFoundException] when computation could not be found
 * * [IllegalStateException] when computation details could not be found
 */
class ClaimWork<ProtocolT, StageT>(
  private val protocol: ProtocolT,
  private val ownerId: String,
  private val lockDuration: Duration,
  private val clock: Clock,
  private val computationTypeEnumHelper: ComputationTypeEnumHelper<ProtocolT>,
  private val protocolStagesEnumHelper: ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
) : PostgresWriter<String?>() {

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
      protocolStagesEnumHelper.longValuesToComputationStageEnum(
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
    return listUnclaimedTasks(protocol, clock.instant())
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
        bind("$1", computationTypeEnumHelper.protocolEnumToLong(protocol))
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

    val writeTime = clock.instant()
    acquireComputationLock(
      unclaimedTask.computationId,
      writeTime,
      ownerId,
      writeTime.plus(lockDuration)
    )
    val stageLongValue =
      protocolStagesEnumHelper
        .computationStageEnumToLongValues(unclaimedTask.computationStage)
        .stage

    insertComputationStageAttempt(
      unclaimedTask.computationId,
      stageLongValue,
      unclaimedTask.nextAttempt,
      beginTime = writeTime,
      details = ComputationStageAttemptDetails.getDefaultInstance()
    )

    updateComputationStage(
      unclaimedTask.computationId,
      stageLongValue,
      nextAttempt = unclaimedTask.nextAttempt + 1
    )

    if (currentLockOwner.lockOwner != null) {
      // The current attempt is the one before the nextAttempt
      val currentAttempt = unclaimedTask.nextAttempt - 1
      val details =
        ComputationStageAttemptReader()
          .readComputationStageDetails(
            transactionContext,
            unclaimedTask.computationId,
            stageLongValue,
            currentAttempt
          )
          ?: throw IllegalStateException("Computation stage details is missing.")
      // If the computation was locked, but that lock was expired we need to finish off the
      // current attempt of the stage.
      updateComputationStageAttempt(
        localId = unclaimedTask.computationId,
        stage = stageLongValue,
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
}
