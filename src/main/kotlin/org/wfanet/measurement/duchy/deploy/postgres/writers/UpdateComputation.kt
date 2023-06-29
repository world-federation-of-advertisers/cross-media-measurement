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

import java.time.Duration
import java.time.Instant
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter


//suspend fun <ProtocolT, StageT, ComputationDT : Message> PostgresWriter.TransactionScope.updateComputation(
//  localId: Long,
//  stage: StageT,
//  updateTime: Instant,
//  creationTime: Instant? = null,
//  globalComputationId: String? = null,
//  protocol: ProtocolT? = null,
//  lockOwner: String? = null,
//  lockExpirationTime: Instant? = null,
//  details: ComputationDT? = null,
//  computationTypeEnumHelper: ComputationTypeEnumHelper<ProtocolT>,
//  computationProtocolStagesEnumHelper:
//  ComputationProtocolStagesEnumHelper<ProtocolT, StageT>,
//) {
//
//  val sql =
//    boundStatement(
//      """
//      UPDATE Computations SET
//        CreationTime = COALESCE($1, CreationTime),
//        UpdateTime = COALESCE($2, UpdateTime),
//        GlobalComputationId = COALESCE($3, GlobalComputationId),
//        Protocol = COALESCE($4, Protocol),
//        LockOwner = COALESCE($5, LockOwner),
//        LockExpirationTime = COALESCE($6, LockExpirationTime),
//        ComputationDetails = COALESCE($7, ComputationDetails),
//        ComputationDetailsJson = COALESCE($8::jsonb, ComputationDetailsJson)
//      WHERE
//        ComputationId = $9 AND ComputationStage = $10
//      """
//    ) {
//      bind("$1", creationTime)
//      bind("$2", updateTime)
//      bind("$3", globalComputationId)
//      bind("$4", protocol?.let { computationTypeEnumHelper.protocolEnumToLong(it) })
//      bind("$5", lockOwner)
//      bind("$6", lockExpirationTime)
//      bind("$7", details?.toByteArray())
//      bind("$8", details?.toJson())
//      bind("$9", localId)
//      bind("$10", computationProtocolStagesEnumHelper.computationStageEnumToLongValues(stage).stage)
//    }
//
//  transactionContext.executeStatement(sql)
//}

suspend fun PostgresWriter.TransactionScope.updateComputation(
  localId: Long,
  stage: Long,
  updateTime: Instant,
  creationTime: Instant? = null,
  globalComputationId: String? = null,
  protocol: Long? = null,
  lockOwner: String? = null,
  lockExpirationTime: Instant? = null,
  details: ByteArray? = null,
  detailsJson: String? = null
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
        ComputationDetailsJson = COALESCE($8::jsonb, ComputationDetailsJson)
      WHERE
        ComputationId = $9 AND ComputationStage = $10
      """
    ) {
      bind("$1", creationTime)
      bind("$2", updateTime)
      bind("$3", globalComputationId)
      bind("$4", protocol)
      bind("$5", lockOwner)
      bind("$6", lockExpirationTime)
      bind("$7", details)
      bind("$8", detailsJson)
      bind("$9", localId)
      bind("$10", stage)
    }

  transactionContext.executeStatement(sql)
}

suspend fun PostgresWriter.TransactionScope.setLock(
  localComputationId: Long,
  ownerId: String?,
  writeTime: Instant?,
  lockDuration: Duration?
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
      bind("$1", writeTime)
      bind("$2", ownerId)
      bind("$3", writeTime?.plus(lockDuration))
      bind("$4", localComputationId)
    }
  transactionContext.executeStatement(sql)
}

suspend fun PostgresWriter.TransactionScope.updateComputationStage(
  localId: Long,
  stage: Long,
  nextAttempt: Long? = null,
  creationTime: Instant? = null,
  endTime: Instant? = null,
  previousStage: Long? = null,
  followingStage: Long? = null,
  details: ByteArray? = null,
  detailsJson: String? = null
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
      bind("$8", details)
      bind("$9", detailsJson)
    }

  transactionContext.executeStatement(sql)
}

suspend fun PostgresWriter.TransactionScope.updateComputationStageAttempt(
  localId: Long,
  stage: Long,
  attempt: Long,
  beginTime: Instant? = null,
  endTime: Instant? = null,
  details: ByteArray? = null,
  detailsJson: String? = null
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
      bind("$6", details)
      bind("$7", detailsJson)
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
  details: ByteArray? = null,
  detailsJson: String? = null
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
      bind("$8", details)
      bind("$9", detailsJson)
    }

  transactionContext.executeStatement(insertComputationStageStatement)
}
