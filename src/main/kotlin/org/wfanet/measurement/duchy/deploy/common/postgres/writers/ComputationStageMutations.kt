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
import java.time.Instant
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.toJson

/** Inserts a new row into the Postgres ComputationStages table. */
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

/**
 * Updates a row in the Postgres ComputationStages table.
 *
 * If an argument is null, its corresponding field in the database will not be updated.
 */
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
