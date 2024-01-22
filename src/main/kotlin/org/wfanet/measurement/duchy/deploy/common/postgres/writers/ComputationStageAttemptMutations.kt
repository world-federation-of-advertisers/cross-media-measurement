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
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/** Inserts a new row in the Postgres ComputationStageAttempt table. */
suspend fun PostgresWriter.TransactionScope.insertComputationStageAttempt(
  localComputationId: Long,
  stage: Long,
  attempt: Long,
  beginTime: Instant,
  endTime: Instant? = null,
  details: ComputationStageAttemptDetails,
) {
  val sql =
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
      bind("$1", localComputationId)
      bind("$2", stage)
      bind("$3", attempt)
      bind("$4", beginTime)
      bind("$5", endTime)
      bind("$6", details.toByteArray())
      bind("$7", details.toJson())
    }

  transactionContext.executeStatement(sql)
}

/** Updates a row in the Postgres ComputationStageAttempt table. */
suspend fun PostgresWriter.TransactionScope.updateComputationStageAttempt(
  localId: Long,
  stage: Long,
  attempt: Long,
  beginTime: Instant? = null,
  endTime: Instant? = null,
  details: Message? = null,
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
