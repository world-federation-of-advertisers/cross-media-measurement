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

package org.wfanet.measurement.duchy.deploy.common.postgres.readers

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/** Performs read operations on ComputationStageAttempts tables */
class ComputationStageAttemptReader {

  data class UnfinishedAttempt(
    val computationId: Long,
    val computationStage: Long,
    val attempt: Long,
    val details: ComputationStageAttemptDetails,
  ) {
    constructor(
      row: ResultRow
    ) : this(
      computationId = row["ComputationId"],
      computationStage = row["ComputationStage"],
      attempt = row["Attempt"],
      details = row.getProtoMessage("Details", ComputationStageAttemptDetails.parser()),
    )
  }

  /**
   * Reads a [ComputationStageAttemptDetails] by localComputationId and stage.
   *
   * @param readContext The transaction context for reading from the Postgres database.
   * @param localId The local identifier for the target computation.
   * @param stage The target stage of this computation.
   * @return [ComputationStageAttemptDetails] when computation stage details is found, or null.
   */
  suspend fun readComputationStageDetails(
    readContext: ReadContext,
    localId: Long,
    stage: Long,
    currentAttempt: Long,
  ): ComputationStageAttemptDetails? {
    val readComputationStageDetailsSql =
      boundStatement(
        """
      SELECT Details
      FROM ComputationStageAttempts
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
        bind("$3", currentAttempt)
      }

    return readContext
      .executeQuery(readComputationStageDetailsSql)
      .consume { it.getProtoMessage("Details", ComputationStageAttemptDetails.parser()) }
      .firstOrNull()
  }

  suspend fun readUnfinishedAttempts(
    readContext: ReadContext,
    localComputationId: Long,
  ): Flow<UnfinishedAttempt> {
    val sql =
      boundStatement(
        """
      SELECT ComputationId, ComputationStage, Attempt, Details
      FROM ComputationStageAttempts
      WHERE ComputationId = $1
        AND EndTime IS NULL
      """
      ) {
        bind("$1", localComputationId)
      }
    return readContext.executeQuery(sql).consume(::UnfinishedAttempt)
  }
}
