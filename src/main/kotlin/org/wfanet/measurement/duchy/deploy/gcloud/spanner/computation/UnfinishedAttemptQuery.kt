// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.deploy.gcloud.spanner.computation

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.gcloud.spanner.getProtoMessage
import org.wfanet.measurement.internal.duchy.ComputationStageAttemptDetails

/** Queries for the attempts of stages for a computation that do not have an end time. */
class UnfinishedAttemptQuery<StageT>(
  val parseStageEnum: (ComputationStageLongValues) -> StageT,
  val localId: Long
) : SqlBasedQuery<UnfinishedAttemptQueryResult<StageT>> {
  companion object {
    private const val parameterizedQueryString =
      """
      SELECT s.ComputationStage, s.Attempt, s.Details, c.Protocol
      FROM ComputationStageAttempts as s
      JOIN Computations AS c USING(ComputationId)
      WHERE ComputationId = @local_id
        AND EndTime IS NULL
      """
  }
  override val sql: Statement =
    Statement.newBuilder(parameterizedQueryString).bind("local_id").to(localId).build()
  override fun asResult(struct: Struct): UnfinishedAttemptQueryResult<StageT> =
    UnfinishedAttemptQueryResult(
      computationId = localId,
      stage = parseStageEnum(
        ComputationStageLongValues(
          struct.getLong("Protocol"),
          struct.getLong("ComputationStage")
        )
      ),
      attempt = struct.getLong("Attempt"),
      details = struct.getProtoMessage("Details", ComputationStageAttemptDetails.parser())
    )
}

/** @see [UnfinishedAttemptQuery.asResult] .*/
data class UnfinishedAttemptQueryResult<StageT>(
  val computationId: Long,
  val stage: StageT,
  val attempt: Long,
  val details: ComputationStageAttemptDetails
)
