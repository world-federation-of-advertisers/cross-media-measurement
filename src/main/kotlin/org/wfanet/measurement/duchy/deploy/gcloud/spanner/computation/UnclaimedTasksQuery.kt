// Copyright 2020 The Measurement System Authors
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

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues

/** Queries for computations which may be claimed at a timestamp. */
class UnclaimedTasksQuery<StageT>(
  val protocol: Long,
  val parseStageEnum: (ComputationStageLongValues) -> StageT,
  timestamp: Timestamp
) : SqlBasedQuery<UnclaimedTaskQueryResult<StageT>> {
  companion object {
    private const val parameterizedQueryString =
      """
      SELECT c.ComputationId,  c.GlobalComputationId,
             c.Protocol, c.ComputationStage, c.UpdateTime,
             cs.NextAttempt
      FROM Computations@
        {
          FORCE_INDEX=ComputationsByLockExpirationTime,
          spanner_emulator.disable_query_null_filtered_index_check=true
        } AS c
      JOIN ComputationStages AS cs USING(ComputationId, ComputationStage)
      WHERE c.Protocol = @protocol
        AND c.LockExpirationTime IS NOT NULL
        AND c.UpdateTime IS NOT NULL
        AND c.LockExpirationTime <= @current_time
      ORDER BY c.LockExpirationTime ASC, c.UpdateTime ASC
      LIMIT 50
      """
  }
  override val sql: Statement =
    Statement.newBuilder(parameterizedQueryString)
      .bind("current_time").to(timestamp)
      .bind("protocol").to(protocol)
      .build()
  override fun asResult(struct: Struct): UnclaimedTaskQueryResult<StageT> =
    UnclaimedTaskQueryResult(
      computationId = struct.getLong("ComputationId"),
      globalId = struct.getString("GlobalComputationId"),
      computationStage = parseStageEnum(
        ComputationStageLongValues(
          struct.getLong("Protocol"),
          struct.getLong("ComputationStage")
        )
      ),
      updateTime = struct.getTimestamp("UpdateTime"),
      nextAttempt = struct.getLong("NextAttempt")
    )
}

/** @see [UnclaimedTasksQuery.asResult] .*/
data class UnclaimedTaskQueryResult<StageT>(
  val computationId: Long,
  val globalId: String,
  val computationStage: StageT,
  val updateTime: Timestamp,
  val nextAttempt: Long
)
