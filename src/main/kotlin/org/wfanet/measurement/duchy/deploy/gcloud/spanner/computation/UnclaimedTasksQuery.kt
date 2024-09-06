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

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.duchy.db.computation.ComputationStageLongValues
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.common.SqlBasedQuery
import org.wfanet.measurement.gcloud.spanner.statement

/** Queries for computations which may be claimed at a timestamp. */
class UnclaimedTasksQuery<StageT>(
  protocol: Long,
  prioritizedStages: List<StageT>,
  val longValuesToComputationStageEnum: (ComputationStageLongValues) -> StageT,
  computationStageEnumToLongValues: (StageT) -> ComputationStageLongValues,
  timestamp: Timestamp,
) : SqlBasedQuery<UnclaimedTaskQueryResult<StageT>> {
  companion object {
    // The column CreationTime is nullable due to later change to the schema. The timestamp of null
    // is regarded as the oldest one (the smallest value).
    private const val parameterizedQueryString =
      """
      SELECT
        ComputationId,
        GlobalComputationId,
        Protocol,
        ComputationStage,
        UpdateTime,
        Computations.CreationTime,
        NextAttempt,
        CASE WHEN ComputationStage IN UNNEST(@prioritized_stages) THEN 0 ELSE 1 END AS Priority
      FROM
        Computations@{
          spanner_emulator.disable_query_null_filtered_index_check=true
        }
        JOIN ComputationStages USING(ComputationId, ComputationStage)
      WHERE
          Protocol = @protocol
          AND LockExpirationTime IS NOT NULL
          AND UpdateTime IS NOT NULL
          AND LockExpirationTime <= @current_time
      ORDER BY
        Priority,
        CreationTime,
        LockExpirationTime,
        UpdateTime
      LIMIT 1
      """
  }

  override val sql: Statement =
    statement(parameterizedQueryString) {
      bind("current_time").to(timestamp)
      bind("protocol").to(protocol)

      val prioritizedStageLongValues =
        prioritizedStages.map(computationStageEnumToLongValues).map { it.stage }
      bind("prioritized_stages").toInt64Array(prioritizedStageLongValues)
    }

  override fun asResult(struct: Struct): UnclaimedTaskQueryResult<StageT> =
    UnclaimedTaskQueryResult(
      computationId = struct.getLong("ComputationId"),
      globalId = struct.getString("GlobalComputationId"),
      computationStage =
        longValuesToComputationStageEnum(
          ComputationStageLongValues(struct.getLong("Protocol"), struct.getLong("ComputationStage"))
        ),
      creationTime = struct.getTimestamp("CreationTime"),
      updateTime = struct.getTimestamp("UpdateTime"),
      nextAttempt = struct.getLong("NextAttempt"),
    )
}

/** @see [UnclaimedTasksQuery.asResult] . */
data class UnclaimedTaskQueryResult<StageT>(
  val computationId: Long,
  val globalId: String,
  val computationStage: StageT,
  val creationTime: Timestamp,
  val updateTime: Timestamp,
  val nextAttempt: Long,
)
