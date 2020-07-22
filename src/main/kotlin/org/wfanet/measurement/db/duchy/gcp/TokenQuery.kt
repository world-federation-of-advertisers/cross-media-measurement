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

package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getNullableString
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.internal.duchy.ComputationDetails

/** Query for fields needed to make a [ComputationToken] .*/
class TokenQuery<StageT>(
  val parseStageEnum: (Long) -> StageT,
  globalId: Long
) :
  SqlBasedQuery<TokenQueryResult<StageT>> {
  companion object {
    private const val parameterizedQueryString =
      """
      SELECT c.ComputationId, c.LockOwner, c.ComputationStage, c.ComputationDetails,
             c.UpdateTime, cs.NextAttempt
      FROM Computations AS c
      JOIN ComputationStages AS cs USING (ComputationId, ComputationStage)
      WHERE c.GlobalComputationId = @global_id
      """
  }

  override val sql: Statement =
    Statement.newBuilder(parameterizedQueryString).bind("global_id").to(globalId).build()
  override fun asResult(struct: Struct): TokenQueryResult<StageT> =
    TokenQueryResult(
      computationId = struct.getLong("ComputationId"),
      computationStage = parseStageEnum(struct.getLong("ComputationStage")),
      lockOwner = struct.getNullableString("LockOwner"),
      updateTime = struct.getTimestamp("UpdateTime"),
      nextAttempt = struct.getLong("NextAttempt"),
      details = struct.getProtoMessage("ComputationDetails", ComputationDetails.parser())
    )
}
/** @see [TokenQuery.asResult] .*/
data class TokenQueryResult<StageT>(
  val computationId: Long,
  val lockOwner: String?,
  val computationStage: StageT,
  val details: ComputationDetails,
  val updateTime: Timestamp,
  val nextAttempt: Long
)
