package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.Timestamp
import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.gcp.getNullableString
import org.wfanet.measurement.db.gcp.getProtoMessage
import org.wfanet.measurement.internal.db.gcp.ComputationDetails

/** Query for fields needed to make a [ComputationToken] .*/
class TokenQuery<StateT>(
  val parseStageEnum: (Long) -> StateT,
  globalId: Long
) :
  SqlBasedQuery<TokenQueryResult<StateT>> {
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
  override fun asResult(struct: Struct): TokenQueryResult<StateT> =
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
data class TokenQueryResult<StateT>(
  val computationId: Long,
  val lockOwner: String?,
  val computationStage: StateT,
  val details: ComputationDetails,
  val updateTime: Timestamp,
  val nextAttempt: Long
)
