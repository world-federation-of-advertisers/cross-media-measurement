package org.wfanet.measurement.db.duchy.gcp

import com.google.cloud.spanner.Statement
import com.google.cloud.spanner.Struct
import org.wfanet.measurement.db.duchy.ProtocolStageEnumHelper

/** Query for all global computation ids in database. */
class GlobalIdsQuery<StageT: Enum<StageT>>(
  protocolStageEnumHelper: ProtocolStageEnumHelper<StageT>,
  filterToStages: Set<StageT>
) : SqlBasedQuery<Long> {
  companion object {
    private const val parameterizedQuery =
      """
      SELECT GlobalComputationId FROM Computations
      WHERE ComputationStage IN UNNEST (@stages)
      """
  }
  override val sql: Statement =
  Statement.newBuilder(parameterizedQuery).bind("stages").toInt64Array(
    filterToStages.map(protocolStageEnumHelper::enumToLong).toLongArray()
  ).build()

  override fun asResult(struct: Struct): Long = struct.getLong("GlobalComputationId")
}
