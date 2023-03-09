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
import org.wfanet.measurement.duchy.db.computation.ComputationTypes
import org.wfanet.measurement.duchy.deploy.gcloud.spanner.common.SqlBasedQuery
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.duchy.ComputationTypeEnum.ComputationType

/** Query for all global computation ids in database filtered by stage and update time. */
class GlobalIdsQuery<StageT>(
  toComputationStageLongValuesFunc: (StageT) -> ComputationStageLongValues,
  filterToStages: Set<StageT>,
  computationType: ComputationType,
  updatedBefore: Timestamp? = null
) : SqlBasedQuery<String> {
  companion object {
    private const val parameterizedQuery =
      """
      SELECT GlobalComputationId FROM Computations
      WHERE ComputationStage IN UNNEST (@stages)
      AND Protocol = @protocol
      """
  }
  override val sql: Statement =
    statement(parameterizedQuery) {
      bind("stages")
        .toInt64Array(
          filterToStages.map { toComputationStageLongValuesFunc(it).stage }.toLongArray()
        )
      bind("protocol").to(ComputationTypes.protocolEnumToLong(computationType))
      if (updatedBefore != null) {
        appendClause(
          """
        AND UpdateTime <= @updatedBefore
        """
            .trimIndent()
        )
        bind("updatedBefore").to(updatedBefore)
      }
    }

  override fun asResult(struct: Struct): String = struct.getString("GlobalComputationId")
}
