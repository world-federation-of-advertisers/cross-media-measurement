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

package org.wfanet.measurement.duchy.deploy.postgres.readers

import kotlinx.coroutines.flow.firstOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.internal.duchy.ExternalRequisitionKey

class RequisitionReader {
  data class RequisitionResult(val computationId: Long, val requisitionId: Long) {
    constructor(
      row: ResultRow
    ) : this(
      computationId = row.get<Long>("ComputationId"),
      requisitionId = row.get<Long>("RequisitionId")
    )
  }

  suspend fun readRequisitionByExternalKey(
    readContext: ReadContext,
    key: ExternalRequisitionKey
  ): RequisitionResult? {
    val sql =
      boundStatement(
        """
        SELECT ComputationId, RequisitionId
        FROM Requisitions
        WHERE
          ExternalRequisitionId = $1
        AND
         RequisitionFingerprint = $2
      """
          .trimIndent()
      ) {
        bind("$1", key.externalRequisitionId)
        bind("$2", key.externalRequisitionIdBytes.toByteArray())
      }
    return readContext.executeQuery(sql).consume(::RequisitionResult).firstOrNull()
  }
}
