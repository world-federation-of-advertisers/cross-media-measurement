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

package org.wfanet.measurement.reporting.deploy.v2.postgres.readers;

import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement

class ReportingSetReader(private val readContext: ReadContext) {
  data class IdResult(
      val measurementConsumerId: Long,
      val reportingSetId: Long,
  )

  suspend fun readId(
    measurementConsumerId: Long,
    externalReportingSetId: Long,
  ): IdResult? {
    val statement = boundStatement(
        """
        SELECT
          MeasurementConsumerId,
          ReportingSetId
        FROM ReportingSets
        WHERE MeasurementConsumerId = $1
          AND ExternalReportingSetId = $2
        """
    ) {
      bind("$1", measurementConsumerId)
      bind("$2", externalReportingSetId)
    }

    return readContext.executeQuery(statement).consume { row: ResultRow ->
      IdResult(row["MeasurementConsumerId"], row["ExternalReportingSetId"])
    }.singleOrNull()
  }
}
