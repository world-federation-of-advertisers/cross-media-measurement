/*
 * Copyright 2023 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.deploy.v2.postgres.readers

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId

class ReportingSetReader(private val readContext: ReadContext) {
  data class ReportingSetIds(
    val measurementConsumerId: InternalId,
    val reportingSetId: InternalId,
    val externalReportingSetId: ExternalId,
  )

  suspend fun readIds(
    measurementConsumerId: InternalId,
    externalReportingSetIds: Collection<ExternalId>
  ): Flow<ReportingSetIds> {
    if (externalReportingSetIds.isEmpty()) {
      return emptyFlow()
    }

    val sql =
      StringBuilder(
        """
        SELECT
          MeasurementConsumerId,
          ReportingSetId,
          ExternalReportingSetId
        FROM ReportingSets
        WHERE MeasurementConsumerId = $1
          AND ExternalReportingSetId IN
        """
      )

    var i = 2
    val bindingMap = mutableMapOf<ExternalId, String>()
    val inList =
      externalReportingSetIds.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = index
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerId)
        externalReportingSetIds.forEach { bind(bindingMap.getValue(it), it) }
      }

    return readContext.executeQuery(statement).consume { row: ResultRow ->
      ReportingSetIds(
        row["MeasurementConsumerId"],
        row["ReportingSetId"],
        row["ExternalReportingSetId"]
      )
    }
  }
}
