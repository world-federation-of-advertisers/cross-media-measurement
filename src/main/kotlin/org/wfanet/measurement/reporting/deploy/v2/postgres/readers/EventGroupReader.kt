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
import org.wfanet.measurement.common.identity.InternalId

class EventGroupReader(private val readContext: ReadContext) {
  data class Result(
    val cmmsDataProviderId: String,
    val cmmsEventGroupId: String,
    val measurementConsumerId: InternalId,
    val eventGroupId: InternalId,
  )

  data class CmmsEventGroupKey(val cmmsDataProviderId: String, val cmmsEventGroupId: String)

  private val baseSql: String =
    """
    SELECT
      MeasurementConsumerId,
      EventGroupId,
      CmmsDataProviderId,
      CmmsEventGroupId
    FROM EventGroups
    """
      .trimIndent()

  private fun translate(row: ResultRow): Result {
    return Result(
      cmmsDataProviderId = row["CmmsDataProviderId"],
      cmmsEventGroupId = row["CmmsEventGroupId"],
      measurementConsumerId = row["MeasurementConsumerId"],
      eventGroupId = row["EventGroupId"],
    )
  }

  suspend fun getByCmmsEventGroupKey(
    cmmsEventGroupKeys: Collection<CmmsEventGroupKey>
  ): Flow<Result> {
    if (cmmsEventGroupKeys.isEmpty()) {
      return emptyFlow()
    }

    val sql = StringBuilder("$baseSql WHERE (CmmsDataProviderId, CmmsEventGroupId) IN ")

    var i = 1
    val bindingMap = mutableMapOf<CmmsEventGroupKey, Int>()
    val inList =
      cmmsEventGroupKeys.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = i
        bindingMap[it] = i
        i += 2
        "($$index, $${index + 1})"
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        cmmsEventGroupKeys.forEach {
          val parameter: Int = bindingMap.getValue(it)
          bind("$$parameter", it.cmmsDataProviderId)
          bind("$${parameter + 1}", it.cmmsEventGroupId)
        }
      }

    return readContext.executeQuery(statement).consume(::translate)
  }
}
