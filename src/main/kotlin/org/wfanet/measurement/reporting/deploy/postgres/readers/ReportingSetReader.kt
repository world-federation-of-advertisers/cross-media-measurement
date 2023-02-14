// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.postgres.readers

import com.google.gson.JsonObject
import com.google.gson.JsonParser
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.emptyFlow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSet.EventGroupKey
import org.wfanet.measurement.internal.reporting.ReportingSetKt
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.reportingSet
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class ReportingSetReader {
  data class Result(
    val measurementConsumerReferenceId: String,
    val reportingSetId: InternalId,
    var reportingSet: ReportingSet
  )

  private val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      ReportingSetId,
      ExternalReportingSetId,
      Filter,
      DisplayName,
      (
        SELECT ARRAY(
          SELECT
            json_build_object(
              'measurementConsumerReferenceId', MeasurementConsumerReferenceId,
              'dataProviderReferenceId', DataProviderReferenceId,
              'eventGroupReferenceId', EventGroupReferenceId
            )
          FROM ReportingSetEventGroups
          WHERE ReportingSetEventGroups.MeasurementConsumerReferenceId = ReportingSets.MeasurementConsumerReferenceId
          AND ReportingSetEventGroups.ReportingSetId = ReportingSets.ReportingSetId
        )
      ) AS EventGroups
    FROM
      ReportingSets
    """

  fun translate(row: ResultRow): Result =
    Result(row["MeasurementConsumerReferenceId"], row["ReportingSetId"], buildReportingSet(row))

  /**
   * Reads a Reporting Set using external ID.
   *
   * Throws a subclass of [ReportingInternalException].
   *
   * @throws [ReportingSetNotFoundException] Reporting Set not found.
   */
  suspend fun readReportingSetByExternalId(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    externalReportingSetId: ExternalId
  ): Result {
    val statement =
      boundStatement(
        baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ExternalReportingSetId = $2
        """
      ) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", externalReportingSetId)
      }

    return readContext.executeQuery(statement).consume(::translate).firstOrNull()
      ?: throw ReportingSetNotFoundException()
  }

  fun listReportingSets(
    client: DatabaseClient,
    filter: StreamReportingSetsRequest.Filter,
    limit: Int = 0
  ): Flow<Result> {
    val statement =
      boundStatement(
        baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ExternalReportingSetId > $2
        ORDER BY ExternalReportingSetId ASC
        LIMIT $3
        """
      ) {
        bind("$1", filter.measurementConsumerReferenceId)
        bind("$2", filter.externalReportingSetIdAfter)
        if (limit > 0) {
          bind("$3", limit)
        } else {
          bind("$3", 50)
        }
      }

    return flow {
      val readContext = client.readTransaction()
      try {
        emitAll(readContext.executeQuery(statement).consume(::translate))
      } finally {
        readContext.close()
      }
    }
  }

  fun getReportingSetsByExternalIds(
    client: DatabaseClient,
    measurementConsumerReferenceId: String,
    externalReportingSetIds: List<Long>
  ): Flow<Result> {
    val sql =
      StringBuilder(
        baseSql +
          """
        WHERE MeasurementConsumerReferenceId = $1
          AND ExternalReportingSetId IN
        """
      )

    if (externalReportingSetIds.isEmpty()) {
      return emptyFlow()
    }

    var i = 2
    val bindingMap = mutableMapOf<Long, String>()
    val inList =
      externalReportingSetIds.joinToString(separator = ",", prefix = "(", postfix = ")") {
        val index = "$$i"
        bindingMap[it] = "$$i"
        i++
        index
      }
    sql.append(inList)

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", measurementConsumerReferenceId)

        externalReportingSetIds.forEach { bind(bindingMap.getValue(it), it) }
      }

    return flow {
      val readContext = client.readTransaction()
      try {
        emitAll(readContext.executeQuery(statement).consume(::translate))
      } finally {
        readContext.close()
      }
    }
  }

  private fun buildReportingSet(row: ResultRow): ReportingSet {
    return reportingSet {
      measurementConsumerReferenceId = row["MeasurementConsumerReferenceId"]
      externalReportingSetId = row["ExternalReportingSetId"]
      filter = row["Filter"]
      displayName = row["DisplayName"]
      val eventGroupsArr = row.get<Array<String>>("EventGroups")
      eventGroupsArr.forEach {
        eventGroupKeys += buildEventGroupKey(JsonParser.parseString(it).asJsonObject)
      }
    }
  }

  private fun buildEventGroupKey(eventGroup: JsonObject): EventGroupKey {
    return ReportingSetKt.eventGroupKey {
      measurementConsumerReferenceId =
        eventGroup.getAsJsonPrimitive("measurementConsumerReferenceId").asString
      dataProviderReferenceId = eventGroup.getAsJsonPrimitive("dataProviderReferenceId").asString
      eventGroupReferenceId = eventGroup.getAsJsonPrimitive("eventGroupReferenceId").asString
    }
  }
}
