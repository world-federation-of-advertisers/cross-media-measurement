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

import io.r2dbc.spi.Row
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.collect
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder.Companion.statementBuilder
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.reportingSet

class ReportingSetReader : PostgresReader<ReportingSetReader.Result>() {
  data class Result(
    val measurementConsumerReferenceId: String,
    val reportingSetId: InternalId,
    var reportingSet: ReportingSet
  )

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      ReportingSetId,
      ExternalReportingSetId,
      Filter,
      DisplayName
    FROM
      ReportingSets
    """

  override fun translate(row: Row): Result =
    Result(
      row.get("MeasurementConsumerReferenceId", String::class.java) as String,
      InternalId(row.get("ReportingSetId", java.lang.Long::class.java) as Long),
      buildReportingSet(row)
    )

  fun listReportingSets(
    client: DatabaseClient,
    filter: StreamReportingSetsRequest.Filter,
    limit: Int = 0
  ): Flow<Result> {
    return flow {
      val readContext = client.readTransaction()
      val builder =
        statementBuilder(
          baseSql +
            """
      WHERE MeasurementConsumerReferenceId = CAST($1 AS text)
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

      execute(readContext, builder).collect { reportingSetResult ->
        reportingSetResult.reportingSet =
          reportingSetResult.reportingSet.copy {
            ReportingSetEventGroupReader()
              .listEventGroupKeys(
                readContext,
                reportingSetResult.measurementConsumerReferenceId,
                reportingSetResult.reportingSetId
              )
              .collect { eventGroupResult -> eventGroupKeys += eventGroupResult.eventGroupKey }
          }
        emit(reportingSetResult)
      }

      readContext.close()
    }
  }

  private fun buildReportingSet(row: Row): ReportingSet {
    return reportingSet {
      measurementConsumerReferenceId =
        row.get("MeasurementConsumerReferenceId", String::class.java) as String
      externalReportingSetId = row.get("ExternalReportingSetId", java.lang.Long::class.java) as Long
      filter = row.get("Filter", String::class.java) as String
      displayName = row.get("DisplayName", String::class.java) as String
    }
  }
}
