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

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.ReadWriteContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.copy
import org.wfanet.measurement.internal.reporting.reportingSet

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
      DisplayName
    FROM
      ReportingSets
    """

  fun translate(row: ResultRow): Result =
    Result(row["MeasurementConsumerReferenceId"], row["ReportingSetId"], buildReportingSet(row))

  suspend fun readReportingSetByExternalId(
    readWriteContext: ReadWriteContext,
    measurementConsumerReferenceId: String,
    externalReportingSetId: ExternalId
  ): Result? {
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

    return readWriteContext.executeQuery(statement).consume(::translate).singleOrNull()
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
        readContext.executeQuery(statement).consume(::translate).collect { reportingSetResult ->
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
    }
  }
}
