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
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.StatementBuilder.Companion.statementBuilder
import org.wfanet.measurement.common.db.r2dbc.getValue
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.ReportingSet
import org.wfanet.measurement.internal.reporting.ReportingSetKt

class ReportingSetEventGroupReader : PostgresReader<ReportingSetEventGroupReader.Result>() {
  data class Result(val eventGroupKey: ReportingSet.EventGroupKey)

  override val baseSql: String =
    """
    SELECT
      MeasurementConsumerReferenceId,
      DataProviderReferenceId,
      EventGroupReferenceId
    FROM
      ReportingSetEventGroups
    """

  override fun translate(row: Row): Result = Result(buildEventGroupKey(row))

  fun listEventGroupKeys(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    reportingSetId: InternalId
  ): Flow<Result> {
    return flow {
      val builder =
        statementBuilder(
          baseSql +
            """
        WHERE MeasurementConsumerReferenceId = CAST($1 AS text)
          AND ReportingSetId = $2
        """
        ) {
          bind("$1", measurementConsumerReferenceId)
          bind("$2", reportingSetId.value)
        }

      execute(readContext, builder).collect { emit(it) }
    }
  }

  private fun buildEventGroupKey(row: Row): ReportingSet.EventGroupKey {
    return ReportingSetKt.eventGroupKey {
      measurementConsumerReferenceId = row.getValue("MeasurementConsumerReferenceId")
      dataProviderReferenceId = row.getValue("DataProviderReferenceId")
      eventGroupReferenceId = row.getValue("EventGroupReferenceId")
    }
  }
}
