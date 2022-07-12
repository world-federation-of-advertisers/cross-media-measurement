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
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.Measurement

class MeasurementResultsReader {
  data class Result(
    val reportId: InternalId,
    val measurementReferenceId: String,
    val state: Measurement.State,
    val result: Measurement.Result?
  )

  private val sql =
    """
    SELECT
      ReportId,
      MeasurementReferenceId,
      Measurements.State,
      Result
    FROM
      (
        SELECT
          MeasurementConsumerReferenceId,
          ReportId
        FROM
          ReportMeasurements
        WHERE
          MeasurementConsumerReferenceId = $1 AND MeasurementReferenceId = $2
      ) AS Reports
      JOIN ReportMeasurements USING (MeasurementConsumerReferenceId, ReportId)
      JOIN Measurements USING (MeasurementConsumerReferenceId, MeasurementReferenceId)
    """

  fun translate(row: ResultRow): Result =
    Result(
      reportId = row["ReportId"],
      measurementReferenceId = row["MeasurementReferenceId"],
      state = Measurement.State.forNumber(row["State"]),
      result = row.getProtoMessage("Result", Measurement.Result.parser())
    )

  suspend fun listMeasurementsForReportsByMeasurementReferenceId(
    readContext: ReadContext,
    measurementConsumerReferenceId: String,
    measurementReferenceId: String
  ): Flow<Result> {
    val statement =
      boundStatement(sql) {
        bind("$1", measurementConsumerReferenceId)
        bind("$2", measurementReferenceId)
      }

    return readContext.executeQuery(statement).consume(::translate)
  }
}
