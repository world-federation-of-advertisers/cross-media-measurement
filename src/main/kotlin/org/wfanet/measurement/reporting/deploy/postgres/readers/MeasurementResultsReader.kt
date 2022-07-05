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

import com.google.gson.JsonArray
import com.google.gson.JsonParser
import com.google.protobuf.util.JsonFormat
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.internal.reporting.Measurement

class MeasurementResultsReader {
  data class MeasurementResult(val state: Measurement.State, val result: Measurement.Result?)
  data class Result(
    val reportId: InternalId,
    val measurementResultsMap: Map<String, MeasurementResult>
  )

  private val sql =
    """
    SELECT
      MeasurementsPerReport.ReportId,
      array_agg(MeasurementsArr) AS Measurements
    FROM (
      SELECT
        ReportIds.ReportId,
        json_build_object(
          'measurements',
          json_agg(
            json_build_object(
              'measurementReferenceId', Measurements.MeasurementReferenceId,
              'state', State,
              'resultJson', ResultJson
            )
          )
        ) AS MeasurementsArr
      FROM (
        SELECT
          ReportId
        FROM ReportMeasurements
        WHERE MeasurementConsumerReferenceId = $1 AND MeasurementReferenceId = $2
      ) AS ReportIds
      JOIN ReportMeasurements
        ON ReportMeasurements.MeasurementConsumerReferenceId = $1
          AND ReportIds.ReportId = ReportMeasurements.ReportId
      JOIN Measurements
        ON ReportMeasurements.MeasurementConsumerReferenceId = Measurements.MeasurementConsumerReferenceId
          AND ReportMeasurements.MeasurementReferenceId = Measurements.MeasurementReferenceId
      GROUP BY ReportIds.ReportId
    ) AS MeasurementsPerReport
    GROUP By ReportId
    """

  fun translate(row: ResultRow): Result =
    Result(
      reportId = row["ReportId"],
      measurementResultsMap =
        buildMeasurementResults(
          JsonParser.parseString(row.get<Array<String>>("measurements")[0])
            .asJsonObject
            .getAsJsonArray("measurements")
        )
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

  private fun buildMeasurementResults(measurementArr: JsonArray): Map<String, MeasurementResult> {
    val measurementResultMap = mutableMapOf<String, MeasurementResult>()
    measurementArr.forEach {
      val measurementObject = it.asJsonObject
      val resultPrimitive = measurementObject.get("resultJson")

      val result =
        if (resultPrimitive.isJsonNull) {
          null
        } else {
          val resultBuilder = Measurement.Result.newBuilder()
          JsonFormat.parser().ignoringUnknownFields().merge(resultPrimitive.asString, resultBuilder)
          resultBuilder.build()
        }

      measurementResultMap[
        measurementObject.getAsJsonPrimitive("measurementReferenceId").asString] =
        MeasurementResult(
          state = Measurement.State.forNumber(measurementObject.getAsJsonPrimitive("state").asInt),
          result = result
        )
    }
    return measurementResultMap
  }
}
