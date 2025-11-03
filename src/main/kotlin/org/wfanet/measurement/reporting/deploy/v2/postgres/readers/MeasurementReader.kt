/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import com.google.type.Interval
import com.google.type.interval
import java.time.Instant
import java.util.UUID
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emptyFlow
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.measurement

class MeasurementReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val measurementId: InternalId,
    val measurement: Measurement,
  )

  private data class MeasurementInfo(
    val measurementConsumerId: InternalId,
    val cmmsMeasurementConsumerId: String,
    val cmmsMeasurementId: String?,
    val cmmsCreateMeasurementRequestId: String,
    val timeInterval: Interval,
    val state: Measurement.State,
    val details: Measurement.Details,
  )

  private val baseSql: String =
    """
    SELECT
      Measurements.MeasurementConsumerId,
      MeasurementConsumers.CmmsMeasurementConsumerId,
      Measurements.MeasurementId,
      Measurements.CmmsCreateMeasurementRequestId,
      Measurements.CmmsMeasurementId,
      Measurements.TimeIntervalStart,
      Measurements.TimeIntervalEndExclusive,
      Measurements.State,
      Measurements.MeasurementDetails
    FROM MeasurementConsumers
    JOIN Measurements USING(MeasurementConsumerId)
    """
      .trimIndent()

  suspend fun readMeasurementsByCmmsId(
    measurementConsumerId: InternalId,
    cmmsMeasurementIds: Collection<String>,
  ): Flow<Result> {
    if (cmmsMeasurementIds.isEmpty()) {
      return emptyFlow()
    }

    val sql =
      StringBuilder(
        """
          WITH CmmsMeasurementIds AS MATERIALIZED (
            SELECT
              CmmsMeasurementId
            FROM (VALUES ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER})
            AS c(CmmsMeasurementId)
          )
          $baseSql
          JOIN CmmsMeasurementIds USING(CmmsMeasurementId)
          WHERE Measurements.MeasurementConsumerId = $1
        """
          .trimIndent()
      )

    val statement =
      valuesListBoundStatement(valuesStartIndex = 1, paramCount = 1, sql.toString()) {
        bind("$1", measurementConsumerId)
        cmmsMeasurementIds.forEach { addValuesBinding { bindValuesParam(0, it) } }
      }

    return createResultFlow(statement)
  }

  private suspend fun createResultFlow(statement: BoundStatement): Flow<Result> {
    val translate: (row: ResultRow) -> Result = { row: ResultRow ->
      val measurementConsumerId: InternalId = row["MeasurementConsumerId"]
      val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
      val measurementId: InternalId = row["MeasurementId"]
      val cmmsCreateMeasurementRequestId: UUID = row["CmmsCreateMeasurementRequestId"]
      val cmmsMeasurementId: String? = row["CmmsMeasurementId"]
      val timeIntervalStart: Instant = row["TimeIntervalStart"]
      val timeIntervalEnd: Instant = row["TimeIntervalEndExclusive"]
      val state: Measurement.State = Measurement.State.forNumber(row["State"])
      val details: Measurement.Details =
        row.getProtoMessage("MeasurementDetails", Measurement.Details.parser())

      val timeInterval = interval {
        startTime = timeIntervalStart.toProtoTime()
        endTime = timeIntervalEnd.toProtoTime()
      }

      val measurement = measurement {
        this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
        if (cmmsMeasurementId != null) {
          this.cmmsMeasurementId = cmmsMeasurementId
        }
        this.cmmsCreateMeasurementRequestId = cmmsCreateMeasurementRequestId.toString()
        this.timeInterval = timeInterval
        this.state = state
        this.details = details
      }

      Result(
        measurementConsumerId = measurementConsumerId,
        measurementId = measurementId,
        measurement = measurement,
      )
    }

    return readContext.executeQuery(statement).consume(translate)
  }
}
