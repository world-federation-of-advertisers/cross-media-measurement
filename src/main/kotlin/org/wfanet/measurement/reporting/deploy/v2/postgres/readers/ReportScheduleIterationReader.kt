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

import java.time.Instant
import java.time.LocalDateTime
import java.time.OffsetDateTime
import java.time.ZoneOffset
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsRequest
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration

class ReportScheduleIterationReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val reportScheduleId: InternalId,
    val reportScheduleIterationId: InternalId,
    val reportScheduleIteration: ReportScheduleIteration,
  )

  private val baseSql: String =
    """
    SELECT
      CmmsMeasurementConsumerId,
      ReportScheduleIterations.MeasurementConsumerId,
      ReportSchedules.ReportScheduleId,
      ReportSchedules.ExternalReportScheduleId,
      ReportScheduleIterations.ReportScheduleIterationId,
      ReportScheduleIterations.ExternalReportScheduleIterationId,
      ReportScheduleIterations.ReportEventTime,
      ReportScheduleIterations.CreateReportRequestId,
      ReportScheduleIterations.NumAttempts,
      ReportScheduleIterations.State,
      ReportScheduleIterations.CreateTime,
      ReportScheduleIterations.UpdateTime,
      Reports.ExternalReportId
    FROM ReportScheduleIterations
      JOIN MeasurementConsumers USING(MeasurementConsumerId)
      JOIN ReportSchedules USING(MeasurementConsumerId, ReportScheduleId)
      LEFT JOIN Reports ON ReportScheduleIterations.MeasurementConsumerId = Reports.MeasurementConsumerId
        AND ReportScheduleIterations.CreateReportRequestId = Reports.CreateReportRequestId
    """
      .trimIndent()

  fun translate(row: ResultRow): Result =
    Result(
      row["MeasurementConsumerId"],
      row["ReportScheduleId"],
      row["ReportScheduleIterationId"],
      buildReportScheduleIteration(row),
    )

  suspend fun readReportScheduleIterationByExternalId(
    cmmsMeasurementConsumerId: String,
    externalReportScheduleId: String,
    externalReportScheduleIterationId: String,
  ): Result? {
    val sql =
      """
        $baseSql
        WHERE CmmsMeasurementConsumerId = $1
          AND ExternalReportScheduleId = $2
          AND ExternalReportScheduleIterationId = $3
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", cmmsMeasurementConsumerId)
        bind("$2", externalReportScheduleId)
        bind("$3", externalReportScheduleIterationId)
      }

    return readContext.executeQuery(statement).consume(::translate).singleOrNull()
  }

  suspend fun readReportScheduleIterations(
    request: ListReportScheduleIterationsRequest
  ): List<Result> {
    val sql =
      """
        $baseSql
        WHERE CmmsMeasurementConsumerId = $1
          AND ExternalReportScheduleId = $2
          AND ReportEventTime < $3
        ORDER BY ReportEventTime DESC
        LIMIT $4
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalReportScheduleId)
        if (request.filter.hasReportEventTimeBefore()) {
          bind("$3", request.filter.reportEventTimeBefore.toInstant().atOffset(ZoneOffset.UTC))
        } else {
          bind("$3", FAR_FUTURE_YEAR)
        }

        if (request.limit > 0) {
          bind("$4", request.limit)
        } else {
          bind("$4", LIST_DEFAULT_LIMIT)
        }
      }

    return readContext.executeQuery(statement).consume(::translate).toList()
  }

  private fun buildReportScheduleIteration(row: ResultRow): ReportScheduleIteration {
    return reportScheduleIteration {
      cmmsMeasurementConsumerId = row["CmmsMeasurementConsumerId"]
      externalReportScheduleId = row["ExternalReportScheduleId"]
      externalReportScheduleIterationId = row["ExternalReportScheduleIterationId"]
      createReportRequestId = row["CreateReportRequestId"]
      state = ReportScheduleIteration.State.forNumber(row["State"])
      numAttempts = row["NumAttempts"]
      reportEventTime = row.get<Instant>("ReportEventTime").toProtoTime()
      createTime = row.get<Instant>("CreateTime").toProtoTime()
      updateTime = row.get<Instant>("UpdateTime").toProtoTime()
      val externalReportId: String? = row["ExternalReportId"]
      if (externalReportId != null) {
        this.externalReportId = externalReportId
      }
    }
  }

  companion object {
    private const val LIST_DEFAULT_LIMIT = 50
    private val FAR_FUTURE_YEAR =
      OffsetDateTime.of(LocalDateTime.now().withYear(5000), ZoneOffset.UTC)
  }
}
