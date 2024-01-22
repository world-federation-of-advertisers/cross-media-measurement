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
import kotlinx.coroutines.flow.singleOrNull
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.reportSchedule
import org.wfanet.measurement.internal.reporting.v2.reportScheduleIteration

class ReportScheduleReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val reportScheduleId: InternalId,
    val createReportScheduleRequestId: String?,
    val reportSchedule: ReportSchedule,
  )

  // TODO(@tristanvuong2021): Only do 2nd and 3rd joins if requested
  private val baseSql: String =
    """
    SELECT
      CmmsMeasurementConsumerId,
      ReportSchedules.MeasurementConsumerId,
      ReportSchedules.ReportScheduleId,
      ReportSchedules.ExternalReportScheduleId,
      ReportSchedules.CreateReportScheduleRequestId,
      ReportSchedules.State AS ReportSchedulesState,
      ReportSchedules.NextReportCreationTime,
      ReportSchedules.CreateTime AS ReportSchedulesCreateTime,
      ReportSchedules.UpdateTime AS ReportSchedulesUpdateTime,
      ReportSchedules.ReportScheduleDetails,
      ReportScheduleIterations.ExternalReportScheduleIterationId,
      ReportScheduleIterations.ReportEventTime,
      ReportScheduleIterations.CreateReportRequestId,
      ReportScheduleIterations.State AS ReportScheduleIterationsState,
      ReportScheduleIterations.NumAttempts,
      ReportScheduleIterations.CreateTime AS ReportScheduleIterationsCreateTime,
      ReportScheduleIterations.UpdateTime AS ReportScheduleIterationsUpdateTime,
      Reports.ExternalReportId
    FROM ReportSchedules
      JOIN MeasurementConsumers USING(MeasurementConsumerId)
      LEFT JOIN ReportScheduleIterations ON ReportSchedules.MeasurementConsumerId = ReportScheduleIterations.MeasurementConsumerId
        AND ReportSchedules.ReportScheduleId = ReportScheduleIterations.ReportScheduleId
        AND ReportSchedules.LatestReportScheduleIterationId = ReportScheduleIterations.ReportScheduleIterationId
      LEFT JOIN Reports ON ReportScheduleIterations.MeasurementConsumerId = Reports.MeasurementConsumerId
        AND ReportScheduleIterations.CreateReportRequestId = Reports.CreateReportRequestId
    """
      .trimIndent()

  fun translate(row: ResultRow): Result =
    Result(
      row["MeasurementConsumerId"],
      row["ReportScheduleId"],
      row["CreateReportScheduleRequestId"],
      buildReportSchedule(row),
    )

  suspend fun readReportScheduleByRequestId(
    measurementConsumerId: InternalId,
    createReportScheduleRequestId: String,
  ): Result? {
    val sql =
      """
        $baseSql
        WHERE ReportSchedules.MeasurementConsumerId = $1
          AND CreateReportScheduleRequestId = $2
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", measurementConsumerId)
        bind("$2", createReportScheduleRequestId)
      }

    return readContext.executeQuery(statement).consume(::translate).singleOrNull()
  }

  suspend fun readReportScheduleByExternalId(
    cmmsMeasurementConsumerId: String,
    externalReportScheduleId: String,
  ): Result? {
    val sql =
      """
        $baseSql
        WHERE CmmsMeasurementConsumerId = $1
          AND ExternalReportScheduleId = $2
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", cmmsMeasurementConsumerId)
        bind("$2", externalReportScheduleId)
      }

    return readContext.executeQuery(statement).consume(::translate).singleOrNull()
  }

  suspend fun readReportSchedules(request: ListReportSchedulesRequest): List<Result> {
    val sql =
      """
        $baseSql
        WHERE CmmsMeasurementConsumerId = $1
          AND ExternalReportScheduleId > $2
        ORDER BY CmmsMeasurementConsumerId ASC, ExternalReportScheduleId ASC
        LIMIT $3
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalReportScheduleIdAfter)

        if (request.limit > 0) {
          bind("$3", request.limit)
        } else {
          bind("$3", LIST_DEFAULT_LIMIT)
        }
      }

    return readContext.executeQuery(statement).consume(::translate).toList()
  }

  suspend fun readReportSchedulesByState(request: ListReportSchedulesRequest): List<Result> {
    val sql =
      """
        $baseSql
        WHERE CmmsMeasurementConsumerId = $1
          AND ExternalReportScheduleId > $2
          AND ReportSchedules.State = $3
        ORDER BY CmmsMeasurementConsumerId ASC, ExternalReportScheduleId ASC
        LIMIT $4
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)
        bind("$2", request.filter.externalReportScheduleIdAfter)
        bind("$3", request.filter.state)

        if (request.limit > 0) {
          bind("$4", request.limit)
        } else {
          bind("$4", LIST_DEFAULT_LIMIT)
        }
      }

    return readContext.executeQuery(statement).consume(::translate).toList()
  }

  private fun buildReportSchedule(row: ResultRow): ReportSchedule {
    val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
    val externalReportScheduleId: String = row["ExternalReportScheduleId"]
    return reportSchedule {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalReportScheduleId = externalReportScheduleId
      state = ReportSchedule.State.forNumber(row["ReportSchedulesState"])
      details = row.getProtoMessage("ReportScheduleDetails", ReportSchedule.Details.parser())
      nextReportCreationTime = row.get<Instant>("NextReportCreationTime").toProtoTime()
      createTime = row.get<Instant>("ReportSchedulesCreateTime").toProtoTime()
      updateTime = row.get<Instant>("ReportSchedulesUpdateTime").toProtoTime()

      val externalReportScheduleIterationId: String? = row["ExternalReportScheduleIterationId"]
      if (externalReportScheduleIterationId != null) {
        val externalReportId: String? = row["ExternalReportId"]
        latestIteration = reportScheduleIteration {
          this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
          this.externalReportScheduleId = externalReportScheduleId
          this.externalReportScheduleIterationId = externalReportScheduleIterationId
          createReportRequestId = row["CreateReportRequestId"]
          state = ReportScheduleIteration.State.forNumber(row["ReportScheduleIterationsState"])
          numAttempts = row["NumAttempts"]
          reportEventTime = row.get<Instant>("ReportEventTime").toProtoTime()
          createTime = row.get<Instant>("ReportScheduleIterationsCreateTime").toProtoTime()
          updateTime = row.get<Instant>("ReportScheduleIterationsUpdateTime").toProtoTime()
          if (externalReportId != null) {
            this.externalReportId = externalReportId
          }
        }
      }
    }
  }

  companion object {
    private const val LIST_DEFAULT_LIMIT = 50
  }
}
