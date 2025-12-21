/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.deploy.v2.gcloud.spanner.db

import com.google.cloud.Date
import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Type
import com.google.cloud.spanner.Value
import com.google.cloud.spanner.ValueBinder
import java.time.LocalDate
import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.toLocalDate
import org.wfanet.measurement.gcloud.common.toProtoDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.struct
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.nonCumulativeStartOrNull

private val REPORTING_WINDOW_STRUCT =
  Type.struct(
    Type.StructField.of("ReportingWindowStartDate", Type.date()),
    Type.StructField.of("ReportingWindowEndDate", Type.date()),
  )

suspend fun AsyncDatabaseClient.ReadContext.reportingWindowResultExists(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  reportingWindowResultId: Long,
): Boolean {
  return readRow(
    "ReportingWindowResults",
    Key.of(measurementConsumerId, reportResultId, reportingSetResultId, reportingWindowResultId),
    listOf("ReportResultId"),
  ) != null
}

suspend fun AsyncDatabaseClient.ReadContext.readReportingWindowResultIds(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  reportingWindows: Iterable<ReportingSetResult.ReportingWindow>,
): Map<ReportingSetResult.ReportingWindow, Long> {
  val sql =
    """
    SELECT
      ReportingWindowResultId,
      ReportingWindowStartDate,
      ReportingWindowEndDate,
    FROM
      ReportingWindowResults
    WHERE
      MeasurementConsumerId = @measurementConsumerId
      AND ReportResultId = @reportResultId
      AND ReportingSetResultId = @reportingSetResultId
      AND EXISTS (
        SELECT 1 
        FROM UNNEST(@reportingWindows) AS reporting_windows
        WHERE
          CASE
            WHEN ReportingWindowStartDate = reporting_windows.ReportingWindowStartDate
              AND ReportingWindowEndDate = reporting_windows.ReportingWindowEndDate THEN TRUE
            WHEN ReportingWindowStartDate IS NULL
              AND reporting_windows.ReportingWindowStartDate IS NULL
              AND ReportingWindowEndDate = reporting_windows.ReportingWindowEndDate THEN TRUE
            ELSE FALSE
          END
      )
    """
      .trimIndent()
  val query =
    statement(sql) {
      bind("measurementConsumerId").to(measurementConsumerId)
      bind("reportResultId").to(reportResultId)
      bind("reportingSetResultId").to(reportingSetResultId)
      bind("reportingWindows")
        .toStructArray(
          REPORTING_WINDOW_STRUCT,
          reportingWindows.map(ReportingSetResult.ReportingWindow::toStruct),
        )
    }
  val rows: Flow<Struct> = executeQuery(query, Options.tag("action=readReportingWindowResultIds"))

  return buildMap {
    rows.collect { row ->
      val reportingWindow =
        ReportingSetResultKt.reportingWindow {
          if (!row.isNull("ReportingWindowStartDate")) {
            nonCumulativeStart = row.getDate("ReportingWindowStartDate").toProtoDate()
          }
          end = row.getDate("ReportingWindowEndDate").toProtoDate()
        }
      put(reportingWindow, row.getLong("ReportingWindowResultId"))
    }
  }
}

fun AsyncDatabaseClient.TransactionContext.insertReportingWindowResult(
  measurementConsumerId: Long,
  reportResultId: Long,
  reportingSetResultId: Long,
  reportingWindowResultId: Long,
  nonCumulativeStartDate: LocalDate?,
  endDate: LocalDate,
) {
  bufferInsertMutation("ReportingWindowResults") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ReportingSetResultId").to(reportingSetResultId)
    set("ReportingWindowResultId").to(reportingWindowResultId)
    set("ReportingWindowStartDate").to(nonCumulativeStartDate)
    set("ReportingWindowEndDate").to(endDate)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun <R> ValueBinder<R>.to(date: LocalDate?): R {
  return if (date == null) {
    to(Value.date(null))
  } else {
    to(Date.fromYearMonthDay(date.year, date.monthValue, date.dayOfMonth))
  }
}

private fun ReportingSetResult.ReportingWindow.toStruct() = struct {
  set("ReportingWindowStartDate").to(nonCumulativeStartOrNull?.toLocalDate())
  set("ReportingWindowEndDate").to(end.toLocalDate())
}
