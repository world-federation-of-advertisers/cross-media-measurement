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
import com.google.cloud.spanner.Value
import com.google.cloud.spanner.ValueBinder
import java.time.LocalDate
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation

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
