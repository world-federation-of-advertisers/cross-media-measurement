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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Value
import com.google.type.DateTime
import java.time.ZoneOffset
import java.time.ZonedDateTime
import org.wfanet.measurement.common.toZonedDateTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation

suspend fun AsyncDatabaseClient.ReadContext.reportResultExists(
  measurementConsumerId: Long,
  reportResultId: Long,
): Boolean {
  return readRow(
    "ReportResults",
    Key.of(measurementConsumerId, reportResultId),
    listOf("ReportResultId"),
  ) != null
}

suspend fun AsyncDatabaseClient.ReadContext.reportResultExistsWithExternalId(
  measurementConsumerId: Long,
  externalReportResultId: Long,
): Boolean {
  return readRowUsingIndex(
    "ReportResults",
    "ReportResultsByExternalReportResultId",
    Key.of(measurementConsumerId, externalReportResultId),
    listOf("ExternalReportResultId"),
  ) != null
}

/** Buffers an insert mutation to the ReportResults table. */
fun AsyncDatabaseClient.TransactionContext.insertReportResult(
  measurementConsumerId: Long,
  reportResultId: Long,
  externalReportResultId: Long,
  reportStart: DateTime,
) {
  val reportStartTime: ZonedDateTime = reportStart.toZonedDateTime()
  val reportStartZoneId = reportStartTime.zone

  bufferInsertMutation("ReportResults") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("ReportResultId").to(reportResultId)
    set("ExternalReportResultId").to(externalReportResultId)
    set("ReportIntervalStartTime").to(reportStartTime.toInstant().toGcloudTimestamp())
    if (reportStartZoneId is ZoneOffset) {
      set("ReportIntervalStartTimeZoneOffset").to(reportStartZoneId.totalSeconds.toLong())
    } else {
      set("ReportIntervalStartTimeZoneId").to(reportStartZoneId.id)
    }
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
  }
}
