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
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.type.DateTime
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.toProtoDateTime
import org.wfanet.measurement.common.toZonedDateTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.reporting.service.internal.ReportResultNotFoundException

data class ReportResultResult(
  val measurementConsumerId: Long,
  val reportResultId: Long,
  val reportResult: ReportResult,
)

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

/**
 * Reads a [ReportResult] given external IDs.
 *
 * @throws ReportResultNotFoundException if the [ReportResult] is not found
 */
suspend fun AsyncDatabaseClient.ReadContext.readReportResult(
  cmmsMeasurementConsumerId: String,
  externalReportResultId: Long,
): ReportResultResult {
  val sql =
    """
    SELECT
      MeasurementConsumerId,
      ReportResultId,
      ReportIntervalStartTime,
      ReportIntervalStartTimeZoneOffset,
      ReportIntervalStartTimeZoneId,
    FROM
      MeasurementConsumers
      JOIN ReportResults USING (MeasurementConsumerId)
    WHERE
      CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
      AND ExternalReportResultId = @externalReportResultId
    """
      .trimIndent()
  val query =
    statement(sql) {
      bind("cmmsMeasurementConsumerId").to(cmmsMeasurementConsumerId)
      bind("externalReportResultId").to(externalReportResultId)
    }
  val row: Struct =
    executeQuery(query, Options.tag("action=readReportResult")).singleOrNull()
      ?: throw ReportResultNotFoundException(cmmsMeasurementConsumerId, externalReportResultId)

  val measurementConsumerId = row.getLong("MeasurementConsumerId")
  val reportResultId = row.getLong("ReportResultId")

  val reportStartTimestamp = row.getTimestamp("ReportIntervalStartTime").toInstant()
  val zoneId =
    if (row.isNull("ReportIntervalStartTimeZoneId")) {
      ZoneOffset.ofTotalSeconds(row.getLong("ReportIntervalStartTimeZoneOffset").toInt())
    } else {
      ZoneId.of(row.getString("ReportIntervalStartTimeZoneId"))
    }
  val reportResult = reportResult {
    this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
    this.externalReportResultId = externalReportResultId
    reportStart = ZonedDateTime.ofInstant(reportStartTimestamp, zoneId).toProtoDateTime()
  }

  return ReportResultResult(measurementConsumerId, reportResultId, reportResult)
}
