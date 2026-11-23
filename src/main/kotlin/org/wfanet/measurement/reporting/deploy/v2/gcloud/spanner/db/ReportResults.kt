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
import com.google.cloud.spanner.KeySet
import com.google.cloud.spanner.Mutation
import com.google.cloud.spanner.Options
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import com.google.type.DateTime
import java.time.Instant
import java.time.ZoneId
import java.time.ZoneOffset
import java.time.ZonedDateTime
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import kotlinx.coroutines.flow.singleOrNull
import org.wfanet.measurement.common.toProtoDateTime
import org.wfanet.measurement.common.toZonedDateTime
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.common.toInstant
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequestKt
import org.wfanet.measurement.internal.reporting.v2.ReportResult
import org.wfanet.measurement.internal.reporting.v2.reportResult
import org.wfanet.measurement.reporting.service.internal.ReportResultNotFoundException

data class ReportResultResult(
  val measurementConsumerId: Long,
  val reportResultId: Long,
  val reportResult: ReportResult,
  val createTime: Instant,
  val assembly: ReportResultAssembly?,
)

/** Ownership and lease of a result tree, retained through completed assembly. */
data class ReportResultAssembly(
  val basicReportId: Long,
  val requestId: String,
  val leaseExpirationTime: Instant,
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
  assembly: ReportResultAssembly?,
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
    if (assembly != null) {
      set("AssemblyBasicReportId").to(assembly.basicReportId)
      set("AssemblyRequestId").to(assembly.requestId)
      set("AssemblyLeaseExpirationTime").to(assembly.leaseExpirationTime.toGcloudTimestamp())
    }
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
      ReportResults.CreateTime,
      AssemblyBasicReportId,
      AssemblyRequestId,
      AssemblyLeaseExpirationTime,
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

  return ReportResultResult(
    measurementConsumerId,
    reportResultId,
    reportResult,
    row.getTimestamp("CreateTime").toInstant(),
    if (row.isNull("AssemblyBasicReportId")) null
    else
      ReportResultAssembly(
        row.getLong("AssemblyBasicReportId"),
        row.getString("AssemblyRequestId"),
        row.getTimestamp("AssemblyLeaseExpirationTime").toInstant(),
      ),
  )
}

/** External reference and creation time of an unassociated result tree. */
data class UnassociatedReportResult(val externalReportResultId: Long, val createTime: Instant)

/** Reads old, unowned and unassociated trees in creation-time and external-ID order. */
fun AsyncDatabaseClient.ReadContext.readUnassociatedReportResults(
  cmmsMeasurementConsumerId: String,
  createdBefore: Instant,
  limit: Int,
  after: UnassociatedReportResult?,
): Flow<UnassociatedReportResult> {
  require(limit > 0)
  val query =
    statement(
      """
    SELECT ReportResults.ExternalReportResultId, ReportResults.CreateTime
    FROM MeasurementConsumers
    JOIN ReportResults USING (MeasurementConsumerId)
    WHERE CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
      AND ReportResults.CreateTime < @createdBefore
      AND ReportResults.AssemblyBasicReportId IS NULL
      AND NOT EXISTS (
        SELECT 1 FROM BasicReports
        WHERE BasicReports.MeasurementConsumerId = ReportResults.MeasurementConsumerId
          AND BasicReports.ReportResultId = ReportResults.ReportResultId
      )
      ${if (after == null) "" else """
      AND (ReportResults.CreateTime > @afterCreateTime
        OR (ReportResults.CreateTime = @afterCreateTime
          AND ReportResults.ExternalReportResultId > @afterExternalReportResultId))
      """}
    ORDER BY ReportResults.CreateTime, ReportResults.ExternalReportResultId
    LIMIT @limit
    """
        .trimIndent()
    ) {
      bind("cmmsMeasurementConsumerId").to(cmmsMeasurementConsumerId)
      bind("createdBefore").to(createdBefore.toGcloudTimestamp())
      bind("limit").to(limit.toLong())
      if (after != null) {
        bind("afterCreateTime").to(after.createTime.toGcloudTimestamp())
        bind("afterExternalReportResultId").to(after.externalReportResultId)
      }
    }
  return executeQuery(query, Options.tag("action=readUnassociatedReportResults")).map { row ->
    UnassociatedReportResult(
      row.getLong("ExternalReportResultId"),
      row.getTimestamp("CreateTime").toInstant(),
    )
  }
}

/**
 * Discards a result tree only if no BasicReport is associated with it.
 *
 * @param createdBefore when non-null, trees created at or after this time are preserved
 * @return whether the tree was deleted
 */
suspend fun AsyncDatabaseClient.TransactionContext.discardReportResult(
  cmmsMeasurementConsumerId: String,
  externalReportResultId: Long,
  createdBefore: Instant? = null,
): Boolean {
  val result =
    try {
      readReportResult(cmmsMeasurementConsumerId, externalReportResultId)
    } catch (e: ReportResultNotFoundException) {
      return false
    }
  if (createdBefore != null && (result.createTime >= createdBefore || result.assembly != null)) {
    return false
  }
  val filter =
    ListBasicReportsRequestKt.filter {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      this.externalReportResultId = externalReportResultId
    }
  // Serialize the association check and deletion with final-batch publication.
  if (readBasicReports(filter).firstOrNull() != null) {
    return false
  }
  buffer(
    Mutation.delete(
      "ReportResults",
      KeySet.singleKey(Key.of(result.measurementConsumerId, result.reportResultId)),
    )
  )
  return true
}

/** Reads the current owned tree, including completed assembly. */
suspend fun AsyncDatabaseClient.ReadContext.readReportResultAssembly(
  cmmsMeasurementConsumerId: String,
  measurementConsumerId: Long,
  basicReportId: Long,
): ReportResultResult? {
  val row =
    readRowUsingIndex(
      "ReportResults",
      "ReportResultsByAssemblyBasicReportId",
      Key.of(measurementConsumerId, basicReportId),
      listOf("ExternalReportResultId"),
    ) ?: return null
  return readReportResult(cmmsMeasurementConsumerId, row.getLong("ExternalReportResultId"))
}

/** Renews the lease in the same transaction as a batch write. */
fun AsyncDatabaseClient.TransactionContext.renewReportResultAssembly(
  result: ReportResultResult,
  leaseExpirationTime: Instant,
) {
  bufferUpdateMutation("ReportResults") {
    set("MeasurementConsumerId").to(result.measurementConsumerId)
    set("ReportResultId").to(result.reportResultId)
    set("AssemblyLeaseExpirationTime").to(leaseExpirationTime.toGcloudTimestamp())
  }
}

/** Reads owned, unfinished attempts eligible for automatic cleanup. */
fun AsyncDatabaseClient.ReadContext.readAbandonedReportResultAssemblies(
  cmmsMeasurementConsumerId: String,
  now: Instant,
  limit: Int,
  externalReportResultId: Long?,
): Flow<Long> {
  val query =
    statement(
      """
    SELECT ReportResults.ExternalReportResultId
    FROM MeasurementConsumers
    JOIN ReportResults USING (MeasurementConsumerId)
    LEFT JOIN BasicReports AS Owner
      ON Owner.MeasurementConsumerId = ReportResults.MeasurementConsumerId
      AND Owner.BasicReportId = ReportResults.AssemblyBasicReportId
    WHERE CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
      AND ReportResults.AssemblyBasicReportId IS NOT NULL
      AND NOT EXISTS (
        SELECT 1 FROM BasicReports
        WHERE BasicReports.MeasurementConsumerId = ReportResults.MeasurementConsumerId
          AND BasicReports.ReportResultId = ReportResults.ReportResultId
      )
      AND (ReportResults.AssemblyLeaseExpirationTime <= @now
        OR Owner.State IN (@failed, @invalid)
        OR Owner.BasicReportId IS NULL)
      ${if (externalReportResultId == null) "" else "AND ReportResults.ExternalReportResultId = @externalReportResultId"}
    ORDER BY ReportResults.AssemblyLeaseExpirationTime, ReportResults.ExternalReportResultId
    LIMIT @limit
    """
        .trimIndent()
    ) {
      bind("cmmsMeasurementConsumerId").to(cmmsMeasurementConsumerId)
      bind("now").to(now.toGcloudTimestamp())
      bind("failed").to(BasicReport.State.FAILED.number.toLong())
      bind("invalid").to(BasicReport.State.INVALID.number.toLong())
      bind("limit").to(limit.toLong())
      if (externalReportResultId != null) {
        bind("externalReportResultId").to(externalReportResultId)
      }
    }
  return executeQuery(query, Options.tag("action=readAbandonedReportResultAssemblies")).map {
    it.getLong("ExternalReportResultId")
  }
}
