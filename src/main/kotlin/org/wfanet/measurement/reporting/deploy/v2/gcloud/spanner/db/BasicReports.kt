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
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.singleOrNullIfEmpty
import org.wfanet.measurement.gcloud.common.toGcloudTimestamp
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportDetails
import org.wfanet.measurement.internal.reporting.v2.BasicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.reporting.service.internal.BasicReportNotFoundException

data class BasicReportResult(val basicReportId: Long, val basicReport: BasicReport)

/**
 * Reads a [BasicReport] by its external ID.
 *
 * @throws BasicReportNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getBasicReportByExternalId(
  cmmsMeasurementConsumerId: String,
  externalBasicReportId: String,
): BasicReportResult {
  val sql =
    """
    SELECT
      BasicReportId,
      CmmsMeasurementConsumerId,
      ExternalBasicReportId,
      BasicReports.CreateTime,
      ExternalCampaignGroupId,
      BasicReportDetails,
      BasicReportResultDetails
    FROM
      (
        SELECT *
        FROM MeasurementConsumers
        WHERE CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
      ) AS MeasurementConsumers
      JOIN BasicReports@{FORCE_INDEX=BasicReportsByExternalBasicReportId} USING (MeasurementConsumerId)
    WHERE
      ExternalBasicReportId = @externalBasicReportId
    """
      .trimIndent()
  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("cmmsMeasurementConsumerId").to(cmmsMeasurementConsumerId)
          bind("externalBasicReportId").to(externalBasicReportId)
        }
      )
      .singleOrNullIfEmpty() ?: throw BasicReportNotFoundException(externalBasicReportId)

  return BasicReportResult(row.getLong("BasicReportId"), buildBasicReport(row))
}

/** Reads [BasicReport]s ordered by create time ascending, external basic report id ascending. */
fun AsyncDatabaseClient.ReadContext.readBasicReports(
  limit: Int,
  filter: ListBasicReportsRequest.Filter,
): Flow<BasicReportResult> {
  val index =
    if (filter.after.hasCreateTime()) {
      "@{FORCE_INDEX=BasicReportsByCreateTime}"
    } else {
      ""
    }

  val sql = buildString {
    appendLine(
      """
      SELECT
        BasicReportId,
        CmmsMeasurementConsumerId,
        ExternalBasicReportId,
        BasicReports.CreateTime,
        ExternalCampaignGroupId,
        BasicReportDetails,
        BasicReportResultDetails
      FROM
        (
          SELECT *
          FROM MeasurementConsumers
          WHERE CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
        ) AS MeasurementConsumers
        JOIN BasicReports${index} USING (MeasurementConsumerId)
      """
        .trimIndent()
    )
    if (filter.hasAfter()) {
      if (filter.after.externalBasicReportId.isEmpty()) {
        appendLine("WHERE BasicReports.CreateTime > @createTime")
      } else {
        appendLine(
          """
          WHERE CASE
            WHEN BasicReports.CreateTime > @createTime THEN TRUE
            WHEN BasicReports.CreateTime = @createTime
              AND ExternalBasicReportId > @externalBasicReportId THEN TRUE
            ELSE FALSE
          END
          """
            .trimIndent()
        )
      }
    }
    appendLine("ORDER BY CreateTime, ExternalBasicReportId")
    if (limit > 0) {
      appendLine("LIMIT @limit")
    }
  }
  val query =
    statement(sql) {
      bind("cmmsMeasurementConsumerId").to(filter.cmmsMeasurementConsumerId)
      if (filter.hasAfter()) {
        bind("createTime").to(filter.after.createTime.toGcloudTimestamp())
        if (filter.after.externalBasicReportId.isNotEmpty()) {
          bind("externalBasicReportId").to(filter.after.externalBasicReportId)
        }
      }
      if (limit > 0) {
        bind("limit").to(limit.toLong())
      }
    }

  return executeQuery(query).map { row ->
    BasicReportResult(row.getLong("BasicReportId"), buildBasicReport(row))
  }
}

/** Buffers an insert mutation for the BasicReports table. */
fun AsyncDatabaseClient.TransactionContext.insertBasicReport(
  basicReportId: Long,
  measurementConsumerId: Long,
  basicReport: BasicReport,
) {
  bufferInsertMutation("BasicReports") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ExternalBasicReportId").to(basicReport.externalBasicReportId)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("BasicReportDetails").to(basicReport.details)
    set("ExternalCampaignGroupId").to(basicReport.externalCampaignGroupId)
    set("BasicReportResultDetails").to(basicReport.resultDetails)
  }
}

/** Returns whether a [BasicReport] with the specified [basicReportId] exists. */
suspend fun AsyncDatabaseClient.ReadContext.basicReportExists(
  measurementConsumerId: Long,
  basicReportId: Long,
): Boolean {
  return readRow(
    "BasicReports",
    Key.of(measurementConsumerId, basicReportId),
    listOf("MeasurementConsumerId", "BasicReportId"),
  ) != null
}

/** Builds a [BasicReport] from a query row response. */
private fun buildBasicReport(row: Struct): BasicReport {
  return basicReport {
    cmmsMeasurementConsumerId = row.getString("CmmsMeasurementConsumerId")
    externalBasicReportId = row.getString("ExternalBasicReportId")
    externalCampaignGroupId = row.getString("ExternalCampaignGroupId")
    resultDetails =
      row.getProtoMessage("BasicReportResultDetails", BasicReportResultDetails.getDefaultInstance())
    details = row.getProtoMessage("BasicReportDetails", BasicReportDetails.getDefaultInstance())
    createTime = row.getTimestamp("CreateTime").toProto()
  }
}
