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
import org.wfanet.measurement.gcloud.spanner.appendClause
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.gcloud.spanner.toInt64
import org.wfanet.measurement.internal.reporting.v2.BasicReport
import org.wfanet.measurement.internal.reporting.v2.BasicReportDetails
import org.wfanet.measurement.internal.reporting.v2.BasicReportKt
import org.wfanet.measurement.internal.reporting.v2.BasicReportResultDetails
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsPageToken
import org.wfanet.measurement.internal.reporting.v2.ListBasicReportsRequest
import org.wfanet.measurement.internal.reporting.v2.basicReport
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.service.internal.BasicReportNotFoundException

data class BasicReportResult(
  val measurementConsumerId: Long,
  val basicReportId: Long,
  val basicReport: BasicReport,
  val reportResultId: Long?,
)

/**
 * Reads a [BasicReport] by its external ID.
 *
 * @throws BasicReportNotFoundException
 */
suspend fun AsyncDatabaseClient.ReadContext.getBasicReportByRequestId(
  measurementConsumerId: Long,
  createRequestId: String,
): BasicReportResult? {
  val sql =
    """
    ${BasicReportsInternal.BASE_SQL}
    WHERE
      MeasurementConsumerId = @measurementConsumerId
      AND CreateRequestId = @createRequestId
    """
      .trimIndent()
  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("measurementConsumerId").to(measurementConsumerId)
          bind("createRequestId").to(createRequestId)
        }
      )
      .singleOrNullIfEmpty() ?: return null

  return buildBasicReportResult(row)
}

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
    ${BasicReportsInternal.BASE_SQL}
    WHERE
      CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId
      AND ExternalBasicReportId = @externalBasicReportId
    """
      .trimIndent()
  val row: Struct =
    executeQuery(
        statement(sql) {
          bind("cmmsMeasurementConsumerId").to(cmmsMeasurementConsumerId)
          bind("externalBasicReportId").to(externalBasicReportId)
        }
      )
      .singleOrNullIfEmpty()
      ?: throw BasicReportNotFoundException(cmmsMeasurementConsumerId, externalBasicReportId)

  return buildBasicReportResult(row)
}

/**
 * Reads [BasicReport]s ordered by create time ascending, external basic report id ascending.
 *
 * Does not set the campaign_group_display_name field in the result.
 */
fun AsyncDatabaseClient.ReadContext.readBasicReports(
  filter: ListBasicReportsRequest.Filter,
  limit: Int? = null,
  pageToken: ListBasicReportsPageToken? = null,
): Flow<BasicReportResult> {
  val query =
    statement(BasicReportsInternal.BASE_SQL) {
      val conjuncts = buildList {
        if (filter.cmmsMeasurementConsumerId.isNotEmpty()) {
          add(
            "BasicReportsIndexShardId >= 0 " +
              "AND CmmsMeasurementConsumerId = @cmmsMeasurementConsumerId"
          )
          bind("cmmsMeasurementConsumerId").to(filter.cmmsMeasurementConsumerId)
        }
        if (filter.externalReportResultId != 0L) {
          add("ExternalReportResultId = @externalReportResultId")
          bind("externalReportResultId").to(filter.externalReportResultId)
        }
        if (filter.state != BasicReport.State.STATE_UNSPECIFIED) {
          add("State = @state")
          bind("state").toInt64(filter.state)
        }
        if (filter.hasCreateTimeAfter()) {
          add("BasicReports.CreateTime > @createTime")
          bind("createTime").to(filter.createTimeAfter.toGcloudTimestamp())
        }
        if (pageToken != null) {
          add(
            """
            (
              BasicReports.CreateTime > @createTime_page
              OR (
                BasicReports.CreateTime = @createTime_page
                AND (
                  ExternalBasicReportId > @externalBasicReportId_page
                  OR (
                    ExternalBasicReportId = @externalBasicReportId_page
                    AND CmmsMeasurementConsumerId > @cmmsMeasurementConsumerId_page
                  )
                )
              )
            )
            """
              .trimIndent()
          )
          bind("createTime_page").to(pageToken.lastBasicReport.createTime.toGcloudTimestamp())
          bind("externalBasicReportId_page").to(pageToken.lastBasicReport.externalBasicReportId)
          bind("cmmsMeasurementConsumerId_page")
            .to(pageToken.lastBasicReport.cmmsMeasurementConsumerId)
        }
      }
      if (conjuncts.isNotEmpty()) {
        appendClause("WHERE ${conjuncts.joinToString(" AND ")}")
      }

      appendClause("ORDER BY CreateTime, ExternalBasicReportId, CmmsMeasurementConsumerId")
      if (limit != null) {
        appendClause("LIMIT @limit")
        bind("limit").to(limit.toLong())
      }
    }

  return executeQuery(query).map { row -> buildBasicReportResult(row) }
}

/** Buffers an insert mutation for the BasicReports table. */
fun AsyncDatabaseClient.TransactionContext.insertBasicReport(
  basicReportId: Long,
  measurementConsumerId: Long,
  basicReport: BasicReport,
  state: BasicReport.State,
  requestId: String?,
) {
  bufferInsertMutation("BasicReports") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ExternalBasicReportId").to(basicReport.externalBasicReportId)
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("BasicReportDetails").to(basicReport.details)
    set("ExternalCampaignGroupId").to(basicReport.externalCampaignGroupId)
    set("BasicReportResultDetails").to(basicReport.resultDetails)
    set("State").to(state)
    if (basicReport.createReportRequestId.isNotEmpty()) {
      set("CreateReportRequestId").to(basicReport.createReportRequestId)
    }
    if (basicReport.externalReportId.isNotEmpty()) {
      set("ExternalReportId").to(basicReport.externalReportId)
    }
    set("CreateRequestId").to(requestId)
    if (basicReport.hasModelLineKey()) {
      set("CmmsModelProviderId").to(basicReport.modelLineKey.cmmsModelProviderId)
      set("CmmsModelSuiteId").to(basicReport.modelLineKey.cmmsModelSuiteId)
      set("CmmsModelLineId").to(basicReport.modelLineKey.cmmsModelLineId)
    }
    set("ModelLineSystemSpecified").to(basicReport.modelLineSystemSpecified)
  }
}

/**
 * Buffers an update mutation that sets ExternalReportId and State to REPORT_CREATED for the
 * BasicReports table.
 */
fun AsyncDatabaseClient.TransactionContext.setExternalReportId(
  measurementConsumerId: Long,
  basicReportId: Long,
  externalReportId: String,
) {
  bufferUpdateMutation("BasicReports") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ExternalReportId").to(externalReportId)
    set("State").to(BasicReport.State.REPORT_CREATED)
  }
}

/** Buffers an update mutation that sets State to FAILED for the BasicReports table. */
fun AsyncDatabaseClient.TransactionContext.setBasicReportStateToFailed(
  measurementConsumerId: Long,
  basicReportId: Long,
) {
  bufferUpdateMutation("BasicReports") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("State").to(BasicReport.State.FAILED)
  }
}

/**
 * Buffers an update mutation to set the State of a BasicReports row to
 * [BasicReport.State.UNPROCESSED_RESULTS_READY].
 */
fun AsyncDatabaseClient.TransactionContext.setBasicReportStateToUnprocessedResultsReady(
  measurementConsumerId: Long,
  basicReportId: Long,
  reportResultId: Long,
) {
  bufferUpdateMutation("BasicReports") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ReportResultId").to(reportResultId)
    set("State").to(BasicReport.State.UNPROCESSED_RESULTS_READY)
  }
}

/**
 * Buffers an update mutation to set the State of a BasicReports row to
 * [BasicReport.State.SUCCEEDED].
 */
fun AsyncDatabaseClient.TransactionContext.setBasicReportStateToSucceeded(
  measurementConsumerId: Long,
  basicReportId: Long,
) {
  bufferUpdateMutation("BasicReports") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("State").to(BasicReport.State.SUCCEEDED)
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

private fun buildBasicReportResult(row: Struct): BasicReportResult {
  val reportResultId: Long? =
    if (row.isNull("ReportResultId")) {
      null
    } else {
      row.getLong("ReportResultId")
    }

  return BasicReportResult(
    measurementConsumerId = row.getLong("MeasurementConsumerId"),
    basicReportId = row.getLong("BasicReportId"),
    basicReport = buildBasicReport(row),
    reportResultId = reportResultId,
  )
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
    // For handling BasicReports that were created before
    // effective_impression_qualification_filters was added.
    if (details.effectiveImpressionQualificationFiltersList.isEmpty()) {
      details =
        details.copy {
          effectiveImpressionQualificationFilters += this@copy.impressionQualificationFilters
        }
    }
    createTime = row.getTimestamp("CreateTime").toProto()
    state = row.getProtoEnum("State", BasicReport.State::forNumber)
    if (!row.isNull("CreateReportRequestId")) {
      createReportRequestId = row.getString("CreateReportRequestId")
    }
    if (!row.isNull("ExternalReportId")) {
      externalReportId = row.getString("ExternalReportId")
    }
    if (!row.isNull("CmmsModelProviderId")) {
      modelLineKey =
        BasicReportKt.modelLineKey {
          cmmsModelProviderId = row.getString("CmmsModelProviderId")
          cmmsModelSuiteId = row.getString("CmmsModelSuiteId")
          cmmsModelLineId = row.getString("CmmsModelLineId")
        }
    }
    modelLineSystemSpecified = row.getBoolean("ModelLineSystemSpecified")
    if (!row.isNull("ExternalReportResultId")) {
      externalReportResultId = row.getLong("ExternalReportResultId")
    }
  }
}

private object BasicReportsInternal {
  val BASE_SQL =
    """
    SELECT
      MeasurementConsumerId,
      BasicReportId,
      CmmsMeasurementConsumerId,
      ExternalBasicReportId,
      BasicReports.CreateTime,
      ExternalCampaignGroupId,
      BasicReportDetails,
      BasicReportResultDetails,
      State,
      CreateReportRequestId,
      ExternalReportId,
      CmmsModelProviderId,
      CmmsModelSuiteId,
      CmmsModelLineId,
      ModelLineSystemSpecified,
      ExternalReportResultId,
      ReportResultId,
    FROM
      MeasurementConsumers
      JOIN BasicReports USING (MeasurementConsumerId)
      LEFT JOIN ReportResults USING (MeasurementConsumerId, ReportResultId)
    """
      .trimIndent()
}
