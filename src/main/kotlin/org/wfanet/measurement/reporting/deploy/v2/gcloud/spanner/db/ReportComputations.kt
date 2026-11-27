/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.gcloud.common.toGcloudByteArray
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.gcloud.spanner.bufferInsertMutation
import org.wfanet.measurement.gcloud.spanner.bufferUpdateMutation
import org.wfanet.measurement.gcloud.spanner.getBytesAsByteString
import org.wfanet.measurement.gcloud.spanner.statement
import org.wfanet.measurement.internal.reporting.v2.ReportComputation
import org.wfanet.measurement.internal.reporting.v2.ResultGroupQueryMapping
import org.wfanet.measurement.internal.reporting.v2.reportComputation

/** A [ReportComputation] with its internal ID. */
data class ReportComputationResult(
  val reportComputationId: Long,
  val reportComputation: ReportComputation,
)

/** Reads the [ReportComputation]s of the BasicReport with the specified internal key. */
fun AsyncDatabaseClient.ReadContext.readReportComputations(
  basicReportResult: BasicReportResult
): Flow<ReportComputationResult> {
  val sql =
    """
    SELECT
      ${ReportComputationsInternal.COLUMNS.joinToString(",\n      ")}
    FROM
      ReportComputations
    WHERE
      MeasurementConsumerId = @measurementConsumerId
      AND BasicReportId = @basicReportId
    ORDER BY ExternalReportComputationId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("measurementConsumerId").to(basicReportResult.measurementConsumerId)
        bind("basicReportId").to(basicReportResult.basicReportId)
      }
    )
    .map { row -> buildReportComputationResult(basicReportResult, row) }
}

/**
 * Reads the [ReportComputation] with the specified external ID of the BasicReport with the
 * specified internal key, or `null` if there is none.
 */
suspend fun AsyncDatabaseClient.ReadContext.getReportComputationByExternalId(
  basicReportResult: BasicReportResult,
  externalReportComputationId: Long,
): ReportComputationResult? {
  val sql =
    """
    SELECT
      ${ReportComputationsInternal.COLUMNS.joinToString(",\n      ")}
    FROM
      ReportComputations
    WHERE
      MeasurementConsumerId = @measurementConsumerId
      AND BasicReportId = @basicReportId
      AND ExternalReportComputationId = @externalReportComputationId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("measurementConsumerId").to(basicReportResult.measurementConsumerId)
        bind("basicReportId").to(basicReportResult.basicReportId)
        bind("externalReportComputationId").to(externalReportComputationId)
      }
    )
    .map { row -> buildReportComputationResult(basicReportResult, row) }
    .firstOrNull()
}

/** Buffers an insert mutation for the ReportComputations table. */
fun AsyncDatabaseClient.TransactionContext.insertReportComputation(
  measurementConsumerId: Long,
  basicReportId: Long,
  reportComputationId: Long,
  reportComputation: ReportComputation,
) {
  bufferInsertMutation("ReportComputations") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ReportComputationId").to(reportComputationId)
    set("ExternalReportComputationId").to(reportComputation.externalReportComputationId)
    set("CmmsCreateMeasurementRequestId").to(reportComputation.cmmsCreateMeasurementRequestId)
    set("Multimeasurement").to(reportComputation.multimeasurement.toGcloudByteArray())
    set("ResultGroupQueryMappings")
      .toProtoMessageArray(reportComputation.mappingsList, ResultGroupQueryMapping.getDescriptor())
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update mutation that sets CmmsMeasurementId for the ReportComputations table. */
fun AsyncDatabaseClient.TransactionContext.setReportComputationMeasurementId(
  measurementConsumerId: Long,
  basicReportId: Long,
  reportComputationId: Long,
  cmmsMeasurementId: String,
) {
  bufferUpdateMutation("ReportComputations") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ReportComputationId").to(reportComputationId)
    set("CmmsMeasurementId").to(cmmsMeasurementId)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun buildReportComputationResult(
  basicReportResult: BasicReportResult,
  row: Struct,
): ReportComputationResult {
  return ReportComputationResult(
    row.getLong("ReportComputationId"),
    reportComputation {
      cmmsMeasurementConsumerId = basicReportResult.basicReport.cmmsMeasurementConsumerId
      externalBasicReportId = basicReportResult.basicReport.externalBasicReportId
      externalReportComputationId = row.getLong("ExternalReportComputationId")
      cmmsCreateMeasurementRequestId = row.getString("CmmsCreateMeasurementRequestId")
      if (!row.isNull("CmmsMeasurementId")) {
        cmmsMeasurementId = row.getString("CmmsMeasurementId")
      }
      multimeasurement = row.getBytesAsByteString("Multimeasurement")
      mappings +=
        row.getProtoMessageList(
          "ResultGroupQueryMappings",
          ResultGroupQueryMapping.getDefaultInstance(),
        )
    },
  )
}

private object ReportComputationsInternal {
  val COLUMNS =
    listOf(
      "ReportComputationId",
      "ExternalReportComputationId",
      "CmmsCreateMeasurementRequestId",
      "CmmsMeasurementId",
      "Multimeasurement",
      "ResultGroupQueryMappings",
    )
}
