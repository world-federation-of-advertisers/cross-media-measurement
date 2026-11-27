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

import com.google.cloud.spanner.Key
import com.google.cloud.spanner.Struct
import com.google.cloud.spanner.Value
import kotlinx.coroutines.flow.Flow
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
import org.wfanet.measurement.reporting.service.internal.ImpressionQualificationFilterMapping

/** Reads the [ReportComputation]s of the BasicReport with the specified internal key. */
fun AsyncDatabaseClient.ReadContext.readReportComputations(
  basicReportResult: BasicReportResult,
  impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
): Flow<ReportComputation> {
  val sql =
    """
    SELECT
      ${ReportComputationsInternal.COLUMNS.joinToString(",\n      ")}
    FROM
      ReportComputations
    WHERE
      MeasurementConsumerId = @measurementConsumerId
      AND BasicReportId = @basicReportId
    ORDER BY ImpressionQualificationFilterId
    """
      .trimIndent()
  return executeQuery(
      statement(sql) {
        bind("measurementConsumerId").to(basicReportResult.measurementConsumerId)
        bind("basicReportId").to(basicReportResult.basicReportId)
      }
    )
    .map { row ->
      buildReportComputation(basicReportResult, impressionQualificationFilterMapping, row)
    }
}

/**
 * Reads the [ReportComputation] of the BasicReport with the specified internal key for the
 * specified internal impression qualification filter ID, or `null` if there is none.
 */
suspend fun AsyncDatabaseClient.ReadContext.getReportComputation(
  basicReportResult: BasicReportResult,
  impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  impressionQualificationFilterId: Long,
): ReportComputation? {
  val row: Struct =
    readRow(
      "ReportComputations",
      Key.of(
        basicReportResult.measurementConsumerId,
        basicReportResult.basicReportId,
        impressionQualificationFilterId,
      ),
      ReportComputationsInternal.COLUMNS,
    ) ?: return null
  return buildReportComputation(basicReportResult, impressionQualificationFilterMapping, row)
}

/**
 * Buffers an insert mutation for the ReportComputations table, keyed by
 * [impressionQualificationFilterId] rather than the external ID in [reportComputation].
 */
fun AsyncDatabaseClient.TransactionContext.insertReportComputation(
  measurementConsumerId: Long,
  basicReportId: Long,
  impressionQualificationFilterId: Long,
  reportComputation: ReportComputation,
) {
  bufferInsertMutation("ReportComputations") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ImpressionQualificationFilterId").to(impressionQualificationFilterId)
    set("CmmsCreateMeasurementRequestId").to(reportComputation.cmmsCreateMeasurementRequestId)
    set("Multimeasurement").to(reportComputation.multimeasurement.toGcloudByteArray())
    set("ResultGroupQueryMappings")
      .toBytesArray(reportComputation.mappingsList.map { it.toByteString().toGcloudByteArray() })
    set("CreateTime").to(Value.COMMIT_TIMESTAMP)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

/** Buffers an update mutation that sets CmmsMeasurementId for the ReportComputations table. */
fun AsyncDatabaseClient.TransactionContext.setReportComputationMeasurementId(
  measurementConsumerId: Long,
  basicReportId: Long,
  impressionQualificationFilterId: Long,
  cmmsMeasurementId: String,
) {
  bufferUpdateMutation("ReportComputations") {
    set("MeasurementConsumerId").to(measurementConsumerId)
    set("BasicReportId").to(basicReportId)
    set("ImpressionQualificationFilterId").to(impressionQualificationFilterId)
    set("CmmsMeasurementId").to(cmmsMeasurementId)
    set("UpdateTime").to(Value.COMMIT_TIMESTAMP)
  }
}

private fun buildReportComputation(
  basicReportResult: BasicReportResult,
  impressionQualificationFilterMapping: ImpressionQualificationFilterMapping,
  row: Struct,
): ReportComputation {
  return reportComputation {
    cmmsMeasurementConsumerId = basicReportResult.basicReport.cmmsMeasurementConsumerId
    externalBasicReportId = basicReportResult.basicReport.externalBasicReportId
    externalImpressionQualificationFilterId =
      ReportingSetResults.getExternalImpressionQualificationFilterId(
        impressionQualificationFilterMapping,
        row.getLong("ImpressionQualificationFilterId"),
      )
    cmmsCreateMeasurementRequestId = row.getString("CmmsCreateMeasurementRequestId")
    if (!row.isNull("CmmsMeasurementId")) {
      cmmsMeasurementId = row.getString("CmmsMeasurementId")
    }
    multimeasurement = row.getBytesAsByteString("Multimeasurement")
    mappings +=
      row.getBytesList("ResultGroupQueryMappings").map {
        ResultGroupQueryMapping.parseFrom(it.toByteArray())
      }
  }
}

private object ReportComputationsInternal {
  val COLUMNS =
    listOf(
      "ImpressionQualificationFilterId",
      "CmmsCreateMeasurementRequestId",
      "CmmsMeasurementId",
      "Multimeasurement",
      "ResultGroupQueryMappings",
    )
}
