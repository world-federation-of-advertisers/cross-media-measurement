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

import com.google.protobuf.Timestamp
import java.time.Instant
import java.time.ZoneOffset
import java.util.Optional
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.firstOrNull
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.ReadContext
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequest
import org.wfanet.measurement.internal.reporting.v2.report

class ReportReader(private val readContext: ReadContext) {
  data class Result(
    val measurementConsumerId: InternalId,
    val reportId: InternalId,
    val createReportRequestId: String,
    val report: Report,
  )

  private data class ReportInfo(
    val measurementConsumerId: InternalId,
    val cmmsMeasurementConsumerId: String,
    val createReportRequestId: String?,
    val reportId: InternalId,
    val externalReportId: String,
    val createTime: Timestamp,
    /** Map of external reporting set ID to [ReportingMetricCalculationSpecInfo]. */
    val reportingSetReportingMetricCalculationSpecInfoMap:
      MutableMap<String, ReportingMetricCalculationSpecInfo>,
    val details: Report.Details,
    val externalReportScheduleId: String?,
  )

  private data class ReportingMetricCalculationSpecInfo(
    /** Map of external metric calculation spec ID to [MetricCalculationSpecInfo]. */
    val metricCalculationSpecInfoMap: MutableMap<String, MetricCalculationSpecInfo>
  )

  private data class MetricCalculationSpecInfo(
    // Key is createMetricRequestId.
    val reportingMetricMap: MutableMap<String, Report.ReportingMetric>
  )

  private val baseSqlSelect: String =
    """
    SELECT
      CmmsMeasurementConsumerId,
      Reports.MeasurementConsumerId,
      Reports.ReportId,
      Reports.ExternalReportId,
      Reports.CreateReportRequestId,
      Reports.CreateTime,
      Reports.ReportDetails,
      MetricCalculationSpecs.ExternalMetricCalculationSpecId,
      ReportingSets.ExternalReportingSetId,
      MetricCalculationSpecReportingMetrics.CreateMetricRequestId,
      MetricCalculationSpecReportingMetrics.ReportingMetricDetails,
      Metrics.ExternalMetricId,
      ReportSchedules.ExternalReportScheduleId
    """
      .trimIndent()

  private val baseSqlJoins: String =
    """
    LEFT JOIN ReportsReportSchedules ON Reports.MeasurementConsumerId = ReportsReportSchedules.MeasurementConsumerId
      AND Reports.ReportId = ReportsReportSchedules.ReportId
    LEFT JOIN ReportSchedules ON Reports.MeasurementConsumerId = ReportSchedules.MeasurementConsumerId
      AND ReportsReportSchedules.ReportScheduleId = ReportSchedules.ReportScheduleId
    JOIN MetricCalculationSpecs ON Reports.MeasurementConsumerId = MetricCalculationSpecs.MeasurementConsumerId
    JOIN MetricCalculationSpecReportingMetrics ON Reports.MeasurementConsumerId = MetricCalculationSpecReportingMetrics.MeasurementConsumerId
      AND Reports.ReportId = MetricCalculationSpecReportingMetrics.ReportId
      AND MetricCalculationSpecs.MetricCalculationSpecId = MetricCalculationSpecReportingMetrics.MetricCalculationSpecId
    JOIN ReportingSets ON Reports.MeasurementConsumerId = ReportingSets.MeasurementConsumerId
      AND MetricCalculationSpecReportingMetrics.ReportingSetId = ReportingSets.ReportingSetId
    LEFT JOIN Metrics ON Reports.MeasurementConsumerId = Metrics.MeasurementConsumerId
      AND MetricCalculationSpecReportingMetrics.MetricId = Metrics.MetricId
    """
      .trimIndent()

  suspend fun readReportByRequestId(
    measurementConsumerId: InternalId,
    createReportRequestId: String,
  ): Result? {
    // There is an index using CreateReportRequestId, but only for Reports. The other tables only
    // have an index using ReportId.
    val sql =
      """
        $baseSqlSelect
        FROM MeasurementConsumers
            JOIN Reports ON MeasurementConsumers.MeasurementConsumerId = $1
            AND MeasurementConsumers.MeasurementConsumerId = Reports.MeasurementConsumerId
            AND ReportId IN (
              SELECT ReportId
              FROM Reports
              WHERE MeasurementConsumerId = $1
                AND CreateReportRequestId = $2
            )
            $baseSqlJoins
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", measurementConsumerId)
        bind("$2", createReportRequestId)
      }

    return createResultFlow(statement).firstOrNull()
  }

  suspend fun readReportByExternalId(
    cmmsMeasurementConsumerId: String,
    externalReportId: String,
  ): Result? {
    // There is an index using ExternalReportId, but only for Reports. The other tables only
    // have an index using ReportId.
    val sql =
      """
        WITH MeasurementConsumerForReport AS (
          SELECT *
          FROM MeasurementConsumers
          WHERE CmmsMeasurementConsumerId = $1
        )
        $baseSqlSelect
        FROM MeasurementConsumerForReport
          JOIN Reports ON MeasurementConsumerForReport.MeasurementConsumerId = Reports.MeasurementConsumerId
            AND ReportId IN (
              SELECT ReportId
              FROM Reports
              WHERE MeasurementConsumerId IN (SELECT MeasurementConsumerId FROM MeasurementConsumerForReport)
                AND ExternalReportId = $2
            )
          $baseSqlJoins
      """
        .trimIndent()

    val statement =
      boundStatement(sql) {
        bind("$1", cmmsMeasurementConsumerId)
        bind("$2", externalReportId)
      }

    return createResultFlow(statement).firstOrNull()
  }

  fun readReports(request: StreamReportsRequest): Flow<Result> {
    val fromClause =
      StringBuilder(
          """
        FROM (
          SELECT *
          FROM MeasurementConsumerForReports
            JOIN Reports USING (MeasurementConsumerId)
        """
        )
        .append(
          if (request.filter.hasAfter()) {
            """
                WHERE MeasurementConsumerId IN (SELECT MeasurementConsumerId FROM MeasurementConsumerForReports)
                  AND ((CreateTime < $3) OR
                  (CreateTime = $3
                  AND ExternalReportId > $4))
            """
          } else {
            """
                WHERE MeasurementConsumerId IN (SELECT MeasurementConsumerId FROM MeasurementConsumerForReports)
            """
          }
        )
        .append(
          """
            ORDER BY MeasurementConsumerId ASC, CreateTime DESC, ExternalReportId ASC
            LIMIT $2
            ) AS Reports
          """
        )

    val sql =
      StringBuilder("")
        .appendLine(
          """
          WITH MeasurementConsumerForReports AS (
            SELECT *
            FROM MeasurementConsumers
            WHERE CmmsMeasurementConsumerId = $1
          )
        """
            .trimIndent()
        )
        .appendLine(baseSqlSelect)
        .appendLine(fromClause)
        .appendLine(baseSqlJoins)
        .append("ORDER BY MeasurementConsumerId ASC, CreateTime DESC, ExternalReportId ASC")

    val statement =
      boundStatement(sql.toString()) {
        bind("$1", request.filter.cmmsMeasurementConsumerId)

        if (request.filter.hasAfter()) {
          bind("$3", request.filter.after.createTime.toInstant().atOffset((ZoneOffset.UTC)))
          bind("$4", request.filter.after.externalReportId)
        }
        if (request.limit > 0) {
          bind("$2", request.limit)
        } else {
          bind("$2", STREAM_DEFAULT_LIMIT)
        }
      }

    return createResultFlow(statement)
  }

  private fun createResultFlow(statement: BoundStatement): Flow<Result> {
    var accumulator: ReportInfo? = null
    val translate: (row: ResultRow) -> Optional<Result> = { row: ResultRow ->
      val measurementConsumerId: InternalId = row["MeasurementConsumerId"]
      val cmmsMeasurementConsumerId: String = row["CmmsMeasurementConsumerId"]
      val createReportRequestId: String? = row["CreateReportRequestId"]
      val reportId: InternalId = row["ReportId"]
      val externalReportId: String = row["ExternalReportId"]
      val createTime: Instant = row["CreateTime"]
      val reportDetails: Report.Details =
        row.getProtoMessage("ReportDetails", Report.Details.parser())
      val externalReportScheduleId: String? = row["ExternalReportScheduleId"]

      var result: Result? = null
      if (accumulator == null) {
        accumulator =
          ReportInfo(
            measurementConsumerId = measurementConsumerId,
            cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
            createReportRequestId = createReportRequestId,
            reportId = reportId,
            externalReportId = externalReportId,
            createTime = createTime.toProtoTime(),
            reportingSetReportingMetricCalculationSpecInfoMap = mutableMapOf(),
            details = reportDetails,
            externalReportScheduleId = externalReportScheduleId,
          )
      } else if (
        accumulator!!.externalReportId != externalReportId ||
          accumulator!!.measurementConsumerId != measurementConsumerId
      ) {
        result = accumulator!!.toResult()
        accumulator =
          ReportInfo(
            measurementConsumerId = measurementConsumerId,
            cmmsMeasurementConsumerId = cmmsMeasurementConsumerId,
            createReportRequestId = createReportRequestId,
            reportId = reportId,
            externalReportId = externalReportId,
            createTime = createTime.toProtoTime(),
            reportingSetReportingMetricCalculationSpecInfoMap = mutableMapOf(),
            details = reportDetails,
            externalReportScheduleId = externalReportScheduleId,
          )
      }

      accumulator!!.update(row)

      Optional.ofNullable(result)
    }

    return flow {
      // TODO(@tristanvuong2021): add null support to consume
      readContext.executeQuery(statement).consume(translate).collect {
        if (it.isPresent) {
          emit(it.get())
        }
      }
      if (accumulator != null) {
        emit(accumulator!!.toResult())
      }
    }
  }

  private fun ReportInfo.update(row: ResultRow) {
    val externalReportingSetId: String = row["ExternalReportingSetId"]
    val externalMetricCalculationSpecId: String = row["ExternalMetricCalculationSpecId"]
    val createMetricRequestId: String = row["CreateMetricRequestId"]
    val reportingMetricDetails: Report.ReportingMetric.Details =
      row.getProtoMessage("ReportingMetricDetails", Report.ReportingMetric.Details.parser())
    val externalMetricId: String? = row["ExternalMetricId"]

    val reportingMetricCalculationSpecInfo =
      reportingSetReportingMetricCalculationSpecInfoMap.computeIfAbsent(externalReportingSetId) {
        ReportingMetricCalculationSpecInfo(metricCalculationSpecInfoMap = mutableMapOf())
      }

    val metricCalculationSpecInfo =
      reportingMetricCalculationSpecInfo.metricCalculationSpecInfoMap.computeIfAbsent(
        externalMetricCalculationSpecId
      ) {
        MetricCalculationSpecInfo(reportingMetricMap = mutableMapOf())
      }

    metricCalculationSpecInfo.reportingMetricMap.computeIfAbsent(createMetricRequestId) {
      ReportKt.reportingMetric {
        this.createMetricRequestId = createMetricRequestId
        if (externalMetricId != null) {
          this.externalMetricId = externalMetricId
        }
        details = reportingMetricDetails
      }
    }
  }

  private fun ReportInfo.toResult(): Result {
    val source = this
    val report = report {
      cmmsMeasurementConsumerId = source.cmmsMeasurementConsumerId
      externalReportId = source.externalReportId
      createTime = source.createTime
      if (source.details != Report.Details.getDefaultInstance()) {
        details = source.details
      }

      source.reportingSetReportingMetricCalculationSpecInfoMap.entries.forEach {
        reportingMetricEntry ->
        val reportingMetricCalculationSpec =
          ReportKt.reportingMetricCalculationSpec {
            reportingMetricEntry.value.metricCalculationSpecInfoMap.entries.forEach {
              metricCalculationSpecEntry ->
              metricCalculationSpecReportingMetrics +=
                ReportKt.metricCalculationSpecReportingMetrics {
                  externalMetricCalculationSpecId = metricCalculationSpecEntry.key
                  reportingMetrics += metricCalculationSpecEntry.value.reportingMetricMap.values
                }
            }
          }

        reportingMetricEntries.put(reportingMetricEntry.key, reportingMetricCalculationSpec)
      }

      if (source.externalReportScheduleId != null) {
        externalReportScheduleId = source.externalReportScheduleId
      }
    }

    val createReportRequestId = source.createReportRequestId ?: ""
    return Result(
      measurementConsumerId = source.measurementConsumerId,
      reportId = source.reportId,
      createReportRequestId = createReportRequestId,
      report = report,
    )
  }

  companion object {
    private const val STREAM_DEFAULT_LIMIT = 50
  }
}
