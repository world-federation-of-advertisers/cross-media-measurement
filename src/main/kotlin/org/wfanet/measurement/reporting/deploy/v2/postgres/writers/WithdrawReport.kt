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

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.ResultRow
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.internal.reporting.v2.Measurement
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.WithdrawReportRequest
import org.wfanet.measurement.internal.reporting.v2.WithdrawReportResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.withdrawReportResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.service.internal.ReportNotFoundException

/** Withdraws a Report and Metrics that no other active Report references. */
class WithdrawReport(private val request: WithdrawReportRequest) :
  PostgresWriter<WithdrawReportResponse>() {
  override suspend fun TransactionScope.runTransaction(): WithdrawReportResponse {
    val reportResult =
      ReportReader(transactionContext)
        .readReportByExternalId(request.cmmsMeasurementConsumerId, request.externalReportId)
        ?: throw ReportNotFoundException {
          "Report ${request.cmmsMeasurementConsumerId}/${request.externalReportId} not found"
        }

    // Coordinate with Metric creation and reuse before deciding which work is exclusive.
    transactionContext
      .executeQuery(
        boundStatement(
          """
          SELECT ReportId
          FROM Reports
          WHERE MeasurementConsumerId = $1 AND ReportId = $2
          FOR UPDATE
          """
            .trimIndent()
        ) {
          bind("$1", reportResult.measurementConsumerId)
          bind("$2", reportResult.reportId)
        }
      )
      .consume {}
      .toList()

    transactionContext
      .executeQuery(
        boundStatement(
          """
          SELECT Metrics.MetricId
          FROM Metrics
          JOIN MetricCalculationSpecReportingMetrics AS ReportMetrics
            ON ReportMetrics.MeasurementConsumerId = Metrics.MeasurementConsumerId
            AND ReportMetrics.MetricId = Metrics.MetricId
          WHERE ReportMetrics.MeasurementConsumerId = $1
            AND ReportMetrics.ReportId = $2
          ORDER BY Metrics.MetricId
          FOR UPDATE OF Metrics
          """
            .trimIndent()
        ) {
          bind("$1", reportResult.measurementConsumerId)
          bind("$2", reportResult.reportId)
        }
      )
      .consume {}
      .toList()

    transactionContext.executeStatement(
      boundStatement(
        """
        UPDATE Reports SET Withdrawn = TRUE
        WHERE MeasurementConsumerId = $1 AND ReportId = $2
        """
          .trimIndent()
      ) {
        bind("$1", reportResult.measurementConsumerId)
        bind("$2", reportResult.reportId)
      }
    )

    transactionContext.executeStatement(
      boundStatement(
        """
        UPDATE Metrics SET State = $1
        WHERE MeasurementConsumerId = $2
          AND State IN ($3, $4, $1)
          AND MetricId IN (
            SELECT CurrentReportMetrics.MetricId
            FROM MetricCalculationSpecReportingMetrics AS CurrentReportMetrics
            WHERE CurrentReportMetrics.MeasurementConsumerId = $2
              AND CurrentReportMetrics.ReportId = $5
              AND CurrentReportMetrics.MetricId IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM MetricCalculationSpecReportingMetrics AS OtherReportMetrics
                JOIN Reports AS OtherReports
                  ON OtherReports.MeasurementConsumerId = OtherReportMetrics.MeasurementConsumerId
                  AND OtherReports.ReportId = OtherReportMetrics.ReportId
                WHERE OtherReportMetrics.MeasurementConsumerId = $2
                  AND OtherReportMetrics.MetricId = CurrentReportMetrics.MetricId
                  AND OtherReportMetrics.ReportId <> $5
                  AND OtherReports.Withdrawn = FALSE
              )
          )
        """
          .trimIndent()
      ) {
        bind("$1", Metric.State.WITHDRAWN)
        bind("$2", reportResult.measurementConsumerId)
        bind("$3", Metric.State.STATE_UNSPECIFIED)
        bind("$4", Metric.State.RUNNING)
        bind("$5", reportResult.reportId)
      }
    )

    val cmmsMeasurementIds: List<String> =
      transactionContext
        .executeQuery(
          boundStatement(
            """
            SELECT DISTINCT Measurements.CmmsMeasurementId
            FROM MetricCalculationSpecReportingMetrics AS ReportMetrics
            JOIN Metrics
              ON Metrics.MeasurementConsumerId = ReportMetrics.MeasurementConsumerId
              AND Metrics.MetricId = ReportMetrics.MetricId
            JOIN MetricMeasurements
              ON MetricMeasurements.MeasurementConsumerId = Metrics.MeasurementConsumerId
              AND MetricMeasurements.MetricId = Metrics.MetricId
            JOIN Measurements
              ON Measurements.MeasurementConsumerId = MetricMeasurements.MeasurementConsumerId
              AND Measurements.MeasurementId = MetricMeasurements.MeasurementId
            WHERE ReportMetrics.MeasurementConsumerId = $1
              AND ReportMetrics.ReportId = $2
              AND Metrics.State = $3
              AND Measurements.State = $4
              AND Measurements.CmmsMeasurementId IS NOT NULL
              AND NOT EXISTS (
                SELECT 1
                FROM MetricCalculationSpecReportingMetrics AS OtherReportMetrics
                JOIN Reports AS OtherReports
                  ON OtherReports.MeasurementConsumerId = OtherReportMetrics.MeasurementConsumerId
                  AND OtherReports.ReportId = OtherReportMetrics.ReportId
                WHERE OtherReportMetrics.MeasurementConsumerId = $1
                  AND OtherReportMetrics.MetricId = ReportMetrics.MetricId
                  AND OtherReportMetrics.ReportId <> $2
                  AND OtherReports.Withdrawn = FALSE
              )
            """
              .trimIndent()
          ) {
            bind("$1", reportResult.measurementConsumerId)
            bind("$2", reportResult.reportId)
            bind("$3", Metric.State.WITHDRAWN)
            bind("$4", Measurement.State.PENDING)
          }
        )
        .consume { row: ResultRow -> row.get<String>("CmmsMeasurementId") }
        .toList()

    return withdrawReportResponse {
      report = reportResult.report.copy { withdrawn = true }
      this.cmmsMeasurementIds += cmmsMeasurementIds
    }
  }
}
