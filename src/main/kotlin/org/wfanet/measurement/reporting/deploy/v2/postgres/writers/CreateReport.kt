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

package org.wfanet.measurement.reporting.deploy.v2.postgres.writers

import java.time.Instant
import java.time.ZoneOffset
import java.util.UUID
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricCalculationSpecReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricCalculationSpecNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Report into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [MetricCalculationSpecNotFoundException] MetricCalculationSpec not found
 * * [ReportScheduleNotFoundException] ReportSchedule not found
 * * [ReportAlreadyExistsException] Report already exists
 */
class CreateReport(private val request: CreateReportRequest) : PostgresWriter<Report>() {
  private data class ReportingMetricEntriesAndStatementComponents(
    val metricCalculationSpecReportingMetricsSql: String,
    val metricCalculationSpecReportingMetricsBinders: List<BoundStatement.Binder.() -> Unit>,
    val updatedReportingMetricEntries: Map<String, Report.ReportingMetricCalculationSpec>,
  )

  private data class MetricCalculationSpecReportingMetricKey(
    val reportingSetId: InternalId,
    val metricCalculationSpecId: InternalId,
  )

  override suspend fun TransactionScope.runTransaction(): Report {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(request.report.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val createReportRequestId = request.requestId
    val report = request.report

    // Request IDs take precedence
    if (createReportRequestId.isNotBlank()) {
      val existingReportResult: ReportReader.Result? =
        ReportReader(transactionContext)
          .readReportByRequestId(measurementConsumerId, createReportRequestId)

      if (existingReportResult != null) {
        return existingReportResult.report
      }
    }

    // If we can't find a report given the request ID but found a report given the external ID,
    // return Already Exists exception.
    if (
      ReportReader(transactionContext)
        .readReportByExternalId(report.cmmsMeasurementConsumerId, request.externalReportId) != null
    ) {
      throw ReportAlreadyExistsException()
    }

    val externalReportingSetIds: Set<String> = buildSet {
      report.reportingMetricEntriesMap.entries.forEach { add(it.key) }
    }

    val reportingSetIdsByExternalId: Map<String, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    if (reportingSetIdsByExternalId.size < externalReportingSetIds.size) {
      throw ReportingSetNotFoundException()
    }

    val externalMetricCalculationSpecIds: Set<String> = buildSet {
      for (reportingMetricCalculationSpec in report.reportingMetricEntriesMap.values) {
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList.forEach {
          add(it.externalMetricCalculationSpecId)
        }
      }
    }

    val metricCalculationSpecsByExternalId: Map<String, MetricCalculationSpecReader.Result> =
      MetricCalculationSpecReader(transactionContext)
        .batchReadByExternalIds(
          request.report.cmmsMeasurementConsumerId,
          externalMetricCalculationSpecIds,
        )
        .associateBy { it.metricCalculationSpec.externalMetricCalculationSpecId }

    if (metricCalculationSpecsByExternalId.size < externalMetricCalculationSpecIds.size) {
      externalMetricCalculationSpecIds.forEach {
        if (!metricCalculationSpecsByExternalId.containsKey(it)) {
          throw MetricCalculationSpecNotFoundException(
            cmmsMeasurementConsumerId = report.cmmsMeasurementConsumerId,
            externalMetricCalculationSpecId = it,
          )
        }
      }
    }

    // Find existing metrics that match all the combinations of (ReportingSetId,
    // MetricCalculationSpecId, TimeInterval) in the report. Note that one combination can result in
    // multiple metrics.
    val reportingMetricKeys: Set<MetricReader.ReportingMetricKey> =
      report.reportingMetricEntriesMap
        .flatMap { entry ->
          val reportingSetId = reportingSetIdsByExternalId.getValue(entry.key)

          entry.value.metricCalculationSpecReportingMetricsList.flatMap {
            metricCalculationSpecReportingMetric ->
            val metricCalculationSpecId =
              metricCalculationSpecsByExternalId
                .getValue(metricCalculationSpecReportingMetric.externalMetricCalculationSpecId)
                .metricCalculationSpecId

            metricCalculationSpecReportingMetric.reportingMetricsList.map { reportingMetric ->
              MetricReader.ReportingMetricKey(
                reportingSetId,
                metricCalculationSpecId,
                reportingMetric.details.timeInterval,
              )
            }
          }
        }
        .toSet()

    // Retrieve existing metrics and group by `MetricCalculationSpecReportingMetricKey`
    val reportingMetricMap:
      Map<MetricCalculationSpecReportingMetricKey, List<MetricReader.ReportingMetric>> =
      MetricReader(transactionContext)
        .readReportingMetricsByReportingMetricKey(measurementConsumerId, reportingMetricKeys)
        .toList()
        .groupBy {
          MetricCalculationSpecReportingMetricKey(
            it.reportingMetricKey.reportingSetId,
            it.reportingMetricKey.metricCalculationSpecId,
          )
        }

    val reportId = idGenerator.generateInternalId()
    val externalReportId = request.externalReportId
    val createTime = Instant.now().atOffset(ZoneOffset.UTC)

    val statement =
      boundStatement(
        """
      INSERT INTO Reports
        (
          MeasurementConsumerId,
          ReportId,
          ExternalReportId,
          CreateReportRequestId,
          CreateTime,
          ReportDetails,
          ReportDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportId)
        bind("$3", externalReportId)
        if (createReportRequestId.isNotEmpty()) {
          bind("$4", createReportRequestId)
        } else {
          bind<String?>("$4", null)
        }
        bind("$5", createTime)
        bind("$6", report.details)
        bind("$7", report.details.toJson())
      }

    val reportingMetricEntriesAndStatementComponents =
      createMetricCalculationSpecStatementComponents(
        measurementConsumerId,
        reportId,
        report,
        reportingSetIdsByExternalId,
        metricCalculationSpecsByExternalId,
        reportingMetricMap,
      )

    val metricCalculationSpecReportingMetricsStatement =
      boundStatement(
        reportingMetricEntriesAndStatementComponents.metricCalculationSpecReportingMetricsSql
      ) {
        for (binder in
          reportingMetricEntriesAndStatementComponents
            .metricCalculationSpecReportingMetricsBinders) {
          binder()
        }
      }

    transactionContext.run {
      executeStatement(statement)
      executeStatement(metricCalculationSpecReportingMetricsStatement)

      if (request.hasReportScheduleInfo()) {
        val reportScheduleId =
          (ReportScheduleReader(transactionContext)
              .readReportScheduleByExternalId(
                request.report.cmmsMeasurementConsumerId,
                request.reportScheduleInfo.externalReportScheduleId,
              )
              ?: throw ReportScheduleNotFoundException(
                cmmsMeasurementConsumerId = request.report.cmmsMeasurementConsumerId,
                externalReportScheduleId = request.reportScheduleInfo.externalReportScheduleId,
              ))
            .reportScheduleId

        val reportsReportSchedulesStatement =
          boundStatement(
            """
            INSERT INTO ReportsReportSchedules
            (
              MeasurementConsumerId,
              ReportId,
              ReportScheduleId
            )
            VALUES ($1, $2, $3)
          """
          ) {
            bind("$1", measurementConsumerId)
            bind("$2", reportId)
            bind("$3", reportScheduleId)
          }
        executeStatement(reportsReportSchedulesStatement)

        val updateTime = Instant.now().atOffset(ZoneOffset.UTC)
        val reportSchedulesStatement =
          boundStatement(
            """
            UPDATE ReportSchedules SET NextReportCreationTime = $1, UpdateTime = $2
            WHERE MeasurementConsumerId = $3 AND ReportScheduleId = $4
            """
              .trimIndent()
          ) {
            bind(
              "$1",
              request.reportScheduleInfo.nextReportCreationTime.toInstant().atOffset(ZoneOffset.UTC),
            )
            bind("$2", updateTime)
            bind("$3", measurementConsumerId)
            bind("$4", reportScheduleId)
          }
        executeStatement(reportSchedulesStatement)
      }
    }

    return request.report.copy {
      this.createTime = createTime.toInstant().toProtoTime()
      this.externalReportId = externalReportId
      externalReportScheduleId = request.reportScheduleInfo.externalReportScheduleId
      reportingMetricEntries.clear()
      reportingMetricEntries.putAll(
        reportingMetricEntriesAndStatementComponents.updatedReportingMetricEntries
      )
    }
  }

  private fun createMetricCalculationSpecStatementComponents(
    measurementConsumerId: InternalId,
    reportId: InternalId,
    report: Report,
    reportingSetIdsByExternalId: Map<String, InternalId>,
    metricCalculationSpecsByExternalId: Map<String, MetricCalculationSpecReader.Result>,
    reportingMetricMap:
      Map<MetricCalculationSpecReportingMetricKey, List<MetricReader.ReportingMetric>>,
  ): ReportingMetricEntriesAndStatementComponents {
    val binders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val rowsSqlList = mutableListOf<String>()
    var curIndex = 1
    val offset = 8
    val updatedReportingMetricEntries =
      mutableMapOf<String, Report.ReportingMetricCalculationSpec>()

    for (entry in report.reportingMetricEntriesMap.entries) {
      val reportingSetId = reportingSetIdsByExternalId.getValue(entry.key)
      val updatedMetricCalculationSpecReportingMetricsList =
        mutableListOf<Report.MetricCalculationSpecReportingMetrics>()

      for (metricCalSpecReportingMetrics in entry.value.metricCalculationSpecReportingMetricsList) {
        val externalMetricCalculationSpecId =
          metricCalSpecReportingMetrics.externalMetricCalculationSpecId
        val metricCalculationSpecResult =
          metricCalculationSpecsByExternalId.getValue(externalMetricCalculationSpecId)
        val metricCalculationSpecReportingMetricKey =
          MetricCalculationSpecReportingMetricKey(
            reportingSetId,
            metricCalculationSpecResult.metricCalculationSpecId,
          )
        val updatedReportingMetricsList = mutableListOf<Report.ReportingMetric>()

        // If we found an existing set of metrics for this `MetricCalculationSpecReportingMetricKey`
        if (reportingMetricMap.contains(metricCalculationSpecReportingMetricKey)) {
          // Note that metric reuse mechanism won't produce the same order of `ReportingMetric`
          // internally, but this won't impact any immutable fields in public resources.
          reportingMetricMap.getValue(metricCalculationSpecReportingMetricKey).forEach {
            val reportingMetric =
              ReportKt.reportingMetric {
                createMetricRequestId = it.createMetricRequestId
                externalMetricId = it.externalMetricId
                details =
                  ReportKt.ReportingMetricKt.details {
                    metricSpec = it.metricSpec
                    timeInterval = it.reportingMetricKey.timeInterval

                    val filters: MutableList<String> = it.metricDetails.filtersList.toMutableList()
                    // The filters in a Metric is the combination of the grouping predicates and
                    // the filter in `MetricCalculationSpec`
                    filters.remove(metricCalculationSpecResult.metricCalculationSpec.details.filter)
                    groupingPredicates += filters
                  }
              }
            val tempIndex = curIndex
            binders.add {
              bind("$${tempIndex}", measurementConsumerId)
              bind("$${tempIndex + 1}", reportId)
              bind("$${tempIndex + 2}", reportingSetIdsByExternalId[entry.key])
              bind("$${tempIndex + 3}", metricCalculationSpecResult.metricCalculationSpecId)
              bind("$${tempIndex + 4}", UUID.fromString(reportingMetric.createMetricRequestId))
              bind("$${tempIndex + 5}", it.metricId)
              bind("$${tempIndex + 6}", reportingMetric.details)
              bind("$${tempIndex + 7}", reportingMetric.details.toJson())
            }
            rowsSqlList.add(generateParameterizedInsertValues(curIndex, curIndex + offset))
            curIndex += offset
            updatedReportingMetricsList.add(reportingMetric)
          }
        } else {
          metricCalSpecReportingMetrics.reportingMetricsList.forEach {
            val createMetricRequestId = UUID.randomUUID()
            val tempIndex = curIndex
            binders.add {
              bind("$${tempIndex}", measurementConsumerId)
              bind("$${tempIndex + 1}", reportId)
              bind("$${tempIndex + 2}", reportingSetIdsByExternalId[entry.key])
              bind("$${tempIndex + 3}", metricCalculationSpecResult.metricCalculationSpecId)
              bind("$${tempIndex + 4}", createMetricRequestId)
              bind<Long?>("$${tempIndex + 5}", null)
              bind("$${tempIndex + 6}", it.details)
              bind("$${tempIndex + 7}", it.details.toJson())
            }
            rowsSqlList.add(generateParameterizedInsertValues(curIndex, curIndex + offset))
            curIndex += offset
            updatedReportingMetricsList.add(
              it.copy { this.createMetricRequestId = createMetricRequestId.toString() }
            )
          }
        }

        updatedMetricCalculationSpecReportingMetricsList.add(
          metricCalSpecReportingMetrics.copy {
            reportingMetrics.clear()
            reportingMetrics.addAll(updatedReportingMetricsList)
          }
        )
      }

      updatedReportingMetricEntries[entry.key] =
        entry.value.copy {
          metricCalculationSpecReportingMetrics.clear()
          metricCalculationSpecReportingMetrics.addAll(
            updatedMetricCalculationSpecReportingMetricsList
          )
        }
    }

    val sql =
      """
      INSERT INTO MetricCalculationSpecReportingMetrics
        (
          MeasurementConsumerId,
          ReportId,
          ReportingSetId,
          MetricCalculationSpecId,
          CreateMetricRequestId,
          MetricId,
          ReportingMetricDetails,
          ReportingMetricDetailsJson
        )
      VALUES
      ${rowsSqlList.joinToString(",")}
      """

    return ReportingMetricEntriesAndStatementComponents(
      metricCalculationSpecReportingMetricsSql = sql,
      metricCalculationSpecReportingMetricsBinders = binders,
      updatedReportingMetricEntries = updatedReportingMetricEntries,
    )
  }
}
