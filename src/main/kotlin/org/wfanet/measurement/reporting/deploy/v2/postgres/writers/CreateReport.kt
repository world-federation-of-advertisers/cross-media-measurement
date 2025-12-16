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

import com.google.type.Interval
import java.time.Instant
import java.time.ZoneOffset
import java.time.temporal.ChronoUnit
import java.util.UUID
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.BoundStatement
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.db.r2dbc.postgres.ValuesListBoundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.valuesListBoundStatement
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricKt
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.Report
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
class CreateReport(
  private val request: CreateReportRequest,
  private val disableMetricsReuse: Boolean,
) : PostgresWriter<Report>() {
  private data class ReportingMetricEntriesAndStatement(
    val metricCalculationSpecReportingMetricsStatement: BoundStatement,
    val updatedReportingMetricEntries: Map<String, Report.ReportingMetricCalculationSpec>,
  )

  private data class MetricCalculationSpecReportingMetricKey(
    val reportingSetId: InternalId,
    val timeInterval: Interval,
    val metricCalculationSpecId: InternalId,
    val metricSpec: MetricSpec,
    val metricDetails: Metric.Details,
  )

  override suspend fun TransactionScope.runTransaction(): Report {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(request.report.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException(request.report.cmmsMeasurementConsumerId))
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

    for (externalReportingSetId in externalReportingSetIds) {
      if (!reportingSetIdsByExternalId.containsKey(externalReportingSetId)) {
        throw ReportingSetNotFoundException(
          report.cmmsMeasurementConsumerId,
          externalReportingSetId,
        )
      }
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

    val reportingMetricMap:
      Map<MetricCalculationSpecReportingMetricKey, MetricReader.ReportingMetric> =
      if (!disableMetricsReuse) {
        buildReusableMetricsMap(
          report,
          measurementConsumerId,
          reportingSetIdsByExternalId,
          metricCalculationSpecsByExternalId,
        )
      } else {
        emptyMap()
      }

    val reportId = idGenerator.generateInternalId()
    val externalReportId = request.externalReportId
    val createTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

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

    val reportingMetricEntriesAndStatement =
      createMetricCalculationSpecStatement(
        measurementConsumerId,
        reportId,
        report,
        reportingSetIdsByExternalId,
        metricCalculationSpecsByExternalId,
        reportingMetricMap,
      )

    transactionContext.run {
      executeStatement(statement)
      executeStatement(
        reportingMetricEntriesAndStatement.metricCalculationSpecReportingMetricsStatement
      )

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

        val updateTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)
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
        reportingMetricEntriesAndStatement.updatedReportingMetricEntries
      )
    }
  }

  private suspend fun TransactionScope.buildReusableMetricsMap(
    report: Report,
    measurementConsumerId: InternalId,
    reportingSetIdsByExternalId: Map<String, InternalId>,
    metricCalculationSpecsByExternalId: Map<String, MetricCalculationSpecReader.Result>,
  ): Map<MetricCalculationSpecReportingMetricKey, MetricReader.ReportingMetric> {
    // Find all combinations of (ReportingSetId, MetricCalculationSpecId, TimeInterval) in the
    // Report.
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

    // Retrieve existing metrics and select the most recently created Metric for each
    // `MetricCalculationSpecReportingMetricKey`.
    return buildMap {
      MetricReader(transactionContext)
        .readReportingMetricsByReportingMetricKey(measurementConsumerId, reportingMetricKeys)
        .collect {
          val key =
            MetricCalculationSpecReportingMetricKey(
              it.reportingMetricKey.reportingSetId,
              it.reportingMetricKey.timeInterval,
              it.reportingMetricKey.metricCalculationSpecId,
              it.metricSpec,
              it.metricDetails,
            )
          val oldValue: MetricReader.ReportingMetric? = get(key)
          if (
            it.state != Metric.State.FAILED &&
              it.state != Metric.State.INVALID &&
              (oldValue == null || it.createTime.isAfter(oldValue.createTime))
          ) {
            put(key, it)
          }
        }
    }
  }

  private fun createMetricCalculationSpecStatement(
    measurementConsumerId: InternalId,
    reportId: InternalId,
    report: Report,
    reportingSetIdsByExternalId: Map<String, InternalId>,
    metricCalculationSpecsByExternalId: Map<String, MetricCalculationSpecReader.Result>,
    reportingMetricMap: Map<MetricCalculationSpecReportingMetricKey, MetricReader.ReportingMetric>,
  ): ReportingMetricEntriesAndStatement {
    val updatedReportingMetricEntries = hashMapOf<String, Report.ReportingMetricCalculationSpec>()

    val statement =
      valuesListBoundStatement(
        valuesStartIndex = 0,
        paramCount = 8,
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
      ${ValuesListBoundStatement.VALUES_LIST_PLACEHOLDER}
      """,
      ) {
        for (entry in report.reportingMetricEntriesMap.entries) {
          val reportingSetId = reportingSetIdsByExternalId.getValue(entry.key)
          val updatedMetricCalculationSpecReportingMetricsList =
            mutableListOf<Report.MetricCalculationSpecReportingMetrics>()

          for (metricCalSpecReportingMetrics in
            entry.value.metricCalculationSpecReportingMetricsList) {
            val externalMetricCalculationSpecId =
              metricCalSpecReportingMetrics.externalMetricCalculationSpecId
            val metricCalculationSpecResult =
              metricCalculationSpecsByExternalId.getValue(externalMetricCalculationSpecId)
            val updatedReportingMetricsList = mutableListOf<Report.ReportingMetric>()
            val encounteredKeys = hashSetOf<MetricCalculationSpecReportingMetricKey>()

            metricCalSpecReportingMetrics.reportingMetricsList.forEach {
              val metricCalculationSpecReportingMetricKey =
                MetricCalculationSpecReportingMetricKey(
                  reportingSetId,
                  it.details.timeInterval,
                  metricCalculationSpecResult.metricCalculationSpecId,
                  it.details.metricSpec,
                  MetricKt.details {
                    filters +=
                      (it.details.groupingPredicatesList +
                          metricCalculationSpecResult.metricCalculationSpec.details.filter)
                        .filter { filter -> filter.isNotBlank() }
                  },
                )

              if (encounteredKeys.contains(metricCalculationSpecReportingMetricKey)) {
                return@forEach
              } else {
                encounteredKeys.add(metricCalculationSpecReportingMetricKey)
              }

              val existingReportingMetric: MetricReader.ReportingMetric? =
                reportingMetricMap[metricCalculationSpecReportingMetricKey]

              val (metricId, createMetricRequestId) =
                if (existingReportingMetric != null) {
                  Pair(
                    existingReportingMetric.metricId,
                    UUID.fromString(existingReportingMetric.createMetricRequestId),
                  )
                } else {
                  Pair(null, UUID.randomUUID())
                }
              addValuesBinding {
                bindValuesParam(0, measurementConsumerId)
                bindValuesParam(1, reportId)
                bindValuesParam(2, reportingSetIdsByExternalId[entry.key])
                bindValuesParam(3, metricCalculationSpecResult.metricCalculationSpecId)
                bindValuesParam(4, createMetricRequestId)
                bindValuesParam(5, metricId)
                bindValuesParam(6, it.details)
                bindValuesParam(7, it.details.toJson())
              }
              updatedReportingMetricsList.add(
                it.copy {
                  this.createMetricRequestId = createMetricRequestId.toString()
                  if (existingReportingMetric != null) {
                    externalMetricId = existingReportingMetric.externalMetricId
                  }
                }
              )
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
      }

    return ReportingMetricEntriesAndStatement(
      metricCalculationSpecReportingMetricsStatement = statement,
      updatedReportingMetricEntries = updatedReportingMetricEntries,
    )
  }
}
