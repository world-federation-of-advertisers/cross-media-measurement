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

import com.google.protobuf.util.Timestamps
import com.google.type.Interval
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
  private data class ReportingMetricEntriesAndBinders(
    val metricCalculationSpecReportingMetricsBinders: List<BoundStatement.Binder.() -> Unit>,
    val updatedReportingMetricEntries: Map<String, Report.ReportingMetricCalculationSpec>,
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

    val reportingSetIdByExternalId: Map<String, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    if (reportingSetIdByExternalId.size < externalReportingSetIds.size) {
      throw ReportingSetNotFoundException()
    }

    val externalMetricCalculationSpecIds: Set<String> = buildSet {
      for (reportingMetricCalculationSpec in report.reportingMetricEntriesMap.values) {
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList.forEach {
          add(it.externalMetricCalculationSpecId)
        }
      }
    }

    val metricCalculationSpecByExternalId: Map<String, MetricCalculationSpecReader.Result> =
      MetricCalculationSpecReader(transactionContext)
        .batchReadByExternalIds(
          request.report.cmmsMeasurementConsumerId,
          externalMetricCalculationSpecIds
        )
        .associateBy { it.metricCalculationSpec.externalMetricCalculationSpecId }

    if (metricCalculationSpecByExternalId.size < externalMetricCalculationSpecIds.size) {
      externalMetricCalculationSpecIds.forEach {
        if (!metricCalculationSpecByExternalId.containsKey(it)) {
          throw MetricCalculationSpecNotFoundException(
            cmmsMeasurementConsumerId = report.cmmsMeasurementConsumerId,
            externalMetricCalculationSpecId = it
          )
        }
      }
    }

    // Find existing sets of metrics for each ReportingMetricEntry from
    // report.reportingMetricEntriesMap. The key is an externalMetricCalculationSpecId.
    val metricListByExternalMetricCalculationSpecId: Map<String, List<MetricReader.Result>> =
      buildMap {
        for (entry in report.reportingMetricEntriesMap) {
          entry.value.metricCalculationSpecReportingMetricsList.forEach {
            val timeIntervals: Set<Interval> = buildSet {
              it.reportingMetricsList.forEach { reportingMetric ->
                add(reportingMetric.details.timeInterval)
              }
            }
            val metricReaderResults =
              MetricReader(transactionContext)
                .readMetricsByTimedReportingMetricEntry(
                  measurementConsumerId,
                  timeIntervals,
                  reportingSetIdByExternalId.getValue(entry.key),
                  metricCalculationSpecByExternalId
                    .getValue(it.externalMetricCalculationSpecId)
                    .metricCalculationSpecId
                )
                .toList()
            // Only store the result when there is any existing metric.
            if (metricReaderResults.isNotEmpty()) {
              put(it.externalMetricCalculationSpecId, metricReaderResults)
            }
          }
        }
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
          Periodic,
          ReportDetails,
          ReportDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
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
        bind("$6", report.timeCase == Report.TimeCase.PERIODIC_TIME_INTERVAL)
        bind("$7", report.details)
        bind("$8", report.details.toJson())
      }

    val reportTimeIntervalsStatement =
      boundStatement(
        """
      INSERT INTO ReportTimeIntervals
        (
          MeasurementConsumerId,
          ReportId,
          TimeIntervalStart,
          TimeIntervalEndExclusive
        )
        VALUES ($1, $2, $3, $4)
      """
      ) {
        createReportTimeIntervalsBindings(measurementConsumerId, reportId, report).forEach {
          addBinding(it)
        }
      }

    val reportingMetricEntriesAndBinders =
      createMetricCalculationSpecBindings(
        measurementConsumerId,
        reportId,
        report,
        reportingSetIdByExternalId,
        metricCalculationSpecByExternalId,
        metricListByExternalMetricCalculationSpecId
      )

    val metricCalculationSpecReportingMetricsStatement =
      boundStatement(
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
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
      """
      ) {
        reportingMetricEntriesAndBinders.metricCalculationSpecReportingMetricsBinders.forEach {
          addBinding(it)
        }
      }

    transactionContext.run {
      executeStatement(statement)
      executeStatement(reportTimeIntervalsStatement)
      executeStatement(metricCalculationSpecReportingMetricsStatement)

      if (request.externalReportScheduleId.isNotEmpty()) {
        val reportScheduleId =
          (ReportScheduleReader(transactionContext)
              .readReportScheduleByExternalId(
                request.report.cmmsMeasurementConsumerId,
                request.externalReportScheduleId
              )
              ?: throw ReportScheduleNotFoundException(
                cmmsMeasurementConsumerId = request.report.cmmsMeasurementConsumerId,
                externalReportScheduleId = request.externalReportScheduleId
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
      }
    }

    return request.report.copy {
      this.createTime = createTime.toInstant().toProtoTime()
      this.externalReportId = externalReportId
      externalReportScheduleId = request.externalReportScheduleId
      reportingMetricEntries.clear()
      reportingMetricEntries.putAll(reportingMetricEntriesAndBinders.updatedReportingMetricEntries)
    }
  }

  private fun createReportTimeIntervalsBindings(
    measurementConsumerId: InternalId,
    reportId: InternalId,
    report: Report,
  ): List<BoundStatement.Binder.() -> Unit> {
    val reportTimeIntervalsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()

    if (report.hasTimeIntervals()) {
      report.timeIntervals.timeIntervalsList.forEach {
        reportTimeIntervalsBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", reportId)
          bind("$3", it.startTime.toInstant().atOffset(ZoneOffset.UTC))
          bind("$4", it.endTime.toInstant().atOffset(ZoneOffset.UTC))
        }
      }
    } else if (report.hasPeriodicTimeInterval()) {
      val periodicTimeInterval = report.periodicTimeInterval
      var startTime = periodicTimeInterval.startTime
      var endTime = Timestamps.add(startTime, periodicTimeInterval.increment)
      for (i in 1..periodicTimeInterval.intervalCount) {
        val intervalStartTime = startTime.toInstant().atOffset(ZoneOffset.UTC)
        val intervalEndTime = endTime.toInstant().atOffset(ZoneOffset.UTC)
        reportTimeIntervalsBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", reportId)
          bind("$3", intervalStartTime)
          bind("$4", intervalEndTime)
        }
        startTime = endTime
        endTime = Timestamps.add(startTime, periodicTimeInterval.increment)
      }
    }

    return reportTimeIntervalsBinders
  }

  private fun TransactionScope.createMetricCalculationSpecBindings(
    measurementConsumerId: InternalId,
    reportId: InternalId,
    report: Report,
    reportingSetIdByExternalId: Map<String, InternalId>,
    metricCalculationSpecByExternalId: Map<String, MetricCalculationSpecReader.Result>,
    metricListByExternalMetricCalculationSpecId: Map<String, List<MetricReader.Result>>
  ): ReportingMetricEntriesAndBinders {
    val metricCalculationSpecReportingMetricsBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()
    val updatedReportingMetricEntries =
      mutableMapOf<String, Report.ReportingMetricCalculationSpec>()

    for (entry in report.reportingMetricEntriesMap.entries) {
      val updatedMetricCalculationSpecReportingMetricsList =
        mutableListOf<Report.MetricCalculationSpecReportingMetrics>()

      for (metricCalculationSpecReportingMetrics in
        entry.value.metricCalculationSpecReportingMetricsList) {
        val externalMetricCalculationSpecId =
          metricCalculationSpecReportingMetrics.externalMetricCalculationSpecId
        val metricCalculationSpecResult =
          metricCalculationSpecByExternalId.getValue(externalMetricCalculationSpecId)
        val updatedReportingMetricsList = mutableListOf<Report.ReportingMetric>()

        // If we found an existing set of metrics for this `MetricCalculationSpec`
        if (metricListByExternalMetricCalculationSpecId.contains(externalMetricCalculationSpecId)) {
          metricListByExternalMetricCalculationSpecId
            .getValue(externalMetricCalculationSpecId)
            .forEach { metricResult ->
              val reportingMetric =
                ReportKt.reportingMetric {
                  createMetricRequestId = metricResult.createMetricRequestId
                  externalMetricId = metricResult.metric.externalMetricId
                  details =
                    ReportKt.ReportingMetricKt.details {
                      metricSpec = metricResult.metric.metricSpec
                      timeInterval = metricResult.metric.timeInterval

                      val filters: MutableList<String> =
                        metricResult.metric.details.filtersList.toMutableList()
                      // The filters in a Metric is the combination of the grouping predicates and
                      // the filter in `MetricCalculationSpec`
                      filters.remove(
                        metricCalculationSpecResult.metricCalculationSpec.details.filter
                      )
                      groupingPredicates += filters
                    }
                }
              metricCalculationSpecReportingMetricsBinders.add {
                bind("$1", measurementConsumerId)
                bind("$2", reportId)
                bind("$3", reportingSetIdByExternalId[entry.key])
                bind("$4", metricCalculationSpecResult.metricCalculationSpecId)
                bind("$5", UUID.fromString(reportingMetric.createMetricRequestId))
                bind("$6", metricResult.metricId)
                bind("$7", reportingMetric.details)
                bind("$8", reportingMetric.details.toJson())
              }
              updatedReportingMetricsList.add(reportingMetric)
            }
        } else {
          metricCalculationSpecReportingMetrics.reportingMetricsList.forEach {
            val createMetricRequestId = UUID.randomUUID()
            metricCalculationSpecReportingMetricsBinders.add {
              bind("$1", measurementConsumerId)
              bind("$2", reportId)
              bind("$3", reportingSetIdByExternalId[entry.key])
              bind("$4", metricCalculationSpecResult.metricCalculationSpecId)
              bind("$5", createMetricRequestId)
              bind<Long?>("$6", null)
              bind("$7", it.details)
              bind("$8", it.details.toJson())
            }
            updatedReportingMetricsList.add(
              it.copy { this.createMetricRequestId = createMetricRequestId.toString() }
            )
          }
        }

        updatedMetricCalculationSpecReportingMetricsList.add(
          metricCalculationSpecReportingMetrics.copy {
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

    return ReportingMetricEntriesAndBinders(
      metricCalculationSpecReportingMetricsBinders = metricCalculationSpecReportingMetricsBinders,
      updatedReportingMetricEntries = updatedReportingMetricEntries,
    )
  }
}
