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
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Report into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [ReportAlreadyExistsException] Report already exists
 */
class CreateReport(private val request: CreateReportRequest) : PostgresWriter<Report>() {
  private data class ReportingMetricEntriesAndBinders(
    val metricCalculationSpecsBinders: List<BoundStatement.Binder.() -> Unit>,
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
    val existingReportResult: ReportReader.Result? =
      ReportReader(transactionContext)
        .readReportByRequestId(measurementConsumerId, createReportRequestId)

    if (existingReportResult != null) {
      return existingReportResult.report
    }

    // If we can't find a report given the request ID but found a report given the external ID,
    // return Already Exists exception.
    if (
      ReportReader(transactionContext)
        .readReportByExternalId(report.cmmsMeasurementConsumerId, request.externalReportId) != null
    ) {
      throw ReportAlreadyExistsException()
    }

    val externalReportingSetIds = mutableListOf<String>()
    report.reportingMetricEntriesMap.entries.forEach { externalReportingSetIds += it.key }

    val reportingSetMap: Map<String, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    if (reportingSetMap.size < externalReportingSetIds.size) {
      throw ReportingSetNotFoundException()
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
      createMetricCalculationSpecBindings(measurementConsumerId, reportId, report, reportingSetMap)

    val metricCalculationSpecsStatement =
      boundStatement(
        """
      INSERT INTO MetricCalculationSpecs
        (
          MeasurementConsumerId,
          ReportId,
          MetricCalculationSpecId,
          ReportingSetId,
          MetricCalculationSpecDetails,
          MetricCalculationSpecDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6)
      """
      ) {
        reportingMetricEntriesAndBinders.metricCalculationSpecsBinders.forEach { addBinding(it) }
      }

    val metricCalculationSpecReportingMetricsStatement =
      boundStatement(
        """
      INSERT INTO MetricCalculationSpecReportingMetrics
        (
          MeasurementConsumerId,
          ReportId,
          MetricCalculationSpecId,
          CreateMetricRequestId,
          ReportingMetricDetails,
          ReportingMetricDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6)
      """
      ) {
        reportingMetricEntriesAndBinders.metricCalculationSpecReportingMetricsBinders.forEach {
          addBinding(it)
        }
      }

    transactionContext.run {
      executeStatement(statement)
      executeStatement(reportTimeIntervalsStatement)
      executeStatement(metricCalculationSpecsStatement)
      executeStatement(metricCalculationSpecReportingMetricsStatement)
    }

    return request.report.copy {
      this.createTime = createTime.toInstant().toProtoTime()
      this.externalReportId = externalReportId
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
    reportingSetMap: Map<String, InternalId>,
  ): ReportingMetricEntriesAndBinders {
    val metricCalculationSpecsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val metricCalculationSpecReportingMetricsBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()
    val updatedReportingMetricEntries =
      mutableMapOf<String, Report.ReportingMetricCalculationSpec>()

    report.reportingMetricEntriesMap.entries.forEach { entry ->
      val updatedMetricCalculationSpecs = mutableListOf<Report.MetricCalculationSpec>()
      entry.value.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        val metricCalculationSpecId = idGenerator.generateInternalId()
        metricCalculationSpecsBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", reportId)
          bind("$3", metricCalculationSpecId)
          bind("$4", reportingSetMap[entry.key])
          bind("$5", metricCalculationSpec.details)
          bind("$6", metricCalculationSpec.details.toJson())
        }

        val updatedReportingMetricsList = mutableListOf<Report.ReportingMetric>()
        metricCalculationSpec.reportingMetricsList.forEach {
          val createMetricRequestId = UUID.randomUUID()
          metricCalculationSpecReportingMetricsBinders.add {
            bind("$1", measurementConsumerId)
            bind("$2", reportId)
            bind("$3", metricCalculationSpecId)
            bind("$4", createMetricRequestId)
            bind("$5", it.details)
            bind("$6", it.details.toJson())
          }
          updatedReportingMetricsList.add(
            it.copy { this.createMetricRequestId = createMetricRequestId.toString() }
          )
        }

        updatedMetricCalculationSpecs.add(
          metricCalculationSpec.copy {
            reportingMetrics.clear()
            reportingMetrics.addAll(updatedReportingMetricsList)
          }
        )
      }

      updatedReportingMetricEntries[entry.key] =
        entry.value.copy {
          metricCalculationSpecs.clear()
          metricCalculationSpecs.addAll(updatedMetricCalculationSpecs)
        }
    }

    return ReportingMetricEntriesAndBinders(
      metricCalculationSpecsBinders = metricCalculationSpecsBinders,
      metricCalculationSpecReportingMetricsBinders = metricCalculationSpecReportingMetricsBinders,
      updatedReportingMetricEntries = updatedReportingMetricEntries,
    )
  }
}
