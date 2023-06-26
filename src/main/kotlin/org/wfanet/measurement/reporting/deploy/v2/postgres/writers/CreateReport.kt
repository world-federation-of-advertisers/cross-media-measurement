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
import org.wfanet.measurement.common.identity.ExternalId
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
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a Report into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 */
class CreateReport(private val request: CreateReportRequest) : PostgresWriter<Report>() {
  private data class ReportingMetricEntriesAndBinders(
    val metricCalculationSpecsBinders: List<BoundStatement.Binder.() -> Unit>,
    val metricCalculationSpecReportingMetricsBinders: List<BoundStatement.Binder.() -> Unit>,
    val updatedReportingMetricEntries: Map<Long, Report.ReportingMetricCalculationSpec>,
  )
  override suspend fun TransactionScope.runTransaction(): Report {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(request.report.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException())
        .measurementConsumerId

    val createReportRequestId = request.requestId
    val report = request.report

    val existingReportResult: ReportReader.Result? =
      ReportReader(transactionContext)
        .readReportByRequestId(measurementConsumerId, createReportRequestId)

    if (existingReportResult != null) {
      return existingReportResult.report
    }

    val externalReportingSetIds = mutableListOf<ExternalId>()
    request.report.reportingMetricEntriesMap.entries.forEach {
      externalReportingSetIds += ExternalId(it.key)
    }

    val reportingSetMap: Map<ExternalId, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    if (reportingSetMap.size < externalReportingSetIds.size) {
      throw ReportingSetNotFoundException()
    }

    val reportId = idGenerator.generateInternalId()
    val externalReportId = idGenerator.generateExternalId()
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
          CreateTime
        )
        VALUES ($1, $2, $3, $4, $5)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportId)
        bind("$3", externalReportId)
        if (createReportRequestId.isNotBlank()) {
          bind("$4", createReportRequestId)
        } else {
          bind<String?>("$4", null)
        }
        bind("$5", createTime)
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
      this.externalReportId = externalReportId.value
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
    reportingSetMap: Map<ExternalId, InternalId>,
  ): ReportingMetricEntriesAndBinders {
    val metricCalculationSpecsBinders = mutableListOf<BoundStatement.Binder.() -> Unit>()
    val metricCalculationSpecReportingMetricsBinders =
      mutableListOf<BoundStatement.Binder.() -> Unit>()
    val updatedReportingMetricEntries = mutableMapOf<Long, Report.ReportingMetricCalculationSpec>()

    report.reportingMetricEntriesMap.entries.forEach { entry ->
      val updatedMetricCalculationSpecs = mutableListOf<Report.MetricCalculationSpec>()
      entry.value.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        val metricCalculationSpecId = idGenerator.generateInternalId()
        metricCalculationSpecsBinders.add {
          bind("$1", measurementConsumerId)
          bind("$2", reportId)
          bind("$3", metricCalculationSpecId)
          bind("$4", reportingSetMap[ExternalId(entry.key)])
          bind("$5", metricCalculationSpec.details)
          bind("$6", metricCalculationSpec.details.toJson())
        }

        val createMetricRequestId = UUID.randomUUID()
        val updatedReportingMetricsList = mutableListOf<Report.ReportingMetric>()
        metricCalculationSpec.reportingMetricsList.forEach {
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
