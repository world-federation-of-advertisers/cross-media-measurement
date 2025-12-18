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
import java.time.temporal.ChronoUnit
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.boundStatement
import org.wfanet.measurement.common.db.r2dbc.postgres.PostgresWriter
import org.wfanet.measurement.common.identity.InternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.common.toJson
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.internal.reporting.v2.CreateReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MeasurementConsumerReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricCalculationSpecReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricCalculationSpecNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

/**
 * Inserts a ReportSchedule into the database.
 *
 * Throws the following on [execute]:
 * * [ReportingSetNotFoundException] ReportingSet not found
 * * [MetricCalculationSpecNotFoundException] MetricCalculationSpec not found
 * * [MeasurementConsumerNotFoundException] MeasurementConsumer not found
 * * [ReportScheduleAlreadyExistsException] ReportSchedule already exists
 */
class CreateReportSchedule(private val request: CreateReportScheduleRequest) :
  PostgresWriter<ReportSchedule>() {
  override suspend fun TransactionScope.runTransaction(): ReportSchedule {
    val measurementConsumerId =
      (MeasurementConsumerReader(transactionContext)
          .getByCmmsId(request.reportSchedule.cmmsMeasurementConsumerId)
          ?: throw MeasurementConsumerNotFoundException(
            request.reportSchedule.cmmsMeasurementConsumerId
          ))
        .measurementConsumerId

    val createReportScheduleRequestId = request.requestId
    val reportSchedule = request.reportSchedule

    // Request IDs take precedence
    if (createReportScheduleRequestId.isNotBlank()) {
      val existingReportScheduleResult: ReportScheduleReader.Result? =
        ReportScheduleReader(transactionContext)
          .readReportScheduleByRequestId(measurementConsumerId, createReportScheduleRequestId)

      if (existingReportScheduleResult != null) {
        return existingReportScheduleResult.reportSchedule
      }
    }

    // If we can't find a report schedule given the request ID but find a report schedule given the
    // external ID, return Already Exists exception.
    if (
      ReportScheduleReader(transactionContext)
        .readReportScheduleByExternalId(
          request.reportSchedule.cmmsMeasurementConsumerId,
          request.externalReportScheduleId,
        ) != null
    ) {
      throw ReportScheduleAlreadyExistsException(
        reportSchedule.cmmsMeasurementConsumerId,
        reportSchedule.externalReportScheduleId,
      )
    }

    val externalReportingSetIds: Set<String> = buildSet {
      reportSchedule.details.reportTemplate.reportingMetricEntriesMap.entries.forEach {
        add(it.key)
      }
    }

    val reportingSetMap: Map<String, InternalId> =
      ReportingSetReader(transactionContext)
        .readIds(measurementConsumerId, externalReportingSetIds)
        .toList()
        .associateBy({ it.externalReportingSetId }, { it.reportingSetId })

    for (externalReportingSetId in externalReportingSetIds) {
      if (!reportingSetMap.containsKey(externalReportingSetId)) {
        throw ReportingSetNotFoundException(
          reportSchedule.cmmsMeasurementConsumerId,
          externalReportingSetId,
        )
      }
    }

    val externalMetricCalculationSpecIds: Set<String> = buildSet {
      for (reportingMetricCalculationSpec in
        reportSchedule.details.reportTemplate.reportingMetricEntriesMap.values) {
        reportingMetricCalculationSpec.metricCalculationSpecReportingMetricsList.forEach {
          add(it.externalMetricCalculationSpecId)
        }
      }
    }

    val metricCalculationSpecMap: Map<String, InternalId> =
      MetricCalculationSpecReader(transactionContext)
        .batchReadByExternalIds(
          reportSchedule.cmmsMeasurementConsumerId,
          externalMetricCalculationSpecIds,
        )
        .associateBy(
          { it.metricCalculationSpec.externalMetricCalculationSpecId },
          { it.metricCalculationSpecId },
        )

    if (metricCalculationSpecMap.size < externalMetricCalculationSpecIds.size) {
      externalMetricCalculationSpecIds.forEach {
        if (!metricCalculationSpecMap.containsKey(it)) {
          throw MetricCalculationSpecNotFoundException(
            cmmsMeasurementConsumerId = reportSchedule.cmmsMeasurementConsumerId,
            externalMetricCalculationSpecId = it,
          )
        }
      }
    }

    val reportScheduleId = idGenerator.generateInternalId()
    val externalReportScheduleId = request.externalReportScheduleId
    val createTime = Instant.now().atOffset(ZoneOffset.UTC).truncatedTo(ChronoUnit.MICROS)

    val statement =
      boundStatement(
        """
      INSERT INTO ReportSchedules
        (
          MeasurementConsumerId,
          ReportScheduleId,
          ExternalReportScheduleId,
          CreateReportScheduleRequestId,
          State,
          NextReportCreationTime,
          CreateTime,
          UpdateTime,
          ReportScheduleDetails,
          ReportScheduleDetailsJson
        )
        VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
      """
      ) {
        bind("$1", measurementConsumerId)
        bind("$2", reportScheduleId)
        bind("$3", externalReportScheduleId)
        if (createReportScheduleRequestId.isNotEmpty()) {
          bind("$4", createReportScheduleRequestId)
        } else {
          bind<String?>("$4", null)
        }
        bind("$5", ReportSchedule.State.ACTIVE_VALUE)
        bind("$6", reportSchedule.nextReportCreationTime.toInstant().atOffset(ZoneOffset.UTC))
        bind("$7", createTime)
        bind("$8", createTime)
        bind("$9", reportSchedule.details)
        bind("$10", reportSchedule.details.toJson())
      }

    transactionContext.run { executeStatement(statement) }

    return request.reportSchedule.copy {
      this.externalReportScheduleId = externalReportScheduleId
      this.createTime = createTime.toInstant().toProtoTime()
      this.updateTime = createTime.toInstant().toProtoTime()
      state = ReportSchedule.State.ACTIVE
    }
  }
}
