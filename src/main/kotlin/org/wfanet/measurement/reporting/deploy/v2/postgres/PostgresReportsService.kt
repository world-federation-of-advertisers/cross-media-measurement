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

package org.wfanet.measurement.reporting.deploy.v2.postgres

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.emitAll
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.SerializableErrors.withSerializableErrorRetries
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.CreateReportRequest
import org.wfanet.measurement.internal.reporting.v2.GetReportRequest
import org.wfanet.measurement.internal.reporting.v2.Report
import org.wfanet.measurement.internal.reporting.v2.ReportsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.StreamReportsRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReport
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class PostgresReportsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : ReportsGrpcKt.ReportsCoroutineImplBase() {
  override suspend fun createReport(request: CreateReportRequest): Report {
    grpcRequire(request.externalReportId.isNotEmpty()) { "External report ID is not set." }
    grpcRequire(request.report.hasTimeIntervals() || request.report.hasPeriodicTimeInterval()) {
      "Report is missing time."
    }

    grpcRequire(request.report.reportingMetricEntriesCount > 0) {
      "Report is missing reporting metric entries."
    }

    request.report.reportingMetricEntriesMap.entries.forEach { entry ->
      entry.value.metricCalculationSpecsList.forEach { metricCalculationSpec ->
        metricCalculationSpec.reportingMetricsList.forEach {
          grpcRequire(entry.key == it.details.externalReportingSetId) {
            "All metrics in a reporting metric entry must have the same external reporting set id as the key"
          }
        }
      }
    }

    return try {
      CreateReport(request).execute(client, idGenerator)
    } catch (e: ReportingSetNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Reporting Set not found.")
    } catch (e: ReportScheduleNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Report Schedule not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement Consumer not found."
      )
    } catch (e: ReportAlreadyExistsException) {
      throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS, "Report already exists")
    }
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    val readContext = client.readTransaction()
    return try {
      ReportReader(readContext)
        .readReportByExternalId(request.cmmsMeasurementConsumerId, request.externalReportId)
        ?.report ?: throw Status.NOT_FOUND.withDescription("Report not found.").asRuntimeException()
    } finally {
      readContext.close()
    }
  }

  override fun streamReports(request: StreamReportsRequest): Flow<Report> {
    grpcRequire(request.filter.cmmsMeasurementConsumerId.isNotEmpty()) {
      "Filter is missing cmms_measurement_consumer_id"
    }

    return flow {
      val readContext = client.readTransaction()
      try {
        emitAll(
          ReportReader(readContext)
            .readReports(request)
            .map { it.report }
            .withSerializableErrorRetries()
        )
      } finally {
        readContext.close()
      }
    }
  }
}
