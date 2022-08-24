// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.deploy.postgres

import io.grpc.Status
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.map
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.CreateReportRequest
import org.wfanet.measurement.internal.reporting.GetReportByIdempotencyKeyRequest
import org.wfanet.measurement.internal.reporting.GetReportRequest
import org.wfanet.measurement.internal.reporting.Report
import org.wfanet.measurement.internal.reporting.ReportsGrpcKt.ReportsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.StreamReportsRequest
import org.wfanet.measurement.reporting.deploy.postgres.SerializableErrors.withSerializableErrorRetries
import org.wfanet.measurement.reporting.deploy.postgres.readers.ReportReader
import org.wfanet.measurement.reporting.deploy.postgres.writers.CreateReport
import org.wfanet.measurement.reporting.service.internal.MeasurementCalculationTimeIntervalNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class PostgresReportsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : ReportsCoroutineImplBase() {
  override suspend fun createReport(request: CreateReportRequest): Report {
    try {
      return CreateReport(request).execute(client, idGenerator)
    } catch (e: ReportingSetNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Reporting Set not found" }
    } catch (e: MeasurementCalculationTimeIntervalNotFoundException) {
      e.throwStatusRuntimeException(Status.INVALID_ARGUMENT) {
        "Measurement Calculation Time Interval not found in Report"
      }
    }
  }

  override suspend fun getReport(request: GetReportRequest): Report {
    try {
      return SerializableErrors.retrying {
        ReportReader()
          .getReportByExternalId(
            client.singleUse(),
            request.measurementConsumerReferenceId,
            request.externalReportId
          )
          .report
      }
    } catch (e: ReportNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Report not found" }
    }
  }

  override suspend fun getReportByIdempotencyKey(
    request: GetReportByIdempotencyKeyRequest
  ): Report {
    try {
      return SerializableErrors.retrying {
        ReportReader()
          .getReportByIdempotencyKey(
            client.singleUse(),
            request.measurementConsumerReferenceId,
            request.reportIdempotencyKey
          )
          .report
      }
    } catch (e: ReportNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Report not found" }
    }
  }

  override fun streamReports(request: StreamReportsRequest): Flow<Report> {
    return ReportReader()
      .listReports(client, request.filter, request.limit)
      .map { result -> result.report }
      .withSerializableErrorRetries()
  }
}
