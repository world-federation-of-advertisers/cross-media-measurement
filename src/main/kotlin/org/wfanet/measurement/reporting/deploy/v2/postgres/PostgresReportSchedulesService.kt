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
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.CreateReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.GetReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportSchedulesResponse
import org.wfanet.measurement.internal.reporting.v2.ReportSchedule
import org.wfanet.measurement.internal.reporting.v2.ReportSchedulesGrpcKt.ReportSchedulesCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StopReportScheduleRequest
import org.wfanet.measurement.internal.reporting.v2.listReportSchedulesResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReportSchedule
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.StopReportSchedule
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricCalculationSpecNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleStateInvalidException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class PostgresReportSchedulesService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportSchedulesCoroutineImplBase(coroutineContext) {
  override suspend fun createReportSchedule(request: CreateReportScheduleRequest): ReportSchedule {
    grpcRequire(request.externalReportScheduleId.isNotEmpty()) {
      "external_report_schedule_id is not set."
    }

    grpcRequire(request.reportSchedule.details.reportTemplate.reportingMetricEntriesCount > 0) {
      "report_template is missing reporting metric entries."
    }

    return try {
      CreateReportSchedule(request).execute(client, idGenerator)
    } catch (e: ReportingSetNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Reporting Set not found.")
    } catch (e: MetricCalculationSpecNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Metric Calculation Spec not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: ReportScheduleAlreadyExistsException) {
      throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS, "Report Schedule already exists")
    }
  }

  override suspend fun getReportSchedule(request: GetReportScheduleRequest): ReportSchedule {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set"
    }

    grpcRequire(request.externalReportScheduleId.isNotEmpty()) {
      "external_report_schedule_id is not set."
    }

    val readContext = client.readTransaction()
    return try {
      ReportScheduleReader(readContext)
        .readReportScheduleByExternalId(
          request.cmmsMeasurementConsumerId,
          request.externalReportScheduleId,
        )
        ?.reportSchedule
        ?: throw Status.NOT_FOUND.withDescription("Report Schedule not found.").asRuntimeException()
    } finally {
      readContext.close()
    }
  }

  override suspend fun listReportSchedules(
    request: ListReportSchedulesRequest
  ): ListReportSchedulesResponse {
    grpcRequire(request.filter.cmmsMeasurementConsumerId.isNotEmpty()) {
      "Filter is missing cmms_measurement_consumer_id"
    }

    val readContext = client.readTransaction()
    return try {
      listReportSchedulesResponse {
        reportSchedules +=
          if (request.filter.state == ReportSchedule.State.STATE_UNSPECIFIED) {
              ReportScheduleReader(readContext).readReportSchedules(request)
            } else {
              ReportScheduleReader(readContext).readReportSchedulesByState(request)
            }
            .map { it.reportSchedule }
      }
    } finally {
      readContext.close()
    }
  }

  override suspend fun stopReportSchedule(request: StopReportScheduleRequest): ReportSchedule {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set."
    }

    grpcRequire(request.externalReportScheduleId.isNotEmpty()) {
      "external_report_schedule_id is not set."
    }

    return try {
      StopReportSchedule(request).execute(client, idGenerator)
    } catch (e: ReportScheduleNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Report Schedule not found.")
    } catch (e: ReportScheduleStateInvalidException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Report Schedule State is not ACTIVE.",
      )
    }
  }
}
