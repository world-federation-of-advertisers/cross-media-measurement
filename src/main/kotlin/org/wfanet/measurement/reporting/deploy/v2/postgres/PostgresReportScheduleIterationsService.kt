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
import org.wfanet.measurement.internal.reporting.v2.GetReportScheduleIterationRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsRequest
import org.wfanet.measurement.internal.reporting.v2.ListReportScheduleIterationsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIteration
import org.wfanet.measurement.internal.reporting.v2.ReportScheduleIterationsGrpcKt.ReportScheduleIterationsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.SetReportScheduleIterationStateRequest
import org.wfanet.measurement.internal.reporting.v2.listReportScheduleIterationsResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportScheduleIterationReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReportScheduleIteration
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetReportScheduleIterationState
import org.wfanet.measurement.reporting.service.internal.ReportScheduleIterationNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleIterationStateInvalidException
import org.wfanet.measurement.reporting.service.internal.ReportScheduleNotFoundException

class PostgresReportScheduleIterationsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : ReportScheduleIterationsCoroutineImplBase(coroutineContext) {
  override suspend fun createReportScheduleIteration(
    request: ReportScheduleIteration
  ): ReportScheduleIteration {
    return try {
      CreateReportScheduleIteration(request).execute(client, idGenerator)
    } catch (e: ReportScheduleNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Report Schedule not found.",
      )
    }
  }

  override suspend fun getReportScheduleIteration(
    request: GetReportScheduleIterationRequest
  ): ReportScheduleIteration {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set"
    }

    grpcRequire(request.externalReportScheduleId.isNotEmpty()) {
      "external_report_schedule_id is not set."
    }

    grpcRequire(request.externalReportScheduleIterationId.isNotEmpty()) {
      "external_report_schedule_iteration_id is not set."
    }

    val readContext = client.readTransaction()
    return try {
      ReportScheduleIterationReader(readContext)
        .readReportScheduleIterationByExternalId(
          cmmsMeasurementConsumerId = request.cmmsMeasurementConsumerId,
          externalReportScheduleId = request.externalReportScheduleId,
          externalReportScheduleIterationId = request.externalReportScheduleIterationId,
        )
        ?.reportScheduleIteration
        ?: throw Status.NOT_FOUND.withDescription("Report Schedule Iteration not found.")
          .asRuntimeException()
    } finally {
      readContext.close()
    }
  }

  override suspend fun listReportScheduleIterations(
    request: ListReportScheduleIterationsRequest
  ): ListReportScheduleIterationsResponse {
    grpcRequire(request.filter.cmmsMeasurementConsumerId.isNotEmpty()) {
      "Filter is missing cmms_measurement_consumer_id"
    }

    val readContext = client.readTransaction()
    return try {
      listReportScheduleIterationsResponse {
        reportScheduleIterations +=
          ReportScheduleIterationReader(readContext).readReportScheduleIterations(request).map {
            it.reportScheduleIteration
          }
      }
    } finally {
      readContext.close()
    }
  }

  override suspend fun setReportScheduleIterationState(
    request: SetReportScheduleIterationStateRequest
  ): ReportScheduleIteration {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set"
    }

    grpcRequire(request.externalReportScheduleId.isNotEmpty()) {
      "external_report_schedule_id is not set."
    }

    grpcRequire(request.externalReportScheduleIterationId.isNotEmpty()) {
      "external_report_schedule_iteration_id is not set."
    }

    return try {
      SetReportScheduleIterationState(request).execute(client, idGenerator)
    } catch (e: ReportScheduleIterationNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.NOT_FOUND,
        "Report Schedule Iteration not found.",
      )
    } catch (e: ReportScheduleIterationStateInvalidException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Report Schedule Iteration is already in the REPORT_CREATED state",
      )
    }
  }
}
