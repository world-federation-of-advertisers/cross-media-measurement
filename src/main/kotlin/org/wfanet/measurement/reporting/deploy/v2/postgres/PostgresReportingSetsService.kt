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
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.db.r2dbc.postgres.SerializableErrors.withSerializableErrorRetries
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.ReportingSetReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReportingSet
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetAlreadyExistsException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

private const val MAX_BATCH_SIZE = 1000

class PostgresReportingSetsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
    grpcRequire(request.externalReportingSetId.isNotEmpty()) {
      "External reporting set ID is not set."
    }
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (request.reportingSet.valueCase) {
      ReportingSet.ValueCase.PRIMITIVE -> {}
      ReportingSet.ValueCase.COMPOSITE -> {
        if (
          request.reportingSet.composite.lhs.operandCase ==
            ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET
        ) {
          failGrpc(Status.INVALID_ARGUMENT) { "lhs operand not specified" }
        }
      }
      ReportingSet.ValueCase.VALUE_NOT_SET -> {
        failGrpc(Status.INVALID_ARGUMENT) { "Reporting Set invalid" }
      }
    }
    return try {
      CreateReportingSet(request).execute(client, idGenerator)
    } catch (e: ReportingSetNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Reporting Set not found")
    } catch (e: ReportingSetAlreadyExistsException) {
      throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS, "Reporting Set already exists")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement Consumer not found",
      )
    }
  }

  override suspend fun batchGetReportingSets(
    request: BatchGetReportingSetsRequest
  ): BatchGetReportingSetsResponse {
    if (request.externalReportingSetIdsList.size > MAX_BATCH_SIZE) {
      failGrpc(Status.INVALID_ARGUMENT) { "Too many Reporting Sets requested" }
    }

    val readContext = client.readTransaction()
    val reportingSets =
      try {
        ReportingSetReader(readContext)
          .batchGetReportingSets(request)
          .map { it.reportingSet }
          .withSerializableErrorRetries()
          .toList()
      } catch (e: ReportingSetNotFoundException) {
        throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Reporting Set not found")
      } finally {
        readContext.close()
      }

    if (reportingSets.size < request.externalReportingSetIdsList.size) {
      failGrpc(Status.NOT_FOUND) { "Reporting Set not found" }
    }

    return batchGetReportingSetsResponse { this.reportingSets += reportingSets }
  }

  override fun streamReportingSets(request: StreamReportingSetsRequest): Flow<ReportingSet> {
    if (request.filter.cmmsMeasurementConsumerId.isEmpty()) {
      failGrpc(Status.INVALID_ARGUMENT) { "Filter is missing cmms_measurement_consumer_id" }
    }

    return flow {
      val readContext = client.readTransaction()
      try {
        emitAll(
          ReportingSetReader(readContext)
            .readReportingSets(request)
            .map { it.reportingSet }
            .withSerializableErrorRetries()
        )
      } finally {
        readContext.close()
      }
    }
  }
}
