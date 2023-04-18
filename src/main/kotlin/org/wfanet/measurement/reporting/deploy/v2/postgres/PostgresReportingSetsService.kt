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
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchGetReportingSetsResponse
import org.wfanet.measurement.internal.reporting.v2.ReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamReportingSetsRequest
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateReportingSet
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class PostgresReportingSetsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : ReportingSetsCoroutineImplBase() {
  override suspend fun createReportingSet(request: ReportingSet): ReportingSet {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    when (request.valueCase) {
      ReportingSet.ValueCase.PRIMITIVE -> {}
      ReportingSet.ValueCase.COMPOSITE -> {
        if (
          request.composite.lhs.operandCase ==
            ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET
        ) {
          failGrpc(Status.INVALID_ARGUMENT) { "Reporting Set invalid" }
        }
      }
      ReportingSet.ValueCase.VALUE_NOT_SET -> {
        failGrpc(Status.INVALID_ARGUMENT) { "Reporting Set invalid" }
      }
    }
    return try {
      CreateReportingSet(request).execute(client, idGenerator)
    } catch (e: ReportingSetNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Reporting Set not found" }
    } catch (e: MeasurementConsumerNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Measurement Consumer not found" }
    }
  }

  override suspend fun batchGetReportingSets(
    request: BatchGetReportingSetsRequest
  ): BatchGetReportingSetsResponse {
    return super.batchGetReportingSets(request)
  }

  override fun streamReportingSets(request: StreamReportingSetsRequest): Flow<ReportingSet> {
    return super.streamReportingSets(request)
  }
}
