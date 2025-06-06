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

import com.google.protobuf.Empty
import io.grpc.Status
import kotlin.coroutines.CoroutineContext
import kotlin.coroutines.EmptyCoroutineContext
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetCmmsMeasurementIds
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetMeasurementFailures
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetMeasurementResults
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException

private const val BATCH_SIZE = 1000

class PostgresMeasurementsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MeasurementsGrpcKt.MeasurementsCoroutineImplBase(coroutineContext) {
  override suspend fun batchSetCmmsMeasurementIds(
    request: BatchSetCmmsMeasurementIdsRequest
  ): Empty {
    grpcRequire(request.measurementIdsList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      SetCmmsMeasurementIds(request).execute(client, idGenerator)
      return Empty.getDefaultInstance()
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }

  override suspend fun batchSetMeasurementResults(
    request: BatchSetMeasurementResultsRequest
  ): Empty {
    grpcRequire(request.measurementResultsList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      SetMeasurementResults(request).execute(client, idGenerator)
      return Empty.getDefaultInstance()
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }

  override suspend fun batchSetMeasurementFailures(
    request: BatchSetMeasurementFailuresRequest
  ): Empty {
    grpcRequire(request.measurementFailuresList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      SetMeasurementFailures(request).execute(client, idGenerator)
      return Empty.getDefaultInstance()
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }
}
