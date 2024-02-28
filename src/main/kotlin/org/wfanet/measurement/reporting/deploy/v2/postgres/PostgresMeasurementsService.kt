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
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.BatchCancelMeasurementsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchCancelMeasurementsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetCmmsMeasurementIdsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementFailuresResponse
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchSetMeasurementResultsResponse
import org.wfanet.measurement.internal.reporting.v2.MeasurementsGrpcKt
import org.wfanet.measurement.internal.reporting.v2.batchCancelMeasurementsResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetCmmsMeasurementIdsResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementFailuresResponse
import org.wfanet.measurement.internal.reporting.v2.batchSetMeasurementResultsResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CancelMeasurements
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetCmmsMeasurementIds
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetMeasurementFailures
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.SetMeasurementResults
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MeasurementNotFoundException

private const val BATCH_SIZE = 1000

class PostgresMeasurementsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : MeasurementsGrpcKt.MeasurementsCoroutineImplBase() {
  override suspend fun batchSetCmmsMeasurementIds(
    request: BatchSetCmmsMeasurementIdsRequest
  ): BatchSetCmmsMeasurementIdsResponse {
    grpcRequire(request.measurementIdsList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      return batchSetCmmsMeasurementIdsResponse {
        measurements += SetCmmsMeasurementIds(request).execute(client, idGenerator)
      }
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found")
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }

  override suspend fun batchSetMeasurementResults(
    request: BatchSetMeasurementResultsRequest
  ): BatchSetMeasurementResultsResponse {
    grpcRequire(request.measurementResultsList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      return batchSetMeasurementResultsResponse {
        measurements += SetMeasurementResults(request).execute(client, idGenerator)
      }
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found")
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }

  override suspend fun batchSetMeasurementFailures(
    request: BatchSetMeasurementFailuresRequest
  ): BatchSetMeasurementFailuresResponse {
    grpcRequire(request.measurementFailuresList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      return batchSetMeasurementFailuresResponse {
        measurements += SetMeasurementFailures(request).execute(client, idGenerator)
      }
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found")
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }

  override suspend fun batchCancelMeasurements(
    request: BatchCancelMeasurementsRequest
  ): BatchCancelMeasurementsResponse {
    grpcRequire(request.cmmsMeasurementIdsList.size <= BATCH_SIZE) { "Too many requests" }

    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "CmmsMeasurementConsumerId is missing"
    }

    try {
      return batchCancelMeasurementsResponse {
        measurements += CancelMeasurements(request).execute(client, idGenerator)
      }
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "MeasurementConsumer not found")
    } catch (e: MeasurementNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Measurement not found")
    }
  }
}
