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
import java.lang.IllegalStateException
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
import org.wfanet.measurement.internal.reporting.v2.BatchCreateMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchCreateMetricsResponse
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricsResponse
import org.wfanet.measurement.internal.reporting.v2.CreateMetricRequest
import org.wfanet.measurement.internal.reporting.v2.Metric
import org.wfanet.measurement.internal.reporting.v2.MetricSpec
import org.wfanet.measurement.internal.reporting.v2.MetricsGrpcKt.MetricsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.StreamMetricsRequest
import org.wfanet.measurement.internal.reporting.v2.batchCreateMetricsResponse
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricsResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateMetrics
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

private const val MAX_BATCH_SIZE = 1000

class PostgresMetricsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : MetricsCoroutineImplBase() {
  override suspend fun createMetric(request: CreateMetricRequest): Metric {
    grpcRequire(request.metric.hasTimeInterval()) { "Metric missing time interval." }

    grpcRequire(!request.metric.metricSpec.typeCase.equals(MetricSpec.TypeCase.TYPE_NOT_SET)) {
      "Metric Spec missing type."
    }

    grpcRequire(request.metric.metricSpec.hasVidSamplingInterval()) {
      "Metric Spec missing vid sampling interval."
    }

    grpcRequire(request.metric.weightedMeasurementsCount > 0) {
      "Metric missing weighted measurements."
    }

    return try {
      CreateMetrics(listOf(request)).execute(client, idGenerator).first()
    } catch (e: ReportingSetNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Reporting Set not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement Consumer not found."
      )
    }
  }

  override suspend fun batchCreateMetrics(
    request: BatchCreateMetricsRequest
  ): BatchCreateMetricsResponse {
    grpcRequire(request.requestsList.size <= MAX_BATCH_SIZE) { "Too many requests." }

    request.requestsList.forEach {
      grpcRequire(it.metric.hasTimeInterval()) { "Metric missing time interval." }

      grpcRequire(!it.metric.metricSpec.typeCase.equals(MetricSpec.TypeCase.TYPE_NOT_SET)) {
        "Metric Spec missing type."
      }

      grpcRequire(it.metric.metricSpec.hasVidSamplingInterval()) {
        "Metric Spec missing vid sampling interval."
      }

      grpcRequire(it.metric.weightedMeasurementsCount > 0) {
        "Metric missing weighted measurements."
      }

      grpcRequire(it.metric.cmmsMeasurementConsumerId.equals(request.cmmsMeasurementConsumerId)) {
        "CmmsMeasurementConsumerId in request doesn't match create metric request"
      }
    }

    return try {
      batchCreateMetricsResponse {
        metrics += CreateMetrics(request.requestsList).execute(client, idGenerator)
      }
    } catch (e: ReportingSetNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.NOT_FOUND, "Reporting Set not found.")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement Consumer not found."
      )
    }
  }

  override suspend fun batchGetMetrics(request: BatchGetMetricsRequest): BatchGetMetricsResponse {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotBlank()) {
      "CmmsMeasurementConsumerId is missing."
    }

    grpcRequire(request.externalMetricIdsList.size <= MAX_BATCH_SIZE) { "Too many requests." }

    val readContext = client.readTransaction()
    val metrics =
      try {
        MetricReader(readContext)
          .batchGetMetrics(request)
          .map { it.metric }
          .withSerializableErrorRetries()
          .toList()
      } catch (e: IllegalStateException) {
        failGrpc(Status.NOT_FOUND) { "Metric is not found" }
      } finally {
        readContext.close()
      }

    if (metrics.size < request.externalMetricIdsList.size) {
      failGrpc(Status.NOT_FOUND) { "Metric is not found" }
    }

    return batchGetMetricsResponse { this.metrics += metrics }
  }

  override fun streamMetrics(request: StreamMetricsRequest): Flow<Metric> {
    grpcRequire(request.filter.cmmsMeasurementConsumerId.isNotBlank()) {
      "Filter is missing CmmsMeasurementConsumerId"
    }

    return flow {
      val readContext = client.readTransaction()
      try {
        emitAll(
          MetricReader(readContext)
            .readMetrics(request)
            .map { it.metric }
            .withSerializableErrorRetries()
        )
      } catch (e: IllegalStateException) {
        failGrpc(Status.NOT_FOUND) { "Metric is not found" }
      } finally {
        readContext.close()
      }
    }
  }
}
