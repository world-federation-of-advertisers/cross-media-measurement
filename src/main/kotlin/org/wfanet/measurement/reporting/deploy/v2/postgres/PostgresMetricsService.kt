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
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateMetrics
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.ReportingSetNotFoundException

class PostgresMetricsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : MetricsCoroutineImplBase() {
  override suspend fun createMetric(request: CreateMetricRequest): Metric {
    grpcRequire(!request.metric.metricSpec.typeCase.equals(MetricSpec.TypeCase.TYPE_NOT_SET)) {
      "Metric Spec missing type."
    }

    return try {
      CreateMetrics(listOf(request)).execute(client, idGenerator).first()
    } catch (e: ReportingSetNotFoundException) {
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Reporting Set not found." }
    } catch (e: MeasurementConsumerNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Measurement Consumer not found." }
    }
  }

  override suspend fun batchCreateMetrics(request: BatchCreateMetricsRequest): BatchCreateMetricsResponse {
    request.requestsList.forEach {
      grpcRequire(!it.metric.metricSpec.typeCase.equals(MetricSpec.TypeCase.TYPE_NOT_SET)) {
        "Metric Spec missing type."
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
      e.throwStatusRuntimeException(Status.NOT_FOUND) { "Reporting Set not found." }
    } catch (e: MeasurementConsumerNotFoundException) {
      e.throwStatusRuntimeException(Status.FAILED_PRECONDITION) { "Measurement Consumer not found." }
    }
  }

  override suspend fun batchGetMetrics(request: BatchGetMetricsRequest): BatchGetMetricsResponse {
    return super.batchGetMetrics(request)
  }

  override fun streamMetrics(request: StreamMetricsRequest): Flow<Metric> {
    return super.streamMetrics(request)
  }
}
