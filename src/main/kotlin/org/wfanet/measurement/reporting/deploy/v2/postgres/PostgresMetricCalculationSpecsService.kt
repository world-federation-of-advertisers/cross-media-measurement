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
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.BatchGetMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.CreateMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.GetMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.batchGetMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricCalculationSpecReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateMetricCalculationSpec
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.reporting.service.internal.MetricCalculationSpecAlreadyExistsException

class PostgresMetricCalculationSpecsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
  coroutineContext: CoroutineContext = EmptyCoroutineContext,
) : MetricCalculationSpecsCoroutineImplBase(coroutineContext) {
  override suspend fun createMetricCalculationSpec(
    request: CreateMetricCalculationSpecRequest
  ): MetricCalculationSpec {
    grpcRequire(request.externalMetricCalculationSpecId.isNotEmpty()) {
      "external_metric_calculation_spec_id is not set."
    }

    grpcRequire(request.metricCalculationSpec.details.metricSpecsCount > 0) {
      "metric_specs cannot be empty."
    }

    return try {
      CreateMetricCalculationSpec(request).execute(client, idGenerator)
    } catch (e: MetricCalculationSpecAlreadyExistsException) {
      throw e.asStatusRuntimeException(
        Status.Code.ALREADY_EXISTS,
        "Metric Calculation Spec already exists",
      )
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(Status.Code.FAILED_PRECONDITION)
    }
  }

  override suspend fun getMetricCalculationSpec(
    request: GetMetricCalculationSpecRequest
  ): MetricCalculationSpec {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set."
    }

    grpcRequire(request.externalMetricCalculationSpecId.isNotEmpty()) {
      "external_metric_calculation_spec_id is not set."
    }

    val readContext = client.readTransaction()
    return try {
      MetricCalculationSpecReader(readContext)
        .readMetricCalculationSpecByExternalId(
          request.cmmsMeasurementConsumerId,
          request.externalMetricCalculationSpecId,
        )
        ?.metricCalculationSpec
        ?: throw Status.NOT_FOUND.withDescription("Metric Calculation Spec not found.")
          .asRuntimeException()
    } finally {
      readContext.close()
    }
  }

  override suspend fun listMetricCalculationSpecs(
    request: ListMetricCalculationSpecsRequest
  ): ListMetricCalculationSpecsResponse {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set."
    }

    val readContext = client.readTransaction()
    val plusOneLimit = request.limit + 1
    return try {
      val metricCalculationSpecs =
        MetricCalculationSpecReader(readContext)
          .readMetricCalculationSpecs(request.copy { limit = plusOneLimit })
          .map { it.metricCalculationSpec }

      if (metricCalculationSpecs.size > request.limit) {
        listMetricCalculationSpecsResponse {
          this.metricCalculationSpecs +=
            metricCalculationSpecs.subList(0, metricCalculationSpecs.size - 1)
          limited = true
        }
      } else {
        listMetricCalculationSpecsResponse {
          this.metricCalculationSpecs += metricCalculationSpecs
          limited = false
        }
      }
    } finally {
      readContext.close()
    }
  }

  override suspend fun batchGetMetricCalculationSpecs(
    request: BatchGetMetricCalculationSpecsRequest
  ): BatchGetMetricCalculationSpecsResponse {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set."
    }

    val readContext = client.readTransaction()
    return try {
      MetricCalculationSpecReader(readContext)
        .batchReadByExternalIds(
          request.cmmsMeasurementConsumerId,
          request.externalMetricCalculationSpecIdsList,
        )
        .let {
          if (it.size < request.externalMetricCalculationSpecIdsList.size) {
            throw Status.NOT_FOUND.withDescription("Metric Calculation Spec not found.")
              .asRuntimeException()
          } else {
            batchGetMetricCalculationSpecsResponse {
              metricCalculationSpecs += it.map { result -> result.metricCalculationSpec }
            }
          }
        }
    } finally {
      readContext.close()
    }
  }
}
