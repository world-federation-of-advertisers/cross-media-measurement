package org.wfanet.measurement.reporting.deploy.v2.postgres

import io.grpc.Status
import org.wfanet.measurement.common.db.r2dbc.DatabaseClient
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.internal.reporting.v2.CreateMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.GetMetricCalculationSpecRequest
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsRequest
import org.wfanet.measurement.internal.reporting.v2.ListMetricCalculationSpecsResponse
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpec
import org.wfanet.measurement.reporting.service.internal.MeasurementConsumerNotFoundException
import org.wfanet.measurement.internal.reporting.v2.MetricCalculationSpecsGrpcKt.MetricCalculationSpecsCoroutineImplBase
import org.wfanet.measurement.internal.reporting.v2.listMetricCalculationSpecsResponse
import org.wfanet.measurement.reporting.deploy.v2.postgres.readers.MetricCalculationSpecReader
import org.wfanet.measurement.reporting.deploy.v2.postgres.writers.CreateMetricCalculationSpec
import org.wfanet.measurement.reporting.service.internal.MetricCalculationSpecAlreadyExistsException

class PostgresMetricCalculationSpecsService(
  private val idGenerator: IdGenerator,
  private val client: DatabaseClient,
) : MetricCalculationSpecsCoroutineImplBase() {
  override suspend fun createMetricCalculationSpec(request: CreateMetricCalculationSpecRequest): MetricCalculationSpec {
    grpcRequire(request.externalMetricCalculationSpecId.isNotEmpty()) {
      "external_metric_calculation_spec_id is not set."
    }

    grpcRequire(request.metricCalculationSpec.details.metricSpecsCount > 0) {
      "metric_specs cannot be empty."
    }

    return try {
      CreateMetricCalculationSpec(request).execute(client, idGenerator)
    } catch (e: MetricCalculationSpecAlreadyExistsException) {
      throw e.asStatusRuntimeException(Status.Code.ALREADY_EXISTS, "Metric Calculation Spec already exists")
    } catch (e: MeasurementConsumerNotFoundException) {
      throw e.asStatusRuntimeException(
        Status.Code.FAILED_PRECONDITION,
        "Measurement Consumer not found"
      )
    }
  }

  override suspend fun getMetricCalculationSpec(request: GetMetricCalculationSpecRequest): MetricCalculationSpec {
    grpcRequire(request.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set."
    }

    grpcRequire(request.externalMetricCalculationSpecId.isNotEmpty()) {
      "external_metric_calculation_spec_id is not set."
    }

    val readContext = client.readTransaction()
    return try {
      MetricCalculationSpecReader(readContext).readMetricCalculationSpecByExternalId(request.cmmsMeasurementConsumerId, request.externalMetricCalculationSpecId)
        ?.metricCalculationSpec
        ?: throw Status.NOT_FOUND.withDescription("Metric Calculation Spec not found.").asRuntimeException()
    } finally {
      readContext.close()
    }
  }

  override suspend fun listMetricCalculationSpecs(request: ListMetricCalculationSpecsRequest): ListMetricCalculationSpecsResponse {
    grpcRequire(request.filter.cmmsMeasurementConsumerId.isNotEmpty()) {
      "cmms_measurement_consumer_id is not set."
    }

    val readContext = client.readTransaction()
    return try {
      listMetricCalculationSpecsResponse {
        metricCalculationSpecs += MetricCalculationSpecReader(readContext).readMetricCalculationSpecs(request).map {
          it.metricCalculationSpec
        }
      }
    } finally {
      readContext.close()
    }
  }
}
