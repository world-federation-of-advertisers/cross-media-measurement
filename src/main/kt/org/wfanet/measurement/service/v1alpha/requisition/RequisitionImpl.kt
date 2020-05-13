package org.wfanet.measurement.service.v1alpha.requisition

import org.wfanet.measurement.api.v1alpha.CreateMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.FulfillMetricsRequisitionRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt


class RequisitionImpl : RequisitionGrpcKt.RequisitionCoroutineImplBase() {
  override suspend fun createMetricRequisition(
    request: CreateMetricRequisitionRequest
  ): MetricRequisition {
    return super.createMetricRequisition(request)
  }

  override suspend fun fulfillMetricRequisition(
    request: FulfillMetricsRequisitionRequest
  ): MetricRequisition {
    return super.fulfillMetricRequisition(request)
  }

  override suspend fun listMetricRequisitions(
    request: ListMetricRequisitionsRequest
  ): ListMetricRequisitionsResponse {
    return super.listMetricRequisitions(request)
  }
}
