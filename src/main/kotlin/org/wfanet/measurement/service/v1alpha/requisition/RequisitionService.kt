package org.wfanet.measurement.service.v1alpha.requisition

import org.wfanet.measurement.api.v1alpha.CreateMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.FulfillMetricsRequisitionRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt
import org.wfanet.measurement.common.ApiId
import org.wfanet.measurement.common.Pagination
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState
import org.wfanet.measurement.kingdom.CampaignExternalKey
import org.wfanet.measurement.kingdom.RequisitionManager
import org.wfanet.measurement.service.v1alpha.common.toExternalKey
import org.wfanet.measurement.service.v1alpha.common.toRequisitionDetails
import org.wfanet.measurement.service.v1alpha.common.toRequisitionState
import org.wfanet.measurement.service.v1alpha.common.toV1Api

class RequisitionService(
  private val requisitionManager: RequisitionManager
) : RequisitionGrpcKt.RequisitionCoroutineImplBase() {

  override suspend fun createMetricRequisition(
    request: CreateMetricRequisitionRequest
  ): MetricRequisition {
    val partialRequisition: Requisition = Requisition.newBuilder().apply {
      externalDataProviderId = ApiId(request.parent.dataProviderId).externalId.value
      externalCampaignId = ApiId(request.parent.campaignId).externalId.value
      state = RequisitionState.UNFULFILLED
      windowStartTime = request.metricsRequisition.collectionInterval.startTime
      windowEndTime = request.metricsRequisition.collectionInterval.endTime
      requisitionDetails = request.metricsRequisition.metricDefinition.toRequisitionDetails()
    }.build()
    val resultRequisition = requisitionManager.createRequisition(partialRequisition)
    return resultRequisition.toV1Api()
  }

  override suspend fun fulfillMetricRequisition(
    request: FulfillMetricsRequisitionRequest
  ): MetricRequisition {
    val key = request.key.toExternalKey()
    return requisitionManager.fulfillRequisition(key).toV1Api()
  }

  override suspend fun listMetricRequisitions(
    request: ListMetricRequisitionsRequest
  ): ListMetricRequisitionsResponse {
    val campaignKey: CampaignExternalKey = request.parent.toExternalKey()
    val states = request.filter.statesList.map(MetricRequisition.State::toRequisitionState).toSet()
    val pagination = Pagination(request.pageSize, request.pageToken)
    val result: RequisitionManager.ListResult =
      requisitionManager.listRequisitions(campaignKey, states, pagination)
    return ListMetricRequisitionsResponse
      .newBuilder()
      .addAllMetricRequisitions(result.requisitions.map(Requisition::toV1Api))
      .setNextPageToken(result.nextPageToken.orEmpty())
      .build()
  }
}
