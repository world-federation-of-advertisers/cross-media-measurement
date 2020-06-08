package org.wfanet.measurement.service.v1alpha.requisition

import com.google.protobuf.Timestamp
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v1alpha.CreateMetricRequisitionRequest
import org.wfanet.measurement.api.v1alpha.FulfillMetricsRequisitionRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt
import org.wfanet.measurement.common.ApiId
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.service.v1alpha.common.grpcRequire
import org.wfanet.measurement.service.v1alpha.common.toRequisitionState
import org.wfanet.measurement.service.v1alpha.common.toV1Api

class RequisitionService(
  private val internalRequisitionStub: RequisitionStorageGrpcKt.RequisitionStorageCoroutineStub
) : RequisitionGrpcKt.RequisitionCoroutineImplBase() {

  override suspend fun createMetricRequisition(
    request: CreateMetricRequisitionRequest
  ): MetricRequisition {
    TODO("Remove this method")
  }

  override suspend fun fulfillMetricRequisition(
    request: FulfillMetricsRequisitionRequest
  ): MetricRequisition {
    val externalId = ApiId(request.key.metricRequisitionId).externalId
    val internalRequest =
      FulfillRequisitionRequest.newBuilder()
        .setExternalRequisitionId(externalId.value)
        .build()
    return internalRequisitionStub.fulfillRequisition(internalRequest).toV1Api()
  }

  override suspend fun listMetricRequisitions(
    request: ListMetricRequisitionsRequest
  ): ListMetricRequisitionsResponse {
    grpcRequire(request.pageSize in 1..1000) {
      "Page size must be between 1 and 1000 in request: $request"
    }
    grpcRequire(request.filter.statesCount > 0) {
      "At least one state must be set in request.filter.states: $request"
    }

    val streamRequest = StreamRequisitionsRequest.newBuilder().apply {
      limit = request.pageSize.toLong()
      filterBuilder.apply {
        if (request.pageToken.isNotBlank()) {
          createdAfter = Timestamp.parseFrom(request.pageToken.base64UrlDecode())
        }

        addAllStates(request.filter.statesList.map(MetricRequisition.State::toRequisitionState))

        addExternalDataProviderIds(ApiId(request.parent.dataProviderId).externalId.value)
        addExternalCampaignIds(ApiId(request.parent.campaignId).externalId.value)
      }
    }.build()

    val results: List<Requisition> =
      internalRequisitionStub.streamRequisitions(streamRequest).toList()

    if (results.isEmpty()) {
      return ListMetricRequisitionsResponse.getDefaultInstance()
    }

    return ListMetricRequisitionsResponse
      .newBuilder()
      .addAllMetricRequisitions(results.map(Requisition::toV1Api))
      .setNextPageToken(results.last().createTime.toByteArray().base64UrlEncode())
      .build()
  }
}
