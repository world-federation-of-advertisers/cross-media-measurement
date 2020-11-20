// Copyright 2020 The Measurement System Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.kingdom.service.api.v1alpha

import com.google.protobuf.Timestamp
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsRequest
import org.wfanet.measurement.api.v1alpha.ListMetricRequisitionsResponse
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.api.v1alpha.RequisitionGrpcKt.RequisitionCoroutineImplBase
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.ExternalId
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest

class RequisitionService(private val internalRequisitionStub: RequisitionsCoroutineStub) :
  RequisitionCoroutineImplBase() {

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

/**
 * Converts internal [Requisition] into a V1 API proto.
 */
private fun Requisition.toV1Api(): MetricRequisition =
  MetricRequisition.newBuilder().apply {
    keyBuilder.apply {
      dataProviderId = ExternalId(externalDataProviderId).apiId.value
      campaignId = ExternalId(externalCampaignId).apiId.value
      metricRequisitionId = ExternalId(externalRequisitionId).apiId.value
    }
    campaignReferenceId = providedCampaignId
    combinedPublicKeyBuilder.combinedPublicKeyId = combinedPublicKeyResourceId
    state = this@toV1Api.state.toV1Api()
  }.build()

/**
 * Converts internal [RequisitionState] into a V1 API proto.
 */
private fun RequisitionState.toV1Api(): MetricRequisition.State =
  when (this) {
    RequisitionState.UNFULFILLED -> MetricRequisition.State.UNFULFILLED
    RequisitionState.FULFILLED -> MetricRequisition.State.FULFILLED
    RequisitionState.PERMANENTLY_UNAVAILABLE -> MetricRequisition.State.PERMANENTLY_UNFILLABLE
    RequisitionState.REQUISITION_STATE_UNKNOWN, RequisitionState.UNRECOGNIZED ->
      MetricRequisition.State.STATE_UNSPECIFIED
  }

/**
 * Converts V1 API proto enum [MetricRequisition.State] into an internal, API-agnostic enum.
 */
private fun MetricRequisition.State.toRequisitionState(): RequisitionState =
  when (this) {
    MetricRequisition.State.UNFULFILLED -> RequisitionState.UNFULFILLED
    MetricRequisition.State.FULFILLED -> RequisitionState.FULFILLED
    MetricRequisition.State.PERMANENTLY_UNFILLABLE -> RequisitionState.PERMANENTLY_UNAVAILABLE
    MetricRequisition.State.STATE_UNSPECIFIED, MetricRequisition.State.UNRECOGNIZED ->
      RequisitionState.REQUISITION_STATE_UNKNOWN
  }
