package org.wfanet.measurement.service.v1alpha.common

import org.wfanet.measurement.api.v1alpha.MetricDefinition
import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionDetails
import org.wfanet.measurement.internal.kingdom.RequisitionState

/**
 * Converts internal [Requisition] into a V1 API proto.
 */
fun Requisition.toV1Api(): MetricRequisition =
  MetricRequisition.newBuilder().apply {
    keyBuilder.apply {
      dataProviderId = ExternalId(externalDataProviderId).apiId.value
      campaignId = ExternalId(externalCampaignId).apiId.value
      metricRequisitionId = ExternalId(externalRequisitionId).apiId.value
    }
    state = this@toV1Api.state.toV1Api()
  }.build()

/**
 * Converts internal [RequisitionState] into a V1 API proto.
 */
fun RequisitionState.toV1Api(): MetricRequisition.State =
  when (this) {
    RequisitionState.UNFULFILLED -> MetricRequisition.State.UNFULFILLED
    RequisitionState.FULFILLED -> MetricRequisition.State.FULFILLED
    else -> MetricRequisition.State.STATE_UNSPECIFIED
  }

/**
 * Converts V1 API proto enum [MetricRequisition.State] into an internal, API-agnostic enum.
 */
fun MetricRequisition.State.toRequisitionState(): RequisitionState =
  when (this) {
    MetricRequisition.State.UNFULFILLED -> RequisitionState.UNFULFILLED
    MetricRequisition.State.FULFILLED -> RequisitionState.FULFILLED
    else -> error("Invalid state: $this")
  }

fun MetricDefinition.toRequisitionDetails(): RequisitionDetails {
  // TODO: implement
  return RequisitionDetails.getDefaultInstance()
}
