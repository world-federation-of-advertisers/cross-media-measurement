package org.wfanet.measurement.service.v1alpha.common

import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.db.Requisition
import org.wfanet.measurement.db.RequisitionState

/**
 * Converts internal [Requisition] into a V1 API proto.
 */
fun Requisition.toV1Api(): MetricRequisition =
  MetricRequisition.newBuilder().apply {
    key = externalKey.toV1Api()
    state = this@toV1Api.state.toV1Api()
  }.build()

/**
 * Converts internal [RequisitionState] into a V1 API proto.
 */
fun RequisitionState.toV1Api(): MetricRequisition.State =
  when (this) {
    RequisitionState.UNFULFILLED -> MetricRequisition.State.UNFULFILLED
    RequisitionState.FULFILLED -> MetricRequisition.State.FULFILLED
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
