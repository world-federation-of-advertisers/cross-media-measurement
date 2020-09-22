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

package org.wfanet.measurement.service.v1alpha.common

import org.wfanet.measurement.api.v1alpha.MetricRequisition
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState

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
    campaignReferenceId = providedCampaignId
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
