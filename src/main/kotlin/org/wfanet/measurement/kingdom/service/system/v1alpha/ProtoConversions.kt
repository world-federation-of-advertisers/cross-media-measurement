// Copyright 2021 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import org.wfanet.measurement.common.identity.externalIdToApiId
import org.wfanet.measurement.internal.kingdom.Requisition as InternalRequisition
import org.wfanet.measurement.system.v1alpha.ComputationParticipantKey
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey

/** Converts a kingdom internal Requisition to system Api Requisition. */
fun InternalRequisition.toSystemRequisition(): Requisition {
  return Requisition.newBuilder()
    .also {
      it.name =
        RequisitionKey(
            externalIdToApiId(externalComputationId),
            externalIdToApiId(externalRequisitionId)
          )
          .toName()
      it.dataProviderId = externalIdToApiId(externalDataProviderId)
      // TODO: get dataProviderCertificate from the external_data_provider_certificate_id
      // TODO: set the requisition_spec_hash
      it.state = state.toSystemRequisitionState()
      it.dataProviderParticipationSignature = details.dataProviderParticipationSignature
      it.fulfillingComputationParticipant =
        ComputationParticipantKey(
            externalIdToApiId(externalComputationId),
            externalFulfillingDuchyId
          )
          .toName()
    }
    .build()
}

/** Converts a kingdom internal Requisition.State to system Api Requisition.State. */
fun InternalRequisition.State.toSystemRequisitionState(): Requisition.State {
  return when (this) {
    InternalRequisition.State.UNFULFILLED -> Requisition.State.UNFULFILLED
    InternalRequisition.State.FULFILLED -> Requisition.State.FULFILLED
    InternalRequisition.State.REFUSED -> Requisition.State.REFUSED
    InternalRequisition.State.STATE_UNSPECIFIED, InternalRequisition.State.UNRECOGNIZED ->
      error("Invalid requisition state.")
  }
}
