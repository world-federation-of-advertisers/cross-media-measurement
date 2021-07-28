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

import org.wfanet.measurement.api.Version
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest as InternalFulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.FulfillRequisitionRequest
import org.wfanet.measurement.system.v1alpha.Requisition
import org.wfanet.measurement.system.v1alpha.RequisitionKey
import org.wfanet.measurement.system.v1alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase

class RequisitionsService(
  private val internalRequisitionsClient: InternalRequisitionsCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity = ::duchyIdentityFromContext
) : RequisitionsCoroutineImplBase() {

  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition {
    val requisitionKey =
      grpcRequireNotNull(RequisitionKey.fromName(request.name)) {
        "Resource name unspecified or invalid."
      }
    grpcRequire(!request.dataProviderParticipationSignature.isEmpty) {
      "data_provider_participation_signature is unspecified."
    }

    val internalRequest =
      InternalFulfillRequisitionRequest.newBuilder()
        .apply {
          externalComputationId = apiIdToExternalId(requisitionKey.computationId)
          externalRequisitionId = apiIdToExternalId(requisitionKey.requisitionId)
          externalFulfillingDuchyId = duchyIdentityProvider().id
          dataProviderParticipationSignature = request.dataProviderParticipationSignature
        }
        .build()
    val internalResponse = internalRequisitionsClient.fulfillRequisition(internalRequest)
    return internalResponse.toSystemRequisition(
      Version.fromString(internalResponse.parentMeasurement.apiVersion)
    )
  }
}
