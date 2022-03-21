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

import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.apiIdToExternalId
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequestKt.computedRequisitionParams
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub as InternalRequisitionsCoroutineStub
import org.wfanet.measurement.internal.kingdom.fulfillRequisitionRequest as internalFulfillRequisitionRequest
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
    grpcRequire(request.nonce != 0L) { "nonce unspecified" }

    val internalResponse =
      internalRequisitionsClient.fulfillRequisition(
        internalFulfillRequisitionRequest {
          externalRequisitionId = apiIdToExternalId(requisitionKey.requisitionId)
          nonce = request.nonce
          computedParams = computedRequisitionParams {
            externalComputationId = apiIdToExternalId(requisitionKey.computationId)
            externalFulfillingDuchyId = duchyIdentityProvider().id
          }
        }
      )
    return internalResponse.toSystemRequisition()
  }
}
