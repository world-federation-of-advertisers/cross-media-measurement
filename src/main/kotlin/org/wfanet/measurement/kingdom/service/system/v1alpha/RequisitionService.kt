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

package org.wfanet.measurement.kingdom.service.system.v1alpha

import org.wfanet.measurement.common.identity.ApiId
import org.wfanet.measurement.common.identity.DuchyIdentity
import org.wfanet.measurement.common.identity.duchyIdentityFromContext
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.system.v1alpha.FulfillMetricRequisitionRequest
import org.wfanet.measurement.system.v1alpha.FulfillMetricRequisitionResponse
import org.wfanet.measurement.system.v1alpha.RequisitionGrpcKt.RequisitionCoroutineImplBase as RequisitionCoroutineService

/** Implementation of Requisition service from system API. */
class RequisitionService private constructor(
  private val requisitionsClient: RequisitionsCoroutineStub,
  private val duchyIdentityProvider: () -> DuchyIdentity
) : RequisitionCoroutineService() {
  constructor(requisitionsClient: RequisitionsCoroutineStub) : this(
    requisitionsClient,
    ::duchyIdentityFromContext
  )

  override suspend fun fulfillMetricRequisition(
    request: FulfillMetricRequisitionRequest
  ): FulfillMetricRequisitionResponse {
    val externalId = ApiId(request.key.metricRequisitionId).externalId
    val internalRequest = FulfillRequisitionRequest.newBuilder().apply {
      externalRequisitionId = externalId.value
      duchyId = duchyIdentityProvider().id
    }.build()
    requisitionsClient.fulfillRequisition(internalRequest)

    return FulfillMetricRequisitionResponse.getDefaultInstance()
  }

  companion object {
    fun forTesting(
      requisitionsClient: RequisitionsCoroutineStub,
      duchyIdentityProvider: () -> DuchyIdentity
    ): RequisitionService {
      return RequisitionService(requisitionsClient, duchyIdentityProvider)
    }
  }
}
