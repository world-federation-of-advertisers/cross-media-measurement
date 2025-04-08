/*
 * Copyright 2020 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.integration.common.fake

import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionRequest
import org.wfanet.measurement.api.v2alpha.FulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.ListRequisitionsResponse
import org.wfanet.measurement.api.v2alpha.RefuseRequisitionRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.State
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.api.v2alpha.fulfillDirectRequisitionResponse
import org.wfanet.measurement.api.v2alpha.listRequisitionsResponse

class RequisitionsService(private val requisitions: List<Requisition>) :
  RequisitionsCoroutineImplBase() {
  private var index = 0

  override suspend fun listRequisitions(
    request: ListRequisitionsRequest
  ): ListRequisitionsResponse {
    return listRequisitionsResponse {
      val numToReturn = minOf(request.pageSize, requisitions.size - index)
      if (numToReturn > 0) {
        this.requisitions += requisitions.subList(index, index + numToReturn)
        this.nextPageToken = "some-fake-token"
        index += numToReturn
      }
    }
  }

  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    return requisitions.filter { request.name == it.name }.single()
  }

  override suspend fun fulfillDirectRequisition(
    request: FulfillDirectRequisitionRequest
  ): FulfillDirectRequisitionResponse {

    return fulfillDirectRequisitionResponse { state = State.FULFILLED }
  }
}
