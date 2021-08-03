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

package org.wfanet.measurement.dataprovider.common

import java.util.ArrayDeque
import org.wfanet.measurement.api.v2alpha.ListRequisitionsRequest
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub

/**
 * Implementation of [UnfulfilledRequisitionProvider] that loads pages of unfulfilled [Requisition]s
 * via gRPC and caches them until needed.
 */
class GrpcUnfulfilledRequisitionProvider(
  externalDataProviderId: String,
  private val requisitions: RequisitionsCoroutineStub
) : UnfulfilledRequisitionProvider {
  private val requestTemplate =
    ListRequisitionsRequest.newBuilder()
      .apply {
        parent = externalDataProviderId
        filterBuilder.addStates(Requisition.State.UNFULFILLED)
      }
      .build()

  private val buffer = ArrayDeque<Requisition>()
  private var pageToken = ""

  override suspend fun get(): Requisition? {
    if (buffer.isEmpty()) {
      refillBuffer()
    }
    return buffer.pollFirst()
  }

  suspend fun refillBuffer() {
    val request = requestTemplate.toBuilder().setPageToken(pageToken).build()
    val response = requisitions.listRequisitions(request)
    pageToken = response.nextPageToken
    buffer.addAll(response.requisitionsList)
  }
}
