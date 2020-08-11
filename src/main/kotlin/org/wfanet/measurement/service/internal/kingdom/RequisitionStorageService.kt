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

package org.wfanet.measurement.service.internal.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.streamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.Requisition.RequisitionState
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest

class RequisitionStorageService(
  private val kingdomRelationalDatabase: KingdomRelationalDatabase
) : RequisitionStorageGrpcKt.RequisitionStorageCoroutineImplBase() {
  override suspend fun createRequisition(request: Requisition): Requisition {
    require(request.externalRequisitionId == 0L) {
      "Cannot create a Requisition with a set externalRequisitionId: $request"
    }
    require(request.state == RequisitionState.UNFULFILLED) {
      "Initial requisitions must be unfulfilled: $request"
    }
    return kingdomRelationalDatabase.writeNewRequisition(request)
  }

  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition {
    return kingdomRelationalDatabase.fulfillRequisition(
      ExternalId(request.externalRequisitionId),
      request.duchyId
    )
  }

  override fun streamRequisitions(request: StreamRequisitionsRequest): Flow<Requisition> {
    val filter = request.filter
    val internalFilter = streamRequisitionsFilter(
      externalDataProviderIds = filter.externalDataProviderIdsList.map(::ExternalId),
      externalCampaignIds = filter.externalCampaignIdsList.map(::ExternalId),
      states = filter.statesList,
      createdAfter = filter.createdAfter.toInstant()
    )
    return kingdomRelationalDatabase.streamRequisitions(internalFilter, request.limit)
  }
}
