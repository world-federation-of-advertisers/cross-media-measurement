package org.wfanet.measurement.service.internal.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.common.toInstant
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.streamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState
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
    return kingdomRelationalDatabase.fulfillRequisition(ExternalId(request.externalRequisitionId))
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
