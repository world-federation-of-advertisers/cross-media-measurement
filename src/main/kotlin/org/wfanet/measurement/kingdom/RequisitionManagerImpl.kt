package org.wfanet.measurement.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.db.kingdom.KingdomRelationalDatabase
import org.wfanet.measurement.db.kingdom.StreamRequisitionsFilter
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionState

class RequisitionManagerImpl(
  private val database: KingdomRelationalDatabase
) : RequisitionManager {
  override suspend fun createRequisition(requisition: Requisition): Requisition {
    require(requisition.externalRequisitionId == 0L) {
      "Cannot create a Requisition with a set externalRequisitionId: $requisition"
    }
    require(requisition.state == RequisitionState.UNFULFILLED) {
      "Initial requisitions must be unfulfilled: $requisition"
    }

    return database.writeNewRequisition(requisition)
  }

  override suspend fun fulfillRequisition(
    externalRequisitionId: ExternalId
  ): Requisition =
    database.fulfillRequisition(externalRequisitionId)

  override fun streamRequisitions(
    filter: StreamRequisitionsFilter,
    limit: Long
  ): Flow<Requisition> = database.streamRequisitions(filter, limit)
}
