package org.wfanet.measurement.service.internal.kingdom

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.ExternalId
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionServiceGrpcKt
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest
import org.wfanet.measurement.kingdom.RequisitionManager

class RequisitionService(
  private val requisitionManager: RequisitionManager
) : RequisitionServiceGrpcKt.RequisitionServiceCoroutineImplBase() {
  override suspend fun createRequisition(request: Requisition): Requisition {
    return requisitionManager.createRequisition(request)
  }

  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition {
    return requisitionManager.fulfillRequisition(ExternalId(request.externalRequisitionId))
  }

  override fun streamRequisitions(request: StreamRequisitionsRequest): Flow<Requisition> {
    TODO()
  }
}
