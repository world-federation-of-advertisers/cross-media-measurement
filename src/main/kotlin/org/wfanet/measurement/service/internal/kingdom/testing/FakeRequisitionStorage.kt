package org.wfanet.measurement.service.internal.kingdom.testing

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.common.testing.ServiceMocker
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition
import org.wfanet.measurement.internal.kingdom.RequisitionStorageGrpcKt.RequisitionStorageCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.StreamRequisitionsRequest

class FakeRequisitionStorage : RequisitionStorageCoroutineImplBase() {
  val mocker = ServiceMocker<RequisitionStorageCoroutineImplBase>()

  override suspend fun createRequisition(request: Requisition): Requisition =
    mocker.handleCall(request)

  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition =
    mocker.handleCall(request)

  override fun streamRequisitions(request: StreamRequisitionsRequest): Flow<Requisition> =
    mocker.handleCall(request)
}
