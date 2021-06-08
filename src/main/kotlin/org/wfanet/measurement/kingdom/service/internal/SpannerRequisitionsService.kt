package org.wfanet.measurement.kingdom.service.internal

import java.time.Clock
import org.wfanet.measurement.common.identity.IdGenerator
import org.wfanet.measurement.gcloud.spanner.AsyncDatabaseClient
import org.wfanet.measurement.internal.kingdom.FulfillRequisitionRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionByDataProviderIdRequest
import org.wfanet.measurement.internal.kingdom.GetRequisitionRequest
import org.wfanet.measurement.internal.kingdom.MeasurementsGrpcKt.MeasurementsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RequisitionsGrpcKt.RequisitionsCoroutineImplBase
import org.wfanet.measurement.internal.kingdom.RefuseRequisitionRequest
import org.wfanet.measurement.internal.kingdom.Requisition

class SpannerRequisitionsService(
  clock: Clock,
  idGenerator: IdGenerator,
  client: AsyncDatabaseClient
) : RequisitionsCoroutineImplBase() {

  override suspend fun getRequisition(request: GetRequisitionRequest): Requisition {
    TODO("not implemented yet")
  }
  override suspend fun getRequisitionByDataProviderId(
    request: GetRequisitionByDataProviderIdRequest
  ): Requisition {
    TODO("not implemented yet")
  }
  override suspend fun fulfillRequisition(request: FulfillRequisitionRequest): Requisition {
    TODO("not implemented yet")
  }
  override suspend fun refuseRequisition(request: RefuseRequisitionRequest): Requisition {
    TODO("not implemented yet")
  }
}
