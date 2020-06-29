package org.wfanet.measurement.service.v1alpha.globalcomputation

import kotlinx.coroutines.flow.Flow
import org.wfanet.measurement.api.v1alpha.GetGlobalComputationRequest
import org.wfanet.measurement.api.v1alpha.GlobalComputation
import org.wfanet.measurement.api.v1alpha.GlobalComputationsGrpcKt.GlobalComputationsCoroutineImplBase
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsRequest
import org.wfanet.measurement.api.v1alpha.StreamActiveGlobalComputationsResponse
import org.wfanet.measurement.internal.kingdom.ReportStorageGrpcKt.ReportStorageCoroutineStub

class GlobalComputationService(
  private val reportStorageStub: ReportStorageCoroutineStub
) : GlobalComputationsCoroutineImplBase() {
  override suspend fun getGlobalComputation(
    request: GetGlobalComputationRequest
  ): GlobalComputation {
    TODO()
  }

  override fun streamActiveGlobalComputations(
    request: StreamActiveGlobalComputationsRequest
  ): Flow<StreamActiveGlobalComputationsResponse> {
    TODO()
  }
}
