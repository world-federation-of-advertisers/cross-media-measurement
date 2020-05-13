package org.wfanet.measurement.service.v1alpha.computationcontrol

import org.wfanet.measurement.api.v1alpha.ComputationControlGrpcKt
import org.wfanet.measurement.api.v1alpha.GlobalComputationState
import org.wfanet.measurement.api.v1alpha.TransitionGlobalComputationStateRequest

class ComputationControlImpl : ComputationControlGrpcKt.ComputationControlCoroutineImplBase() {
  override suspend fun transitionGlobalComputationState(
    request: TransitionGlobalComputationStateRequest
  ): GlobalComputationState {
    return super.transitionGlobalComputationState(request)
  }
}
