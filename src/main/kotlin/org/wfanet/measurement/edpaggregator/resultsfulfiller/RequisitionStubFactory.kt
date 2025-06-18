package org.wfanet.measurement.edpaggregator.resultsfulfiller

import org.wfanet.measurement.api.v2alpha.RequisitionsGrpcKt.RequisitionsCoroutineStub
import org.wfanet.measurement.edpaggregator.v1alpha.ResultsFulfillerParams

/** A factory for building stubs needed by a Result Fulfiller. */
interface RequisitionStubFactory {

  fun buildRequisitionsStub(fulfillerParams: ResultsFulfillerParams): RequisitionsCoroutineStub
}
