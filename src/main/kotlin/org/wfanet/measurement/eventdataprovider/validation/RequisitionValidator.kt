package org.wfanet.measurement.eventdataprovider.validation


import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * Validator for a [RequisitionAndSpec]. Requisitions that fail validation will be refused and we
 * will not compute a result for them.
 */
fun interface RequisitionValidator {

  /**
   * Validates the given [requisitionAndSpec]. If the requisition is not valid, returns a
   * [RefusalResult] describing the reason. Returns null if the requisition is valid.
   */
  fun validate(    requisition: Requisition,
    requisitionSpec: RequisitionSpec): Requisition.Refusal?
}