package org.wfanet.measurement.eventdataprovider.validation

import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.RequisitionSpec

/**
 * [RequisitionValidator] that validates a requisition against each validator in [validators].
 * Validators are applied in order, and the first non-valid result is returned.
 */
class CompoundValidator(private val validators: List<RequisitionValidator>) : RequisitionValidator {

  override fun validate(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec
  ): Requisition.Refusal? =
    validators.firstNotNullOfOrNull { it.validate(requisition, requisitionSpec) }
}
