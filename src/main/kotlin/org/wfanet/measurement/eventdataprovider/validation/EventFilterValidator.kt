package org.wfanet.measurement.eventdataprovider.validation

import com.google.protobuf.Message
import org.wfanet.measurement.api.v2alpha.Requisition
import org.wfanet.measurement.api.v2alpha.Requisition.Refusal.Justification
import org.wfanet.measurement.api.v2alpha.RequisitionKt.refusal
import org.wfanet.measurement.api.v2alpha.RequisitionSpec
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException

/** A [RequisitionValidator] that ensures the syntax of the Event Filter CEL Expression is valid. */
class EventFilterValidator(
  private val celEventTemplate: Message,
) : RequisitionValidator {

  override fun validate(
    requisition: Requisition,
    requisitionSpec: RequisitionSpec
  ): Requisition.Refusal? {
    for (eventGroup in requisitionSpec.eventGroupsList) {
      try {
        EventFilters.compileProgram(
          celEventTemplate.getDescriptorForType(),
          eventGroup.value.filter.expression
        )
      } catch (e: EventFilterValidationException) {
        return refusal {
          justification = Justification.SPECIFICATION_INVALID
          message =
            "Syntax error in Event Filter CEL Expression: ${eventGroup.value.filter.expression}."
        }
      }
    }
    return null
  }
}
