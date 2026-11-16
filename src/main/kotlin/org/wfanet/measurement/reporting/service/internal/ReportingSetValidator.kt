/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.reporting.service.internal

import org.wfanet.measurement.internal.reporting.v2.ReportingSet

/**
 * Validates ReportingSet identities and their complete EventGroup universe for one request.
 *
 * [readReportingSet] must read under [cmmsMeasurementConsumerId]. Reads and validated memberships
 * are cached for this instance. A standalone custom set does not need a Campaign Group.
 */
class ReportingSetValidator(
  private val cmmsMeasurementConsumerId: String,
  private val readReportingSet: suspend (String) -> ReportingSet,
) {
  private val reportingSets = mutableMapOf<String, ReportingSet>()

  private data class Universe(
    val eventGroupKeys: Set<ReportingSet.Primitive.EventGroupKey>,
    val primitiveReportingSetIds: Set<String>,
  ) {
    operator fun plus(other: Universe): Universe =
      Universe(
        eventGroupKeys + other.eventGroupKeys,
        primitiveReportingSetIds + other.primitiveReportingSetIds,
      )
  }

  private val memberships = mutableMapOf<String, Universe>()
  private val visiting = mutableSetOf<String>()

  /** Resolves an existing ID in this MeasurementConsumer, without changing its identity. */
  suspend fun getReportingSet(externalReportingSetId: String): ReportingSet {
    requireValid(externalReportingSetId.isNotEmpty(), "ReportingSet ID is empty")
    reportingSets[externalReportingSetId]?.let {
      return it
    }
    val reportingSet = readReportingSet(externalReportingSetId)
    requireValid(
      reportingSet.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId &&
        reportingSet.externalReportingSetId == externalReportingSetId,
      "ReportingSet identity does not match the requested MeasurementConsumer and ID",
    )
    reportingSets[externalReportingSetId] = reportingSet
    return reportingSet
  }

  /** Validates a nonempty, self-linked primitive Campaign Group and returns its universe. */
  suspend fun validateCampaignGroup(
    reportingSet: ReportingSet
  ): Set<ReportingSet.Primitive.EventGroupKey> {
    validateCampaignGroupShape(reportingSet)
    return validate(reportingSet)
  }

  /**
   * Validates the set, its expression, and all referenced primitive bases.
   *
   * The universe includes both operands of every operation, including DIFFERENCE. An empty list of
   * weighted unions is allowed because a valid expression can cancel algebraically.
   *
   * @throws InvalidFieldValueException if a set or reference is malformed or cyclic.
   */
  suspend fun validate(reportingSet: ReportingSet): Set<ReportingSet.Primitive.EventGroupKey> =
    validateUniverse(reportingSet).eventGroupKeys

  private suspend fun validateUniverse(reportingSet: ReportingSet): Universe {
    val id = reportingSet.externalReportingSetId
    requireValid(id.isNotEmpty(), "ReportingSet ID is empty")
    requireValid(
      reportingSet.cmmsMeasurementConsumerId == cmmsMeasurementConsumerId,
      "ReportingSet belongs to a different MeasurementConsumer",
    )
    memberships[id]?.let {
      return it
    }
    requireValid(visiting.add(id), "ReportingSet expression contains a cycle")
    reportingSets[id] = reportingSet
    try {
      val membership =
        when (reportingSet.valueCase) {
          ReportingSet.ValueCase.PRIMITIVE ->
            Universe(reportingSet.primitive.eventGroupKeysList.toSet(), setOf(id))
          ReportingSet.ValueCase.COMPOSITE -> {
            val universe = validateExpression(reportingSet.composite)
            for (union in reportingSet.weightedSubsetUnionsList) {
              requireValid(
                union.primitiveReportingSetBasesCount > 0,
                "Weighted subset union has no primitive bases",
              )
              for (basis in union.primitiveReportingSetBasesList) {
                val primitive = getReportingSet(basis.externalReportingSetId)
                requireValid(primitive.hasPrimitive(), "ReportingSet basis is not primitive")
                requireValid(
                  basis.externalReportingSetId in universe.primitiveReportingSetIds,
                  "ReportingSet basis does not identify a primitive leaf of the expression",
                )
              }
            }
            universe
          }
          ReportingSet.ValueCase.VALUE_NOT_SET,
          null ->
            throw InvalidFieldValueException("reporting_set") { "ReportingSet value is empty" }
        }
      requireValid(membership.eventGroupKeys.isNotEmpty(), "ReportingSet has no EventGroups")
      requireValid(
        membership.eventGroupKeys.all {
          it.cmmsDataProviderId.isNotEmpty() && it.cmmsEventGroupId.isNotEmpty()
        },
        "ReportingSet EventGroup identity is incomplete",
      )
      val campaignGroupId = reportingSet.externalCampaignGroupId
      if (campaignGroupId.isNotEmpty()) {
        if (campaignGroupId == id) {
          validateCampaignGroupShape(reportingSet)
        } else {
          val campaignGroup = getReportingSet(campaignGroupId)
          requireValid(
            validateCampaignGroup(campaignGroup).containsAll(membership.eventGroupKeys),
            "ReportingSet is outside its own Campaign Group",
          )
        }
      }
      memberships[id] = membership
      return membership
    } finally {
      visiting.remove(id)
    }
  }

  private suspend fun validateExpression(expression: ReportingSet.SetExpression): Universe {
    requireValid(
      when (expression.operation) {
        ReportingSet.SetExpression.Operation.UNION,
        ReportingSet.SetExpression.Operation.DIFFERENCE,
        ReportingSet.SetExpression.Operation.INTERSECTION -> true
        ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED,
        ReportingSet.SetExpression.Operation.UNRECOGNIZED,
        null -> false
      },
      "ReportingSet expression operation is invalid",
    )
    val lhs = validateOperand(expression.lhs)
    // Unary expressions are supported by the existing Reporting API.
    return if (expression.hasRhs()) lhs + validateOperand(expression.rhs) else lhs
  }

  private suspend fun validateOperand(operand: ReportingSet.SetExpression.Operand): Universe {
    return when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.EXTERNAL_REPORTING_SET_ID ->
        validateUniverse(getReportingSet(operand.externalReportingSetId))
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION ->
        validateExpression(operand.expression)
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET,
      null -> throw InvalidFieldValueException("reporting_set") { "ReportingSet operand is empty" }
    }
  }

  private fun validateCampaignGroupShape(reportingSet: ReportingSet) {
    if (
      !reportingSet.hasPrimitive() ||
        reportingSet.externalCampaignGroupId != reportingSet.externalReportingSetId ||
        reportingSet.primitive.eventGroupKeysCount == 0 ||
        reportingSet.primitive.eventGroupKeysList.any {
          it.cmmsDataProviderId.isEmpty() || it.cmmsEventGroupId.isEmpty()
        }
    ) {
      throw CampaignGroupInvalidException(
        cmmsMeasurementConsumerId,
        reportingSet.externalReportingSetId,
      )
    }
  }

  private fun requireValid(condition: Boolean, message: String) {
    if (!condition) {
      throw InvalidFieldValueException("reporting_set") { message }
    }
  }
}
