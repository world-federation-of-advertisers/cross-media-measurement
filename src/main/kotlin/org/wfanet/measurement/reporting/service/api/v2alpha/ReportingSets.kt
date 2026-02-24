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

package org.wfanet.measurement.reporting.service.api.v2alpha

import io.grpc.StatusException
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey as CmmsMeasurementConsumerKey
import org.wfanet.measurement.common.grpc.errorInfo
import org.wfanet.measurement.internal.reporting.ErrorCode
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetRequest as InternalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub as InternalReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest as internalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.reporting.service.api.ReportingSetNotFoundException
import org.wfanet.measurement.reporting.service.api.RequiredFieldNotSetException
import org.wfanet.measurement.reporting.service.internal.ReportingInternalException
import org.wfanet.measurement.reporting.v2alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.ReportingSet

/** Utilities for [ReportingSet]s. */
object ReportingSets {
  private data class PrimitiveReportingSetBasis(
    val externalReportingSetId: String,
    val filters: Set<String>,
  )

  private val setExpressionCompiler = SetExpressionCompiler()

  /**
   * Retrieves the [InternalReportingSet]s directly and transitively referenced by
   * [reportingSetKeys].
   *
   * @throws ReportingSetNotFoundException if any referenced [ReportingSet] could not be found
   * @throws InternalReportingSetsException if there was some other error retrieving
   *   [InternalReportingSet]s
   */
  suspend fun getReferencedReportingSets(
    internalReportingSetsStub: InternalReportingSetsCoroutineStub,
    reportingSetKeys: Set<ReportingSetKey>,
  ): Map<ReportingSetKey, InternalReportingSet> {
    var keysToFetch: Set<ReportingSetKey> = reportingSetKeys
    return buildMap {
      while (keysToFetch.isNotEmpty()) {
        val internalReportingSets: Map<ReportingSetKey, InternalReportingSet> =
          batchGetInternalReportingSets(internalReportingSetsStub, keysToFetch)
        putAll(internalReportingSets)

        // Process the ReportingSets that were just fetched to determine the next batch of keys.
        keysToFetch = buildSet {
          for (value: InternalReportingSet in internalReportingSets.values) {
            if (value.hasComposite()) {
              for (referencedKey in
                getReferencedReportingSetKeys(value.toReportingSet().composite.expression)) {
                if (referencedKey in this) {
                  // Skip any that have already been fetched.
                  continue
                }
                add(referencedKey)
              }
            }
          }
        }
      }
    }
  }

  /**
   * Returns [InternalReportingSet]s for the specified keys.
   *
   * @param reportingSetKeys keys of [InternalReportingSet]s to retrieve belonging to the same
   *   parent
   * @throws ReportingSetNotFoundException if an [InternalReportingSet] is not found for any of the
   *   specified keys
   * @throws InternalReportingSetsException if there was some other error retrieving
   *   [InternalReportingSet]s
   */
  private suspend fun batchGetInternalReportingSets(
    internalReportingSetsStub: InternalReportingSetsCoroutineStub,
    reportingSetKeys: Set<ReportingSetKey>,
  ): Map<ReportingSetKey, InternalReportingSet> {
    if (reportingSetKeys.isEmpty()) {
      return mutableMapOf()
    }

    val parentKey = reportingSetKeys.first().parentKey
    val internalReportingSetsResponse =
      try {
        internalReportingSetsStub.batchGetReportingSets(
          batchGetReportingSetsRequest {
            cmmsMeasurementConsumerId = parentKey.measurementConsumerId
            for (reportingSetKey in reportingSetKeys) {
              require(reportingSetKey.parentKey == parentKey)
              externalReportingSetIds += reportingSetKey.reportingSetId
            }
          }
        )
      } catch (e: StatusException) {
        throw when (ReportingInternalException.getErrorCode(e)) {
          ErrorCode.REPORTING_SET_NOT_FOUND -> {
            val reportingSetId = e.errorInfo!!.metadataMap.getValue("externalReportingSetId")
            ReportingSetNotFoundException(ReportingSetKey(parentKey, reportingSetId).toName(), e)
          }
          ErrorCode.REPORTING_SET_ALREADY_EXISTS,
          ErrorCode.MEASUREMENT_ALREADY_EXISTS,
          ErrorCode.MEASUREMENT_NOT_FOUND,
          ErrorCode.MEASUREMENT_CALCULATION_TIME_INTERVAL_NOT_FOUND,
          ErrorCode.REPORT_NOT_FOUND,
          ErrorCode.MEASUREMENT_STATE_INVALID,
          ErrorCode.MEASUREMENT_CONSUMER_NOT_FOUND,
          ErrorCode.MEASUREMENT_CONSUMER_ALREADY_EXISTS,
          ErrorCode.METRIC_ALREADY_EXISTS,
          ErrorCode.REPORT_ALREADY_EXISTS,
          ErrorCode.REPORT_SCHEDULE_ALREADY_EXISTS,
          ErrorCode.REPORT_SCHEDULE_NOT_FOUND,
          ErrorCode.REPORT_SCHEDULE_STATE_INVALID,
          ErrorCode.REPORT_SCHEDULE_ITERATION_NOT_FOUND,
          ErrorCode.REPORT_SCHEDULE_ITERATION_STATE_INVALID,
          ErrorCode.METRIC_CALCULATION_SPEC_NOT_FOUND,
          ErrorCode.METRIC_CALCULATION_SPEC_ALREADY_EXISTS,
          ErrorCode.CAMPAIGN_GROUP_INVALID,
          ErrorCode.UNKNOWN_ERROR,
          ErrorCode.UNRECOGNIZED,
          null -> InternalReportingSetsException("Error retrieving ReportingSets", e)
        }
      }
    return internalReportingSetsResponse.reportingSetsList.associateBy {
      ReportingSetKey(it.cmmsMeasurementConsumerId, it.externalReportingSetId)
    }
  }

  /**
   * Returns the keys of the [ReportingSet]s directly referenced by [setExpression], i.e. those that
   * do not require dereferencing a [ReportingSet].
   */
  fun getReferencedReportingSetKeys(
    setExpression: ReportingSet.SetExpression
  ): Set<ReportingSetKey> {
    return buildSet {
      addAll(getReferencedReportingSetKeys(setExpression.lhs))
      if (setExpression.hasRhs()) {
        addAll(getReferencedReportingSetKeys(setExpression.rhs))
      }
    }
  }

  /**
   * Returns the keys of the [ReportingSet]s directly referenced by [operand], i.e. those that do
   * not require dereferencing a [ReportingSet].
   */
  private fun getReferencedReportingSetKeys(
    operand: ReportingSet.SetExpression.Operand
  ): Set<ReportingSetKey> {
    return when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.REPORTING_SET ->
        setOf(checkNotNull(ReportingSetKey.fromName(operand.reportingSet)))
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION ->
        getReferencedReportingSetKeys(operand.expression)
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> error("Operand not set")
    }
  }

  /**
   * Builds an [InternalCreateReportingSetRequest] from a parsed, validated
   * [CreateReportingSetRequest].
   *
   * This performs set expression compilation as-needed for composite ReportingSets.
   */
  fun buildInternalCreateReportingSetRequest(
    request: CreateReportingSetRequest,
    parentKey: CmmsMeasurementConsumerKey,
    campaignGroupKey: ReportingSetKey?,
    referencedReportingSets: Map<ReportingSetKey, InternalReportingSet>,
  ): InternalCreateReportingSetRequest {
    val internalReportingSetsByName = referencedReportingSets.mapKeys { it.key.toName() }
    return internalCreateReportingSetRequest {
      externalReportingSetId = request.reportingSetId
      reportingSet = internalReportingSet {
        cmmsMeasurementConsumerId = parentKey.measurementConsumerId
        if (campaignGroupKey != null) {
          externalCampaignGroupId = campaignGroupKey.reportingSetId
        }
        displayName = request.reportingSet.displayName
        filter = request.reportingSet.filter

        details = InternalReportingSetKt.details { tags.putAll(request.reportingSet.tagsMap) }

        when (request.reportingSet.valueCase) {
          ReportingSet.ValueCase.PRIMITIVE -> {
            primitive = request.reportingSet.primitive.toInternal()
          }
          ReportingSet.ValueCase.COMPOSITE -> {
            composite = request.reportingSet.composite.expression.toInternal()
            weightedSubsetUnions +=
              compileWeightedSubsetUnions(
                request.reportingSet.filter,
                request.reportingSet.composite.expression,
                cmmsMeasurementConsumerId,
                internalReportingSetsByName,
              )
          }
          ReportingSet.ValueCase.VALUE_NOT_SET ->
            throw RequiredFieldNotSetException("reporting_set.value")
        }
      }
    }
  }

  /** Compiles [InternalReportingSet.WeightedSubsetUnion]s for a composite [ReportingSet]. */
  private fun compileWeightedSubsetUnions(
    filter: String,
    setExpression: ReportingSet.SetExpression,
    cmmsMeasurementConsumerId: String,
    internalReportingSetsByName: Map<String, InternalReportingSet>,
  ): List<InternalReportingSet.WeightedSubsetUnion> {
    val primitiveReportingSetBasesMap =
      mutableMapOf<PrimitiveReportingSetBasis, SetExpressionCompiler.ReportingSet>()
    val initialFiltersStack = mutableListOf<String>()

    if (filter.isNotEmpty()) {
      initialFiltersStack += filter
    }

    val setOperationExpression: SetExpressionCompiler.SetOperationExpression =
      buildSetOperationExpression(
        setExpression,
        initialFiltersStack,
        primitiveReportingSetBasesMap,
        cmmsMeasurementConsumerId,
        internalReportingSetsByName,
      )

    val primitiveBasisByReportingSet:
      Map<SetExpressionCompiler.ReportingSet, PrimitiveReportingSetBasis> =
      primitiveReportingSetBasesMap.entries.associateBy({ it.value }) { it.key }

    if (primitiveBasisByReportingSet.size != primitiveReportingSetBasesMap.size) {
      error("The reporting set ID in the set operation expression should be indexed uniquely.")
    }

    val weightedSubsetUnions: List<SetExpressionCompiler.WeightedSubsetUnion> =
      setExpressionCompiler.compileSetExpression(
        setOperationExpression,
        primitiveBasisByReportingSet.size,
      )

    return weightedSubsetUnions.map { weightedSubsetUnion ->
      buildInternalWeightedSubsetUnion(weightedSubsetUnion, primitiveBasisByReportingSet)
    }
  }

  /**
   * Builds an [InternalReportingSet.WeightedSubsetUnion] from a
   * [SetExpressionCompiler.WeightedSubsetUnion].
   */
  private fun buildInternalWeightedSubsetUnion(
    weightedSubsetUnion: SetExpressionCompiler.WeightedSubsetUnion,
    primitiveBasisByReportingSet:
      Map<SetExpressionCompiler.ReportingSet, PrimitiveReportingSetBasis>,
  ): InternalReportingSet.WeightedSubsetUnion {
    return InternalReportingSetKt.weightedSubsetUnion {
      primitiveReportingSetBases +=
        weightedSubsetUnion.reportingSets.map { reportingSet: SetExpressionCompiler.ReportingSet ->
          InternalReportingSetKt.primitiveReportingSetBasis {
            val primitiveReportingSetBasis = primitiveBasisByReportingSet.getValue(reportingSet)
            externalReportingSetId = primitiveReportingSetBasis.externalReportingSetId
            filters += primitiveReportingSetBasis.filters.toList()
          }
        }
      weight = weightedSubsetUnion.coefficient
      binaryRepresentation = weightedSubsetUnion.binaryRepresentation
    }
  }

  /**
   * Builds a [SetExpressionCompiler.SetOperationExpression] by expanding the given
   * [ReportingSet.SetExpression].
   */
  private fun buildSetOperationExpression(
    expression: ReportingSet.SetExpression,
    filters: MutableList<String>,
    primitiveReportingSetBasesMap:
      MutableMap<PrimitiveReportingSetBasis, SetExpressionCompiler.ReportingSet>,
    cmmsMeasurementConsumerId: String,
    internalReportingSetsByName: Map<String, InternalReportingSet>,
  ): SetExpressionCompiler.SetOperationExpression {
    val lhs =
      buildSetOperationExpressionOperand(
        expression.lhs,
        filters,
        primitiveReportingSetBasesMap,
        cmmsMeasurementConsumerId,
        internalReportingSetsByName,
      )

    val rhs =
      if (expression.hasRhs()) {
        buildSetOperationExpressionOperand(
          expression.rhs,
          filters,
          primitiveReportingSetBasesMap,
          cmmsMeasurementConsumerId,
          internalReportingSetsByName,
        )
      } else {
        null
      }

    return SetExpressionCompiler.SetOperationExpression(
      setOperator = expression.operation.toSetOperator(),
      lhs = lhs,
      rhs = rhs,
    )
  }

  /** Builds a [SetExpressionCompiler.Operand] from a [ReportingSet.SetExpression.Operand]. */
  private fun buildSetOperationExpressionOperand(
    operand: ReportingSet.SetExpression.Operand,
    filters: MutableList<String>,
    primitiveReportingSetBasesMap:
      MutableMap<PrimitiveReportingSetBasis, SetExpressionCompiler.ReportingSet>,
    cmmsMeasurementConsumerId: String,
    internalReportingSetsByName: Map<String, InternalReportingSet>,
  ): SetExpressionCompiler.Operand {
    return when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.REPORTING_SET -> {
        val internalReportingSet: InternalReportingSet =
          internalReportingSetsByName.getValue(operand.reportingSet)

        when (internalReportingSet.valueCase) {
          // Reach the leaf node
          InternalReportingSet.ValueCase.PRIMITIVE -> {
            val primitiveReportingSetBasis =
              PrimitiveReportingSetBasis(
                externalReportingSetId = internalReportingSet.externalReportingSetId,
                filters =
                  (filters + internalReportingSet.filter).filter { !it.isNullOrBlank() }.toSet(),
              )

            // Avoid duplicates
            if (!primitiveReportingSetBasesMap.contains(primitiveReportingSetBasis)) {
              // New ID == current size of the map
              primitiveReportingSetBasesMap[primitiveReportingSetBasis] =
                SetExpressionCompiler.ReportingSet(primitiveReportingSetBasesMap.size)
            }

            // Return the leaf reporting set
            primitiveReportingSetBasesMap.getValue(primitiveReportingSetBasis)
          }
          InternalReportingSet.ValueCase.COMPOSITE -> {
            // Add the reporting set's filter to the stack.
            if (!internalReportingSet.filter.isEmpty()) {
              filters += internalReportingSet.filter
            }

            // Return the set operation expression
            buildSetOperationExpression(
                internalReportingSet.composite.toExpression(cmmsMeasurementConsumerId),
                filters,
                primitiveReportingSetBasesMap,
                cmmsMeasurementConsumerId,
                internalReportingSetsByName,
              )
              .also {
                // Remove the reporting set's filter from the stack if there is any.
                if (!internalReportingSet.filter.isEmpty()) {
                  filters.removeLast()
                }
              }
          }
          InternalReportingSet.ValueCase.VALUE_NOT_SET -> error("Value not set")
        }
      }
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
        buildSetOperationExpression(
          operand.expression,
          filters,
          primitiveReportingSetBasesMap,
          cmmsMeasurementConsumerId,
          internalReportingSetsByName,
        )
      }
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> error("Operand not set")
    }
  }

  /** Converts a [ReportingSet.SetExpression] to an [InternalReportingSet.SetExpression]. */
  private fun ReportingSet.SetExpression.toInternal(): InternalReportingSet.SetExpression {
    val source = this

    return InternalReportingSetKt.setExpression {
      operation = source.operation.toInternal()

      lhs = source.lhs.toInternal()
      if (source.hasRhs()) {
        rhs = source.rhs.toInternal()
      }
    }
  }

  /**
   * Converts a [ReportingSet.SetExpression.Operand] to an
   * [InternalReportingSet.SetExpression.Operand].
   */
  private fun ReportingSet.SetExpression.Operand.toInternal():
    InternalReportingSet.SetExpression.Operand {
    val source = this
    return InternalReportingSetKt.SetExpressionKt.operand {
      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.operandCase) {
        ReportingSet.SetExpression.Operand.OperandCase.REPORTING_SET -> {
          val reportingSetKey = ReportingSetKey.fromName(source.reportingSet)
          externalReportingSetId = reportingSetKey!!.reportingSetId
        }
        ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
          expression = source.expression.toInternal()
        }
        ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {}
      }
    }
  }

  /**
   * Converts a [ReportingSet.SetExpression.Operation] to an
   * [InternalReportingSet.SetExpression.Operation].
   */
  private fun ReportingSet.SetExpression.Operation.toInternal():
    InternalReportingSet.SetExpression.Operation {
    return when (this) {
      ReportingSet.SetExpression.Operation.UNION -> {
        InternalReportingSet.SetExpression.Operation.UNION
      }
      ReportingSet.SetExpression.Operation.DIFFERENCE -> {
        InternalReportingSet.SetExpression.Operation.DIFFERENCE
      }
      ReportingSet.SetExpression.Operation.INTERSECTION -> {
        InternalReportingSet.SetExpression.Operation.INTERSECTION
      }
      ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED,
      ReportingSet.SetExpression.Operation.UNRECOGNIZED -> error("operation not set or invalid")
    }
  }

  /** Converts a [ReportingSet.SetExpression.Operation] to a [SetExpressionCompiler.Operator]. */
  private fun ReportingSet.SetExpression.Operation.toSetOperator(): SetExpressionCompiler.Operator {
    return when (this) {
      ReportingSet.SetExpression.Operation.UNION -> SetExpressionCompiler.Operator.UNION
      ReportingSet.SetExpression.Operation.DIFFERENCE -> SetExpressionCompiler.Operator.DIFFERENCE
      ReportingSet.SetExpression.Operation.INTERSECTION -> SetExpressionCompiler.Operator.INTERSECT
      ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED -> error("Operation unspecified")
      ReportingSet.SetExpression.Operation.UNRECOGNIZED -> error("Operation unrecognized")
    }
  }
}
