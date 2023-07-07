/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

import io.grpc.Status
import io.grpc.StatusException
import kotlin.math.min
import kotlinx.coroutines.flow.toList
import org.wfanet.measurement.api.v2alpha.EventGroupKey as CmmsEventGroupKey
import org.wfanet.measurement.api.v2alpha.MeasurementConsumerKey
import org.wfanet.measurement.common.base64UrlDecode
import org.wfanet.measurement.common.base64UrlEncode
import org.wfanet.measurement.common.grpc.failGrpc
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.internal.reporting.v2.CreateReportingSetRequest as InternalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.ReportingSet as InternalReportingSet
import org.wfanet.measurement.internal.reporting.v2.ReportingSetKt as InternalReportingSetKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetsGrpcKt.ReportingSetsCoroutineStub
import org.wfanet.measurement.internal.reporting.v2.batchGetReportingSetsRequest
import org.wfanet.measurement.internal.reporting.v2.createReportingSetRequest as internalCreateReportingSetRequest
import org.wfanet.measurement.internal.reporting.v2.reportingSet as internalReportingSet
import org.wfanet.measurement.reporting.v2alpha.CreateReportingSetRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsPageTokenKt.previousPageEnd
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsRequest
import org.wfanet.measurement.reporting.v2alpha.ListReportingSetsResponse
import org.wfanet.measurement.reporting.v2alpha.ReportingSet
import org.wfanet.measurement.reporting.v2alpha.ReportingSetsGrpcKt.ReportingSetsCoroutineImplBase
import org.wfanet.measurement.reporting.v2alpha.copy
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsPageToken
import org.wfanet.measurement.reporting.v2alpha.listReportingSetsResponse

private const val MIN_PAGE_SIZE = 1
private const val DEFAULT_PAGE_SIZE = 50
private const val MAX_PAGE_SIZE = 1000

class ReportingSetsService(private val internalReportingSetsStub: ReportingSetsCoroutineStub) :
  ReportingSetsCoroutineImplBase() {

  data class PrimitiveReportingSetBasis(
    val externalReportingSetId: String,
    val filters: Set<String>,
  )

  private val setExpressionCompiler = SetExpressionCompiler()

  override suspend fun createReportingSet(request: CreateReportingSetRequest): ReportingSet {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot create a ReportingSet for another MeasurementConsumer."
          }
        }
      }
    }

    grpcRequire(request.hasReportingSet()) { "ReportingSet is not specified." }
    grpcRequire(request.reportingSetId.matches(RESOURCE_ID_REGEX)) {
      "Reporting set ID is invalid."
    }

    val internalCreateReportingSetRequest: InternalCreateReportingSetRequest = request.toInternal()

    return try {
      internalReportingSetsStub
        .createReportingSet(internalCreateReportingSetRequest)
        .toReportingSet()
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.ALREADY_EXISTS ->
            Status.ALREADY_EXISTS.withDescription(
              "Metric with ID ${request.reportingSetId} already exists under ${request.parent}"
            )
          Status.Code.NOT_FOUND -> Status.NOT_FOUND.withDescription("Child ReportingSet not found.")
          Status.Code.FAILED_PRECONDITION ->
            Status.FAILED_PRECONDITION.withDescription(
              "Unable to create ReportingSet. The measurement consumer not found."
            )
          else -> Status.UNKNOWN.withDescription("Unable to create ReportingSet.")
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  /**
   * Compiles a public composite [ReportingSet] to a list of
   * [InternalReportingSet.WeightedSubsetUnion]s.
   */
  private suspend fun compileCompositeReportingSet(
    rootReportingSet: ReportingSet,
    cmmsMeasurementConsumerId: String
  ): List<InternalReportingSet.WeightedSubsetUnion> {
    val primitiveReportingSetBasesMap = mutableMapOf<PrimitiveReportingSetBasis, Int>()
    val initialFiltersStack = mutableListOf<String>()

    if (!rootReportingSet.filter.isNullOrBlank()) {
      initialFiltersStack += rootReportingSet.filter
    }

    val setOperationExpression =
      buildSetOperationExpression(
        rootReportingSet.composite.expression,
        initialFiltersStack,
        primitiveReportingSetBasesMap,
        cmmsMeasurementConsumerId
      )

    val idToPrimitiveReportingSetBasis: Map<Int, PrimitiveReportingSetBasis> =
      primitiveReportingSetBasesMap.entries.associateBy({ it.value }) { it.key }

    if (idToPrimitiveReportingSetBasis.size != primitiveReportingSetBasesMap.size) {
      error("The reporting set ID in the set operation expression should be indexed uniquely.")
    }

    val weightedSubsetUnions: List<WeightedSubsetUnion> =
      setExpressionCompiler.compileSetExpression(
        setOperationExpression,
        idToPrimitiveReportingSetBasis.size
      )

    return weightedSubsetUnions.map { weightedSubsetUnion ->
      buildInternalWeightedSubsetUnion(weightedSubsetUnion, idToPrimitiveReportingSetBasis)
    }
  }

  /** Builds an [InternalReportingSet.WeightedSubsetUnion] from a [WeightedSubsetUnion]. */
  private fun buildInternalWeightedSubsetUnion(
    weightedSubsetUnion: WeightedSubsetUnion,
    idToPrimitiveReportingSetBasis: Map<Int, PrimitiveReportingSetBasis>,
  ): InternalReportingSet.WeightedSubsetUnion {
    return InternalReportingSetKt.weightedSubsetUnion {
      weight = weightedSubsetUnion.coefficient
      primitiveReportingSetBases +=
        weightedSubsetUnion.reportingSetIds.map { reportingSetId ->
          InternalReportingSetKt.primitiveReportingSetBasis {
            val primitiveReportingSetBasis = idToPrimitiveReportingSetBasis.getValue(reportingSetId)
            externalReportingSetId = primitiveReportingSetBasis.externalReportingSetId
            filters += primitiveReportingSetBasis.filters.toList()
          }
        }
    }
  }

  /** Builds a [SetOperationExpression] by expanding the given [ReportingSet.SetExpression]. */
  private suspend fun buildSetOperationExpression(
    expression: ReportingSet.SetExpression,
    filters: MutableList<String>,
    primitiveReportingSetBasesMap: MutableMap<PrimitiveReportingSetBasis, Int>,
    cmmsMeasurementConsumerId: String,
  ): SetOperationExpression {
    return SetOperationExpression(
      setOperator = expression.operation.toSetOperator(),
      lhs =
        grpcRequireNotNull(
          buildSetOperationExpressionOperand(
            expression.lhs,
            filters,
            primitiveReportingSetBasesMap,
            cmmsMeasurementConsumerId
          )
        ) {
          "lhs of a set expression must be set."
        },
      rhs =
        buildSetOperationExpressionOperand(
          expression.rhs,
          filters,
          primitiveReportingSetBasesMap,
          cmmsMeasurementConsumerId
        )
    )
  }

  /** Builds a nullable [Operand] from a [ReportingSet.SetExpression.Operand]. */
  private suspend fun buildSetOperationExpressionOperand(
    operand: ReportingSet.SetExpression.Operand,
    filters: MutableList<String>,
    primitiveReportingSetBasesMap: MutableMap<PrimitiveReportingSetBasis, Int>,
    cmmsMeasurementConsumerId: String,
  ): Operand? {
    @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
    return when (operand.operandCase) {
      ReportingSet.SetExpression.Operand.OperandCase.REPORTING_SET -> {
        val internalReportingSet =
          getInternalReportingSet(operand.reportingSet, cmmsMeasurementConsumerId)

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
                primitiveReportingSetBasesMap.size
            }

            // Return the leaf reporting set
            ReportingSet(primitiveReportingSetBasesMap.getValue(primitiveReportingSetBasis))
          }
          InternalReportingSet.ValueCase.COMPOSITE -> {
            // Add the reporting set's filter to the stack.
            if (!internalReportingSet.filter.isNullOrBlank()) {
              filters += internalReportingSet.filter
            }

            // Return the set operation expression
            buildSetOperationExpression(
                internalReportingSet.composite.toExpression(cmmsMeasurementConsumerId),
                filters,
                primitiveReportingSetBasesMap,
                cmmsMeasurementConsumerId
              )
              .also {
                // Remove the reporting set's filter from the stack if there is any.
                if (!internalReportingSet.filter.isNullOrBlank()) {
                  filters.removeLast()
                }
              }
          }
          InternalReportingSet.ValueCase.VALUE_NOT_SET -> {
            error("The reporting set [${operand.reportingSet}] value type should've been set. ")
          }
        }
      }
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
        buildSetOperationExpression(
          operand.expression,
          filters,
          primitiveReportingSetBasesMap,
          cmmsMeasurementConsumerId
        )
      }
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {
        null
      }
    }
  }

  /** Gets an [InternalReportingSet] given the reporting set resource name. */
  private suspend fun getInternalReportingSet(
    reportingSet: String,
    cmmsMeasurementConsumerId: String
  ): InternalReportingSet {
    val reportingSetKey = buildReportingSetKey(reportingSet, cmmsMeasurementConsumerId)

    return try {
      internalReportingSetsStub
        .batchGetReportingSets(
          batchGetReportingSetsRequest {
            this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
            externalReportingSetIds += reportingSetKey.reportingSetId
          }
        )
        .reportingSetsList
        .first()
    } catch (e: StatusException) {
      throw when (e.status.code) {
          Status.Code.NOT_FOUND ->
            Status.NOT_FOUND.withDescription("ReportingSet [$reportingSet] not found.")
          else -> Status.UNKNOWN.withDescription("Unable to get ReportingSet [$reportingSet].")
        }
        .withCause(e)
        .asRuntimeException()
    }
  }

  override suspend fun listReportingSets(
    request: ListReportingSetsRequest
  ): ListReportingSetsResponse {
    val parentKey: MeasurementConsumerKey =
      grpcRequireNotNull(MeasurementConsumerKey.fromName(request.parent)) {
        "Parent is either unspecified or invalid."
      }
    val listReportingSetsPageToken = request.toListReportingSetsPageToken()

    when (val principal: ReportingPrincipal = principalFromCurrentContext) {
      is MeasurementConsumerPrincipal -> {
        if (parentKey != principal.resourceKey) {
          failGrpc(Status.PERMISSION_DENIED) {
            "Cannot list ReportingSets belonging to other MeasurementConsumers."
          }
        }
      }
    }

    val results: List<InternalReportingSet> =
      internalReportingSetsStub
        .streamReportingSets(listReportingSetsPageToken.toStreamReportingSetsRequest())
        .toList()

    if (results.isEmpty()) {
      return ListReportingSetsResponse.getDefaultInstance()
    }

    return listReportingSetsResponse {
      reportingSets +=
        results
          .subList(0, min(results.size, listReportingSetsPageToken.pageSize))
          .map(InternalReportingSet::toReportingSet)

      if (results.size > listReportingSetsPageToken.pageSize) {
        val pageToken =
          listReportingSetsPageToken.copy {
            lastReportingSet = previousPageEnd {
              cmmsMeasurementConsumerId = results[results.lastIndex - 1].cmmsMeasurementConsumerId
              externalReportingSetId = results[results.lastIndex - 1].externalReportingSetId
            }
          }
        nextPageToken = pageToken.toByteString().base64UrlEncode()
      }
    }
  }

  /** Converts a [CreateReportingSetRequest] to an [InternalCreateReportingSetRequest]. */
  private suspend fun CreateReportingSetRequest.toInternal(): InternalCreateReportingSetRequest {
    val source = this
    val cmmsMeasurementConsumerId =
      checkNotNull(MeasurementConsumerKey.fromName(source.parent)).measurementConsumerId

    val internalReportingSet = internalReportingSet {
      this.cmmsMeasurementConsumerId = cmmsMeasurementConsumerId
      displayName = source.reportingSet.displayName
      if (!source.reportingSet.filter.isNullOrBlank()) {
        filter = source.reportingSet.filter
      }

      @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA")
      when (source.reportingSet.valueCase) {
        ReportingSet.ValueCase.PRIMITIVE -> {
          primitive = source.reportingSet.primitive.toInternal()
        }
        ReportingSet.ValueCase.COMPOSITE -> {
          grpcRequire(source.reportingSet.composite.hasExpression()) {
            "Set expression in the composite reporting set is not set."
          }
          composite = source.reportingSet.composite.expression.toInternal()
          weightedSubsetUnions +=
            compileCompositeReportingSet(source.reportingSet, cmmsMeasurementConsumerId)
        }
        ReportingSet.ValueCase.VALUE_NOT_SET -> {
          failGrpc(Status.INVALID_ARGUMENT) { "ReportingSet value type is not set." }
        }
      }
    }

    return internalCreateReportingSetRequest {
      this.reportingSet = internalReportingSet
      this.externalReportingSetId = source.reportingSetId
    }
  }

  companion object {
    private val RESOURCE_ID_REGEX = Regex("^[a-z]([a-z0-9-]{0,61}[a-z0-9])?$")
  }
}

/** Converts a public [ListReportingSetsRequest] to a [ListReportingSetsPageToken]. */
private fun ListReportingSetsRequest.toListReportingSetsPageToken(): ListReportingSetsPageToken {
  grpcRequire(pageSize >= 0) { "Page size cannot be less than 0" }

  val source = this
  val parentKey: MeasurementConsumerKey =
    grpcRequireNotNull(MeasurementConsumerKey.fromName(parent)) {
      "Parent is either unspecified or invalid."
    }

  return if (source.pageToken.isNotBlank()) {
    ListReportingSetsPageToken.parseFrom(source.pageToken.base64UrlDecode()).copy {
      grpcRequire(this.cmmsMeasurementConsumerId == parentKey.measurementConsumerId) {
        "Arguments must be kept the same when using a page token"
      }

      if (
        source.pageSize != 0 && source.pageSize >= MIN_PAGE_SIZE && source.pageSize <= MAX_PAGE_SIZE
      ) {
        pageSize = source.pageSize
      }
    }
  } else {
    listReportingSetsPageToken {
      pageSize =
        when {
          source.pageSize < MIN_PAGE_SIZE -> DEFAULT_PAGE_SIZE
          source.pageSize > MAX_PAGE_SIZE -> MAX_PAGE_SIZE
          else -> source.pageSize
        }
      this.cmmsMeasurementConsumerId = parentKey.measurementConsumerId
    }
  }
}

/** Converts a [ReportingSet.SetExpression.Operation] to an [Operator]. */
private fun ReportingSet.SetExpression.Operation.toSetOperator(): Operator {
  return when (this) {
    ReportingSet.SetExpression.Operation.UNION -> {
      Operator.UNION
    }
    ReportingSet.SetExpression.Operation.DIFFERENCE -> {
      Operator.DIFFERENCE
    }
    ReportingSet.SetExpression.Operation.INTERSECTION -> {
      Operator.INTERSECT
    }
    ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED -> {
      failGrpc(Status.INVALID_ARGUMENT) { "Set expression operation type unspecified." }
    }
    ReportingSet.SetExpression.Operation.UNRECOGNIZED -> {
      failGrpc(Status.INVALID_ARGUMENT) { "Unrecognized set expression operation type." }
    }
  }
}

/** Builds a [ReportingSetKey] with a given reporting set resource name. */
private fun buildReportingSetKey(
  reportingSetName: String,
  cmmsMeasurementConsumerId: String = "",
): ReportingSetKey {
  val reportingSetKey =
    grpcRequireNotNull(ReportingSetKey.fromName(reportingSetName)) {
      "Invalid reporting set name ${reportingSetName}."
    }
  if (
    cmmsMeasurementConsumerId.isNotBlank() &&
      reportingSetKey.cmmsMeasurementConsumerId != cmmsMeasurementConsumerId
  ) {
    failGrpc(Status.PERMISSION_DENIED) {
      "Cannot create a ReportingSet [${reportingSetName}] for another MeasurementConsumer."
    }
  }
  return reportingSetKey
}

/** Converts a [ReportingSet.SetExpression] to an [InternalReportingSet.SetExpression]. */
private fun ReportingSet.SetExpression.toInternal(): InternalReportingSet.SetExpression {
  val source = this

  return InternalReportingSetKt.setExpression {
    operation = source.operation.toInternal()

    grpcRequire(source.hasLhs()) { "lhs of a set expression must be set" }
    lhs = source.lhs.toInternal()
    rhs = source.rhs.toInternal()
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
    ReportingSet.SetExpression.Operation.OPERATION_UNSPECIFIED -> {
      failGrpc(Status.INVALID_ARGUMENT) { "Set expression operation type unspecified." }
    }
    ReportingSet.SetExpression.Operation.UNRECOGNIZED -> {
      failGrpc(Status.INVALID_ARGUMENT) { "Unrecognized set expression operation type." }
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
        val reportingSetKey = buildReportingSetKey(source.reportingSet)
        externalReportingSetId = reportingSetKey.reportingSetId
      }
      ReportingSet.SetExpression.Operand.OperandCase.EXPRESSION -> {
        expression = source.expression.toInternal()
      }
      ReportingSet.SetExpression.Operand.OperandCase.OPERAND_NOT_SET -> {}
    }
  }
}

/** Converts a [ReportingSet.Primitive] to an [InternalReportingSet.Primitive]. */
private fun ReportingSet.Primitive.toInternal(): InternalReportingSet.Primitive {
  val source = this

  grpcRequire(source.cmmsEventGroupsList.isNotEmpty()) { "No event group specified." }

  return InternalReportingSetKt.primitive {
    eventGroupKeys +=
      source.cmmsEventGroupsList.map { cmmsEventGroup ->
        val cmmsEventGroupKey =
          grpcRequireNotNull(CmmsEventGroupKey.fromName(cmmsEventGroup)) {
            "Invalid event group name $cmmsEventGroup."
          }

        InternalReportingSetKt.PrimitiveKt.eventGroupKey {
          cmmsDataProviderId = cmmsEventGroupKey.dataProviderId
          cmmsEventGroupId = cmmsEventGroupKey.eventGroupId
        }
      }
  }
}
