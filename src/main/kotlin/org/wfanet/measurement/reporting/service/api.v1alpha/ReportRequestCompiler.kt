package org.wfanet.measurement.reporting.service.api.v1alpha

import kotlin.math.pow
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.reporting.v1alpha.Metric.NamedSetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.Report.EventGroupUniverse

typealias PrimitiveRegion = Int

typealias UnionOnlySet = Int

typealias UnionOnlySetToCoefficient = Map<UnionOnlySet, Int>

typealias PrimitiveRegionToUnionOnlySets = MutableMap<PrimitiveRegion, UnionOnlySetToCoefficient>

private enum class Operator {
  UNION,
  INTERSECT,
  DIFFERENCE
}

private interface Operand

private data class ReportingSet(val id: Int, val resourceName: String) : Operand

private data class SetOperationExpression(
  val setOperator: Operator,
  val lhs: Operand,
  val rhs: Operand?,
) : Operand

class ReportRequestCompiler(
  private val reportName: String,
  private val measurementConsumerReferenceId: String,
  private val eventGroupUniverse: EventGroupUniverse,
) {

  val primitiveRegionCache = listOf<PrimitiveRegionToUnionOnlySets>()

  suspend fun compileSetOperation(namedSetOperation: NamedSetOperation) {
    val reportingSetsMap = createReportingSetsMap(namedSetOperation.setOperation)
    val setOperationExpression =
      namedSetOperation.setOperation.toSetOperationExpression(reportingSetsMap)
    val numReportingSets = reportingSetsMap.size

    // Part 1
    val primitiveRegion: List<Int> =
      setOperationExpressionToPrimitiveRegions(numReportingSets, setOperationExpression)

    // Part 2
  }

  private fun setOperationExpressionToPrimitiveRegions(
    numReportingSets: Int,
    setOperationExpression: SetOperationExpression
  ): List<Int> {
    val allPrimitiveRegionsSet: List<Set<Int>> = getAllPrimitiveRegions(numReportingSets)
    return decomposeSetOperationExpression(setOperationExpression, allPrimitiveRegionsSet)
  }

  private fun decomposeSetOperationExpression(
    setOperationExpression: SetOperationExpression,
    allPrimitiveRegionsSet: List<Set<Int>>
  ): List<Int> {
    val lhsPrimitiveRegions = decomposeOperand(setOperationExpression.lhs, allPrimitiveRegionsSet)
    val rhsPrimitiveRegions = decomposeOperand(setOperationExpression.rhs, allPrimitiveRegionsSet)
    return calculateBinarySetOperation(
        lhsPrimitiveRegions,
        rhsPrimitiveRegions,
        setOperationExpression.setOperator
      )
      .toList()
  }

  private fun decomposeOperand(
    operand: Operand?,
    allPrimitiveRegionsSet: List<Set<Int>>
  ): Set<Int> {
    return when (operand) {
      is SetOperationExpression -> {
        val lhsPrimitiveRegions = decomposeOperand(operand.lhs, allPrimitiveRegionsSet)
        val rhsPrimitiveRegions = decomposeOperand(operand.rhs, allPrimitiveRegionsSet)
        calculateBinarySetOperation(lhsPrimitiveRegions, rhsPrimitiveRegions, operand.setOperator)
      }
      is ReportingSet -> {
        allPrimitiveRegionsSet[operand.id]
      }
      else -> setOf()
    }
  }

  private fun calculateBinarySetOperation(
    lhs: Set<Int>,
    rhs: Set<Int>,
    operator: Operator
  ): Set<Int> {
    return when (operator) {
      Operator.UNION -> lhs union rhs
      Operator.INTERSECT -> lhs intersect rhs
      Operator.DIFFERENCE -> lhs subtract rhs
    }
  }

  private fun getAllPrimitiveRegions(numReportingSets: Int): List<Set<Int>> {
    val numPrimitiveRegions = 2.0.pow(numReportingSets).toInt() - 1
    val allPrimitiveRegionsSet: List<MutableSet<Int>> = List(numReportingSets) { mutableSetOf() }

    for (region in 1..numPrimitiveRegions) {
      for (reportingSetId in 0 until numReportingSets) {
        val hasRegion: Boolean = (region and (1 shl reportingSetId)) != 0
        if (hasRegion) {
          allPrimitiveRegionsSet[reportingSetId].add(region)
        }
      }
    }

    return allPrimitiveRegionsSet
  }

  private fun createReportingSetsMap(operation: SetOperation): Map<String, ReportingSet> {
    val reportingSets: List<String> = operation.toReportingSets()

    val reportingSetsMap: MutableMap<String, ReportingSet> = mutableMapOf()

    for ((id, reportingSet) in reportingSets.sorted().withIndex()) {
      grpcRequire(!reportingSetsMap.containsKey(reportingSet)) {
        "Reporting sets in SetOperation should be unique."
      }
      reportingSetsMap[reportingSet] = ReportingSet(id, reportingSet)
    }
    return reportingSetsMap.toMap()
  }
}

/** Gets all reporting sets used in this [SetOperation]. */
private fun SetOperation.toReportingSets(): List<String> {
  val root = this
  grpcRequire(root.hasLhs()) { "lhs in SetOperation must be set." }
  grpcRequire(root.lhs.hasReportingSet() || root.lhs.hasOperation()) {
    "Operand type of lhs in SetOperation must be set."
  }

  val reportingSets = mutableListOf<String>()

  storeReportingSets(reportingSets, root.lhs)
  storeReportingSets(reportingSets, root.rhs)

  return reportingSets.sorted()
}

private fun storeReportingSets(
  reportingSets: MutableList<String>,
  node: SetOperation.Operand,
) {
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  when (node.operandCase) {
    // Leaf node
    SetOperation.Operand.OperandCase.REPORTING_SET -> reportingSets.add(node.reportingSet)
    SetOperation.Operand.OperandCase.OPERATION -> {
      reportingSets.addAll(node.operation.toReportingSets())
    }
    // Empty node
    SetOperation.Operand.OperandCase.OPERAND_NOT_SET -> return
  }
}

private fun SetOperation.toSetOperationExpression(
  reportingSetsMap: Map<String, ReportingSet>
): SetOperationExpression {
  val root = this
  grpcRequire(root.hasLhs()) { "lhs in SetOperation must be set." }
  val lhs =
    grpcRequireNotNull(root.lhs.toOperand(reportingSetsMap)) { "lhs in SetOperation must be set." }
  val rhs = root.rhs.toOperand(reportingSetsMap)

  return SetOperationExpression(root.type.toOperator(), lhs, rhs)
}

private fun SetOperation.Operand.toOperand(reportingSetsMap: Map<String, ReportingSet>): Operand? {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source.operandCase) {
    SetOperation.Operand.OperandCase.REPORTING_SET -> {
      return reportingSetsMap[source.reportingSet]
    }
    SetOperation.Operand.OperandCase.OPERATION ->
      return source.operation.toSetOperationExpression(reportingSetsMap)
    SetOperation.Operand.OperandCase.OPERAND_NOT_SET -> null
  }
}

private fun SetOperation.Type.toOperator(): Operator {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source) {
    SetOperation.Type.TYPE_UNSPECIFIED -> error("Set operator type is not specified.")
    SetOperation.Type.UNION -> Operator.UNION
    SetOperation.Type.DIFFERENCE -> Operator.DIFFERENCE
    SetOperation.Type.INTERSECTION -> Operator.INTERSECT
    SetOperation.Type.UNRECOGNIZED -> error("Unrecognized Set operator type.")
  }
}
