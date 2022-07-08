package org.wfanet.measurement.reporting.service.api.v1alpha

import kotlin.math.pow
import org.wfanet.measurement.common.grpc.grpcRequire
import org.wfanet.measurement.common.grpc.grpcRequireNotNull
import org.wfanet.measurement.reporting.v1alpha.Metric.NamedSetOperation
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation
import org.wfanet.measurement.reporting.v1alpha.Report.EventGroupUniverse

private typealias PrimitiveRegion = Int

private typealias UnionOnlySet = Int

private typealias ReportingSetCount = Int

private typealias UnionOnlySetCoefficientMap = Map<UnionOnlySet, Int>

private typealias PrimitiveRegionToUnionOnlySetCoefficientMap =
  MutableMap<PrimitiveRegion, UnionOnlySetCoefficientMap>

private typealias PrimitiveRegionCache =
  MutableMap<ReportingSetCount, PrimitiveRegionToUnionOnlySetCoefficientMap>

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

data class WeightedMeasurement(val reportingSets: List<String>, val coefficient: Int)

class ReportRequestCompiler(
  private val reportName: String,
  private val eventGroupUniverse: EventGroupUniverse,
) {

  private var primitiveRegionCache: PrimitiveRegionCache = mutableMapOf()

  suspend fun compileSetOperation(namedSetOperation: NamedSetOperation): List<WeightedMeasurement> {
    val sortedReportingSets = namedSetOperation.setOperation.toSortedReportingSets()
    val reportingSetsMap = createReportingSetsMap(sortedReportingSets)
    val setOperationExpression =
      namedSetOperation.setOperation.toSetOperationExpression(reportingSetsMap)
    val numReportingSets = reportingSetsMap.size

    // Part 1 - Gets the primitive regions that cover the set operation
    val primitiveRegions: List<Int> =
      setOperationExpressionToPrimitiveRegions(numReportingSets, setOperationExpression)

    // Part 2 - Converts the list of primitive regions to a list of pairs of union-set to
    // coefficients
    val unionOnlySetCoefficientMap =
      primitiveRegionsToUnionOnlySetCoefficientMap(numReportingSets, primitiveRegions)

    return unionOnlySetCoefficientMap.map {
      convertUnionOnlySetToWeightedMeasurements(it.key, it.value, sortedReportingSets)
    }
  }

  private fun convertUnionOnlySetToWeightedMeasurements(
    unionOnlySet: UnionOnlySet,
    coefficient: Int,
    sortedReportingSets: List<String>
  ): WeightedMeasurement {
    val reportingSets =
      (0 until sortedReportingSets.size).mapNotNull { bitPosition ->
        val isSet: Boolean = (unionOnlySet and (1 shl bitPosition)) != 0
        if (isSet) sortedReportingSets[bitPosition] else null
      }

    return WeightedMeasurement(reportingSets, coefficient)
  }

  private fun primitiveRegionsToUnionOnlySetCoefficientMap(
    numReportingSets: Int,
    primitiveRegions: List<Int>
  ): UnionOnlySetCoefficientMap {
    val primitiveRegionsToUnionOnlySetCoefficients: PrimitiveRegionToUnionOnlySetCoefficientMap =
      mutableMapOf()

    for (region in primitiveRegions) {
      // Reuse previous computation
      primitiveRegionCache[numReportingSets]?.also {
        cachedPrimitiveRegionsToUnionOnlySetCoefficients ->
        cachedPrimitiveRegionsToUnionOnlySetCoefficients[region]?.also {
          cachedUnionOnlySetCoefficientMap ->
          primitiveRegionsToUnionOnlySetCoefficients[region] = cachedUnionOnlySetCoefficientMap
        }
      }
      if (primitiveRegionsToUnionOnlySetCoefficients.containsKey(region)) {
        continue
      }

      val setBitPositions = mutableListOf<Int>()
      val unsetBitPositions = mutableListOf<Int>()

      for (bitPosition in 0 until numReportingSets) {
        val isSet: Boolean = (region and (1 shl bitPosition)) != 0
        if (isSet) {
          setBitPositions.add(bitPosition)
        } else {
          unsetBitPositions.add(bitPosition)
        }
      }
      val primeRegionWeight = setBitPositions.size

      // Always starts from -1 unless the primitive region = 2^numReportingSets - 1 = b'11...1'
      var sign = if (primeRegionWeight != numReportingSets) -1 else 1

      val unionOnlySetCoefficients = mutableMapOf<UnionOnlySet, Int>()

      for (size in 1..numReportingSets) {
        // Skips it if the union-only set is too light
        if (size + primeRegionWeight < numReportingSets) {
          continue
        }

        val validUnionOnlySets: MutableList<Int> =
          findValidUnionOnlySets(setBitPositions, unsetBitPositions, size)
        unionOnlySetCoefficients += validUnionOnlySets.associateWith { sign }
        sign = -sign
      }
      primitiveRegionsToUnionOnlySetCoefficients[region] = unionOnlySetCoefficients.toMap()
    }

    // Update the memory cache
    primitiveRegionCache[numReportingSets] =
      (primitiveRegionCache.getOrDefault(numReportingSets, mutableMapOf()) +
          primitiveRegionsToUnionOnlySetCoefficients)
        .toMutableMap()

    val result = mutableMapOf<UnionOnlySet, Int>()
    for ((_, unionOnlySetCoefficients) in primitiveRegionsToUnionOnlySetCoefficients) {
      for ((unionOnlySet, coefficient) in unionOnlySetCoefficients) {
        result[unionOnlySet] = result.getOrDefault(unionOnlySet, 0) + coefficient
      }
    }

    return result.toSortedMap().toMap()
  }

  private fun findValidUnionOnlySets(
    setBitPositions: MutableList<Int>,
    unsetBitPositions: MutableList<Int>,
    size: Int
  ): MutableList<Int> {
    val result = mutableListOf<Int>()

    if (unsetBitPositions.size > size || setBitPositions.size + unsetBitPositions.size < size) {
      return result
    }

    backtracking(size, 0, setBitPositions, unsetBitPositions, result)

    return result
  }

  private fun backtracking(
    size: Int,
    start: Int,
    choices: MutableList<Int>,
    combination: MutableList<Int>,
    result: MutableList<Int>
  ) {
    if (combination.size == size) {
      result.add(combination.sumOf { 1 shl it })
      return
    }

    for (i in start until choices.size) {
      combination.add(choices[i])
      backtracking(size, i + 1, choices, combination, result)
      combination.removeLast()
    }

    return
  }

  /** Gets the primitive regions that cover the set operation expression. */
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

  private fun createReportingSetsMap(reportingSets: List<String>): Map<String, ReportingSet> {
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
private fun SetOperation.toSortedReportingSets(): List<String> {
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
      reportingSets.addAll(node.operation.toSortedReportingSets())
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
