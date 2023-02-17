// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.reporting.service.api.v1alpha

import kotlin.math.pow
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.launch
import org.wfanet.measurement.reporting.v1alpha.Metric.SetOperation

/**
 * A primitive region of a Venn diagram is the intersection of a set of reporting sets, and it is
 * represented by a bit representation of an integer. Only the reporting sets with IDs equal to the
 * bit positions of set bits constitute the primitive region. Ex: Given a Venn Diagram of 3
 * reporting sets (rs0, rs1, rs2), a primitive region with an integer value equal to 3 has the bit
 * representation b’011’. This means this primitive region is only covered by the intersection of
 * rs0 and rs1 and not covered by rs2 (the order of the bit positions is from right to left). In
 * other words, rs0 INTERSECT rs1 INTERSECT COMPLEMENT(rs2). Note that primitive regions are
 * disjoint.
 */
private typealias PrimitiveRegion = ULong

/**
 * A union set is the union of a set of reporting sets, and it is represented by a bit
 * representation of an integer. Only the reporting sets with IDs equal to the bit positions of set
 * bits constitute the union set. Given a Venn Diagram of 3 reporting sets (rs0, rs1, rs2), a
 * union-set with an integer value equal to 3 has the bit representation b’011’. This means this
 * union-set is only covered by the union of rs0 and rs1 (the order of the bit positions is from
 * right to left).
 */
private typealias UnionSet = ULong

private typealias NumberReportingSets = Int

/** A mapping from a [UnionSet] to its coefficient in the Venn diagram region decomposition. */
private typealias UnionSetCoefficientMap = Map<UnionSet, Int>

/**
 * A mapping for cardinality computation from a [PrimitiveRegion] to its decomposition in terms of
 * union-sets represented by [UnionSetCoefficientMap]. Take a case of 3 reporting sets (rs0, rs1,
 * rs2) as an example. A primitive region with its value equal to 3 (b'011') means rs0 INTERSECT rs1
 * INTERSECT COMPLEMENT(rs2). The decomposition of the cardinality of the region =
 * PrimitiveRegionToUnionSetCoefficientMap\[region\] = {4: -1, 5: 1, 6: 1, 7: -1}, i.e. |union-set5|
 * + |union-set6| - |union-set4| - |union-set7|.
 */
private typealias PrimitiveRegionToUnionSetCoefficientMap =
  MutableMap<PrimitiveRegion, UnionSetCoefficientMap>

/**
 * A memory cache that stores the Venn diagram region cardinality decompositions for different
 * numbers of reporting sets.
 */
private typealias PrimitiveRegionCache =
  MutableMap<NumberReportingSets, PrimitiveRegionToUnionSetCoefficientMap>

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

class SetOperationCompiler {

  private var primitiveRegionCache: PrimitiveRegionCache = mutableMapOf()

  // For unit test only.
  fun getPrimitiveRegionCache():
    Map<NumberReportingSets, Map<PrimitiveRegion, UnionSetCoefficientMap>> {
    return primitiveRegionCache.mapValues { it.value.toMap() }.toMap()
  }

  /**
   * Compiles a set operation to a list of [WeightedMeasurement]s which will be used for the
   * cardinality computation. For example, given a set = primitiveRegion1 UNION primitiveRegion2,
   * Count(set) = Count(primitiveRegion1) + Count(primitiveRegion2) = Count(unionSet1) -
   * Count(unionSet2) + Count(unionSet3) - Count(unionSet2) = Count(unionSet1) + Count(unionSet3) -
   * 2 * Count(unionSet2).
   */
  suspend fun compileSetOperation(setOperation: SetOperation): List<WeightedMeasurement> {
    val reportingSetNames = mutableSetOf<String>()
    setOperation.storeReportingSetNames(reportingSetNames)

    // Sorts the list in alphabetical order to make sure the IDs are consistent for the same run.
    val sortedReportingSetNames = reportingSetNames.sortedBy { it }
    val reportingSetsMap = createReportingSetsMap(sortedReportingSetNames)
    val numReportingSets = reportingSetsMap.size

    val setOperationExpression = setOperation.toSetOperationExpression(reportingSetsMap)

    // Step 1 - Gets the primitive regions that form the set operation
    val primitiveRegions =
      setOperationExpressionToPrimitiveRegions(numReportingSets, setOperationExpression)

    // Step 2 - Converts a set of primitive regions to a map of union-set to its coefficients for
    // cardinality computation.
    val unionSetCoefficientMap =
      convertPrimitiveRegionsToUnionSetCoefficientMap(numReportingSets, primitiveRegions)

    return unionSetCoefficientMap.map { (unionSet, coefficient) ->
      convertUnionSetToWeightedMeasurements(unionSet, coefficient, sortedReportingSetNames)
    }
  }

  /** Converts unionSetCoefficientMap to WeightedMeasurements. */
  private fun convertUnionSetToWeightedMeasurements(
    unionSet: UnionSet,
    coefficient: Int,
    sortedReportingSetNames: List<String>
  ): WeightedMeasurement {
    // Find the reporting sets in the union-set.
    val reportingSetNames =
      (sortedReportingSetNames.indices).mapNotNull { bitPosition ->
        if (isBitSet(unionSet, bitPosition)) sortedReportingSetNames[bitPosition] else null
      }

    return WeightedMeasurement(reportingSetNames, coefficient)
  }

  /**
   * Converts a set of primitive regions to a map of union-set to its coefficients for cardinality
   * computation
   */
  private suspend fun convertPrimitiveRegionsToUnionSetCoefficientMap(
    numReportingSets: Int,
    primitiveRegions: Set<PrimitiveRegion>
  ): UnionSetCoefficientMap {

    val primitiveRegionsToUnionSetCoefficients: PrimitiveRegionToUnionSetCoefficientMap =
      mutableMapOf()

    coroutineScope {
      for (region in primitiveRegions) {
        // Reuse previous computation if available
        if (
          reusePreviousComputation(numReportingSets, region, primitiveRegionsToUnionSetCoefficients)
        ) {
          continue
        }

        launch {
          convertSinglePrimitiveRegionToUnionSetCoefficientMap(
            numReportingSets,
            region,
            primitiveRegionsToUnionSetCoefficients
          )
        }
      }
    }

    // Updates the memory cache with new computation result.
    primitiveRegionCache.getOrPut(numReportingSets, ::mutableMapOf) +=
      primitiveRegionsToUnionSetCoefficients

    return aggregateCoefficientsByUnionSets(primitiveRegionsToUnionSetCoefficients)
  }

  /** Aggregates the coefficients by union-sets. */
  private fun aggregateCoefficientsByUnionSets(
    primitiveRegionsToUnionSetCoefficients: PrimitiveRegionToUnionSetCoefficientMap
  ): UnionSetCoefficientMap {
    val aggregatedResult = mutableMapOf<UnionSet, Int>()
    for ((_, unionSetCoefficients) in primitiveRegionsToUnionSetCoefficients) {
      for ((unionSet, coefficient) in unionSetCoefficients) {
        aggregatedResult[unionSet] = aggregatedResult.getOrDefault(unionSet, 0) + coefficient

        // Remove the entry if its coefficient is zero.
        if (aggregatedResult[unionSet] == 0) {
          aggregatedResult.remove(unionSet)
        }
      }
    }
    // Sort the aggregatedResult to make sure the result is consistent every time.
    return aggregatedResult.toSortedMap().toMap()
  }

  /**
   * Converts a single primitive region to a map of union-set to its coefficients, where the
   * cardinality of the input primitive region is equal to the linear combination of the
   * cardinalities of the union-sets with the coefficients.
   *
   * The algorithm is based on the observation on the linear transformation matrix from primitive
   * regions to union-sets. Ex:
   * ```
   *           b'01'    b'10'    b'11'
   *     A      0        -1       1
   *     B     -1         0       1
   *   A U B    1         1      -1
   * ```
   */
  private fun convertSinglePrimitiveRegionToUnionSetCoefficientMap(
    numReportingSets: Int,
    region: PrimitiveRegion,
    primitiveRegionsToUnionSetCoefficients: PrimitiveRegionToUnionSetCoefficientMap
  ) {
    // For a given region, we first find which bit positions are set and which are not.
    val setBitPositions = mutableListOf<Int>()
    val unsetBitPositions = mutableListOf<Int>()

    for (bitPosition in 0 until numReportingSets) {
      if (isBitSet(region, bitPosition)) {
        setBitPositions.add(bitPosition)
      } else {
        unsetBitPositions.add(bitPosition)
      }
    }
    val primitiveRegionWeight = setBitPositions.size

    // Always starts from -1 unless the primitive region = (2^numReportingSets - 1) = b'11...1'.
    val baseSign = if (primitiveRegionWeight != numReportingSets) -1 else 1
    var count = 0

    val unionSetCoefficients = mutableMapOf<UnionSet, Int>()

    for (size in 1..numReportingSets) {
      // Skips it if the union-only set is too light
      if (size + primitiveRegionWeight < numReportingSets) {
        continue
      }

      // Instead of flipping the sign at the end of the loop, this could avoid the race condition.
      count++
      val sign = if (count % 2 == 1) baseSign else -baseSign

      val composingUnionSets = findComposingUnionSets(setBitPositions, unsetBitPositions, size)
      unionSetCoefficients += composingUnionSets.associateWith { sign }
    }

    if (unionSetCoefficients.isNotEmpty()) {
      primitiveRegionsToUnionSetCoefficients[region] = unionSetCoefficients.toMap()
    }
  }

  /** Reuses previous result in the memory cache if there is any. */
  private fun reusePreviousComputation(
    numReportingSets: Int,
    region: PrimitiveRegion,
    primitiveRegionsToUnionSetCoefficients: PrimitiveRegionToUnionSetCoefficientMap,
  ): Boolean {
    // If the compiler has already run the case where the number of reporting sets equal to
    // `numReportingSets`.
    primitiveRegionCache[numReportingSets]?.also { cachedPrimitiveRegionsToUnionSetCoefficients ->
      // If the compiler has calculated this region before.
      cachedPrimitiveRegionsToUnionSetCoefficients[region]?.also { cachedUnionSetCoefficientMap ->
        primitiveRegionsToUnionSetCoefficients[region] = cachedUnionSetCoefficientMap
      }
    }
    return primitiveRegionsToUnionSetCoefficients.containsKey(region)
  }

  /**
   * Finds the union-sets which will be part of the combination to form the target region.
   * Essentially, given the size of a combination, we are finding all the combinations of the bit
   * positions where at least unset bit positions are selected.
   */
  private fun findComposingUnionSets(
    setBitPositions: MutableList<Int>,
    unsetBitPositions: MutableList<Int>,
    size: Int
  ): MutableList<UnionSet> {
    val composingUnionSets = mutableListOf<UnionSet>()

    // If the size is not large enough to at least contain all unset bit positions or the size is
    // too large to fill, return empty result.
    if (unsetBitPositions.size > size || setBitPositions.size + unsetBitPositions.size < size) {
      return composingUnionSets
    }

    findValidUnionSets(size, 0, setBitPositions, unsetBitPositions, composingUnionSets)

    return composingUnionSets
  }

  /** Finds the valid combinations as [UnionSet]s using backtracking. */
  private fun findValidUnionSets(
    size: Int,
    start: Int,
    choices: MutableList<Int>,
    combination: MutableList<Int>,
    result: MutableList<UnionSet>
  ) {
    if (combination.size == size) {
      result.add(combination.sumOf { 1.toUnionSet() shl it })
      return
    }

    for (i in start until choices.size) {
      combination.add(choices[i])
      findValidUnionSets(size, i + 1, choices, combination, result)
      combination.removeLast()
    }

    return
  }

  /** Gets the set of the primitive regions that form the set from the set operation expression. */
  private fun setOperationExpressionToPrimitiveRegions(
    numReportingSets: Int,
    setOperationExpression: SetOperationExpression
  ): Set<PrimitiveRegion> {
    val allPrimitiveRegionSetsList = buildAllPrimitiveRegions(numReportingSets)
    return setOperationExpression.decompose(allPrimitiveRegionSetsList)
  }
}

/**
 * Decomposes the set operation expression to a set of primitive regions by calculating the set
 * operation between each two operands.
 */
private fun SetOperationExpression.decompose(
  allPrimitiveRegionSetsList: List<Set<PrimitiveRegion>>
): Set<PrimitiveRegion> {
  val source = this
  val lhsPrimitiveRegions = source.lhs.decompose(allPrimitiveRegionSetsList)
  val rhsPrimitiveRegions = source.rhs.decompose(allPrimitiveRegionSetsList)
  return calculateBinarySetOperation(lhsPrimitiveRegions, rhsPrimitiveRegions, source.setOperator)
}

/** Decomposes the operand to a set of primitive regions. */
private fun Operand?.decompose(
  allPrimitiveRegionSetsList: List<Set<PrimitiveRegion>>
): Set<PrimitiveRegion> {
  return when (val operand = this) {
    is SetOperationExpression -> {
      operand.decompose(allPrimitiveRegionSetsList)
    }
    is ReportingSet -> {
      allPrimitiveRegionSetsList[operand.id]
    }
    else -> setOf()
  }
}

/** Calculates the binary set operation. */
private fun calculateBinarySetOperation(
  lhs: Set<PrimitiveRegion>,
  rhs: Set<PrimitiveRegion>,
  operator: Operator
): Set<PrimitiveRegion> {
  return when (operator) {
    Operator.UNION -> lhs union rhs
    Operator.INTERSECT -> lhs intersect rhs
    Operator.DIFFERENCE -> lhs subtract rhs
  }
}

/**
 * Builds a list of primitive regions where the index represents the reporting set ID and the
 * element is the set of primitive regions which forms the corresponding reporting set. For example,
 * if reportingSetId = 1, then allPrimitiveRegionSetsList\[reportingSetId\] = setOf(1(=b’001’),
 * 3(=b’011’), 5(=b’101’), 7(=b’111’)).
 */
private fun buildAllPrimitiveRegions(numReportingSets: Int): List<Set<PrimitiveRegion>> {
  val numPrimitiveRegions = 2.0.pow(numReportingSets).toPrimitiveRegion() - 1.toPrimitiveRegion()
  val allPrimitiveRegionSetsList: List<MutableSet<PrimitiveRegion>> =
    List(numReportingSets) { mutableSetOf() }

  // A region is in the set of reportingSet when its bit at bit position == reportingSetId is set.
  for (region in 1.toPrimitiveRegion()..numPrimitiveRegions) {
    for (reportingSetId in 0 until numReportingSets) {
      if (isBitSet(region, reportingSetId)) {
        allPrimitiveRegionSetsList[reportingSetId].add(region)
      }
    }
  }

  return allPrimitiveRegionSetsList.map(MutableSet<PrimitiveRegion>::toSet)
}

/** Converts a [Int] to a [PrimitiveRegion] */
private fun Int.toPrimitiveRegion(): PrimitiveRegion {
  return this.toULong()
}

/** Converts a [Double] to a [PrimitiveRegion] */
private fun Double.toPrimitiveRegion(): PrimitiveRegion {
  return this.toULong()
}

/** Converts a [Int] to a [UnionSet] */
private fun Int.toUnionSet(): UnionSet {
  return this.toULong()
}

/** Checks if the bit at `bitPosition` of a number is set or not. */
fun isBitSet(number: ULong, bitPosition: Int): Boolean {
  return (number and (1UL shl bitPosition)) != 0UL
}

/** Creates a map of resource names of reporting sets to [ReportingSet]s. */
private fun createReportingSetsMap(
  sortedReportingSetNames: List<String>
): Map<String, ReportingSet> {
  val reportingSetsMap: MutableMap<String, ReportingSet> = mutableMapOf()
  for ((id, reportingSetName) in sortedReportingSetNames.withIndex()) {
    reportingSetsMap[reportingSetName] = ReportingSet(id, reportingSetName)
  }
  return reportingSetsMap.toMap()
}

/** Gets all resource names of the reporting sets used in this [SetOperation]. */
private fun SetOperation.storeReportingSetNames(reportingSetNames: MutableSet<String>) {
  val root = this
  if (!root.hasLhs()) {
    throw IllegalArgumentException("lhs in SetOperation must be set.")
  }
  if (!root.lhs.hasReportingSet() && !root.lhs.hasOperation()) {
    throw IllegalArgumentException("Operand type of lhs in SetOperation must be set.")
  }

  root.lhs.storeReportingSetNames(reportingSetNames)
  root.rhs.storeReportingSetNames(reportingSetNames)
}

/** Gets all resource names of the reporting sets used in this [SetOperation.Operand]. */
private fun SetOperation.Operand.storeReportingSetNames(
  reportingSetNames: MutableSet<String>,
) {
  val node = this
  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  when (node.operandCase) {
    // Leaf node
    SetOperation.Operand.OperandCase.REPORTING_SET -> reportingSetNames.add(node.reportingSet)
    SetOperation.Operand.OperandCase.OPERATION -> {
      node.operation.storeReportingSetNames(reportingSetNames)
    }
    // Empty node. No further action.
    SetOperation.Operand.OperandCase.OPERAND_NOT_SET -> return
  }
}

/** Converts a public [SetOperation] to a [SetOperationExpression]. */
private fun SetOperation.toSetOperationExpression(
  reportingSetsMap: Map<String, ReportingSet>
): SetOperationExpression {
  val root = this

  if (!root.hasLhs()) {
    throw IllegalArgumentException("lhs in SetOperation must be set.")
  }

  val lhs =
    root.lhs.toOperand(reportingSetsMap)
      ?: throw IllegalArgumentException("Operand type of lhs in SetOperation must be set.")

  val rhs = root.rhs.toOperand(reportingSetsMap)

  return SetOperationExpression(root.type.toOperator(), lhs, rhs)
}

/** Converts a public [SetOperation.Operand] to a nullable [Operand]. */
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

/** Converts a public [SetOperation.Type] to a [Operator]. */
private fun SetOperation.Type.toOperator(): Operator {
  val source = this

  @Suppress("WHEN_ENUM_CAN_BE_NULL_IN_JAVA") // Proto enum fields are never null.
  return when (source) {
    SetOperation.Type.TYPE_UNSPECIFIED ->
      throw IllegalArgumentException("Set operator type is not specified.")
    SetOperation.Type.UNION -> Operator.UNION
    SetOperation.Type.DIFFERENCE -> Operator.DIFFERENCE
    SetOperation.Type.INTERSECTION -> Operator.INTERSECT
    SetOperation.Type.UNRECOGNIZED ->
      throw IllegalArgumentException("Unrecognized Set operator type.")
  }
}
