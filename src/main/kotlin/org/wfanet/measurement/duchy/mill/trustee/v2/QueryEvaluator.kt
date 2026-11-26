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

package org.wfanet.measurement.duchy.mill.trustee.v2

import java.nio.ByteBuffer
import java.security.MessageDigest
import org.wfanet.measurement.internal.duchy.Query
import org.wfanet.measurement.internal.duchy.VectorSet
import org.wfanet.measurement.internal.duchy.VidFilter

/**
 * Evaluates TrusTEE v2 [Query]s over base frequency vectors, without noise or suppression.
 *
 * Each [Query] reads as `SELECT reach, frequency, impressions FROM summands WHERE vid_filter GROUP
 * BY grouping_paths`. The vectors are walked in position order in aligned chunks passed to [add],
 * and [cells] returns one [Cell] per query per grouping combination that the subpopulations
 * produce, including combinations no VID reached.
 *
 * An evaluator built by [corrective] evaluates only the cells it is given, reading the vectors of
 * each cell's failed `DataProvider`s as zero.
 *
 * @param queries the queries to evaluate
 * @param dataProviders the resource name of the `DataProvider` that fulfilled each base vector,
 *   indexed by vector index
 * @param subpopulations the subpopulation of each position and its attribute values
 * @param failedDataProviders for a corrective pass, the `DataProvider`s whose vectors read as zero
 *   in each cell to evaluate; `null` for a first pass over every cell
 * @param impressionCap the most impressions one VID adds to a cell, or `null` for no cap
 */
class QueryEvaluator
private constructor(
  queries: List<Query>,
  private val dataProviders: List<String>,
  private val subpopulations: SubpopulationIndex,
  failedDataProviders: Map<CellKey, Set<String>>?,
  private val impressionCap: Int?,
) {
  /** Creates an evaluator for the first pass, which evaluates every cell of [queries]. */
  constructor(
    queries: List<Query>,
    dataProviders: List<String>,
    subpopulations: SubpopulationIndex,
    impressionCap: Int? = null,
  ) : this(queries, dataProviders, subpopulations, null, impressionCap)

  private val vectorCount: Int = dataProviders.size
  private val compiledQueries: List<CompiledQuery>

  init {
    require(queries.map { it.id }.toSet().size == queries.size) { "Query ids must be distinct" }
    compiledQueries = queries.map { CompiledQuery(it, failedDataProviders) }
    if (failedDataProviders != null) {
      val knownCells: Set<CellKey> = compiledQueries.flatMap { it.cellKeys }.toSet()
      for (key in failedDataProviders.keys) {
        require(key in knownCells) { "Cell $key is not a cell of any query" }
      }
    }
  }

  /**
   * Adds the positions in [chunk] to every evaluated cell.
   *
   * @throws IllegalArgumentException if [chunk] does not hold one aligned array per base vector
   */
  fun add(chunk: VectorChunk) {
    require(chunk.counts.size == vectorCount) {
      "Chunk has ${chunk.counts.size} vectors, expected $vectorCount"
    }
    val length = chunk.counts.firstOrNull()?.size ?: 0
    require(chunk.counts.all { it.size == length }) { "Chunk vectors must have the same length" }
    for (offset in 0 until length) {
      val position = chunk.startPosition + offset
      val subpopulation = subpopulations.subpopulationOf(position)
      for (query in compiledQueries) {
        query.add(chunk.counts, offset, position, subpopulation)
      }
    }
  }

  /** Returns the evaluated cells so far, in query order, then grouping-combination order. */
  fun cells(): List<Cell> = compiledQueries.flatMap { it.cells() }

  /** A [Query] with its per-subpopulation filter and grouping resolved. */
  private inner class CompiledQuery(
    private val query: Query,
    failedDataProviders: Map<CellKey, Set<String>>?,
  ) {
    private val summands: IntArray = query.summands.toIndexes()
    private val reachClauses: List<CompiledReachClause> =
      query.vidFilter.reachClausesList.map { CompiledReachClause(it) }
    private val maxFrequency: Int = query.maxFrequency

    /** Whether each subpopulation passes [VidFilter.getAttributesMap]. */
    private val passesAttributes: BooleanArray
    /** The grouping combination of each subpopulation, as an index into [groups]. */
    private val groupOfSubpopulation: IntArray
    private val groups: List<List<String?>>
    /** The number of VIDs in each group that pass [VidFilter.getAttributesMap]. */
    private val populationSizes: LongArray
    /** Whether each group is evaluated in this pass. */
    private val evaluated: BooleanArray
    /** For each group, the vectors read as zero in it, or `null` if none are. */
    private val zeroedVectors: Array<BooleanArray?>
    private val accumulators: Array<CellAccumulator>
    /** Scratch buffer for folding a position and its summand counts into a fingerprint. */
    private val fingerprintBuffer: ByteBuffer =
      ByteBuffer.allocate(Int.SIZE_BYTES * (1 + summands.size))

    val cellKeys: List<CellKey>
      get() = groups.map { CellKey(query.id, it) }

    init {
      require(maxFrequency >= 1) { "Query ${query.id} has max_frequency $maxFrequency" }
      require(impressionCap == null || impressionCap >= 1) {
        "Invalid impression cap $impressionCap"
      }
      val attributeFilter: Map<String, String> =
        query.vidFilter.attributesMap.mapValues { (path, value) ->
          require(value.hasEnumValue()) { "Query ${query.id} attribute $path has no value" }
          value.enumValue
        }
      val subpopulationCount = subpopulations.subpopulationCount
      passesAttributes =
        BooleanArray(subpopulationCount) { subpopulation ->
          attributeFilter.all { (path, symbol) ->
            subpopulations.attributeValue(subpopulation, path) == symbol
          }
        }
      val groupIndexes = mutableMapOf<List<String?>, Int>()
      groupOfSubpopulation =
        IntArray(subpopulationCount) { subpopulation ->
          val combination =
            query.groupingPathsList.map { path ->
              subpopulations.attributeValue(subpopulation, path)
            }
          groupIndexes.getOrPut(combination) { groupIndexes.size }
        }
      groups = groupIndexes.entries.sortedBy { it.value }.map { it.key }
      populationSizes = LongArray(groups.size)
      for (subpopulation in 0 until subpopulationCount) {
        if (passesAttributes[subpopulation]) {
          populationSizes[groupOfSubpopulation[subpopulation]] +=
            subpopulations.populationSize(subpopulation)
        }
      }
      evaluated =
        BooleanArray(groups.size) { group ->
          failedDataProviders == null || CellKey(query.id, groups[group]) in failedDataProviders
        }
      zeroedVectors =
        Array(groups.size) { group ->
          val failed: Set<String> =
            failedDataProviders?.get(CellKey(query.id, groups[group])) ?: emptySet()
          if (failed.isEmpty()) null else BooleanArray(vectorCount) { dataProviders[it] in failed }
        }
      accumulators = Array(groups.size) { CellAccumulator(maxFrequency) }
    }

    fun add(counts: List<IntArray>, offset: Int, position: Int, subpopulation: Int) {
      if (!passesAttributes[subpopulation]) return
      val group = groupOfSubpopulation[subpopulation]
      if (!evaluated[group]) return
      val zeroed: BooleanArray? = zeroedVectors[group]
      if (reachClauses.isNotEmpty() && reachClauses.none { it.holds(counts, offset, zeroed) }) {
        return
      }
      var frequency = 0L
      for (vector in summands) {
        frequency += countAt(counts, vector, offset, zeroed)
      }
      if (frequency == 0L) return
      val accumulator = accumulators[group]
      accumulator.addFrequency(frequency, impressionCap)

      fingerprintBuffer.clear()
      fingerprintBuffer.putInt(position)
      for (vector in summands) {
        fingerprintBuffer.putInt(countAt(counts, vector, offset, zeroed))
      }
      accumulator.fingerprint.update(fingerprintBuffer.array(), 0, fingerprintBuffer.position())

      // A VID counts once toward each DataProvider's slice, however many of that DataProvider's
      // vectors reached it.
      val reachedDataProviders = mutableSetOf<String>()
      for (vector in summands) {
        if (countAt(counts, vector, offset, zeroed) > 0) {
          reachedDataProviders.add(dataProviders[vector])
        }
      }
      for (dataProvider in reachedDataProviders) {
        accumulator.addSliceReach(dataProvider)
      }
    }

    fun cells(): List<Cell> =
      groups.indices
        .filter { evaluated[it] }
        .map { group ->
          val accumulator = accumulators[group]
          Cell(
            queryId = query.id,
            groupingPaths = query.groupingPathsList,
            attributeValues = groups[group],
            frequencyHistogram = accumulator.histogram.copyOf(),
            impressions = accumulator.impressions,
            populationSize = populationSizes[group],
            sliceReach = accumulator.sliceReach.toMap(),
            summandDataProviders = summands.map { dataProviders[it] }.toSet(),
            fingerprint = accumulator.fingerprintDigest(),
          )
        }
  }

  /** A [VidFilter.ReachClause] over vector indexes. */
  private inner class CompiledReachClause(clause: VidFilter.ReachClause) {
    private val reachedSets: List<IntArray> = clause.reachedSetsList.map { it.toIndexes() }
    private val unreachedSet: IntArray = clause.unreachedSet.toIndexes()

    fun holds(counts: List<IntArray>, offset: Int, zeroed: BooleanArray?): Boolean =
      reachedSets.all { set -> set.any { countAt(counts, it, offset, zeroed) > 0 } } &&
        unreachedSet.none { countAt(counts, it, offset, zeroed) > 0 }
  }

  /** Returns the count of [vector] at [offset], or zero if [zeroed] marks the vector. */
  private fun countAt(
    counts: List<IntArray>,
    vector: Int,
    offset: Int,
    zeroed: BooleanArray?,
  ): Int = if (zeroed != null && zeroed[vector]) 0 else counts[vector][offset]

  private fun VectorSet.toIndexes(): IntArray {
    val indexes = baseVectorsList.toIntArray()
    require(indexes.toSet().size == indexes.size) { "VectorSet $baseVectorsList repeats a vector" }
    for (index in indexes) {
      require(index in 0 until vectorCount) { "VectorSet names vector $index, which is not held" }
    }
    return indexes
  }

  /** Per-cell running totals. */
  private class CellAccumulator(maxFrequency: Int) {
    /** Bin `k - 1` counts VIDs with frequency `k`; the last bin also counts higher frequencies. */
    val histogram = LongArray(maxFrequency)
    var impressions = 0L
      private set

    val sliceReach = mutableMapOf<String, Long>()

    /** Running SHA-256 over each reached VID's position and summand counts, in position order. */
    val fingerprint: MessageDigest = MessageDigest.getInstance(FINGERPRINT_ALGORITHM)

    /** Adds a VID reached [frequency] times, contributing at most [impressionCap] impressions. */
    fun addFrequency(frequency: Long, impressionCap: Int?) {
      histogram[(minOf(frequency, histogram.size.toLong()) - 1).toInt()]++
      impressions +=
        if (impressionCap == null) frequency else minOf(frequency, impressionCap.toLong())
    }

    fun addSliceReach(dataProvider: String) {
      sliceReach[dataProvider] = (sliceReach[dataProvider] ?: 0L) + 1L
    }

    /** Returns the fingerprint so far, leaving the running digest unchanged. */
    fun fingerprintDigest(): ByteArray = (fingerprint.clone() as MessageDigest).digest()
  }

  /**
   * One aligned chunk of every base vector.
   *
   * @property startPosition the frequency vector position of element 0 of each array
   * @property counts the counts, one array per base vector, indexed by vector index
   */
  class VectorChunk(val startPosition: Int, val counts: List<IntArray>)

  /**
   * Identifies a cell: a query and one of its grouping combinations.
   *
   * @property attributeValues the enum symbol of each grouping path, or `null` where unset
   */
  data class CellKey(val queryId: String, val attributeValues: List<String?>)

  /**
   * The unnoised figures for one query and grouping combination.
   *
   * @property attributeValues the enum symbol of each of [groupingPaths] for this cell, or `null`
   *   where the subpopulation does not set it
   * @property frequencyHistogram bin `k - 1` counts VIDs reached `k` times, the last bin folding in
   *   higher frequencies; reach is its sum
   * @property impressions the sum of the summed counts over reached VIDs, each capped at the
   *   evaluator's impression cap
   * @property populationSize the number of VIDs in this cell's subpopulations that pass the query's
   *   attribute filter, across the whole population
   * @property sliceReach the VIDs in this cell reached by each `DataProvider`'s summand vectors
   * @property summandDataProviders the `DataProvider`s with a vector among the query's summands,
   *   whose slices the suppression test covers
   * @property fingerprint SHA-256 over each reached VID's position and summand counts, in position
   *   order, which seeds the cell's noise
   */
  class Cell(
    val queryId: String,
    val groupingPaths: List<String>,
    val attributeValues: List<String?>,
    val frequencyHistogram: LongArray,
    val impressions: Long,
    val populationSize: Long,
    val sliceReach: Map<String, Long>,
    val summandDataProviders: Set<String>,
    val fingerprint: ByteArray,
  ) {
    val reach: Long
      get() = frequencyHistogram.sum()

    val key: CellKey
      get() = CellKey(queryId, attributeValues)
  }

  companion object {
    private const val FINGERPRINT_ALGORITHM = "SHA-256"

    /**
     * Creates an evaluator for the corrective pass, which evaluates only the cells in
     * [failedDataProviders] and reads the vectors of each cell's failed `DataProvider`s as zero, in
     * its summands and reach clauses alike.
     *
     * @throws IllegalArgumentException if a key of [failedDataProviders] is not a cell of [queries]
     */
    fun corrective(
      queries: List<Query>,
      dataProviders: List<String>,
      subpopulations: SubpopulationIndex,
      failedDataProviders: Map<CellKey, Set<String>>,
      impressionCap: Int? = null,
    ): QueryEvaluator =
      QueryEvaluator(queries, dataProviders, subpopulations, failedDataProviders, impressionCap)
  }
}
