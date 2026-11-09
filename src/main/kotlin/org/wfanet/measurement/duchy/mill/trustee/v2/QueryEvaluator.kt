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

import org.wfanet.measurement.internal.duchy.Query
import org.wfanet.measurement.internal.duchy.VectorSet
import org.wfanet.measurement.internal.duchy.VidFilter

/**
 * Evaluates TrusTEE v2 [Query]s over base frequency vectors, without noise or suppression.
 *
 * Each [Query] reads as `SELECT reach, frequency, impressions FROM summands WHERE vid_filter GROUP
 * BY grouping_paths`. The vectors are walked in position order in aligned chunks passed to [add],
 * and [cells] returns one [Cell] per query per grouping combination that some position reached.
 *
 * @param queries the queries to evaluate
 * @param dataProviders the resource name of the `DataProvider` that fulfilled each base vector,
 *   indexed by vector index
 * @param subpopulations the subpopulation of each position and its attribute values
 */
class QueryEvaluator(
  queries: List<Query>,
  private val dataProviders: List<String>,
  private val subpopulations: SubpopulationIndex,
) {
  private val vectorCount: Int = dataProviders.size
  private val compiledQueries: List<CompiledQuery>

  init {
    require(queries.map { it.id }.toSet().size == queries.size) { "Query ids must be distinct" }
    compiledQueries = queries.map { CompiledQuery(it) }
  }

  /**
   * Adds the positions in [chunk] to every query's cells.
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
      val subpopulation = subpopulations.subpopulationOf(chunk.startPosition + offset)
      for (query in compiledQueries) {
        query.add(chunk.counts, offset, subpopulation)
      }
    }
  }

  /** Returns the cells accumulated so far, in query order, then grouping-combination order. */
  fun cells(): List<Cell> = compiledQueries.flatMap { it.cells() }

  /** A [Query] with its per-subpopulation filter and grouping resolved. */
  private inner class CompiledQuery(private val query: Query) {
    private val summands: IntArray = query.summands.toIndexes()
    private val reachClauses: List<CompiledReachClause> =
      query.vidFilter.reachClausesList.map { CompiledReachClause(it) }
    private val maxFrequency: Int = query.maxFrequency

    /** Whether each subpopulation passes [VidFilter.getAttributesMap]. */
    private val passesAttributes: BooleanArray
    /** The grouping combination of each subpopulation, as an index into [groups]. */
    private val groupOfSubpopulation: IntArray
    private val groups: List<List<String?>>
    private val accumulators: Array<CellAccumulator>

    init {
      require(maxFrequency >= 1) { "Query ${query.id} has max_frequency $maxFrequency" }
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
      accumulators = Array(groups.size) { CellAccumulator(maxFrequency) }
    }

    fun add(counts: List<IntArray>, offset: Int, subpopulation: Int) {
      if (!passesAttributes[subpopulation]) return
      if (reachClauses.isNotEmpty() && reachClauses.none { it.holds(counts, offset) }) return
      var frequency = 0L
      for (vector in summands) {
        frequency += counts[vector][offset]
      }
      if (frequency == 0L) return
      val accumulator = accumulators[groupOfSubpopulation[subpopulation]]
      accumulator.addFrequency(frequency)
      // A VID counts once toward each DataProvider's slice, however many of that DataProvider's
      // vectors reached it.
      val reachedDataProviders = mutableSetOf<String>()
      for (vector in summands) {
        if (counts[vector][offset] > 0) reachedDataProviders.add(dataProviders[vector])
      }
      for (dataProvider in reachedDataProviders) {
        accumulator.addSliceReach(dataProvider)
      }
    }

    fun cells(): List<Cell> =
      groups.indices
        .filter { accumulators[it].reach > 0 }
        .map { group ->
          val accumulator = accumulators[group]
          Cell(
            queryId = query.id,
            groupingPaths = query.groupingPathsList,
            attributeValues = groups[group],
            frequencyHistogram = accumulator.histogram.copyOf(),
            impressions = accumulator.impressions,
            sliceReach = accumulator.sliceReach.toMap(),
          )
        }
  }

  /** A [VidFilter.ReachClause] over vector indexes. */
  private inner class CompiledReachClause(clause: VidFilter.ReachClause) {
    private val reachedSets: List<IntArray> = clause.reachedSetsList.map { it.toIndexes() }
    private val unreachedSet: IntArray = clause.unreachedSet.toIndexes()

    fun holds(counts: List<IntArray>, offset: Int): Boolean =
      reachedSets.all { set -> set.any { counts[it][offset] > 0 } } &&
        unreachedSet.none { counts[it][offset] > 0 }
  }

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

    var reach = 0L
      private set

    val sliceReach = mutableMapOf<String, Long>()

    fun addFrequency(frequency: Long) {
      histogram[(minOf(frequency, histogram.size.toLong()) - 1).toInt()]++
      impressions += frequency
      reach++
    }

    fun addSliceReach(dataProvider: String) {
      sliceReach[dataProvider] = (sliceReach[dataProvider] ?: 0L) + 1L
    }
  }

  /**
   * One aligned chunk of every base vector.
   *
   * @property startPosition the frequency vector position of element 0 of each array
   * @property counts the counts, one array per base vector, indexed by vector index
   */
  class VectorChunk(val startPosition: Int, val counts: List<IntArray>)

  /**
   * The unnoised figures for one query and grouping combination.
   *
   * @property attributeValues the enum symbol of each of [groupingPaths] for this cell, or `null`
   *   where the subpopulation does not set it
   * @property frequencyHistogram bin `k - 1` counts VIDs reached `k` times, the last bin folding in
   *   higher frequencies; reach is its sum
   * @property impressions the sum of the summed counts over reached VIDs
   * @property sliceReach the VIDs in this cell reached by each `DataProvider`'s summand vectors
   */
  class Cell(
    val queryId: String,
    val groupingPaths: List<String>,
    val attributeValues: List<String?>,
    val frequencyHistogram: LongArray,
    val impressions: Long,
    val sliceReach: Map<String, Long>,
  ) {
    val reach: Long
      get() = frequencyHistogram.sum()
  }

  companion object {
    /**
     * Requires that [vectorIndexes], the indexes of the base vectors held, cover `0 until
     * vectorIndexes.size` exactly once.
     *
     * @throws IllegalArgumentException otherwise
     */
    fun requireIndexesCoverRange(vectorIndexes: Collection<Int>) {
      require(vectorIndexes.sorted() == (0 until vectorIndexes.size).toList()) {
        "Vector indexes $vectorIndexes do not cover 0 until ${vectorIndexes.size} exactly once"
      }
    }
  }
}
