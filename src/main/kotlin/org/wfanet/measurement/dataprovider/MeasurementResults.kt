/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider

import com.google.protobuf.TypeRegistry
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold
import kotlinx.coroutines.flow.toSet
import org.projectnessie.cel.Program
import org.wfanet.measurement.populationdataprovider.PopulationInfo
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller

/** Utilities for computing Measurement results. */
object MeasurementResults {
  data class ReachAndFrequency(val reach: Int, val relativeFrequencyDistribution: Map<Int, Double>)

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  suspend fun computeReachAndFrequency(
    filteredVids: Flow<Long>,
    maxFrequency: Int,
  ): ReachAndFrequency {
    // Count occurrences of each VID using fold operation on the flow
    val eventsPerVid: Map<Long, Int> =
      filteredVids.fold(mutableMapOf()) { acc, vid ->
        acc[vid] = acc.getOrDefault(vid, 0) + 1
        acc
      }

    return computeReachAndFrequency(eventsPerVid, maxFrequency)
  }

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  fun computeReachAndFrequency(filteredVids: Sequence<Long>, maxFrequency: Int): ReachAndFrequency {
    val eventsPerVid: Map<Long, Int> = filteredVids.groupingBy { it }.eachCount()
    return computeReachAndFrequency(eventsPerVid, maxFrequency)
  }

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  fun computeReachAndFrequency(filteredVids: Iterable<Long>, maxFrequency: Int): ReachAndFrequency =
    computeReachAndFrequency(filteredVids.asSequence(), maxFrequency)

  private fun computeReachAndFrequency(
    eventsPerVid: Map<Long, Int>,
    maxFrequency: Int,
  ): ReachAndFrequency {
    val reach: Int = eventsPerVid.keys.size

    // If the filtered VIDs is empty, set the distribution with all 0s up to maxFrequency.
    if (reach == 0) {
      return ReachAndFrequency(reach, (1..maxFrequency).associateWith { 0.0 })
    }

    // Build frequency histogram as a 0-based array.
    val frequencyArray = IntArray(maxFrequency)
    for (count in eventsPerVid.values) {
      val bucket = count.coerceAtMost(maxFrequency)
      frequencyArray[bucket - 1]++
    }

    val frequencyDistribution: Map<Int, Double> =
      frequencyArray.withIndex().associateBy({ it.index + 1 }, { it.value.toDouble() / reach })
    return ReachAndFrequency(reach, frequencyDistribution)
  }

  /** Computes reach using the "deterministic count distinct" methodology. */
  suspend fun computeReach(filteredVids: Flow<Long>): Int {
    val distinctVids: Set<Long> = filteredVids.toSet()
    return distinctVids.size
  }

  /** Computes reach using the "deterministic count distinct" methodology. */
  fun computeReach(filteredVids: Sequence<Long>): Int {
    return filteredVids.toSet().size
  }

  /** Computes reach using the "deterministic count distinct" methodology. */
  fun computeReach(filteredVids: Iterable<Long>): Int = computeReach(filteredVids.asSequence())

  /** Computes impression using the "deterministic count" methodology. */
  suspend fun computeImpression(filteredVids: Flow<Long>, maxFrequency: Int): Long {
    // Count occurrences of each VID using fold operation on the flow
    val eventsPerVid: Map<Long, Int> =
      filteredVids.fold(mutableMapOf()) { acc, vid ->
        acc[vid] = acc.getOrDefault(vid, 0) + 1
        acc
      }

    return computeImpression(eventsPerVid, maxFrequency)
  }

  /** Computes impression using the "deterministic count" methodology. */
  fun computeImpression(filteredVids: Sequence<Long>, maxFrequency: Int): Long {
    val eventsPerVid: Map<Long, Int> = filteredVids.groupingBy { it }.eachCount()
    return computeImpression(eventsPerVid, maxFrequency)
  }

  /** Computes impression using the "deterministic count" methodology. */
  fun computeImpression(filteredVids: Iterable<Long>, maxFrequency: Int): Long =
    computeImpression(filteredVids.asSequence(), maxFrequency)

  private fun computeImpression(eventsPerVid: Map<Long, Int>, maxFrequency: Int): Long {
    // Cap each count at `maxFrequency`.
    return eventsPerVid.values.sumOf { count -> count.coerceAtMost(maxFrequency).toLong() }
  }

  /** Computes population using the "deterministic count" methodology. */
  fun computePopulation(
    populationInfo: PopulationInfo,
    program: Program,
    typeRegistry: TypeRegistry,
  ): Long {
    return PopulationRequisitionFulfiller.computePopulation(populationInfo, program, typeRegistry)
  }
}
