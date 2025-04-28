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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.fold

/** Utilities for computing Measurement results. */
object MeasurementResults {
  data class ReachAndFrequency(val reach: Int, val relativeFrequencyDistribution: Map<Int, Double>)

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  suspend fun computeReachAndFrequency(filteredVids: Flow<Long>, maxFrequency: Int): ReachAndFrequency {
    // Count occurrences of each VID using fold operation on the flow
    val eventsPerVid = filteredVids.fold(mutableMapOf<Long, Int>()) { acc, vid ->
      acc[vid] = acc.getOrDefault(vid, 0) + 1
      acc
    }

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
    // Use a mutable set to track distinct VIDs as they flow through
    val distinctVids = mutableSetOf<Long>()

    filteredVids.collect { vid ->
      distinctVids.add(vid)
    }

    return distinctVids.size
  }
}
