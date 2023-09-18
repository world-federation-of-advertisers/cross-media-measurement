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

package org.wfanet.measurement.loadtest.dataprovider

/** Utilities for computing Measurement results. */
object MeasurementResults {
  data class ReachAndFrequency(val reach: Int, val relativeFrequencyDistribution: Map<Int, Double>)

  /**
   * Computes reach and frequency using the "deterministic count distinct" methodology and the
   * "deterministic distribution" methodology.
   */
  fun computeReachAndFrequency(sampledVids: Iterable<Long>, maxFrequency: Int): ReachAndFrequency {
    val eventsPerVid: Map<Long, Int> = sampledVids.groupingBy { it }.eachCount()
    val reach: Int = eventsPerVid.keys.size

    val frequencyHistogram = mutableMapOf<Int, Int>()
    for (count in eventsPerVid.values) {
      val bucket = count.coerceAtMost(maxFrequency)
      frequencyHistogram[bucket] = frequencyHistogram.getOrDefault(bucket, 0) + 1
    }

    return ReachAndFrequency(reach, frequencyHistogram.mapValues { it.value.toDouble() / reach })
  }

  /** Computes reach using the "deterministic count distinct" methodology. */
  fun computeReach(sampledVids: Iterable<Long>): Int {
    return sampledVids.distinct().size
  }
}
