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

import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClipping
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.api.v2alpha.PrivacyQueryMapper

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

    // If the sampled VIDs is empty, set the distribution with all 0s up to maxFrequency.
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
  fun computeReach(sampledVids: Iterable<Long>): Int {
    return sampledVids.distinct().size
  }

  /** Computes impression using the "deterministic count" methodology. */
  fun computeImpression(sampledVids: Iterable<Long>, maxFrequency: Int): Long {
    val eventsPerVid: Map<Long, Int> = sampledVids.groupingBy { it }.eachCount()
    // Cap each count at `maxFrequency`.
    return eventsPerVid.values.sumOf { count -> count.coerceAtMost(maxFrequency).toLong() }
  }

  /** Computes impression using the "Dynamic Clipping" methodology. */
  fun computeDynamicClipImpression(
    sampledVids: Iterable<Long>,
    privacyParams: DpParams,
  ): DynamicClipping.Result {
    val eventsPerVid: Map<Long, Int> = sampledVids.groupingBy { it }.eachCount()

    val frequencyHistogram = mutableMapOf<Long, Long>()
    for (value in eventsPerVid.values) {
      val bucket = value.toLong()
      frequencyHistogram[bucket] = frequencyHistogram.getOrDefault(bucket, 0) + 1
    }

    val acdpCharge =
      AcdpParamsConverter.getDirectAcdpCharge(privacyParams, PrivacyQueryMapper.SENSITIVITY)
    val dynamicClipping =
      DynamicClipping(acdpCharge.rho, DynamicClipping.MeasurementType.IMPRESSION)

    return dynamicClipping.computeImpressionCappedHistogram(frequencyHistogram)
  }
}
