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

package org.wfanet.measurement.computation

object ImpressionComputations {
  /**
   * Computes the impression count from a histogram of frequencies, applying noise through [noiser].
   *
   * The impression count is the weighted sum of histogram entries, where each frequency bucket
   * contributes (frequency * count) to the total. [rawHistogram] is expected to be already capped
   * at the measurement's maximum frequency per user, as [HistogramComputations.buildHistogram]
   * produces it.
   *
   * @param rawHistogram A histogram represented as a [LongArray], where each element corresponds to
   *   the count of impressions at a given frequency.
   * @param vidSamplingIntervalWidth The width of the sampling interval for VIDs, used to scale the
   *   impression count.
   * @param noiser The mechanism applied to the released quantities. Pass [NoNoise] for none.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   * @return The (potentially noised) impression count as a [Long]. If noise results in a negative
   *   count, zero is returned instead.
   */
  fun computeImpressionCount(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Double,
    noiser: ResultNoiser,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): Long {
    val noisedImpressionCount = noiser.noiseImpressionsFromFrequencyHistogram(rawHistogram)
    val scaledImpressionCount: Long =
      if (noisedImpressionCount < 0) 0L
      else (noisedImpressionCount / vidSamplingIntervalWidth).toLong()

    if (resultMinimumThresholds == null) {
      return scaledImpressionCount
    }
    // The user count is a distinct-user quantity, so it takes the reach draw's unit sensitivity.
    val noisedUserCount = noiser.noiseReach(rawHistogram.sum())
    val scaledUserCount: Long =
      if (noisedUserCount < 0) 0L else (noisedUserCount / vidSamplingIntervalWidth).toLong()
    return if (
      scaledImpressionCount < resultMinimumThresholds.minImpressions ||
        scaledUserCount < resultMinimumThresholds.minUsers
    ) {
      0
    } else {
      scaledImpressionCount
    }
  }
}
