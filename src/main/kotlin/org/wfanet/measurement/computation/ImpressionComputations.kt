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

/**
 * A dynamically clipped impression count and the variance of that released value.
 *
 * @param value The released impression count.
 * @param variance The variance of [value], which the caller reports as a custom direct methodology
 *   because the reporting server cannot derive it: the clip is data-derived and the noise is spread
 *   over the bars rather than applied as a single draw.
 */
data class DynamicallyClippedImpressions(val value: Long, val variance: Double)

object ImpressionComputations {
  /**
   * Computes the impression count from a dynamically clipped noised cumulative histogram.
   *
   * `noisedCumulativeHistogram[k]` is the noised count of users with frequency at least `k + 1`, so
   * summing the first [clip] bars is the impression count with each user clipped at [clip]. Bars at
   * or above [clip] are dropped rather than released: the clip search never reads past its own
   * choice, so leaving them out is what keeps the released quantities within the charge the search
   * already paid.
   *
   * Bar 0 is the noised user count, which gates `min_users` without a further draw. It carries the
   * bar noise, which is calibrated across the whole histogram and so is larger than the
   * unit-sensitivity draw the fixed-cap path gates on. The gate therefore admits and rejects more
   * often near the threshold; raise `min_users` if the suppression needs to bite as hard.
   *
   * @param noisedCumulativeHistogram The noised cumulative histogram, already carrying its noise.
   * @param clip The per-user clip the histogram was searched for.
   * @param barNoiseVariance The noise variance a single bar carries.
   * @param vidSamplingIntervalWidth The width of the sampling interval for VIDs.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   */
  fun computeDynamicallyClippedImpressionCount(
    noisedCumulativeHistogram: List<Double>,
    clip: Int,
    barNoiseVariance: Double,
    vidSamplingIntervalWidth: Double,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): DynamicallyClippedImpressions {
    require(clip > 0) { "clip must be positive, got $clip" }
    require(vidSamplingIntervalWidth > 0.0) {
      "vidSamplingIntervalWidth must be positive, got $vidSamplingIntervalWidth"
    }

    val noisedImpressionCount: Double = noisedCumulativeHistogram.take(clip).sum()
    val scaledImpressionCount: Long =
      if (noisedImpressionCount < 0) 0L
      else (noisedImpressionCount / vidSamplingIntervalWidth).toLong()

    val value: Long =
      if (resultMinimumThresholds == null) {
        scaledImpressionCount
      } else {
        val noisedUserCount: Double = noisedCumulativeHistogram.firstOrNull() ?: 0.0
        val scaledUserCount: Long =
          if (noisedUserCount < 0) 0L else (noisedUserCount / vidSamplingIntervalWidth).toLong()
        if (
          scaledImpressionCount < resultMinimumThresholds.minImpressions ||
            scaledUserCount < resultMinimumThresholds.minUsers
        ) {
          0L
        } else {
          scaledImpressionCount
        }
      }

    // Both terms are in [clip], never in the number of bars actually summed. The histogram is only
    // as long as the highest frequency in the data, so a bar count would put that raw value into a
    // released quantity; [clip] comes out of the noised search and is already safe to release. When
    // the clip overshoots the histogram this overstates the variance, which is the safe direction.
    //
    // The sampling term mirrors the reporting server's deterministic scalar variance at a cap of
    // [clip]. The noise term differs: the count sums one draw per bar rather than taking a single
    // draw calibrated to [clip], so it grows linearly in the clip, not with its square.
    val samplingVariance: Double =
      clip.toDouble() *
        value.toDouble() *
        vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth)
    val noiseVariance: Double = clip.toDouble() * barNoiseVariance
    val variance: Double =
      (samplingVariance + noiseVariance) / (vidSamplingIntervalWidth * vidSamplingIntervalWidth)

    return DynamicallyClippedImpressions(value, variance.coerceAtLeast(0.0))
  }

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
