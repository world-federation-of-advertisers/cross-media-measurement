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

import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClipping
import org.wfanet.measurement.eventdataprovider.differentialprivacy.DynamicClippingNoiseSource

/**
 * A dynamically clipped impression count.
 *
 * @param value The released impression count.
 * @param variance The variance of [value]. A caller releasing this through the CMMS reports it as a
 *   custom direct methodology, because the reporting server cannot derive it: the clip is
 *   data-derived and the noise is spread across the histogram bars.
 * @param clip The per-user clip the count was taken at. Chosen by the noised search, so it is safe
 *   to release.
 */
data class DynamicallyClippedImpressions(val value: Long, val variance: Double, val clip: Int)

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

  /**
   * Computes the impression count with a per-user clip derived from [frequencyVector]'s own
   * distribution, rather than from a cap fixed in advance.
   *
   * [DynamicClipping] noises the cumulative histogram it searches, and `cumulativeHistogram[k]` is
   * the number of users with frequency at least `k + 1`, so
   *
   * ```
   * sum over k < clip of cumulativeHistogram[k]  =  sum over users of min(frequency, clip)
   * ```
   *
   * The search and the count therefore come out of one charge, with no second draw. Bars at or
   * above the clip are dropped rather than released: the search never reads past its own choice, so
   * leaving them out keeps the released quantities inside the charge already paid.
   *
   * [queryRho] and [noiseSource] are whatever the calling mechanism supplies, so a protocol seeding
   * draws from a combined multi-party frequency vector uses this unchanged.
   *
   * @param frequencyVector The per-user frequencies to count.
   * @param queryRho The ACDP rho charged for the release.
   * @param noiseSource The standard-normal draws added to the bars.
   * @param vidSamplingIntervalWidth The width of the sampling interval for VIDs.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   */
  fun computeDynamicallyClippedImpressionCount(
    frequencyVector: IntArray,
    queryRho: Double,
    noiseSource: DynamicClippingNoiseSource,
    vidSamplingIntervalWidth: Double,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): DynamicallyClippedImpressions {
    require(vidSamplingIntervalWidth > 0.0) {
      "vidSamplingIntervalWidth must be positive, got $vidSamplingIntervalWidth"
    }

    val searched =
      DynamicClipping(
          queryRho = queryRho,
          measurementType = DynamicClipping.MeasurementType.IMPRESSION,
          maxThreshold = MAX_REPRESENTABLE_FREQUENCY,
          noiseSource = noiseSource,
        )
        .computeImpressionCappedHistogram(frequencyHistogram(frequencyVector))
    val clip: Int = searched.threshold
    val bars: List<Double> = searched.noisedCumulativeHistogramList

    val scaledImpressionCount: Long = scaleAndClamp(bars.take(clip).sum(), vidSamplingIntervalWidth)
    val value: Long =
      if (resultMinimumThresholds == null) {
        scaledImpressionCount
      } else {
        // Bar 0 is the noised count of users with any impression, so the min_users gate costs no
        // further draw. It carries the bar noise, which is calibrated across the whole histogram
        // and so is larger than the unit-sensitivity draw a fixed-cap count gates on; the gate
        // therefore admits and rejects more often near the threshold.
        val scaledUserCount: Long =
          scaleAndClamp(bars.firstOrNull() ?: 0.0, vidSamplingIntervalWidth)
        if (
          scaledImpressionCount < resultMinimumThresholds.minImpressions ||
            scaledUserCount < resultMinimumThresholds.minUsers
        ) {
          0L
        } else {
          scaledImpressionCount
        }
      }

    // Both terms scale by the clip, never by the number of bars actually summed. The histogram is
    // only as long as the highest frequency in the data, so a bar count would carry that raw value
    // into a released quantity, while the clip comes out of the noised search. When the clip
    // overshoots the histogram this overstates the variance, which is the safe direction.
    //
    // The sampling term matches what the reporting server computes for a fixed cap. The noise term
    // does not: the count sums one draw per bar rather than taking a single draw calibrated to the
    // clip, so it grows linearly in the clip rather than with its square.
    val samplingVariance: Double =
      clip.toDouble() *
        value.toDouble() *
        vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth)
    val noiseVariance: Double = clip.toDouble() * searched.barNoiseVariance
    val variance: Double =
      (samplingVariance + noiseVariance) / (vidSamplingIntervalWidth * vidSamplingIntervalWidth)

    return DynamicallyClippedImpressions(value, variance.coerceAtLeast(0.0), clip)
  }

  /**
   * Returns the frequency histogram to search, as a count of users per frequency.
   *
   * A vector with no impressions has no maximum frequency to size a histogram from. It gets a
   * single empty bar rather than skipping the search: releasing an exact zero here, beside a noised
   * value for a vector holding one impression, would leave the two distinguishable.
   */
  /**
   * The highest per-user frequency a frequency vector can hold.
   *
   * `StripedByteFrequencyVector` saturates each VID at [Byte.MAX_VALUE], so a clip above it would
   * bound nothing, and noise calibrated for a wider search would be calibrated for bars that cannot
   * carry a user.
   */
  private const val MAX_REPRESENTABLE_FREQUENCY = Byte.MAX_VALUE.toInt()

  private fun frequencyHistogram(frequencyVector: IntArray): Map<Long, Long> =
    frequencyVector
      .asSequence()
      .filter { it > 0 }
      .groupingBy { it.toLong() }
      .eachCount()
      .mapValues { it.value.toLong() }
      .ifEmpty { mapOf(1L to 0L) }

  private fun scaleAndClamp(count: Double, vidSamplingIntervalWidth: Double): Long =
    if (count < 0) 0L else (count / vidSamplingIntervalWidth).toLong()
}
