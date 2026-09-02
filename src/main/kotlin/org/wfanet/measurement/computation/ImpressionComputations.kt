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
 * @param variance The correction variance of [value]. This includes the estimator variance and,
 *   when minimum thresholding changes a positive count to zero, the fixed threshold correction
 *   variance. A caller releasing this through the CMMS reports it as a custom direct methodology,
 *   because the reporting server cannot derive it: the clip is data-derived and the noise is spread
 *   across the histogram bars.
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
   * @return The released impression count and minimum-threshold variance. If noise results in a
   *   negative count, the released value is zero. Variance is present only when thresholding
   *   changes a positive value to zero.
   */
  fun computeImpressionCount(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Double,
    noiser: ResultNoiser,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): MinimumThresholdResult<Long> {
    val noisedImpressionCount = noiser.noiseImpressionsFromFrequencyHistogram(rawHistogram)
    val scaledImpressionCount: Long =
      if (noisedImpressionCount < 0) 0L
      else (noisedImpressionCount / vidSamplingIntervalWidth).toLong()

    if (resultMinimumThresholds == null) {
      return MinimumThresholdResult(scaledImpressionCount, variance = null)
    }
    // The user count is a distinct-user quantity, so it takes the reach draw's unit sensitivity.
    val noisedUserCount = noiser.noiseReach(rawHistogram.sum())
    val scaledUserCount: Long =
      if (noisedUserCount < 0) 0L else (noisedUserCount / vidSamplingIntervalWidth).toLong()
    val failsThreshold =
      scaledImpressionCount < resultMinimumThresholds.minImpressions ||
        scaledUserCount < resultMinimumThresholds.minUsers
    val thresholdStandardDeviation = resultMinimumThresholds.minImpressions.toDouble()
    return MinimumThresholdResult(
      value = if (failsThreshold) 0L else scaledImpressionCount,
      variance =
        if (failsThreshold && scaledImpressionCount > 0L) {
          noiser.impressionVariance / (vidSamplingIntervalWidth * vidSamplingIntervalWidth) +
            thresholdStandardDeviation * thresholdStandardDeviation
        } else {
          null
        },
    )
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
   * [queryRho], [maxFrequency] and [noiseSource] are whatever the calling mechanism supplies, so a
   * protocol seeding draws from a combined multi-party frequency vector uses this unchanged.
   *
   * @param frequencyVector The per-user frequencies to count.
   * @param queryRho The ACDP rho charged for the release.
   * @param maxFrequency The highest per-user frequency [frequencyVector] can hold, which bounds the
   *   clip and the bars the search charges for.
   * @param noiseSource The standard-normal draws added to the bars.
   * @param vidSamplingIntervalWidth The width of the sampling interval for VIDs.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   */
  fun computeDynamicallyClippedImpressionCount(
    frequencyVector: IntArray,
    queryRho: Double,
    maxFrequency: Int,
    noiseSource: DynamicClippingNoiseSource,
    vidSamplingIntervalWidth: Double,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): DynamicallyClippedImpressions {
    require(maxFrequency > 0) { "maxFrequency must be positive, got $maxFrequency" }
    require(vidSamplingIntervalWidth > 0.0) {
      "vidSamplingIntervalWidth must be positive, got $vidSamplingIntervalWidth"
    }

    val searched =
      DynamicClipping(
          queryRho = queryRho,
          measurementType = DynamicClipping.MeasurementType.IMPRESSION,
          maxThreshold = maxFrequency,
          noiseSource = noiseSource,
        )
        .computeImpressionCappedHistogram(frequencyHistogram(frequencyVector))
    val clip: Int = searched.threshold
    val bars: List<Double> = searched.noisedCumulativeHistogramList

    val scaledImpressionCount: Long = scaleAndClamp(bars.take(clip).sum(), vidSamplingIntervalWidth)
    // Bar 0 is the noised count of users with any impression, so the min_users gate costs no
    // further draw. It carries the bar noise, which is calibrated across the whole histogram and
    // so is larger than the unit-sensitivity draw a fixed-cap count gates on; the gate therefore
    // admits and rejects more often near the threshold.
    val scaledUserCount: Long = scaleAndClamp(bars.firstOrNull() ?: 0.0, vidSamplingIntervalWidth)
    val isBelowMinimum =
      resultMinimumThresholds != null &&
        (scaledImpressionCount < resultMinimumThresholds.minImpressions ||
          scaledUserCount < resultMinimumThresholds.minUsers)
    val value: Long = if (isBelowMinimum) 0L else scaledImpressionCount
    val thresholdStandardDeviation: Double =
      resultMinimumThresholds?.minImpressions?.toDouble() ?: 0.0
    val thresholdCorrectionVariance: Double =
      if (isBelowMinimum && scaledImpressionCount > 0L) {
        thresholdStandardDeviation * thresholdStandardDeviation
      } else {
        0.0
      }

    // TODO(world-federation-of-advertisers/cross-media-measurement#4437): The target is
    // Var(X | C). The calculation below substitutes P(X) for P(X | C), which assumes the clip is
    // independent of the draws. It is not: the clip is a deterministic function of them, so
    // P(C | X) is an indicator, and conditioning restricts the draws to the region where the
    // search stops at that clip. The figure is therefore biased, and measurement shows the bias
    // is downward.
    //
    // The released value estimates sum(min(frequency, C)) over users, so the clip defines the
    // quantity being estimated.
    //
    // Write C for the clip, w for the sampling interval width, and s2 for the per-bar noise
    // variance.
    //
    // Noise: the released sum is the sum of C noised bars. Treating the draws as independent
    // across bars, the sum has variance C * s2, and the value scales by 1 / w, giving C * s2 / w^2.
    // After the remaining-charge pass s2 is itself proportional to C, so this reduces to the
    // fixed-cap figure C^2 / (2 * rho).
    //
    // Sampling: a user is in the VID sample independently with probability w and contributes
    // c_i = min(frequency_i, C). The sampled sum is the sum over users of a Bernoulli(w) indicator
    // times c_i, scaled by 1 / w, so it has variance sum(c_i^2) * w * (1 - w) / w^2. Since
    // c_i <= C, sum(c_i^2) <= C * sum(c_i), and sum(c_i) is the population total the value
    // estimates. That bounds the term by C * value * w * (1 - w) / w^2, tight when every user sits
    // at or above the clip.
    val samplingVariance: Double =
      clip.toDouble() *
        value.toDouble() *
        vidSamplingIntervalWidth *
        (1.0 - vidSamplingIntervalWidth)
    val noiseVariance: Double = clip.toDouble() * searched.barNoiseVariance
    val variance: Double =
      (samplingVariance + noiseVariance) / (vidSamplingIntervalWidth * vidSamplingIntervalWidth) +
        thresholdCorrectionVariance

    return DynamicallyClippedImpressions(value, variance.coerceAtLeast(0.0), clip)
  }

  /**
   * Returns the frequency histogram to search, as a count of users per frequency.
   *
   * A vector with no impressions has no maximum frequency to size a histogram from. It gets a
   * single empty bar rather than skipping the search: releasing an exact zero here, beside a noised
   * value for a vector holding one impression, would leave the two distinguishable.
   */
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
