// Copyright 2025 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package org.wfanet.measurement.computation

import kotlin.math.max
import kotlin.math.min

object ReachAndFrequencyComputations {

  /**
   * Computes the reach from a [ReachAndFrequency], applying [noiser] to the released quantities.
   *
   * @param sampled The in-sample reach and frequency histogram. [ReachAndFrequency.sampledReach] is
   *   the reach in the sample; [ReachAndFrequency.frequencyHistogram] supplies the impression count
   *   for the small-cell suppression threshold.
   * @param vidSamplingIntervalWidth The sampling rate used to select VIDs.
   * @param vectorSize The total size of the frequency vector space, used for capping the result
   *   before scaling. If null, no capping is applied.
   * @param noiser The noise mechanism. Use [NoNoise] to release the raw values.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   * @return The released reach and minimum-threshold variance. Variance is present only when
   *   thresholding changes a positive value to zero.
   */
  fun computeReach(
    sampled: ReachAndFrequency,
    vidSamplingIntervalWidth: Double,
    vectorSize: Int?,
    noiser: ResultNoiser,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): MinimumThresholdResult<Long> {
    val maxPossibleScaledReach =
      if (vectorSize != null) {
        (vectorSize / vidSamplingIntervalWidth).toLong()
      } else {
        Long.MAX_VALUE
      }

    val noisedReachInSample = noiser.noiseReach(sampled.sampledReach)
    val scaledNoisedReach =
      if (noisedReachInSample < 0) 0L else (noisedReachInSample / vidSamplingIntervalWidth).toLong()
    val minScaledNoisedReach = min(scaledNoisedReach, maxPossibleScaledReach)

    if (resultMinimumThresholds == null) {
      return MinimumThresholdResult(minScaledNoisedReach, variance = null)
    }

    val impressionCount = noiser.noiseImpressionsFromFrequencyHistogram(sampled.frequencyHistogram)
    val scaledImpressionCount = (impressionCount / vidSamplingIntervalWidth).toLong()
    val failsThreshold =
      scaledImpressionCount < resultMinimumThresholds.minImpressions ||
        minScaledNoisedReach < resultMinimumThresholds.minUsers
    val thresholdStandardDeviation = resultMinimumThresholds.minUsers.toDouble()
    return MinimumThresholdResult(
      value = if (failsThreshold) 0L else minScaledNoisedReach,
      variance =
        if (failsThreshold && minScaledNoisedReach > 0L) {
          noiser.reachVariance / (vidSamplingIntervalWidth * vidSamplingIntervalWidth) +
            thresholdStandardDeviation * thresholdStandardDeviation
        } else {
          null
        },
    )
  }

  /**
   * Computes the reach from a raw histogram, deriving the in-sample reach as the histogram sum.
   *
   * Equivalent to [computeReach] on `ReachAndFrequency(rawHistogram.sum(), rawHistogram)`.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   */
  fun computeReach(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Double,
    vectorSize: Int?,
    noiser: ResultNoiser,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): MinimumThresholdResult<Long> =
    computeReach(
      ReachAndFrequency(rawHistogram.sum(), rawHistogram),
      vidSamplingIntervalWidth,
      vectorSize,
      noiser,
      resultMinimumThresholds,
    )

  /**
   * Computes the frequency distribution among VIDs with non-zero frequencies, applying differential
   * privacy noise if parameters are provided.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   * @param maxFrequency The maximum frequency to reveal in the distribution. The input
   *   `rawHistogram` must have this size.
   * @param noiser The noise mechanism. Use [NoNoise] to release the raw values.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   * @param vidSamplingIntervalWidth The sampling rate used to select VIDs. Required if small-cell
   *   suppression thresholds are set.
   * @return The released frequency distribution and minimum-threshold count variance. Variance is
   *   present only when terminal suppression changes a nonempty histogram to all zeros.
   */
  fun computeFrequencyDistribution(
    rawHistogram: LongArray,
    maxFrequency: Int,
    noiser: ResultNoiser,
    resultMinimumThresholds: ResultMinimumThresholds?,
    vidSamplingIntervalWidth: Double?,
  ): MinimumThresholdResult<Map<Long, Double>> {
    require(rawHistogram.size == maxFrequency) {
      "Invalid histogram size: ${rawHistogram.size} against maxFrequency: $maxFrequency"
    }
    val noisedHistogram =
      LongArray(maxFrequency) { index -> noiser.noiseFrequencyBucket(index, rawHistogram[index]) }
    val numNoisedActiveRegisters = noisedHistogram.sum()
    if (numNoisedActiveRegisters == 0L) {
      return MinimumThresholdResult(
        (1..maxFrequency).associate { it.toLong() to 0.0 },
        variance = null,
      )
    }

    if (resultMinimumThresholds == null) {
      return MinimumThresholdResult(
        noisedHistogram.withIndex().associate { (index, count) ->
          (index + 1L) to count.toDouble() / numNoisedActiveRegisters
        },
        variance = null,
      )
    }

    requireNotNull(vidSamplingIntervalWidth) {
      "vidSamplingIntervalWidth must be set if resultMinimumThresholds are set"
    }
    val thresholdedHistogram = noisedHistogram.copyOf()
    // Fold down from highest frequency to lowest. When a bucket fails the threshold,
    // its user count is added to the next lower bucket, which is then re-evaluated.
    for (index in thresholdedHistogram.indices.reversed()) {
      val frequency = index + 1L
      val count = thresholdedHistogram[index]
      if (
        count / vidSamplingIntervalWidth < resultMinimumThresholds.minUsers ||
          frequency * count / vidSamplingIntervalWidth < resultMinimumThresholds.minImpressions
      ) {
        thresholdedHistogram[index] = 0
        if (index > 0) {
          thresholdedHistogram[index - 1] += count
        }
      }
    }
    val numThresholdedActiveRegisters = thresholdedHistogram.sum()
    return MinimumThresholdResult(
      value =
        thresholdedHistogram.withIndex().associate { (index, count) ->
          val frequency = index + 1L
          if (numThresholdedActiveRegisters == 0L) {
            frequency to 0.0
          } else {
            frequency to count.toDouble() / numThresholdedActiveRegisters
          }
        },
      variance =
        if (numThresholdedActiveRegisters == 0L) {
          val thresholdStandardDeviation =
            max(resultMinimumThresholds.minUsers, resultMinimumThresholds.minImpressions).toDouble()
          val noiseVariance =
            maxFrequency * noiser.frequencyBucketVariance /
              (vidSamplingIntervalWidth * vidSamplingIntervalWidth)
          noiseVariance + thresholdStandardDeviation * thresholdStandardDeviation
        } else {
          null
        },
    )
  }
}
