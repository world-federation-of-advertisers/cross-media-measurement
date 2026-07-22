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

import com.google.privacy.differentialprivacy.GaussianNoise
import kotlin.math.min

object ReachAndFrequencyComputations {
  private const val L0_SENSITIVITY = 1
  private const val L_INFINITE_SENSITIVITY = 1L

  /**
   * Computes the reach from a [SampledReachAndFrequency], applying differential privacy noise if
   * parameters are provided.
   *
   * @param sampled The in-sample reach and frequency histogram.
   *   [SampledReachAndFrequency.sampledReach] is the reach in the sample;
   *   [SampledReachAndFrequency.frequencyHistogram] supplies the impression count for the
   *   small-cell suppression threshold.
   * @param vidSamplingIntervalWidth The sampling rate used to select VIDs.
   * @param vectorSize The total size of the frequency vector space, used for capping the result
   *   before scaling. If null, no capping is applied.
   * @param dpParams The privacy parameters for the reach computation. When null, no noise is added
   *   (the values are treated as already noised, or noise is disabled).
   * @param resultMinimumThresholds Optional result minimum thresholds.
   * @return The reach value, potentially with noise applied.
   */
  fun computeReach(
    sampled: SampledReachAndFrequency,
    vidSamplingIntervalWidth: Double,
    vectorSize: Int?,
    dpParams: DifferentialPrivacyParams?,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): Long {
    val maxPossibleScaledReach =
      if (vectorSize != null) {
        (vectorSize / vidSamplingIntervalWidth).toLong()
      } else {
        Long.MAX_VALUE
      }

    val reachInSample = sampled.sampledReach
    val minScaledNoisedReach = run {
      if (dpParams == null) {
        val scaledReach = (reachInSample / vidSamplingIntervalWidth).toLong()
        min(scaledReach, maxPossibleScaledReach)
      } else {

        val noise = GaussianNoise()
        val noisedReachInSample =
          noise.addNoise(
            reachInSample,
            L0_SENSITIVITY,
            L_INFINITE_SENSITIVITY,
            dpParams.epsilon,
            dpParams.delta,
          )
        val scaledNoisedReach =
          if (noisedReachInSample < 0) 0L
          else (noisedReachInSample / vidSamplingIntervalWidth).toLong()
        min(scaledNoisedReach, maxPossibleScaledReach)
      }
    }
    if (resultMinimumThresholds == null) {
      return minScaledNoisedReach
    }
    val thresholdedImpressionCount = run {
      if (dpParams == null) {
        val rawImpressionCount =
          sampled.frequencyHistogram.withIndex().sumOf { (index, count) ->
            val frequency = index + 1L
            frequency * count
          }
        val scaledImpressionCount = (rawImpressionCount / vidSamplingIntervalWidth).toLong()
        if (
          scaledImpressionCount < resultMinimumThresholds.minImpressions ||
            minScaledNoisedReach < resultMinimumThresholds.minUsers
        ) {
          0
        } else {
          minScaledNoisedReach
        }
      } else {
        val rawImpressionCount =
          sampled.frequencyHistogram.withIndex().sumOf { (index, count) ->
            val frequency = min(resultMinimumThresholds.reachMaxFrequencyPerUser, index + 1)
            frequency * count
          }

        val noise = GaussianNoise()
        val noisedImpressionCount =
          noise.addNoise(
            rawImpressionCount,
            1,
            resultMinimumThresholds.reachMaxFrequencyPerUser.toLong(),
            dpParams.epsilon,
            dpParams.delta,
          )
        val scaledNoisedImpressionCount =
          (noisedImpressionCount / vidSamplingIntervalWidth).toLong()
        if (
          scaledNoisedImpressionCount < resultMinimumThresholds.minImpressions ||
            minScaledNoisedReach < resultMinimumThresholds.minUsers
        ) {
          0
        } else {
          minScaledNoisedReach
        }
      }
    }
    return thresholdedImpressionCount
  }

  /**
   * Computes the reach from a raw histogram, deriving the in-sample reach as the histogram sum.
   *
   * Equivalent to [computeReach] on `SampledReachAndFrequency(rawHistogram.sum(), rawHistogram)`.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   */
  fun computeReach(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Double,
    vectorSize: Int?,
    dpParams: DifferentialPrivacyParams?,
    resultMinimumThresholds: ResultMinimumThresholds?,
  ): Long =
    computeReach(
      SampledReachAndFrequency(rawHistogram.sum(), rawHistogram),
      vidSamplingIntervalWidth,
      vectorSize,
      dpParams,
      resultMinimumThresholds,
    )

  /**
   * Computes the frequency distribution among VIDs with non-zero frequencies, applying differential
   * privacy noise if parameters are provided.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   * @param maxFrequency The maximum frequency to reveal in the distribution. The input
   *   `rawHistogram` must have this size.
   * @param dpParams The privacy parameters for the reach computation.
   * @param resultMinimumThresholds Optional result minimum thresholds.
   * @param vidSamplingIntervalWidth The sampling rate used to select VIDs. Required if small-cell
   *   suppression thresholds are set.
   * @return A map representing the frequency distribution for frequencies 1 through `maxFrequency`.
   */
  fun computeFrequencyDistribution(
    rawHistogram: LongArray,
    maxFrequency: Int,
    dpParams: DifferentialPrivacyParams?,
    resultMinimumThresholds: ResultMinimumThresholds?,
    vidSamplingIntervalWidth: Double?,
  ): Map<Long, Double> {
    require(rawHistogram.size == maxFrequency) {
      "Invalid histogram size: ${rawHistogram.size} against maxFrequency: $maxFrequency"
    }
    val noisedHistogram: LongArray = run {
      if (dpParams == null) {
        val numActiveRegisters = rawHistogram.sum()
        if (numActiveRegisters == 0L) {
          return (1..maxFrequency).associate { it.toLong() to 0.0 }
        }
        rawHistogram
      } else {
        val noise = GaussianNoise()
        val noisedHistogram: LongArray =
          rawHistogram
            .map {
              val noisedValue =
                noise.addNoise(
                  it,
                  L0_SENSITIVITY,
                  L_INFINITE_SENSITIVITY,
                  dpParams.epsilon,
                  dpParams.delta,
                )
              if (noisedValue < 0) 0L else noisedValue
            }
            .toLongArray()

        val numNoisedActiveRegisters = noisedHistogram.sum()
        if (numNoisedActiveRegisters == 0L) {
          return (1..maxFrequency).associate { it.toLong() to 0.0 }
        }
        noisedHistogram
      }
    }

    if (resultMinimumThresholds == null) {
      val numNoisedActiveRegisters = noisedHistogram.sum()
      return noisedHistogram.withIndex().associate { (index, count) ->
        val frequency = index + 1L
        if (numNoisedActiveRegisters === 0L) {
          frequency to 0.0
        } else {
          frequency to count.toDouble() / numNoisedActiveRegisters
        }
      }
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
    return thresholdedHistogram.withIndex().associate { (index, count) ->
      val frequency = index + 1L
      if (numThresholdedActiveRegisters === 0L) {
        frequency to 0.0
      } else {
        frequency to count.toDouble() / numThresholdedActiveRegisters
      }
    }
  }
}
