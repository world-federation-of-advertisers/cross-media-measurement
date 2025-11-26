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
   * Computes the reach, applying differential privacy noise if parameters are provided.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   * @param vidSamplingIntervalWidth The sampling rate used to select VIDs.
   * @param vectorSize The total size of the frequency vector space, used for capping the result.
   *   before scaling. If it is null (the default), no capping is applied.
   * @param dpParams The privacy parameters for the reach computation.
   * @param kAnonymityParams Optional k-anonymity params.
   * @return The reach value, potentially with noise applied.
   */
  fun computeReach(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Float,
    vectorSize: Int? = null,
    dpParams: DifferentialPrivacyParams?,
    kAnonymityParams: KAnonymityParams?,
  ): Long {
    val maxPossibleScaledReach =
      if (vectorSize != null) {
        (vectorSize / vidSamplingIntervalWidth).toLong()
      } else {
        Long.MAX_VALUE
      }

    // The histogram is built only from non-zero frequencies, so its sum is the reach in the sample.
    val reachInSample = rawHistogram.sum()
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
    if (kAnonymityParams == null) {
      return minScaledNoisedReach
    }
    val kAnonymityImpressionCount = run {
      if (dpParams == null) {
        val rawImpressionCount =
          rawHistogram.withIndex().sumOf { (index, count) ->
            val frequency = index + 1L
            frequency * count
          }
        val scaledImpressionCount = (rawImpressionCount / vidSamplingIntervalWidth).toLong()
        if (
          scaledImpressionCount < kAnonymityParams.minImpressions ||
            minScaledNoisedReach < kAnonymityParams.minUsers
        ) {
          0
        } else {
          minScaledNoisedReach
        }
      } else {
        val rawImpressionCount =
          rawHistogram.withIndex().sumOf { (index, count) ->
            val frequency = min(kAnonymityParams.reachMaxFrequencyPerUser, index + 1)
            frequency * count
          }

        val noise = GaussianNoise()
        val noisedImpressionCount =
          noise.addNoise(
            rawImpressionCount,
            1,
            kAnonymityParams.reachMaxFrequencyPerUser.toLong(),
            dpParams.epsilon,
            dpParams.delta,
          )
        val scaledNoisedImpressionCount =
          (noisedImpressionCount / vidSamplingIntervalWidth).toLong()
        if (
          scaledNoisedImpressionCount < kAnonymityParams.minImpressions ||
            minScaledNoisedReach < kAnonymityParams.minUsers
        ) {
          0
        } else {
          minScaledNoisedReach
        }
      }
    }
    return kAnonymityImpressionCount
  }

  /**
   * Computes the frequency distribution among VIDs with non-zero frequencies, applying differential
   * privacy noise if parameters are provided.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   * @param maxFrequency The maximum frequency to reveal in the distribution. The input
   *   `rawHistogram` must have this size.
   * @param dpParams The privacy parameters for the reach computation.
   * @param kAnonymityParams Optional k-anonymity params.
   * @param vidSamplingIntervalWidth The sampling rate used to select VIDs. Required if k-anonymity
   *   params are set.
   * @return A map representing the frequency distribution for frequencies 1 through `maxFrequency`.
   */
  fun computeFrequencyDistribution(
    rawHistogram: LongArray,
    maxFrequency: Int,
    dpParams: DifferentialPrivacyParams?,
    kAnonymityParams: KAnonymityParams?,
    vidSamplingIntervalWidth: Float?,
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

    if (kAnonymityParams == null) {
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
      "vidSamplingIntervalWidth must be set if kAnonymityParams are set"
    }
    val kAnonymityHistogram =
      noisedHistogram.withIndex().map { (index, count) ->
        val frequency = index + 1L
        if (
          count / vidSamplingIntervalWidth < kAnonymityParams.minUsers ||
            frequency * count / vidSamplingIntervalWidth < kAnonymityParams.minImpressions
        ) {
          0
        } else {
          count
        }
      }
    val numKActiveRegisters = kAnonymityHistogram.sum()
    return kAnonymityHistogram.withIndex().associate { (index, count) ->
      val frequency = index + 1L
      if (numKActiveRegisters === 0L) {
        frequency to 0.0
      } else {
        frequency to count.toDouble() / numKActiveRegisters
      }
    }
  }
}
