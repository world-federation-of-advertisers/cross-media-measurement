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

package org.wfanet.measurement.duchy.mill.trustee.processor

import com.google.privacy.differentialprivacy.GaussianNoise
import kotlin.math.min
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams

object ReachAndFrequencyComputations {
  private const val L0_SENSITIVITY = 1
  private const val L_INFINITE_SENSITIVITY = 1L

  /**
   * Computes the reach, applying differential privacy noise if parameters are provided.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   * @param dpParams The privacy parameters for the reach computation. If `null`, the raw reach is
   *   computed.
   * @return The reach value, potentially with noise applied.
   */
  fun computeReach(
    rawHistogram: LongArray,
    vectorSize: Int,
    vidSamplingIntervalWidth: Float,
    dpParams: DifferentialPrivacyParams?,
  ): Long {
    val maxPossibleScaledReach = (vectorSize / vidSamplingIntervalWidth).toLong()

    // The histogram is built only from non-zero frequencies, so its sum is the reach in the sample.
    val reachInSample = rawHistogram.sum()

    if (dpParams == null) {
      val scaledReach = (reachInSample / vidSamplingIntervalWidth).toLong()
      return min(scaledReach, maxPossibleScaledReach)
    }

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
      if (noisedReachInSample < 0) 0L else (noisedReachInSample / vidSamplingIntervalWidth).toLong()

    return min(scaledNoisedReach, maxPossibleScaledReach)
  }

  /**
   * Computes the frequency distribution among VIDs with non-zero frequencies, applying differential
   * privacy noise if parameters are provided.
   *
   * @param rawHistogram A histogram of counts for frequencies 1 to `maxFrequency`.
   * @param dpParams The privacy parameters for the frequency computation. If `null`, the raw
   *   distribution is computed.
   * @return A map representing the frequency distribution for frequencies 1 through `maxFrequency`.
   */
  fun computeFrequencyDistribution(
    rawHistogram: LongArray,
    maxFrequency: Int,
    dpParams: DifferentialPrivacyParams?,
  ): Map<Long, Double> {
    require(rawHistogram.size == maxFrequency) {
      "Invalid histogram size: ${rawHistogram.size} against maxFrequency: $maxFrequency"
    }

    if (dpParams == null) {
      val totalReachedUsers = rawHistogram.sum()
      if (totalReachedUsers == 0L) {
        return (1..maxFrequency).associate { it.toLong() to 0.0 }
      }
      return rawHistogram.withIndex().associate { (index, count) ->
        val frequency = index + 1L
        frequency to count.toDouble() / totalReachedUsers
      }
    }

    val noise = GaussianNoise()
    val noisedHistogram =
      rawHistogram.map {
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

    val totalNoisedReachedUsers = noisedHistogram.sum()
    if (totalNoisedReachedUsers == 0L) {
      return (1..maxFrequency).associate { it.toLong() to 0.0 }
    }

    return noisedHistogram.withIndex().associate { (index, count) ->
      val frequency = index + 1L
      frequency to count.toDouble() / totalNoisedReachedUsers
    }
  }

  /**
   * Builds a histogram from a frequency vector, counting only non-zero frequencies.
   *
   * Frequencies greater than [maxFrequency] are treated as [maxFrequency].
   *
   * @param frequencyVector An array where each element is the frequency for a given VID.
   * @param maxFrequency The maximum possible frequency value. The histogram will have
   *   `maxFrequency` buckets.
   * @return A [LongArray] representing the histogram, where index `k-1` is the count of VIDs with
   *   frequency `k`.
   */
  fun buildHistogram(frequencyVector: IntArray, maxFrequency: Int): LongArray {
    val histogram = LongArray(maxFrequency)
    for (frequency in frequencyVector) {
      if (frequency > 0) {
        // Cap the frequency at maxFrequency.
        val cappedFrequency = min(frequency, maxFrequency)
        histogram[cappedFrequency - 1]++
      }
    }
    return histogram
  }
}
