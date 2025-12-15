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

import com.google.privacy.differentialprivacy.GaussianNoise
import kotlin.math.min

object ImpressionComputations {
  private const val L_0_SENSITIVITY: Int = 1

  /**
   * Computes the impression count from a histogram of frequencies, applying differential privacy
   * noise if parameters are provided.
   *
   * The impression count is calculated as the weighted sum of histogram entries, where each
   * frequency bucket contributes (frequency * count) to the total.
   *
   * @param rawHistogram A histogram represented as a [LongArray], where each element corresponds to
   *   the count of impressions at a given frequency.
   * @param vidSamplingIntervalWidth The width of the sampling interval for VIDs, used to scale the
   *   impression count.
   * @param maxFrequency The maximum impression frequency per user. Used for both impression
   *   calculations as well as the lInfiniteSensitivity, if noise is applied.
   * @param dpParams Optional differential privacy parameters. If `null`, no noise is added and the
   *   raw impression count is scaled and returned.
   * @param kAnonymityParams Optional k-anonymity params.
   * @return The (potentially noised) impression count as a [Long]. If noise results in a negative
   *   count, zero is returned instead.
   */
  fun computeImpressionCount(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Float,
    maxFrequency: Long?,
    dpParams: DifferentialPrivacyParams?,
    kAnonymityParams: KAnonymityParams?,
  ): Long {
    val rawImpressionCount =
      rawHistogram.withIndex().sumOf { (index, count) ->
        val frequency =
          if (maxFrequency == null) {
            index + 1L
          } else {
            min(maxFrequency, index + 1L)
          }
        frequency * count
      }
    val scaledImpressionCount: Long =
      if (dpParams == null) {
        (rawImpressionCount / vidSamplingIntervalWidth).toLong()
      } else {
        check(maxFrequency != null) { "maxFrequency cannot be null if dpParams are set" }
        val noise = GaussianNoise()
        val noisedImpressionCount =
          noise.addNoise(
            rawImpressionCount,
            L_0_SENSITIVITY,
            maxFrequency,
            dpParams.epsilon,
            dpParams.delta,
          )
        if (noisedImpressionCount < 0) 0L
        else (noisedImpressionCount / vidSamplingIntervalWidth).toLong()
      }
    if (kAnonymityParams == null) {
      return scaledImpressionCount
    }
    val kAnonymityImpressionCount = run {
      val rawUserCount = rawHistogram.sum()
      val scaledUserCount: Long =
        if (dpParams == null) {
          (rawUserCount / vidSamplingIntervalWidth).toLong()
        } else {
          val noise = GaussianNoise()
          val lInfSensitivity = 1L
          val noisedUserCount =
            noise.addNoise(
              rawUserCount,
              L_0_SENSITIVITY,
              lInfSensitivity,
              dpParams.epsilon,
              dpParams.delta,
            )
          if (noisedUserCount < 0) 0L else (noisedUserCount / vidSamplingIntervalWidth).toLong()
        }
      if (
        scaledImpressionCount < kAnonymityParams.minImpressions ||
          scaledUserCount < kAnonymityParams.minUsers
      ) {
        0
      } else {
        scaledImpressionCount
      }
    }
    return kAnonymityImpressionCount
  }
}
