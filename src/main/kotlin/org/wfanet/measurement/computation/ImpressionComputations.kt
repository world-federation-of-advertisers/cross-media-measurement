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

object ImpressionComputations {
  private const val L0_SENSITIVITY = 1
  private const val L_INFINITE_SENSITIVITY = 1L
  /**
   * Computes the impression count from a histogram of frequencies, applying differential privacy
   * noise if parameters are provided.
   *
   * The impression count is calculated as the number of non-zero entries in the `rawHistogram`. If
   * differential privacy parameters (`dpParams`) are provided, Gaussian noise is added to the raw
   * count to ensure privacy guarantees.
   *
   * @param rawHistogram A histogram represented as a [LongArray], where each element corresponds to
   *   the count of impressions at a given frequency.
   * @param vidSamplingIntervalWidth The width of the sampling interval for VIDs, used to scale the
   *   impression count.
   * @param dpParams Optional differential privacy parameters. If `null`, no noise is added and the
   *   raw impression count is scaled and returned.
   * @return The (potentially noised) impression count as a [Long]. If noise results in a negative
   *   count, zero is returned instead.
   */
  fun computeImpressionCount(
    rawHistogram: LongArray,
    vidSamplingIntervalWidth: Float,
    dpParams: DifferentialPrivacyParams?,
  ): Long {
    val rawImpressionCount =
      rawHistogram.withIndex().sumOf { (index, count) ->
        val frequency = index + 1L
        frequency * count
      }
    if (dpParams == null) {
      return (rawImpressionCount / vidSamplingIntervalWidth).toLong()
    }
    val noise = GaussianNoise()
    val noisedImpressionCount =
      noise.addNoise(
        rawImpressionCount,
        L0_SENSITIVITY,
        L_INFINITE_SENSITIVITY,
        dpParams.epsilon,
        dpParams.delta,
      )
    return if (noisedImpressionCount < 0) 0L else noisedImpressionCount
  }
}
