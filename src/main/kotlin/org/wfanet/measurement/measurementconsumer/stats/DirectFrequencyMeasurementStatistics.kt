/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.measurementconsumer.stats

import kotlin.math.max
import kotlin.math.pow
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser

/** Computes statistics for direct frequency measurements. */
class DirectFrequencyMeasurementStatistics :
  DirectMeasurementStatistics(), FrequencyMeasurementStatistics {

  override val reachMeasurementStatistics = DirectReachMeasurementStatistics()

  /**
   * Outputs the variance of the given [reachRatio] at a certain frequency.
   *
   * Different types of frequency histograms have different values of [multiplier].
   */
  override fun frequencyRelativeVariance(
    totalReach: Int,
    reachRatio: Double,
    frequencyMeasurementParams: FrequencyMeasurementParams,
    multiplier: Int
  ): Double {
    val frequencyNoiseVariance: Double =
      GaussianNoiser.getSigma(frequencyMeasurementParams.frequencyDpParams)
    val varPart1 =
      reachRatio * (1 - reachRatio) * (1 - frequencyMeasurementParams.vidSamplingIntervalWidth) /
        (totalReach * frequencyMeasurementParams.vidSamplingIntervalWidth)
    var varPart2 = (1 - 2 * reachRatio) * multiplier
    varPart2 += reachRatio.pow(2) * frequencyMeasurementParams.maximumFrequency
    varPart2 *=
      (frequencyNoiseVariance /
        (totalReach * frequencyMeasurementParams.vidSamplingIntervalWidth).pow(2))
    return max(0.0, varPart1 + varPart2)
  }

  /**
   * Outputs the variance of the given reach count at a certain frequency.
   *
   * Reach count = [totalReach] * [reachRatio]
   */
  override fun frequencyCountVariance(
    totalReach: Int,
    totalReachVariance: Double,
    reachRatio: Double,
    reachRatioVariance: Double
  ): Double {
    var variance = reachRatioVariance.pow(2) * totalReachVariance.pow(2)
    variance += reachRatioVariance.pow(2) * totalReach.toDouble().pow(2)
    variance += totalReachVariance.pow(2) * reachRatio.pow(2)
    return max(0.0, variance)
  }
}
