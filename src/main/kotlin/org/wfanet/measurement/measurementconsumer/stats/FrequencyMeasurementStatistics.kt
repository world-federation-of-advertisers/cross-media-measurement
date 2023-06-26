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

interface FrequencyMeasurementStatistics {

  val reachMeasurementStatistics: ReachMeasurementStatistics

  /** Outputs [FrequencyVariances] of the given reach-frequency measurement. */
  fun variances(
    totalReach: Int,
    relativeFrequencyDistribution: Map<Int, Double>,
    params: FrequencyMeasurementParams
  ): FrequencyVariances {
    var suffixSum = 0.0

    // There is no estimate of zero-frequency reach
    val kPlusRelativeFrequencyDistribution: Map<Int, Double> =
      (params.maximumFrequency downTo 1).associateWith { frequency ->
        suffixSum += relativeFrequencyDistribution.getOrDefault(frequency, 0.0)
        suffixSum
      }

    val reachVariance =
      reachMeasurementStatistics.variance(
        totalReach,
        ReachMeasurementParams(params.vidSamplingIntervalWidth, params.reachDpParams)
      )

    val relativeVariances: Map<Int, Double> =
      (1..params.maximumFrequency).associateWith { frequency ->
        frequencyRelativeVariance(
          totalReach,
          relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
          params,
          1
        )
      }

    val kPlusRelativeVariances: Map<Int, Double> =
      (1..params.maximumFrequency).associateWith { frequency ->
        frequencyRelativeVariance(
          totalReach,
          kPlusRelativeFrequencyDistribution.getValue(frequency),
          params,
          params.maximumFrequency - frequency + 1
        )
      }

    val countVariances: Map<Int, Double> =
      (1..params.maximumFrequency).associateWith { frequency ->
        frequencyCountVariance(
          totalReach,
          reachVariance,
          relativeFrequencyDistribution.getOrDefault(frequency, 0.0),
          relativeVariances.getValue(frequency)
        )
      }

    val kPlusCountVariances: Map<Int, Double> =
      (1..params.maximumFrequency).associateWith { frequency ->
        frequencyCountVariance(
          totalReach,
          reachVariance,
          kPlusRelativeFrequencyDistribution.getValue(frequency),
          kPlusRelativeVariances.getValue(frequency)
        )
      }

    return FrequencyVariances(
      relativeVariances,
      kPlusRelativeVariances,
      countVariances,
      kPlusCountVariances
    )
  }

  /**
   * Outputs the variance of the given [reachRatio] at a certain frequency.
   *
   * Different types of frequency histograms have different values of [multiplier].
   */
  fun frequencyRelativeVariance(
    totalReach: Int,
    reachRatio: Double,
    frequencyMeasurementParams: FrequencyMeasurementParams,
    multiplier: Int
  ): Double

  /**
   * Outputs the variance of the given reach count at a certain frequency.
   *
   * Reach count = [totalReach] * [reachRatio]
   */
  fun frequencyCountVariance(
    totalReach: Int,
    totalReachVariance: Double,
    reachRatio: Double,
    reachRatioVariance: Double,
  ): Double
}
