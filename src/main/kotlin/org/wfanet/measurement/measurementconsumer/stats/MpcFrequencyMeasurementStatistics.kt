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
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AcdpParamsConverter

/** Computes statistics for MPC-based frequency measurements. */
class MpcFrequencyMeasurementStatistics(decayRate: Double, sketchSize: Double) :
  MpcMeasurementStatistics(decayRate, sketchSize), FrequencyMeasurementStatistics {

  override val reachMeasurementStatistics = MpcReachMeasurementStatistics(decayRate, sketchSize)

  /**
   * Calculates the expected number of non-destroyed registers.
   *
   * A non-destroyed register is a register that includes exactly one reached fingerprint.
   */
  private fun expectedNumberOfNonDestroyedRegisters(
    totalReach: Int,
    vidSamplingIntervalWidth: Double
  ): Double {
    // Expected sampled reach
    val expectedReach = totalReach * vidSamplingIntervalWidth
    if (expectedReach < 2.0) {
      return 0.0
    }
    return expectedReach * sumOfRegisterProbabilityPowers(1.0, expectedReach - 1)
  }

  /** Calculates the variance of the number of non-destroyed registers. */
  private fun varianceOfNumberOfNonDestroyedRegisters(
    totalReach: Int,
    vidSamplingIntervalWidth: Double
  ): Double {
    // Expected sampled reach
    val expectedReach = totalReach * vidSamplingIntervalWidth
    // The mathematical formulas below assume the sampled reach >= 3. If sampled reach < 3, the
    // variance is close to 0, so returning 0.
    if (expectedReach < 3.0) {
      return 0.0
    }
    val t1: ((Double) -> Double) = { y -> sumOfRegisterProbabilityPowers(1.0, y) }
    val t2: ((Double) -> Double) = { y -> sumOfRegisterProbabilityPowers(2.0, y) }

    var ret = expectedReach * t1(expectedReach - 1)
    ret +=
      expectedReach *
        (expectedReach - 1.0) *
        ((t1(expectedReach - 2.0)).pow(2.0) - t2(2.0 * expectedReach - 4.0))
    ret -= expectedReach.pow(3.0) * t2(expectedReach - 2.0).pow(2.0)
    ret -= (expectedReach * t1(expectedReach - 1)).pow(2.0)
    ret +=
      expectedReach *
        (1.0 - vidSamplingIntervalWidth) *
        (t1(expectedReach - 1.0) - expectedReach * t2(expectedReach - 1.0)).pow(2.0)
    return ret
  }

  /**
   * Outputs the variance of the number of non-destroyed registers that includes a fingerprint at a
   * certain frequency level, given the total reach and the reach ratio of a frequency.
   */
  private fun varianceOfNumberOfNonDestroyedRegistersPerFrequency(
    totalReach: Int,
    reachRatio: Double,
    vidSamplingIntervalWidth: Double
  ): Double {
    val tmp = reachRatio * (1.0 - reachRatio) / totalReach
    val expectedRegisterNum =
      expectedNumberOfNonDestroyedRegisters(totalReach, vidSamplingIntervalWidth)
    return (reachRatio.pow(2.0) - tmp) *
      varianceOfNumberOfNonDestroyedRegisters(totalReach, vidSamplingIntervalWidth) +
      tmp * expectedRegisterNum * (totalReach - expectedRegisterNum)
  }

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
    val expectedRegisterNum =
      expectedNumberOfNonDestroyedRegisters(
        totalReach,
        frequencyMeasurementParams.vidSamplingIntervalWidth
      )
    if (expectedRegisterNum < 1.0) {
      return 0.0
    }

    val registerNumVariance =
      varianceOfNumberOfNonDestroyedRegisters(
        totalReach,
        frequencyMeasurementParams.vidSamplingIntervalWidth
      )
    val registerNumVariancePerFrequency =
      varianceOfNumberOfNonDestroyedRegistersPerFrequency(
        totalReach,
        reachRatio,
        frequencyMeasurementParams.vidSamplingIntervalWidth
      )

    val frequencyNoiseVariance: Double =
      AcdpParamsConverter.computeSigmaDistributedDiscreteGaussian(
          frequencyMeasurementParams.frequencyDpParams,
          1
        )
        .pow(2)

    val covariance = (reachRatio * registerNumVariance + multiplier * frequencyNoiseVariance)
    val variance =
      (reachRatio / expectedRegisterNum).pow(2.0) *
        (registerNumVariance +
          frequencyMeasurementParams.maximumFrequency * frequencyNoiseVariance) +
        (1.0 / expectedRegisterNum).pow(2.0) *
          (registerNumVariancePerFrequency + multiplier * frequencyNoiseVariance) -
        2.0 * reachRatio / expectedRegisterNum.pow(2.0) * covariance

    return max(0.0, variance)
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
    reachRatioVariance: Double,
  ): Double {
    val variance =
      (reachRatioVariance + reachRatio.pow(2.0)) *
        (totalReachVariance + totalReach.toDouble().pow(2.0)) - (reachRatio * totalReach).pow(2.0)
    return max(0.0, variance)
  }
}
