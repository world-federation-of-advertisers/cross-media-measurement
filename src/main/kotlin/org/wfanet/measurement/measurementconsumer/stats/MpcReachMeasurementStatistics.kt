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

/** Computes statistics for MPC-based reach measurements. */
class MpcReachMeasurementStatistics(decayRate: Double, sketchSize: Double) :
  MpcMeasurementStatistics(decayRate, sketchSize), ReachMeasurementStatistics {
  /** The covariance between two MPC reach measurements with an inflation term. */
  private fun inflatedCovariance(
    thisReach: Int,
    thatReach: Int,
    overlapReach: Int,
    thisSamplingWidth: Double,
    thatSamplingWidth: Double,
    overlapSamplingWidth: Double,
    inflation: Double = 0.0
  ): Double {
    val y1 = max(1.0, thisReach * thisSamplingWidth)
    val y2 = max(1.0, thatReach * thatSamplingWidth)
    val y12 = max(1.0, overlapReach * overlapSamplingWidth)

    var ret =
      sumOfRegisterProbabilityPowers(0.0, y1 + y2 - y12) -
        sumOfRegisterProbabilityPowers(0.0, y1 + y2) +
        y12 * sumOfRegisterProbabilityPowers(2.0, y1 + y2) -
        y12 * sumOfRegisterProbabilityPowers(1.0, y1) * sumOfRegisterProbabilityPowers(1.0, y2)
    ret += inflation
    ret /=
      thisSamplingWidth *
        thatSamplingWidth *
        sumOfRegisterProbabilityPowers(1.0, y1) *
        sumOfRegisterProbabilityPowers(1.0, y2)
    ret += overlapReach * (overlapSamplingWidth / thisSamplingWidth / thatSamplingWidth - 1.0)
    return ret
  }

  /** Calculates the variance of an MPC reach measurement. */
  override fun variance(reach: Int, params: ReachMeasurementParams): Double {
    val gaussianNoiseVariance: Double = GaussianNoiser.getSigma(params.dpParams).pow(2)

    val variance =
      this.inflatedCovariance(
        reach,
        reach,
        reach,
        params.vidSamplingIntervalWidth,
        params.vidSamplingIntervalWidth,
        params.vidSamplingIntervalWidth,
        gaussianNoiseVariance
      )

    return max(0.0, variance)
  }

  /**
   * Calculates the covariance between union reach MPC measurement results.
   *
   * Precisely, the covariance between any two union reach results, when both reaches are from MPC
   * measurement.
   */
  override fun covariance(
    thisReach: Int,
    thatReach: Int,
    unionReach: Int,
    thisSamplingWidth: Double,
    thatSamplingWidth: Double,
    unionSamplingWidth: Double
  ): Double {
    return this.inflatedCovariance(
      thisReach,
      thatReach,
      thisReach + thatReach - unionReach,
      thisSamplingWidth,
      thatSamplingWidth,
      thisSamplingWidth + thatSamplingWidth - unionSamplingWidth,
      0.0
    )
  }
}
