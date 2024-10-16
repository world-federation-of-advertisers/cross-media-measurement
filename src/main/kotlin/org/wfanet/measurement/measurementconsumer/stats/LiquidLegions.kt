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

import kotlin.math.exp
import kotlin.math.max
import kotlin.math.pow
import org.apache.commons.numbers.gamma.IncompleteBeta
import org.apache.commons.numbers.gamma.IncompleteGamma

/** The small value used in incomplete gamma upper function to approximate exponential integral. */
private const val EXPONENTIAL_INTEGRAL_EPSILON = 1e-16
/** Decay rate threshold. */
private const val DECAY_RATE_THRESHOLD = 0.01
/** Minimum value of the summation of the powers of register probabilities. */
private const val MIN_SUM_OF_REGISTER_PROBABILITY_POWERS = 1e-16

/** The parameters that are used to compute Liquid Legions sketch. */
data class LiquidLegionsSketchParams(val decayRate: Double, val sketchSize: Long) {
  val constValue =
    if (decayRate < DECAY_RATE_THRESHOLD) {
      1.0 / sketchSize
    } else {
      decayRate / sketchSize / (1.0 - exp(-decayRate))
    }
}

/** Verifies that the liquid legions sketch params are valid. */
fun verifyLiquidLegionsSketchParams(sketchParams: LiquidLegionsSketchParams) {
  require(sketchParams.sketchSize > 0) {
    "Sketch size must be positive, but got ${sketchParams.sketchSize}."
  }
  require(sketchParams.decayRate > 0) {
    "Decay rate must be positive, but got ${sketchParams.decayRate}."
  }
}

/** Functions to compute statistics of Liquid Legions sketch based measurements. */
object LiquidLegions {
  /** Exponential integral function for negative inputs. */
  private fun expIntForNegativeInput(x: Double): Double {
    // Exponential integral can be calculated using incomplete gamma upper when the input is less
    // than 0.
    if (x >= 0.0) {
      throw IllegalArgumentException("Input must be negative.")
    }
    // Argument `a` in the original equation should be 0, but IncompleteGamma.Upper.value cannot
    // take a==0. Here we set `a` to be very small. The error of this approximation increases when x
    // is small. In our experiment, the error of IncompleteGamma.Upper.value(a=1e-16, x=1e-12) is
    // less than 1e-8. In our usage, x > 1e-12.
    val result = -IncompleteGamma.Upper.value(EXPONENTIAL_INTEGRAL_EPSILON, -x)

    if (result.isNaN()) {
      throw IllegalArgumentException("Invalid input causing NaN value: {x=$x}.")
    }
    return result
  }

  /** Incomplete Beta function with invalid output handling. */
  private fun incompleteBeta(x: Double, a: Double, b: Double): Double {
    val result = IncompleteBeta.value(x, a, b)
    if (result.isNaN()) {
      throw IllegalArgumentException("Invalid inputs causing NaN value: {x=$x, a=$a, b=$b}.")
    }
    return result
  }

  /**
   * Summation of the powers of register probabilities.
   *
   * It calculates the summation of p_i^k * (1 - p_i)^y for i from 1 to
   * [LiquidLegionsSketchParams.sketchSize], where p_i is the probability that a random vid is
   * assigned to register i. Specifically, p_i = [LiquidLegionsSketchParams.constValue] *
   * exp(-[LiquidLegionsSketchParams.decayRate] * i / [LiquidLegionsSketchParams.sketchSize]). This
   * type of summations is used in many formulas.
   *
   * @param k >= 0, indicating the power of p_i in the summation.
   * @param y >= 1, indicating 1 plus the power of 1 - p_i in the summation.
   * @return The summation of p_i^k * (1 - p_i)^y for i from 1 to m. Note that this summation
   *   converges to an integral for large [LiquidLegionsSketchParams.sketchSize]
   *   ([LiquidLegionsSketchParams.sketchSize] > 1000), and we implement the integral in this
   *   function.
   */
  private fun sumOfRegisterProbabilityPowers(
    k: Double,
    y: Double,
    liquidLegionsSketchParams: LiquidLegionsSketchParams,
  ): Double {
    verifyLiquidLegionsSketchParams(liquidLegionsSketchParams)
    if (k < 0.0) {
      throw IllegalArgumentException("Invalid inputs: k=$k < 0.")
    }
    if (y < 1.0) {
      throw IllegalArgumentException("Invalid inputs: y=$y < 1.")
    }

    val decayRate = liquidLegionsSketchParams.decayRate
    val sketchSize = liquidLegionsSketchParams.sketchSize
    val constValue = liquidLegionsSketchParams.constValue

    val prob =
      if (decayRate < DECAY_RATE_THRESHOLD) {
        sketchSize.toDouble().pow(1.0 - k) * exp(-y / sketchSize)
      } else if (k == 0.0) {
        sketchSize / decayRate *
          (expIntForNegativeInput(-constValue * y) -
            expIntForNegativeInput(-exp(-decayRate) * constValue * y))
      } else {
        sketchSize / decayRate *
          (incompleteBeta(constValue, k, y + 1.0) -
            incompleteBeta(exp(-decayRate) * constValue, k, y + 1.0))
      }

    return max(prob, MIN_SUM_OF_REGISTER_PROBABILITY_POWERS)
  }

  /** The covariance between two LiquidLegions-based reach measurements with an inflation term. */
  fun inflatedReachCovariance(
    sketchParams: LiquidLegionsSketchParams,
    reach: Long,
    otherReach: Long,
    overlapReach: Long,
    samplingWidth: Double,
    otherSamplingWidth: Double,
    overlapSamplingWidth: Double,
    inflation: Double = 0.0,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    require(reach > 0) { "Reach must be positive, but got $reach." }
    require(otherReach > 0) { "Other reach must be positive, but got $otherReach." }
    require(samplingWidth > 0.0 && samplingWidth <= 1.0) {
      "Sampling width must be greater than 0 and less than or equal to 1, but got $samplingWidth."
    }
    require(otherSamplingWidth > 0.0 && otherSamplingWidth <= 1.0) {
      "Other sampling width must be greater than 0 and less than or equal to 1, but got " +
        "$otherSamplingWidth."
    }
    require(overlapSamplingWidth >= 0.0 && overlapSamplingWidth <= 1.0) {
      "Other sampling width must be greater than or equal to 0 and less than or equal to 1, but " +
        "got $overlapSamplingWidth."
    }

    val y1 = max(1.0, reach * samplingWidth)
    val y2 = max(1.0, otherReach * otherSamplingWidth)
    val y12 = max(1.0, overlapReach * overlapSamplingWidth)

    var ret =
      sumOfRegisterProbabilityPowers(0.0, y1 + y2 - y12, sketchParams) -
        sumOfRegisterProbabilityPowers(0.0, y1 + y2, sketchParams) +
        y12 * sumOfRegisterProbabilityPowers(2.0, y1 + y2, sketchParams) -
        y12 *
          sumOfRegisterProbabilityPowers(1.0, y1, sketchParams) *
          sumOfRegisterProbabilityPowers(1.0, y2, sketchParams)
    ret += inflation
    ret /=
      samplingWidth *
        otherSamplingWidth *
        sumOfRegisterProbabilityPowers(1.0, y1, sketchParams) *
        sumOfRegisterProbabilityPowers(1.0, y2, sketchParams)
    ret += overlapReach * (overlapSamplingWidth / samplingWidth / otherSamplingWidth - 1.0)
    return ret
  }

  /**
   * Calculates the expected number of non-destroyed registers.
   *
   * A non-destroyed register is a register that includes exactly one reached fingerprint.
   */
  private fun expectedNumberOfNonDestroyedRegisters(
    sketchParams: LiquidLegionsSketchParams,
    collisionResolution: Boolean,
    totalReach: Long,
    vidSamplingIntervalWidth: Double,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    require(totalReach > 0) { "Total reach must be positive, but got $totalReach." }
    require(vidSamplingIntervalWidth > 0.0 && vidSamplingIntervalWidth <= 1.0) {
      "Vid sampling width must be greater than 0 and less than or equal to 1, but got " +
        "$vidSamplingIntervalWidth."
    }
    // Expected sampled reach
    val expectedReach = totalReach * vidSamplingIntervalWidth
    if (expectedReach < 2.0) {
      return 0.0
    }
    if (collisionResolution) {
      return sketchParams.sketchSize -
        sumOfRegisterProbabilityPowers(0.0, expectedReach, sketchParams)
    }
    return expectedReach * sumOfRegisterProbabilityPowers(1.0, expectedReach - 1, sketchParams)
  }

  /** Calculates the variance of the number of non-destroyed registers. */
  private fun varianceOfNumberOfNonDestroyedRegisters(
    sketchParams: LiquidLegionsSketchParams,
    collisionResolution: Boolean,
    totalReach: Long,
    vidSamplingIntervalWidth: Double,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    require(totalReach > 0) { "Total reach must be positive, but got $totalReach" }
    require(vidSamplingIntervalWidth > 0.0 && vidSamplingIntervalWidth <= 1.0) {
      "Vid sampling width must be greater than 0 and less than or equal to 1, but got " +
        "$vidSamplingIntervalWidth."
    }
    // Expected sampled reach
    val expectedReach = totalReach * vidSamplingIntervalWidth
    // The mathematical formulas below assume the sampled reach >= 3. If sampled reach < 3, the
    // variance is close to 0, so returning 0.
    if (expectedReach < 3.0) {
      return 0.0
    }
    if (collisionResolution) {
      return sumOfRegisterProbabilityPowers(0.0, expectedReach, sketchParams) -
        sumOfRegisterProbabilityPowers(0.0, 2 * expectedReach, sketchParams) +
        expectedReach * sumOfRegisterProbabilityPowers(2.0, 2 * expectedReach, sketchParams) -
        expectedReach * sumOfRegisterProbabilityPowers(1.0, expectedReach, sketchParams).pow(2.0)
    }

    var ret = expectedReach * sumOfRegisterProbabilityPowers(1.0, expectedReach - 1, sketchParams)
    ret +=
      expectedReach *
        (expectedReach - 1.0) *
        (sumOfRegisterProbabilityPowers(1.0, expectedReach - 2.0, sketchParams).pow(2.0) -
          sumOfRegisterProbabilityPowers(2.0, 2.0 * expectedReach - 4.0, sketchParams))
    ret -=
      expectedReach.pow(3.0) *
        sumOfRegisterProbabilityPowers(2.0, expectedReach - 2.0, sketchParams).pow(2.0)
    ret -=
      (expectedReach * sumOfRegisterProbabilityPowers(1.0, expectedReach - 1, sketchParams)).pow(
        2.0
      )
    ret +=
      expectedReach *
        (1.0 - vidSamplingIntervalWidth) *
        (sumOfRegisterProbabilityPowers(1.0, expectedReach - 1.0, sketchParams) -
            expectedReach * sumOfRegisterProbabilityPowers(2.0, expectedReach - 1.0, sketchParams))
          .pow(2.0)
    return ret
  }

  /**
   * Outputs the variance of the number of non-destroyed registers that includes a fingerprint at a
   * certain frequency level, given the total reach and the reach ratio of a frequency.
   */
  private fun varianceOfNumberOfNonDestroyedRegistersPerFrequency(
    sketchParams: LiquidLegionsSketchParams,
    collisionResolution: Boolean,
    totalReach: Long,
    reachRatio: Double,
    vidSamplingIntervalWidth: Double,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    require(totalReach > 0) { "Total reach must be positive, but got $totalReach." }
    require(reachRatio >= 0.0 && reachRatio <= 1.0) {
      "Reach ratio must be greater than or equal to 0 and less than or equal to 1, but got " +
        "$reachRatio."
    }
    require(vidSamplingIntervalWidth > 0.0 && vidSamplingIntervalWidth <= 1.0) {
      "Vid sampling width must be greater than 0 and less than or equal to 1, but got " +
        "$vidSamplingIntervalWidth."
    }
    val tmp = reachRatio * (1.0 - reachRatio) / totalReach
    val expectedRegisterNum =
      expectedNumberOfNonDestroyedRegisters(
        sketchParams,
        collisionResolution,
        totalReach,
        vidSamplingIntervalWidth,
      )
    return (reachRatio.pow(2.0) - tmp) *
      varianceOfNumberOfNonDestroyedRegisters(
        sketchParams,
        collisionResolution,
        totalReach,
        vidSamplingIntervalWidth,
      ) + tmp * expectedRegisterNum * (totalReach - expectedRegisterNum)
  }

  /**
   * Outputs the variance of the given [reachRatio] at a certain frequency computed from the Liquid
   * Legions based distribution methodology.
   *
   * Different types of frequency histograms have different values of [multiplier].
   */
  fun liquidLegionsFrequencyRelativeVariance(
    sketchParams: LiquidLegionsSketchParams,
    collisionResolution: Boolean,
    frequencyNoiseVariance: Double,
    relativeFrequencyMeasurementVarianceParams: RelativeFrequencyMeasurementVarianceParams,
  ): Double {
    verifyLiquidLegionsSketchParams(sketchParams)
    val (
      totalReach: Long,
      reachMeasurementVariance: Double,
      reachRatio: Double,
      frequencyMeasurementParams: FrequencyMeasurementParams,
      multiplier: Int) =
      relativeFrequencyMeasurementVarianceParams

    val expectedRegisterNum =
      expectedNumberOfNonDestroyedRegisters(
        sketchParams,
        collisionResolution,
        totalReach,
        frequencyMeasurementParams.vidSamplingInterval.width,
      )

    // When reach is too small, we have little info to estimate frequency, and thus the estimate of
    // relative frequency is equivalent to a uniformly random guess of a probability in [0, 1].
    if (
      isReachTooSmallForComputingRelativeFrequencyVariance(totalReach, reachMeasurementVariance) ||
        expectedRegisterNum < 1.0
    ) {
      return if (frequencyMeasurementParams.maximumFrequency == multiplier) 0.0
      else VARIANCE_OF_UNIFORMLY_RANDOM_PROBABILITY
    }

    val registerNumVariance =
      varianceOfNumberOfNonDestroyedRegisters(
        sketchParams,
        collisionResolution,
        totalReach,
        frequencyMeasurementParams.vidSamplingInterval.width,
      )
    val registerNumVariancePerFrequency =
      varianceOfNumberOfNonDestroyedRegistersPerFrequency(
        sketchParams,
        collisionResolution,
        totalReach,
        reachRatio,
        frequencyMeasurementParams.vidSamplingInterval.width,
      )

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
}
