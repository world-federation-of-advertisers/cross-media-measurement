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
import kotlin.math.pow
import org.apache.commons.numbers.gamma.IncompleteBeta
import org.apache.commons.numbers.gamma.IncompleteGamma

/** Class that computes statistics of MPC measurements. */
abstract class MpcMeasurementStatistics(
  private val decayRate: Double,
  private val sketchSize: Double
) : MeasurementStatistics {
  val const =
    if (decayRate < DECAY_RATE_THRESHOLD) {
      1.0 / sketchSize
    } else {
      decayRate / sketchSize / (1.0 - exp(-decayRate))
    }

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
    val result = -IncompleteGamma.Upper.value(1e-16, -x)

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
   * A type of summations of the powers of register probabilities.
   *
   * It calculates the summation of p_i^k * (1 - p_i)^y for i from 1 to [sketchSize], where p_i is
   * the probability that a random vid is assigned to register i. Specifically, p_i = [const] *
   * exp(-[decayRate] * i / [sketchSize]). This type of summations is used in many formulas.
   *
   * @param k >= 0, indicating the power of p_i in the summation.
   * @param y >= 1, indicating 1 plus the power of 1 - p_i in the summation.
   * @return The summation of p_i^k * (1 - p_i)^y for i from 1 to m. Note that this summation
   *   converges to an integral for large [sketchSize] ([sketchSize] > 1000), and we implement the
   *   integral in this function.
   */
  protected fun sumOfRegisterProbabilityPowers(k: Double, y: Double): Double {
    if (k < 0.0) {
      throw IllegalArgumentException("Invalid inputs: k=$k < 0.")
    }
    if (y < 1.0) {
      throw IllegalArgumentException("Invalid inputs: y=$y < 1.")
    }

    if (decayRate < DECAY_RATE_THRESHOLD) {
      return sketchSize.pow(1.0 - k) * exp(-y / sketchSize)
    }
    if (k == 0.0) {
      return sketchSize / decayRate *
        (expIntForNegativeInput(-const * y) - expIntForNegativeInput(-exp(-decayRate) * const * y))
    }
    return sketchSize / decayRate *
      (incompleteBeta(const, k, y + 1.0) - incompleteBeta(exp(-decayRate) * const, k, y + 1.0))
  }

  companion object {
    const val DECAY_RATE_THRESHOLD = 0.01
  }
}
