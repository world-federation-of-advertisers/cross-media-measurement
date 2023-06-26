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
      1 / sketchSize
    } else {
      decayRate / sketchSize / (1 - exp(-decayRate))
    }

  /** Exponential integral function for negative inputs. */
  private fun expIntForNegativeInput(x: Double): Double {
    // Exponential integral can be calculated using incomplete gamma upper when the input is less
    // than 0.
    assert(x < 0) { "Input must be negative." }
    return -IncompleteGamma.Upper.value(0.0, -x)
  }

  /** Incomplete Beta function with invalid output handling. */
  private fun incompleteBeta(x: Double, a: Double, b: Double): Double {
    val result = IncompleteBeta.value(x, a, b)
    assert(!result.isNaN()) { "Invalid inputs causing NaN value: {x=$x, a=$a, b=$b}." }
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
    assert(k >= 0) { "Invalid inputs: k=$k < 0." }
    assert(y >= 1) { "Invalid inputs: y=$y < 1." }

    if (decayRate < DECAY_RATE_THRESHOLD) {
      return sketchSize.pow(1 - k) * exp(-y / sketchSize)
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
