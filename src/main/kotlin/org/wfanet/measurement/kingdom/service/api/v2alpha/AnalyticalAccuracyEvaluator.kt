// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.kingdom.service.api.v2alpha

import kotlin.math.exp

private const val DECAY_RATE_THRESHOLD = 0.01

/**
 * Analytical formulas of the variances of MPC reach and frequency estimates.
 *
 * @param [a] decay rate of LiquidLegions.
 * @param [m] number of registers in LiquidLegions.
 * @param [pi] sampling fraction, which equals 1 / <number of sampling buckets>.
 * @param [ssreach] variance of noise added on the number of non-empty registers.
 * @param [ssfreq] variance of noise added on each bucket of the frequency histogram of active
 *   registers.
 */
class AnalyticalAccuracyEvaluator(
  private val a: Double,
  private val m: Int,
  private val pi: Double,
  private val ssreach: Double,
  private val ssfreq: Double,
) {
  private val c =
    if (a < DECAY_RATE_THRESHOLD) {
      1.0 / m
    } else {
      a / m / (1 - exp(-a))
    }

  /**
   * An operation repeatedly used in the formulas.
   *
   * @param n reach value
   * @param multiplier the multiplier
   */
  private fun localMultiply(n: Int, multiplier: Double): Double {
    return multiplier * c * n
  }

  /**
   * An operation repeatedly used in the formulas.
   *
   * @param n reach value
   * @param multiplier the multiplier
   */
  private fun localExp(n: Int, multiplier: Double): Double {
    return exp(-localMultiply(n, multiplier))
  }

  /**
   * An operation repeatedly used in the formulas.
   *
   * @param n reach value
   * @param multiplier the multiplier
   */
  private fun localScaledExp(n: Int, multiplier: Double): Double {
    return localMultiply(n, multiplier) * localExp(n, multiplier)
  }

  /**
   * An operation repeatedly used in the formulas.
   *
   * @param n reach value
   * @param multiplier the multiplier
   */
  fun localEi(n: Int, multiplier: Double): Double {
    return ei(-localMultiply(n, multiplier))
  }
}
