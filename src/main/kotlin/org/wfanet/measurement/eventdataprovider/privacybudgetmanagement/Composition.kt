/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.util.concurrent.ConcurrentHashMap
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.pow

data class AdvancedCompositionKey(
  val charge: Charge,
  val repetitionCount: Int,
  val totalDelta: Float
)

object Composition {
  /** Memoized computation of log-factorials. */
  private val logFactorials = ConcurrentHashMap<Int, Double>()

  init {
    logFactorials.put(0, 0.0)
  }

  /** Memoized computation of advanced composition results. */
  private val advancedCompositionResults = ConcurrentHashMap<AdvancedCompositionKey, Float>()

  /**
   * Log of factorial.
   *
   * @param k Value whose factorial is to be computed.
   * @return log(1 * 2 * ... * k)
   */
  private fun logFactorial(k: Int): Double {
    return logFactorials.getOrPut(k) { ln(k.toDouble()) + logFactorial(k - 1) }
  }

  /**
   * Computes log of the number of distinct ways to choose k items from a set of n.
   *
   * @param n The size of the set.
   * @param k The size of the subset that is drawn.
   * @return The log of the number of distinct ways to draw k items from a set of size n.
   *   Alternatively, the log of the coefficient of x^k in the expansion of (1 + x)^n.
   */
  private fun logBinomial(n: Int, k: Int): Double {
    return logFactorial(n) - logFactorial(k) - logFactorial(n - k)
  }

  private fun calculateAdvancedComposition(
    charge: Charge,
    repetitionCount: Int,
    totalDelta: Float
  ): Float {
    val epsilon = charge.epsilon.toDouble()
    val delta = charge.delta.toDouble()
    val k = repetitionCount
    val logDeltaIDivisor = -k.toDouble() * ln(1.0 + exp(epsilon))
    // The calculation follows Theorem 3.3 of https://arxiv.org/pdf/1311.0776.pdf
    for (i in k / 2 downTo 0) {
      var deltaI = 0.0
      for (l in 0..i - 1) {
        deltaI +=
          exp(logBinomial(k, l) + logDeltaIDivisor + (epsilon * (k - l).toDouble())) -
            exp(logBinomial(k, l) + logDeltaIDivisor + epsilon * (k - 2 * i + l).toDouble())
      }
      if (1.0 - (1 - delta).pow(k) * (1.0 - deltaI) <= totalDelta) {
        return (epsilon * (k - 2 * i)).toFloat()
      }
    }
    return Float.MAX_VALUE
  }

  /**
   * Computes total DP parameters after applying an algorithm with given privacy parameters multiple
   * times.
   *
   * Using the optimal advanced composition theorem, Theorem 3.3 from the paper Kairouz, Oh,
   * Viswanath. "The Composition Theorem for Differential Privacy", to compute the total DP
   * parameters given that we are applying an algorithm with a given privacy parameters for a given
   * number of times.
   *
   * This code is a Kotlin implementation of the advanced_composition() function in the Google
   * differential privacy accounting library, located at
   * https://github.com/google/differential-privacy.git
   *
   * @param charge The privacy charge of a single query.
   * @param repetitionCount Number of times the algorithm is invoked.
   * @param totalDelta The target value of total delta of the privacy parameters for the multiple
   *   runs of the algorithm.
   * @return totalEpsilon such that, when applying the algorithm the given number of times, the
   *   result is still (totalEpsilon, totalDelta)-DP. None when the total_delta is less than 1 -
   *   (1 - delta)^repetitionCount, for which no guarantee of (totalEpsilon, totalDelta)-DP is
   *   possible for any value of totalEpsilon.
   */
  fun totalPrivacyBudgetUsageUnderAdvancedComposition(
    charge: Charge,
    repetitionCount: Int,
    totalDelta: Float
  ): Float =
    advancedCompositionResults.getOrPut(
      AdvancedCompositionKey(charge, repetitionCount, totalDelta)
    ) {
      calculateAdvancedComposition(charge, repetitionCount, totalDelta)
    }
}
