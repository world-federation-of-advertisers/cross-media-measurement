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
import kotlin.math.max
import kotlin.math.pow
import org.apache.commons.math3.optim.MaxEval
import org.apache.commons.math3.optim.MaxIter
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import org.apache.commons.math3.optim.univariate.BrentOptimizer
import org.apache.commons.math3.optim.univariate.SearchInterval
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction

data class AdvancedCompositionKey(
  val dpCharge: DpCharge,
  val repetitionCount: Int,
  val totalDelta: Float
)

object Composition {
  /** Memoized computation of log-factorials. */
  private val logFactorials = ConcurrentHashMap<Int, Double>()
  private const val THRESHOLD = 1E-3
  private const val MAX_ITER_EVAL = 10000

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
    dpCharge: DpCharge,
    repetitionCount: Int,
    totalDelta: Float
  ): Float {
    val epsilon = dpCharge.epsilon.toDouble()
    val delta = dpCharge.delta.toDouble()
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
   * @param dpCharge The privacy charge of a single query.
   * @param repetitionCount Number of times the algorithm is invoked.
   * @param totalDelta The target value of total delta of the privacy parameters for the multiple
   *   runs of the algorithm.
   * @return totalEpsilon such that, when applying the algorithm the given number of times, the
   *   result is still (totalEpsilon, totalDelta)-DP. None when the total_delta is less than 1 -
   *   (1 - delta)^repetitionCount, for which no guarantee of (totalEpsilon, totalDelta)-DP is
   *   possible for any value of totalEpsilon.
   */
  fun totalPrivacyBudgetUsageUnderAdvancedComposition(
    dpCharge: DpCharge,
    repetitionCount: Int,
    totalDelta: Float
  ): Float =
    advancedCompositionResults.getOrPut(
      AdvancedCompositionKey(dpCharge, repetitionCount, totalDelta)
    ) {
      calculateAdvancedComposition(dpCharge, repetitionCount, totalDelta)
    }

  /**
   * Computes total DP delta parameter after applying ACDP composition with given target Epsilon
   *
   * @param acdpCharges The privacy ACDP charge(rho, theta) of queries.
   * @param targetEpsilon The maximum total Epsilon.
   * @return totalDelta such that, under ACDP composition, the result is still (totalEpsilon,
   *   totalDelta)-DP.
   */
  fun totalPrivacyBudgetUsageUnderAcdpComposition(
    acdpCharges: List<AcdpCharge>,
    targetEpsilon: Float
  ): Float {
    val totalRho: Double = acdpCharges.sumOf { it.rho }
    val totalTheta: Double = acdpCharges.sumOf { it.theta }

    fun computeDeltaGivenAlpha(alpha: Double): Double {
      return (exp(targetEpsilon) + 1) * totalTheta +
        (exp((alpha - 1) * (alpha * totalRho - targetEpsilon)) / (alpha - 1)) *
          (1 - 1 / alpha).pow(alpha)
    }

    val minAlpha = max(1 + THRESHOLD, (targetEpsilon + totalRho) / (2 * totalRho))
    val maxAlpha = max((targetEpsilon + totalRho + 1) / (2 * totalRho), 2.0)
    val optimizer = BrentOptimizer(THRESHOLD, THRESHOLD)
    val res =
      optimizer.optimize(
        MaxIter(MAX_ITER_EVAL),
        MaxEval(MAX_ITER_EVAL),
        SearchInterval(minAlpha, maxAlpha),
        UnivariateObjectiveFunction { alpha: Double -> computeDeltaGivenAlpha(alpha) },
        GoalType.MINIMIZE
      )

    return res.value.toFloat()
  }
}
