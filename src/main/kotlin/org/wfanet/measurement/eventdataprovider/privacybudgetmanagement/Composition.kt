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

import kotlin.math.exp
import kotlin.math.max
import kotlin.math.pow
import org.apache.commons.math3.optim.MaxEval
import org.apache.commons.math3.optim.MaxIter
import org.apache.commons.math3.optim.nonlinear.scalar.GoalType
import org.apache.commons.math3.optim.univariate.BrentOptimizer
import org.apache.commons.math3.optim.univariate.SearchInterval
import org.apache.commons.math3.optim.univariate.UnivariateObjectiveFunction

object Composition {
  private const val THRESHOLD = 1E-3
  private const val MAX_ITER_EVAL = 10000

  /**
   * Computes total DP delta parameter after applying ACDP composition with given target Epsilon
   *
   * @param acdpCharges The privacy ACDP charge(rho, theta) of queries.
   * @param targetEpsilon The maximum total Epsilon specified from Privacy Budget Manager.
   *   Recommended value is below 10.
   * @return totalDelta such that, under ACDP composition, the result is still (totalEpsilon,
   *   totalDelta)-DP.
   */
  fun totalPrivacyBudgetUsageUnderAcdpComposition(
    acdpCharges: List<AcdpCharge>,
    targetEpsilon: Double,
  ): Double {
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
        GoalType.MINIMIZE,
      )

    return res.value
  }
}
