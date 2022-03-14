/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import kotlin.math.exp
import kotlin.math.pow

/** Memoized computation of binomial coefficients. */
class Binom {
  var memoizedCoeffs = HashMap<Pair<Int, Int>, Float>()

  /**
   * Computes the number of distinct ways to choose k items from a set of n.
   *
   * @param n The size of the set.
   * @param k The size of the subset that is drawn.
   * @return The number of distinct ways to draw k items from a set of size n. Alternatively, the
   * coefficient of x^k in the expansion of (1 + x)^n.
   */
  fun coeff(n: Int, k: Int): Float {
    if ((n < 0) || (k < 0) || (n < k)) {
      return 0.0f
    } else if ((k == 0) || (n == k)) {
      return 1.0f
    } else if (n - k < k) {
      return coeff(n, n - k)
    } else if (memoizedCoeffs.containsKey(Pair(n, k))) {
      return memoizedCoeffs.get(Pair(n, k)) ?: 0.0f
    } else {
      val c = coeff(n - 1, k - 1) + coeff(n - 1, k)
      memoizedCoeffs.put(Pair(n, k), c)
      return c
    }
  }
}

/**
 * Computes total DP parameters after applying an algorithm with given privacy parameters multiple
 * times.
 *
 * Using the optimal advanced composition theorem, Theorem 3.3 from the paper Kairouz, Oh,
 * Viswanath. "The Composition Theorem for Differential Privacy", to compute the total DP parameters
 * given that we are applying an algorithm with a given privacy parameters for a given number of
 * times.
 *
 * This code is a Kotlin implementation of the advanced_composition() function in the Google
 * differential privacy accounting library, located at
 * https://github.com/google/differential-privacy.git
 *
 * @param privacyParameters The privacy guarantee of a single query.
 * @param numQueries Number of times the algorithm is invoked.
 * @param totalDelta The target value of total delta of the privacy parameters for the multiple runs
 * of the algorithm.
 *
 * @return totalEpsilon such that, when applying the algorithm the given number of times, the result
 * is still (totalEpsilon, totalDelta)-DP. None when the total_delta is less than 1 - (1 -
 * delta)^repetitionCount, for which no guarantee of (totalEpsilon, totalDelta)-DP is possible for
 * any value of totalEpsilon.
 */
fun totalPrivacyBudgetUsageUnderAdvancedComposition(
  charge: PrivacyCharge,
  repetitionCount: Int,
  totalDelta: Float
): Float? {
  val epsilon = charge.epsilon
  val delta = charge.delta
  val k = repetitionCount
  val binom = Binom()

  // The calculation follows Theorem 3.3 of https://arxiv.org/pdf/1311.0776.pdf
  for (i in k / 2 downTo 0) {
    var deltaI = 0.0f
    for (l in 0..i - 1) {
      deltaI +=
        binom.coeff(k, l) *
          (exp(epsilon * (k - l).toFloat()) - exp(epsilon * (k - 2 * i + l).toFloat()))
    }
    deltaI /= (1.0f + exp(epsilon)).pow(k)
    if (1.0f - (1 - delta).pow(k) * (1.0f - deltaI) <= totalDelta) {
      return epsilon * (k - 2 * i).toFloat()
    }
  }
  return null
}
