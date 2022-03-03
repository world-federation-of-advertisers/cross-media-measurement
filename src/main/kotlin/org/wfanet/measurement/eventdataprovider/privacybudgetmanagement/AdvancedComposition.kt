// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import kotlin.math.exp

/**
 * Computes total DP parameters after applying an algorithm with given privacy parameters multiple times.
 *
 * Using the optimal advanced composition theorem, Theorem 3.3 from the paper
 * Kairouz, Oh, Viswanath. "The Composition Theorem for Differential Privacy",
 * to compute the total DP parameters given that we are applying an algorithm
 * with a given privacy parameters for a given number of times.
 *
 * Note:  This code is a Kotlin implementation of the advanced_composition()
 * function in the Google differential privacy accounting library, located at
 *   https://github.com/google/differential-privacy.git
 *
 * @param privacyParameters The privacy guarantee of a single query.
 * @param numQueries Number of times the algorithm is invoked.
 * @param totalDelta The target value of total delta of the privacy parameters
 *   for the multiple runs of the algorithm.
 *
 * @return
 *  totalEpsilon such that, when applying the algorithm the given number of
 *  times, the result is still (totalEpsilon, totalDelta)-DP.
 *  None when the total_delta is less than 1 - (1 - delta)^num_queries, for
 *  which no guarantee of (totalEpsilon, totalDelta)-DP is possible for any
 *  value of totalEpsilon.
 */

internal fun totalPrivacyBudgetUsageUnderAdvancedComposition(charge: PrivacyCharge, repetitionCount: Int): Float {
	val epsilon = charge.epsilon
  val delta = charge.delta
  val k = repetitionCount

  # The calculation follows Theorem 3.3 of https://arxiv.org/pdf/1311.0776.pdf
	for i in k / 2 downTo 0 step -1 {
    deltaI = 0
		for l in 0..i-1 {
      deltaI += special.binom(k, l) * (
          exp(epsilon * (k - l)) - exp(epsilon * (k - 2 * i + l)))
			deltaI /= ((1 + exp(epsilon))**k)
			if 1 - ((1 - delta) ** k) * (1 - delta_i) <= total_delta:
				return epsilon * (k - 2 * i)
		}
	}
  return None

}
