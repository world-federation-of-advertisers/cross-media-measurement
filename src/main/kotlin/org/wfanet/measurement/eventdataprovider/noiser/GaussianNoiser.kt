/*
 * Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.noiser

import java.util.Random
import java.util.concurrent.ConcurrentHashMap
import kotlin.math.exp
import org.apache.commons.math3.analysis.solvers.BisectionSolver
import org.apache.commons.math3.distribution.NormalDistribution
import org.apache.commons.math3.random.RandomGeneratorFactory

class GaussianNoiser(privacyParams: DpParams, random: Random) : AbstractNoiser() {
  override val distribution: NormalDistribution = getNormalDistribution(privacyParams, random)

  private fun getNormalDistribution(privacyParams: DpParams, random: Random): NormalDistribution {
    val sigma = getSigma(privacyParams)

    return NormalDistribution(RandomGeneratorFactory.createRandomGenerator(random), 0.0, sigma)
  }

  override val variance: Double
    get() = distribution.numericalVariance

  companion object {
    private val standardNormalDistribution = NormalDistribution(0.0, 1.0)
    private const val MAX_EVAL = 10000
    /** Memoized computation of Gaussian sigma results. */
    private val gaussianSigmaResults = ConcurrentHashMap<DpParams, Double>()

    fun getSigma(privacyParams: DpParams): Double =
      gaussianSigmaResults.getOrPut(privacyParams) {
        solveSigma(privacyParams.epsilon, privacyParams.delta)
      }

    /**
     * Assuming sensitivity = 1, solve std given epsilon and delta.
     *
     * The exact solution satisfies that solveDelta(epsilon, std) = delta. This is a transcendental
     * equation. Note that the left-hand side is a decreasing function in std. To solve it, we first
     * find an upper bound of sigma and then apply the bisection search.
     */
    private fun solveSigma(epsilon: Double, delta: Double, startingSigma: Double = 1e-3): Double {
      var sigma = startingSigma
      while (solveDelta(epsilon, sigma) < delta) {
        sigma /= 2
      }

      while (solveDelta(epsilon, sigma) > delta) {
        sigma *= 2
      }

      return BisectionSolver()
        .solve(MAX_EVAL, { x: Double -> solveDelta(epsilon, x) - delta }, sigma / 2, sigma)
    }

    /**
     * Assuming sensitivity = 1, solve delta given epsilon and std.
     *
     * With sensitivity=1 and any std, the difference in log(likelihood) = -(1/2) (x - 1 / std)^2 -
     * (1/2) x^2 = (1 / std) x - 1 / (2 std^2). Let it equal epsilon, we have x = std * epsilon + 1
     * / (2 std). We want that for this x, stats.norm.sf(x - 1/std) = exp(epsilon) *
     * stats.norm.sf(x) + delta. That is, delta = stats.norm.sf(x - 1/std) - exp(epsilon) *
     * stats.norm.sf(x)(Theorem 8 of https://arxiv.org/pdf/1805.06530.pdf).
     *
     * stats.norm.sf is the survival function(also defined as 1 - cdf) of standard normal
     * distribution N(0,1)
     */
    private fun solveDelta(epsilon: Double, sigma: Double): Double {
      val x = sigma * epsilon + 1 / (2 * sigma)

      return (1 - standardNormalDistribution.cumulativeProbability(x - 1 / sigma)) -
        exp(epsilon) * (1 - standardNormalDistribution.cumulativeProbability(x))
    }
  }
}
