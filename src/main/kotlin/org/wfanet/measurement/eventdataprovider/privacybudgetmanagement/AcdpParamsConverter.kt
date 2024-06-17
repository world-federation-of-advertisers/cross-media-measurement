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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.util.concurrent.ConcurrentHashMap
import kotlin.math.PI
import kotlin.math.ceil
import kotlin.math.exp
import kotlin.math.ln
import kotlin.math.min
import kotlin.math.pow
import kotlin.math.sqrt
import org.wfanet.measurement.eventdataprovider.noiser.DpParams
import org.wfanet.measurement.eventdataprovider.noiser.GaussianNoiser

/**
 * A utility Object to convert per-query DP params(epsilon, delta) to ACDP params(rho, theta) for
 * ACDP accounting. It can perform conversions for both MPC and direct measurements. This conversion
 * only works for Gaussian noise and will not work with other noising mechanism(e.g. Laplace).
 *
 * TODO(@ple13): Refactor the AcdpParamsConverter object to take the noise type into account.
 */
object AcdpParamsConverter {
  /** Memoized computation of Mpc based MPC ACDP params conversion results. */
  private val MpcAcdpParamsConversionResults =
    ConcurrentHashMap<MpcAcdpParamsConversionKey, AcdpCharge>()

  /**
   * Convert MPC per-query DP charge(epsilon, delta) to ACDP charge(rho, theta). The computation
   * result is memoized.
   *
   * @param privacyParams Internal DifferentialPrivacyParams.
   * @param contributorCount Number of Duchies.
   * @return ACDP charge(rho, theta).
   */
  fun getMpcAcdpCharge(privacyParams: DpParams, contributorCount: Int): AcdpCharge {
    require(privacyParams.epsilon > 0 && privacyParams.delta > 0 && contributorCount > 0) {
      "Epsilon, delta, and contributor count must be positive, but got: epsilon=${privacyParams.epsilon} delta=${privacyParams.delta} contributorCount=$contributorCount"
    }

    return MpcAcdpParamsConversionResults.getOrPut(
      MpcAcdpParamsConversionKey(privacyParams, contributorCount)
    ) {
      computeMpcRhoAndTheta(privacyParams, contributorCount)
    }
  }

  /**
   * Convert Direct per-query DP charge(epsilon, delta) to ACDP charge(rho, theta).
   *
   * @param privacyParams Internal DifferentialPrivacyParams.
   * @param sensitivity Sensitivity parameter in ACDP conversion formulas.
   * @return ACDP charge(rho, theta).
   */
  fun getDirectAcdpCharge(privacyParams: DpParams, sensitivity: Double): AcdpCharge {
    require(privacyParams.epsilon > 0 && privacyParams.delta > 0) {
      "Epsilon and delta must be positive, but got: epsilon=${privacyParams.epsilon} delta=${privacyParams.delta}"
    }

    val rho = 0.5 / (GaussianNoiser.getSigma(privacyParams) / sensitivity).pow(2)
    // For direct measurements, theta will be 0.
    val theta = 0.0

    return AcdpCharge(rho, theta)
  }

  /**
   * The sum of delta1 and delta2 should be delta. In practice, set delta1 = delta2 = 0.5 * delta
   * for simplicity.
   */
  private fun getMpcDeltas(delta: Double): MpcDeltas = MpcDeltas(0.5 * delta, 0.5 * delta)

  fun computeMpcSigmaDistributedDiscreteGaussian(
    privacyParams: DpParams,
    contributorCount: Int,
  ): Double {
    // The sigma calculation formula is a closed-form formula from The Algorithmic
    // Foundations of Differential Privacy p.265 Theorem A.1
    // https://www.cis.upenn.edu/~aaroth/Papers/privacybook.pdf
    // This sigma formula is valid only for continuous Gaussian noise and used as
    // an approximation for discrete Gaussian noise here. It generally works for
    // epsilon <= 1 but not epsilon > 1

    val deltas = getMpcDeltas(privacyParams.delta)
    val delta1 = deltas.delta1

    // This simple formula to derive sigmaDistributed is valid only for
    // continuous Gaussian and is used as an approximation here. The formula using ACDP params is
    // sigma = 1 / sqrt(2 * rho)
    val sigma = sqrt(2 * ln(1.25 / delta1)) / privacyParams.epsilon

    return sigma / sqrt(contributorCount.toDouble())
  }

  private fun computeMpcMuDiscreteGaussian(
    privacyParams: DpParams,
    sigmaDistributed: Double,
    contributorCount: Int,
  ): Double {
    // The sum of delta1 and delta2 should be delta.
    // The selection of these two parameters have the following effect: setting delta2 larger
    // results in smaller truncation threshold but larger noise standard
    // deviation.
    val deltas = getMpcDeltas(privacyParams.delta)
    val delta2 = deltas.delta2

    return ceil(
      sigmaDistributed * sqrt(2 * ln(contributorCount * (1 + exp(privacyParams.epsilon)) / delta2))
    )
  }

  private fun computeMpcRhoAndTheta(privacyParams: DpParams, contributorCount: Int): AcdpCharge {
    val sigmaDistributed =
      computeMpcSigmaDistributedDiscreteGaussian(privacyParams, contributorCount)
    val mu = computeMpcMuDiscreteGaussian(privacyParams, sigmaDistributed, contributorCount)

    // For reach and frequency, the sensitivity Delta should be 1.
    val sensitivity = 1.0
    val sigma = sigmaDistributed * sqrt(contributorCount.toDouble())
    val eps = sensitivity / sigma

    var lambda = 0.0
    for (i in 1 until contributorCount) {
      lambda +=
        exp(
          -(2 * PI.pow(2) * sigma.pow(2) / contributorCount.toDouble()) *
            (i.toDouble() / (i.toDouble() + 1))
        )
    }
    val epsPrime = min(sqrt(eps.pow(2) + 5 * lambda), eps + 10 * lambda)

    val rho = 0.5 * epsPrime.pow(2)
    val theta = contributorCount * exp(-mu.pow(2) / (2 * sigmaDistributed.pow(2)))

    return AcdpCharge(rho, theta)
  }
}

private data class MpcAcdpParamsConversionKey(
  val privacyParams: DpParams,
  val contributorCount: Int,
)

private data class MpcDeltas(val delta1: Double, val delta2: Double)
