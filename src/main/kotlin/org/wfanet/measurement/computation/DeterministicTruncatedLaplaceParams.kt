/*
 * Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

/**
 * The system's privacy parameters for deterministic truncated-Laplace noise.
 *
 * These are compiled into the attested images that draw the noise (the TrusTEE image and the EDP
 * Aggregator image) rather than set by the measurement consumer, and the reporting server mirrors
 * them to derive the matching variance. The noise targets ([EPSILON], [DELTA])-differential
 * privacy; each draw's sampler comes from
 * [DeterministicTruncatedLaplaceNoiseSampler.forDifferentialPrivacy] with these constants and the
 * draw's L1 sensitivity.
 */
object DeterministicTruncatedLaplaceParams {
  const val EPSILON = 1.0
  const val DELTA = 1.0 / 1000.0

  /** The truncation bound a draw of L1 [sensitivity] takes under these parameters. */
  fun truncationBound(sensitivity: Double): Double = truncationBoundFor(EPSILON, DELTA, sensitivity)

  /**
   * The continuous truncated-Laplace variance for L1 [sensitivity], before the sampler rounds its
   * draw to an integer.
   */
  fun variance(sensitivity: Double): Double {
    val scale = sensitivity / EPSILON
    val bound = truncationBound(sensitivity)
    val tailMass = StrictMath.exp(-bound / scale)
    val normalizer = 1.0 - tailMass
    val untruncatedVariance = 2.0 * scale * scale
    val truncatedTail = tailMass * (bound * bound + 2.0 * bound * scale + 2.0 * scale * scale)
    return (untruncatedVariance - truncatedTail) / normalizer
  }

  /**
   * The smallest bound at which a Laplace of scale `sensitivity / epsilon`, truncated to `[-bound,
   * bound]`, is ([epsilon], [delta])-differentially private.
   *
   * Geng et al., "Privacy and Utility Tradeoff in Approximate Differential Privacy"
   * (arXiv:1810.00877), Definition 3, proved for all ([epsilon], [delta]) and [sensitivity].
   *
   * Anything deriving a variance from this noise must use the same threshold, so both the sampler
   * and the reporting server call this rather than restating the formula.
   */
  fun truncationBoundFor(epsilon: Double, delta: Double, sensitivity: Double): Double {
    require(epsilon > 0.0) { "epsilon must be positive, got $epsilon" }
    require(delta > 0.0 && delta < 1.0) { "delta must be in (0, 1), got $delta" }
    require(sensitivity > 0.0) { "sensitivity must be positive, got $sensitivity" }
    return (sensitivity / epsilon) *
      StrictMath.log(1.0 + (StrictMath.exp(epsilon) - 1.0) / (2.0 * delta))
  }
}
