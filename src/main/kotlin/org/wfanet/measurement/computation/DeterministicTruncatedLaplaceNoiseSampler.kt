// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

/**
 * Draws keyless, deterministic truncated-Laplace noise: identical input parts always draw the same
 * value (non-averageable, no privacy-budget ledger).
 *
 * It composes a [DeterministicUniformSampler] with a [TruncatedLaplaceNoiseDistribution]: the parts
 * seed a uniform, which the distribution maps to a draw. There is no key; the seed is public, so
 * the guarantee is non-averageability resting on the secrecy of the input (the frequency vector the
 * consumer never sees), not computational DP. A caller noising an aggregate over several frequency
 * vectors draws once per vector (`sampleRounded(frequencyVector, outputLabel)`) and sums the draws.
 */
class DeterministicTruncatedLaplaceNoiseSampler(
  private val distribution: TruncatedLaplaceNoiseDistribution,
  private val uniformSampler: DeterministicUniformSampler = DeterministicUniformSampler(),
) {
  /**
   * Returns the noise to release: a truncated-Laplace draw for [parts], rounded to an integer.
   *
   * The draw is deterministic in [parts], not random: [DeterministicUniformSampler] seeds a uniform
   * from the parts, which [TruncatedLaplaceNoiseDistribution.inverseCdf] maps to the draw.
   * Identical parts always yield the same value.
   *
   * The rounding is the release safeguard. The underlying continuous draw is a floating-point value
   * whose low bits depend on the quantity being noised, which breaks differential privacy if
   * released (see [TruncatedLaplaceNoiseDistribution.inverseCdf]); rounding to an integer discards
   * those bits. [StrictMath.rint] is round-half-to-even and IEEE-754 exact (no libm), so the
   * rounded result stays bit-reproducible across JVMs like the draw it rounds.
   */
  fun sampleRounded(vararg parts: ByteArray): Long =
    StrictMath.rint(distribution.inverseCdf(uniformSampler.sample(*parts))).toLong()

  companion object {
    /**
     * A sampler drawing ([epsilon], [delta])-differentially private truncated-Laplace noise for a
     * query of L1 [sensitivity].
     *
     * The noise is Laplace with scale `b = sensitivity / epsilon` (the scale that makes the
     * untruncated Laplace epsilon-DP), truncated to `[-T, T]` at the optimal threshold:
     * ```
     * T = (sensitivity / epsilon) * ln(1 + (e^epsilon - 1) / (2 * delta))
     * ```
     *
     * This is Geng et al., "Privacy and Utility Tradeoff in Approximate Differential Privacy"
     * (arXiv:1810.00877), Definition 3, where the truncated Laplace is proved to be the optimal
     * ([epsilon], [delta])-DP additive-noise mechanism and this threshold is proved tight. It holds
     * for every ([epsilon], [delta]) and [sensitivity], so no regime check is needed.
     *
     * [StrictMath] keeps T bit-reproducible across JVMs, matching the draw it bounds and any
     * variance derived from it.
     */
    fun forDifferentialPrivacy(
      epsilon: Double,
      delta: Double,
      sensitivity: Double,
    ): DeterministicTruncatedLaplaceNoiseSampler {
      require(epsilon > 0.0) { "epsilon must be positive, got $epsilon" }
      require(delta > 0.0 && delta < 1.0) { "delta must be in (0, 1), got $delta" }
      require(sensitivity > 0.0) { "sensitivity must be positive, got $sensitivity" }
      val scale = sensitivity / epsilon
      val bound = scale * StrictMath.log(1.0 + (StrictMath.exp(epsilon) - 1.0) / (2.0 * delta))
      return DeterministicTruncatedLaplaceNoiseSampler(
        TruncatedLaplaceNoiseDistribution(scale, bound)
      )
    }
  }
}
