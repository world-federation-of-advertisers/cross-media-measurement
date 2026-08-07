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
     * untruncated Laplace epsilon-DP), truncated to `[-T, T]`. T is chosen by the two-sided
     * tail-mass rule: truncate where the discarded mass equals [delta]. For a zero-mean Laplace
     * `P(|Y| > T) = e^(-T/b)`, so setting that to [delta] gives `T = b * ln(1 / delta)`. Discarding
     * at most [delta] of the mass makes the truncated mechanism differ from the epsilon-DP Laplace
     * only on an event of probability at most [delta], which is ([epsilon], [delta])-DP. T is
     * rounded up and bumped by 1 for a strictly-conservative integer threshold:
     * ```
     * T = ceil((sensitivity / epsilon) * ln(1 / delta)) + 1
     * ```
     *
     * This is looser than the tight optimal threshold
     * `(sensitivity / epsilon) * ln(1 + (e^epsilon - 1) / (2 * delta))` (Geng et al., "Privacy and
     * Utility Tradeoff in Approximate Differential Privacy", arXiv:1810.00877, Definition 3), which
     * accounts for only the uncovered end-interval mass and so adds less noise. The conservative
     * tail-mass form is the value compiled into the attested image. [StrictMath] keeps T
     * bit-reproducible across JVMs, matching the draw it bounds and any variance derived from it.
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
      val bound = StrictMath.ceil(scale * StrictMath.log(1.0 / delta)) + 1.0
      return DeterministicTruncatedLaplaceNoiseSampler(
        TruncatedLaplaceNoiseDistribution(scale, bound)
      )
    }
  }
}
