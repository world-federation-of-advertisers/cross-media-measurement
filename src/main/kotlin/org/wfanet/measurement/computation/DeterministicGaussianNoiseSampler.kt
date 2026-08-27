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
 * Draws keyless, deterministic standard-normal noise: identical input parts always draw the same
 * value (non-averageable, no privacy-budget ledger).
 *
 * The Gaussian counterpart of [DeterministicTruncatedLaplaceNoiseSampler], and the same
 * construction: [DeterministicUniformSampler] seeds a uniform from the parts, which
 * [StandardNormalNoiseDistribution.inverseCdf] maps to a draw. There is no key; the seed is public,
 * so the guarantee is non-averageability resting on the secrecy of the input (the frequency vector
 * the consumer never sees), not computational DP.
 *
 * Draws are standard normal. A caller calibrating noise to a standard deviation multiplies the draw
 * by it, which lets one sampler serve a mechanism whose standard deviation changes between draws.
 *
 * Unlike a stream-based generator, a draw is addressed by its parts rather than by its position in
 * a sequence, so a mechanism that varies how many draws it takes still reproduces each individual
 * draw.
 */
class DeterministicGaussianNoiseSampler(
  private val distribution: StandardNormalNoiseDistribution = StandardNormalNoiseDistribution(),
  private val uniformSampler: DeterministicUniformSampler = DeterministicUniformSampler(),
) {
  /**
   * Returns the standard-normal draw for [parts], deterministic in [parts] rather than random.
   *
   * The draw is continuous, so it must not be released as noise on its own; round the aggregate it
   * noises to an integer before release. See [StandardNormalNoiseDistribution.inverseCdf].
   */
  fun sample(vararg parts: ByteArray): Double =
    distribution.inverseCdf(uniformSampler.sample(*parts))
}
