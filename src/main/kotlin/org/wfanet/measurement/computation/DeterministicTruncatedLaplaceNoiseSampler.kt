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
 * A [DeterministicSampler] returning keyless, deterministic truncated-Laplace noise: identical
 * input [parts] always draw the same value (non-averageable, no privacy-budget ledger).
 *
 * It composes a [DeterministicUniformSampler] with a [TruncatedLaplaceNoiseDistribution]: the parts
 * seed a uniform, which the distribution maps to a draw. There is no key; the seed is public, so
 * the guarantee is non-averageability resting on the secrecy of the input (the frequency vector the
 * consumer never sees), not computational DP. A caller noising an aggregate over several frequency
 * vectors draws once per vector (`sample(frequencyVector, outputLabel)`) and sums the draws.
 */
class DeterministicTruncatedLaplaceNoiseSampler(
  private val distribution: TruncatedLaplaceNoiseDistribution,
  private val uniformSampler: DeterministicUniformSampler = DeterministicUniformSampler(),
) : DeterministicSampler {
  constructor(
    epsilon: Double,
    sensitivity: Double,
    truncationBound: Int,
  ) : this(TruncatedLaplaceNoiseDistribution(epsilon, sensitivity, truncationBound))

  override fun sample(vararg parts: ByteArray): Double =
    distribution.inverseCdf(uniformSampler.sample(*parts))
}
