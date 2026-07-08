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
 * A [Laplace distribution](https://en.wikipedia.org/wiki/Laplace_distribution) with mean 0 and
 * scale `sensitivity / epsilon`, [truncated](https://en.wikipedia.org/wiki/Truncated_distribution)
 * to `[-truncationBound, truncationBound]`.
 *
 * [inverseCdf] maps a uniform in `[0, 1)` to a draw by
 * [inverse transform sampling](https://en.wikipedia.org/wiki/Inverse_transform_sampling). Every
 * floating-point operation uses [StrictMath] (fdlibm, specified to be identical on every JVM), so
 * the draw is bit-reproducible across builds and hosts.
 *
 * @param epsilon the Laplace scale is `sensitivity / epsilon`.
 * @param sensitivity L1 sensitivity of the noised output: the most one VID can change it.
 * @param truncationBound draws are confined to `[-truncationBound, truncationBound]`.
 */
class TruncatedLaplaceNoiseDistribution(
  epsilon: Double,
  sensitivity: Double,
  truncationBound: Int,
) {
  init {
    require(epsilon > 0.0) { "epsilon must be positive, got $epsilon" }
    require(sensitivity > 0.0) { "sensitivity must be positive, got $sensitivity" }
    require(truncationBound > 0) { "truncationBound must be positive, got $truncationBound" }
  }

  private val scale: Double = sensitivity / epsilon
  private val bound: Double = truncationBound.toDouble()
  private val cdfLow: Double = laplaceCdf(-bound)
  private val cdfHigh: Double = laplaceCdf(bound)

  /** Maps a uniform [u] in `[0, 1)` to a draw in `[-bound, bound]` (the inverse CDF / quantile). */
  fun inverseCdf(u: Double): Double {
    require(u >= 0.0 && u < 1.0) { "u must be in [0, 1), got $u" }
    val p: Double = cdfLow + u * (cdfHigh - cdfLow)
    return laplaceQuantile(p)
  }

  private fun laplaceCdf(x: Double): Double =
    if (x < 0.0) 0.5 * StrictMath.exp(x / scale) else 1.0 - 0.5 * StrictMath.exp(-x / scale)

  private fun laplaceQuantile(p: Double): Double =
    if (p < 0.5) scale * StrictMath.log(2.0 * p) else -scale * StrictMath.log(2.0 * (1.0 - p))
}
