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

import org.apache.commons.math3.special.Erf

/**
 * A standard normal distribution (mean 0, standard deviation 1).
 *
 * [inverseCdf] maps a uniform in `[0, 1)` to a draw by
 * [inverse transform sampling](https://en.wikipedia.org/wiki/Inverse_transform_sampling), so a
 * caller holding a deterministic uniform gets a deterministic draw. Scale the draw by the standard
 * deviation the caller wants; keeping this distribution standard lets one instance serve draws
 * calibrated differently.
 *
 * Reproducibility rests on [Erf.erfInv], a rational approximation evaluated in pure Java double
 * arithmetic, so it yields the same bits on every JVM for a given commons-math version.
 * commons-math is version-pinned in MODULE.bazel, and StandardNormalNoiseDistributionTest holds
 * golden vectors so a version bump that moves the values fails loudly rather than silently changing
 * noise.
 */
class StandardNormalNoiseDistribution {
  /**
   * Maps a uniform [u] in `[0, 1)` to a standard normal draw (the inverse CDF / quantile).
   *
   * This continuous draw MUST NOT be released as noise on its own. A floating-point inverse-CDF
   * draw lands on an uneven lattice of representable doubles whose low bits depend on the true
   * value being noised, which breaks differential privacy (Mironov, "On Significance of the Least
   * Significant Bits for Differential Privacy", CCS 2012,
   * https://dl.acm.org/doi/10.1145/2382196.2382264). Round to an integer before release.
   */
  fun inverseCdf(u: Double): Double {
    require(u >= 0.0 && u < 1.0) { "u must be in [0, 1), got $u" }
    // erfInv diverges at +/-1. u is at most 1 - 2^-53, so the upper end stays finite; clamp the
    // lower end, which u hits when the digest's top 53 bits are all zero.
    val x: Double = (2.0 * u - 1.0).coerceAtLeast(LOWEST_FINITE_ERF_INPUT)
    return SQRT_2 * Erf.erfInv(x)
  }

  companion object {
    private val SQRT_2 = StrictMath.sqrt(2.0)
    /** The value 2u - 1 takes at the smallest uniform above 0, used as the floor at u = 0. */
    private const val LOWEST_FINITE_ERF_INPUT = -1.0 + 1.0 / (1L shl 52).toDouble()
  }
}
