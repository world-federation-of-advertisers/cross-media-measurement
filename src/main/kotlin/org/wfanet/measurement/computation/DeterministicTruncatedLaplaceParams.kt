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
 * them to derive the matching variance. The noise targets ([EPSILON], [DELTA])-differential privacy.
 *
 * The truncation bound is not a compiled constant: it is [truncationBound], a function of the
 * per-draw L1 sensitivity. The released quantities have different sensitivities (reach and a
 * frequency bucket move by 1 per VID, the capped impression count moves by the per-user cap), and a
 * single bound would conform to [DELTA] at only one of them.
 */
object DeterministicTruncatedLaplaceParams {
  const val EPSILON = 1.0
  const val DELTA = 1.0 / 1000.0

  /**
   * The truncation bound for the truncated-Laplace mechanism at [sensitivity] and [epsilon]: the
   * smallest bound that keeps the truncated tail mass within [DELTA].
   *
   * This is the truncated-Laplace relation
   * `bound = (sensitivity / epsilon) * ln(1 + (e^epsilon - 1) / (2 * delta))` (see the
   * `LaplaceBoundedNoise` mechanism in IBM's differential-privacy-library, and Geng et al.,
   * "Privacy and Utility Tradeoff in Approximate Differential Privacy", arXiv:1810.00877). At
   * [EPSILON] and [DELTA] it is ~6.76 per unit of sensitivity.
   *
   * Uses [StrictMath] so the bound is bit-reproducible across JVMs, matching the draw it bounds and
   * the variance the reporting server derives from it.
   */
  fun truncationBound(epsilon: Double, sensitivity: Double): Double =
    (sensitivity / epsilon) *
      StrictMath.log(1.0 + (StrictMath.exp(epsilon) - 1.0) / (2.0 * DELTA))
}
