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

package org.wfanet.measurement.duchy.mill.trustee.v2

import org.wfanet.measurement.computation.ResultMinimumThresholds
import org.wfanet.measurement.internal.duchy.NoiseMechanism

/**
 * The noise mechanism and small-cell suppression thresholds of a TrusTEE v2 Mill.
 *
 * The attested image fixes these, so neither travels with the computation.
 *
 * @property noiseMechanism the mechanism every cell is noised with, either [NoiseMechanism.NONE] or
 *   [NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE]
 * @property resultMinimumThresholds the small-cell suppression thresholds, or `null` for none, in
 *   which case neither the slice test nor the result threshold suppresses any cell
 */
class CellPrivacyParams(
  val noiseMechanism: NoiseMechanism,
  val resultMinimumThresholds: ResultMinimumThresholds?,
) {
  init {
    require(noiseMechanism in SUPPORTED_NOISE_MECHANISMS) {
      "Unsupported noise mechanism $noiseMechanism for TrusTEE v2"
    }
    if (resultMinimumThresholds != null) {
      require(resultMinimumThresholds.minUsers > 0) {
        "minUsers must be greater than 0, got ${resultMinimumThresholds.minUsers}"
      }
      require(resultMinimumThresholds.minImpressions > 0) {
        "minImpressions must be greater than 0, got ${resultMinimumThresholds.minImpressions}"
      }
    }
  }

  /**
   * The per-user cap the impression draw is calibrated to, chosen as the TrusTEE v1 processor
   * chooses it.
   */
  val maxFrequencyPerUser: Int
    get() = resultMinimumThresholds?.reachMaxFrequencyPerUser ?: 1

  companion object {
    private val SUPPORTED_NOISE_MECHANISMS =
      setOf(NoiseMechanism.NONE, NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
  }
}
