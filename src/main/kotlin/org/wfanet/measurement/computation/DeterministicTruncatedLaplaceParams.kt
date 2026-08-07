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
}
