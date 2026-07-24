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
 * In-sample reach and frequency, before scaling and thresholding.
 *
 * Reach is carried as a scalar so its derivation (the histogram sum for the raw case, or
 * `vectorSize - noisedBucket0` for deterministic noise) is decoupled from the reach computation.
 * Optionally noised as a unit (see the deterministic noise path) and then passed to
 * [ReachAndFrequencyComputations.computeReach] and
 * [ReachAndFrequencyComputations.computeFrequencyDistribution].
 *
 * Carrier only: the [frequencyHistogram] array gives this type referential `equals`/`hashCode`.
 *
 * @property sampledReach the reach in the sample.
 * @property frequencyHistogram counts for frequencies `1..maxFrequency` (a
 *   [HistogramComputations.buildHistogram] result).
 */
data class SampledReachAndFrequency(val sampledReach: Long, val frequencyHistogram: LongArray)
