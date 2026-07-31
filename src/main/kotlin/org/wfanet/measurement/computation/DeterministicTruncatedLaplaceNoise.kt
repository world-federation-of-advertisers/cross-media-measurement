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

import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * The seed for deterministic truncated-Laplace noise.
 *
 * The noise is keyless and reproducible: [DeterministicTruncatedLaplaceResultNoiser] derives every
 * draw from this seed and an output label, so the same measurement always yields the same noise and
 * it cannot be averaged away across repeated queries.
 */
object DeterministicTruncatedLaplaceNoise {
  /**
   * Fingerprint of the combined frequency vector and the number of contributions aggregated into
   * it, used as the noise seed.
   *
   * SHA-256 binds the seed to the vector's contents and [contributionCount], so the noise cannot
   * change unless the data changes, and adding or removing a contribution reseeds every draw even
   * when the capped aggregate is byte-identical (a fully-contained contribution).
   * [contributionCount] is the count after input suppression, so a dropped sub-threshold
   * contribution does not change it.
   */
  fun fingerprint(combinedFrequencyVector: IntArray, contributionCount: Int): ByteArray {
    val buffer = ByteBuffer.allocate((combinedFrequencyVector.size + 1) * Int.SIZE_BYTES)
    buffer.putInt(contributionCount)
    buffer.asIntBuffer().put(combinedFrequencyVector)
    return MessageDigest.getInstance("SHA-256").digest(buffer.array())
  }
}
