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
 * A [DeterministicSampler] returning a uniform in `[0, 1)`, derived from the SHA-256 of its
 * [parts].
 *
 * Each part is length-prefixed (a 4-byte big-endian count) before hashing, so the parts are
 * unambiguously separated even when a part contains the framing bytes: `sample(a, b)` collides with
 * `sample(c, d)` only when the parts are equal. The uniform is the top 53 bits (the double
 * mantissa) of the digest's first 8 bytes, the standard construction.
 *
 * The value is bit-reproducible across builds and hosts: SHA-256 with exact integer and IEEE-754
 * arithmetic, no RNG algorithm whose bit output could drift between JVMs.
 */
class DeterministicUniformSampler : DeterministicSampler {
  override fun sample(vararg parts: ByteArray): Double {
    val digest = MessageDigest.getInstance("SHA-256")
    for (part in parts) {
      digest.update(ByteBuffer.allocate(Int.SIZE_BYTES).putInt(part.size).array())
      digest.update(part)
    }
    val bits: Long = ByteBuffer.wrap(digest.digest(), 0, java.lang.Long.BYTES).long
    // Top 53 bits (the double mantissa) over 2^53: the standard bits-to-[0,1) construction.
    return (bits ushr 11).toDouble() / (1L shl 53).toDouble()
  }
}
