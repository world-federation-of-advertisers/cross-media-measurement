// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.shareshuffle

import com.google.protobuf.ByteString
import org.roaringbitmap.RoaringBitmap
import org.wfanet.frequencycount.FrequencyVector

/**
 * A compressed representation of a [FrequencyVector].
 *
 * Given a frequency vector `<r1, r2, r3, ..., rN>` of registers, this representation stores a
 * mapping of frequency to bitset of registers, where each bitset contains the registers with the
 * given frequency.
 *
 * Uses [RoaringBitmap] for the bitset implementation. Bitsets are optimized and serialized for
 * compactness.
 */
data class CompressedFrequencyVector(
  /** The number of registers in the original (uncompressed) [FrequencyVector]. */
  val registerCount: Int,

  /** The maximum frequency tracked by this [CompressedFrequencyVector]. */
  val maxFrequency: Int,

  /**
   * Serialized, optimized [RoaringBitmap]s, one for each frequency up to [maxFrequency]. The bitmap
   * at index i contains the set of register indexes with frequency value i+1.
   */
  val registerBitmaps: List<ByteString>,
) : Iterable<RegisterAndFrequency> {

  init {
    require(registerBitmaps.size == maxFrequency) {
      "Expected one bitmap for each frequency in 1..maxFrequency"
    }
  }

  override fun iterator(): Iterator<RegisterAndFrequency> {
    return iterator {
      for ((frequencyMinusOne, serializedBitmap) in registerBitmaps.withIndex()) {
        val bitmap =
          RoaringBitmap().also { it.deserialize(serializedBitmap.asReadOnlyByteBuffer()) }
        for (register in bitmap) {
          yield(RegisterAndFrequency(register = register, frequency = frequencyMinusOne + 1))
        }
      }
    }
  }
}

/** Container for a frequency vector [register] and its associated [frequency]. */
data class RegisterAndFrequency(val register: Int, val frequency: Int)
