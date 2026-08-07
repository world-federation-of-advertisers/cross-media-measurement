// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.virtualpeople.core.common

import com.google.common.hash.Hashing
import com.google.protobuf.ByteString
import com.google.protobuf.kotlin.toByteString
import java.nio.charset.StandardCharsets

object Hashing {

  /**
   * Returns a ByteString containing the Fingerprint64 digest of [input].
   *
   * Consumers can convert it to Long using the toLong() method specifying the desired byte order.
   */
  // TODO(@marcopremier): Move this function in common-jvm
  fun hashFingerprint64(input: String): ByteString {
    return Hashing.farmHashFingerprint64()
      .hashString(input, StandardCharsets.UTF_8)
      .asBytes()
      .toByteString()
  }

  /**
   * Returns the Fingerprint64 digest of [input] as a Long, in the same byte order Guava produces
   * (matches `hashFingerprint64(input).toLong(LITTLE_ENDIAN)`) but skips the byte[]/ByteString
   * round-trip.
   */
  fun hashFingerprint64Long(input: String): Long {
    return Hashing.farmHashFingerprint64().hashString(input, StandardCharsets.UTF_8).asLong()
  }
}
