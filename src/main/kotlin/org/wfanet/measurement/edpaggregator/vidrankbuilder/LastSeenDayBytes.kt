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

package org.wfanet.measurement.edpaggregator.vidrankbuilder

import com.google.protobuf.ByteString

/**
 * Big-endian read/write helpers for the packed `uint16` epoch-day format of
 * `RankIndexMap.last_seen_days`: each entry is exactly [WIDTH] bytes, packed back-to-back with no
 * per-entry framing, positionally aligned with the 12-byte `fingerprints` and the `ranks` array
 * (the same `bytes`-with-fixed-stride trick [EventIdDigestBytes] uses for fingerprints).
 *
 * `uint16` covers epoch days `[0, MAX_DAY]` = dates `[1970-01-01, 2149-06-06]`. [read] returns the
 * unsigned value as an `Int`; [write] rejects out-of-range days so an overflow fails loudly instead
 * of silently wrapping.
 */
object LastSeenDayBytes {
  /** Width of one packed epoch-day, in bytes. */
  const val WIDTH = 2

  /** Largest representable epoch-day (`0xFFFF`, i.e. 2149-06-06). */
  const val MAX_DAY = 0xFFFF

  fun read(b: ByteString, off: Int): Int =
    ((b.byteAt(off).toInt() and 0xFF) shl 8) or (b.byteAt(off + 1).toInt() and 0xFF)

  fun write(out: ByteArray, off: Int, day: Int) {
    require(day in 0..MAX_DAY) { "last_seen epoch-day $day out of uint16 range [0, $MAX_DAY]" }
    out[off] = (day ushr 8).toByte()
    out[off + 1] = day.toByte()
  }
}
