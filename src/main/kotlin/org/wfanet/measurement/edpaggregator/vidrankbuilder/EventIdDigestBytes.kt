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
 * Big-endian read/write helpers for the 12-byte packed fingerprint format shared across Phase-0 and
 * Phase-1 (`EventIdDigest` / `Bytes12IntMap` / `SubpoolFingerprints` / `RankIndexMap`): bytes 0..7
 * are the high `Long`, bytes 8..11 the low `Int`. Packed back-to-back with no per-entry framing.
 */
object EventIdDigestBytes {
  /** Width of one packed fingerprint, in bytes. */
  const val WIDTH = 12

  fun readHi(b: ByteString, off: Int): Long =
    ((b.byteAt(off).toLong() and 0xFF) shl 56) or
      ((b.byteAt(off + 1).toLong() and 0xFF) shl 48) or
      ((b.byteAt(off + 2).toLong() and 0xFF) shl 40) or
      ((b.byteAt(off + 3).toLong() and 0xFF) shl 32) or
      ((b.byteAt(off + 4).toLong() and 0xFF) shl 24) or
      ((b.byteAt(off + 5).toLong() and 0xFF) shl 16) or
      ((b.byteAt(off + 6).toLong() and 0xFF) shl 8) or
      (b.byteAt(off + 7).toLong() and 0xFF)

  fun readLo(b: ByteString, off: Int): Int =
    ((b.byteAt(off).toInt() and 0xFF) shl 24) or
      ((b.byteAt(off + 1).toInt() and 0xFF) shl 16) or
      ((b.byteAt(off + 2).toInt() and 0xFF) shl 8) or
      (b.byteAt(off + 3).toInt() and 0xFF)

  fun writeHi(out: ByteArray, off: Int, value: Long) {
    out[off] = (value ushr 56).toByte()
    out[off + 1] = (value ushr 48).toByte()
    out[off + 2] = (value ushr 40).toByte()
    out[off + 3] = (value ushr 32).toByte()
    out[off + 4] = (value ushr 24).toByte()
    out[off + 5] = (value ushr 16).toByte()
    out[off + 6] = (value ushr 8).toByte()
    out[off + 7] = value.toByte()
  }

  fun writeLo(out: ByteArray, off: Int, value: Int) {
    out[off] = (value ushr 24).toByte()
    out[off + 1] = (value ushr 16).toByte()
    out[off + 2] = (value ushr 8).toByte()
    out[off + 3] = value.toByte()
  }
}
