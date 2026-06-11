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

package org.wfanet.measurement.edpaggregator.rawimpressions

import com.google.protobuf.ByteString
import java.nio.ByteBuffer
import java.security.MessageDigest

/**
 * Computes the 12-byte truncated SHA-256 [EventIdDigest] of an event identifier.
 *
 * The caller supplies the raw identifier bytes (extracted from whichever source format — parquet
 * column, proto field, etc.). Keeping the input as a [ByteString] lets the call site do
 * format-specific extraction without dragging that concern into this class.
 *
 * Hot-path discipline:
 * - One `SHA-256 MessageDigest` plus one 32-byte output buffer per thread, bundled in [State] and
 *   reused via a single `ThreadLocal` lookup per `extract()` call.
 * - `digest.digest(out, 0, 32)` writes the digest into the pre-allocated buffer — avoids the
 *   per-event `byte[]` JCE would otherwise allocate.
 * - Unpacking to `(Long, Int)` is done with manual big-endian shifts on the output buffer, avoiding
 *   the `ByteBuffer.wrap` + `.order` wrapper allocations.
 * - Only per-event allocation in this class is the returned [EventIdDigest] data class. Eliminate
 *   that only if profiling shows it material.
 *
 * The JDK's `SHA-256` implementation uses SHA-NI hardware intrinsics on modern x86 (including the
 * n2d-highmem-16 EPYC target); on small inputs this puts the per-event floor in the ~100-200 ns
 * range, dominated by the digest itself.
 *
 * Thread-safety: instances are safe to share across threads.
 */
class EventIdDigestExtractor {

  /** Returns the 12-byte truncated SHA-256 of [idBytes]. */
  fun extract(idBytes: ByteString): EventIdDigest {
    val state = STATE.get()
    val digest = state.digest
    val out = state.out
    digest.reset()
    digest.update(idBytes.asReadOnlyByteBuffer())
    digest.digest(out, 0, SHA256_BYTES)
    return EventIdDigest(high = readLongBE(out, 0), low = readIntBE(out, 8))
  }

  /** Convenience overload for callers that already hold a [ByteBuffer]. */
  fun extract(idBytes: ByteBuffer): EventIdDigest {
    val state = STATE.get()
    val digest = state.digest
    val out = state.out
    digest.reset()
    digest.update(idBytes)
    digest.digest(out, 0, SHA256_BYTES)
    return EventIdDigest(high = readLongBE(out, 0), low = readIntBE(out, 8))
  }

  /** Per-thread reusable state — one `ThreadLocal.get()` per [extract]. */
  private class State {
    val digest: MessageDigest = MessageDigest.getInstance("SHA-256")
    val out: ByteArray = ByteArray(SHA256_BYTES)
  }

  companion object {
    private const val SHA256_BYTES = 32

    private val STATE = ThreadLocal.withInitial { State() }

    private fun readLongBE(b: ByteArray, off: Int): Long =
      ((b[off].toLong() and 0xFF) shl 56) or
        ((b[off + 1].toLong() and 0xFF) shl 48) or
        ((b[off + 2].toLong() and 0xFF) shl 40) or
        ((b[off + 3].toLong() and 0xFF) shl 32) or
        ((b[off + 4].toLong() and 0xFF) shl 24) or
        ((b[off + 5].toLong() and 0xFF) shl 16) or
        ((b[off + 6].toLong() and 0xFF) shl 8) or
        (b[off + 7].toLong() and 0xFF)

    private fun readIntBE(b: ByteArray, off: Int): Int =
      ((b[off].toInt() and 0xFF) shl 24) or
        ((b[off + 1].toInt() and 0xFF) shl 16) or
        ((b[off + 2].toInt() and 0xFF) shl 8) or
        (b[off + 3].toInt() and 0xFF)
  }
}
