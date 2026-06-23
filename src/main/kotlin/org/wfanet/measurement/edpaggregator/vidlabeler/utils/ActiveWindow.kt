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

package org.wfanet.measurement.edpaggregator.vidlabeler.utils

import java.time.Instant

/**
 * A model line's half-open active interval `[startMicros, endMicros)`, in epoch microseconds.
 * Mirrors `ModelLine.active_start_time` / `active_end_time` in the public API.
 *
 * Bounds are kept as primitive `long` epoch-micros rather than [Instant] on purpose: [contains] is
 * evaluated once per impression per model line — on the hot path — so it must not allocate.
 * Comparing primitive `long`s avoids both the per-row `Instant` materialization and
 * `Instant.isBefore` calls. A single [ActiveWindow] is built once per model line (via [of]) and
 * reused across every row, so the only allocation is O(model lines), never O(rows).
 *
 * The window check is intentionally NOT baked into the raw-impression reader: window membership is
 * model-line-specific, whereas the reader's decode / decrypt / fingerprint / shard work is
 * model-line-independent and is paid only once across all model lines in a single pass.
 *
 * @property startMicros inclusive lower bound, epoch micros.
 * @property endMicros exclusive upper bound, epoch micros; [OPEN_ENDED] means no upper bound.
 */
class ActiveWindow(val startMicros: Long, val endMicros: Long) {
  init {
    require(endMicros > startMicros) {
      "endMicros ($endMicros) must be strictly after startMicros ($startMicros)"
    }
  }

  /** Inclusive lower bound, exclusive upper bound. Allocation-free. */
  fun contains(eventMicros: Long): Boolean = eventMicros >= startMicros && eventMicros < endMicros

  companion object {
    /** Sentinel [endMicros] for an open-ended (no upper bound) window. */
    const val OPEN_ENDED: Long = Long.MAX_VALUE

    /**
     * Builds an [ActiveWindow] from [Instant] bounds; a `null` [end] yields an open-ended window.
     * Sub-microsecond nanos in the bounds are truncated.
     */
    fun of(start: Instant, end: Instant?): ActiveWindow =
      ActiveWindow(instantToEpochMicros(start), end?.let { instantToEpochMicros(it) } ?: OPEN_ENDED)
  }
}

/** Converts an epoch-microseconds `long` to an [Instant]. */
fun epochMicrosToInstant(epochMicros: Long): Instant {
  val seconds = Math.floorDiv(epochMicros, 1_000_000L)
  val nanos = Math.floorMod(epochMicros, 1_000_000L) * 1_000L
  return Instant.ofEpochSecond(seconds, nanos)
}

/** Converts an [Instant] to epoch microseconds, truncating sub-microsecond nanos. */
fun instantToEpochMicros(instant: Instant): Long =
  Math.addExact(
    Math.multiplyExact(instant.epochSecond, 1_000_000L),
    (instant.nano / 1_000).toLong(),
  )
