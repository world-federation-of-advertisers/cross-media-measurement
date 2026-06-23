/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.vidlabeler.utils

/**
 * Single-threaded, primitive open-addressing hash table mapping 12-byte keys to 32-bit `Int`
 * values. Optimized for the memoized VID pipeline, where 12-byte keys are typically the leading
 * bytes of a cryptographic fingerprint (e.g. SHA-256) and values carry domain-specific meaning such
 * as a rank (Phase 1 ranker) or a presence sentinel when the table is used as a membership set
 * (Phase 0 subpool buckets, Phase 1 subpool filter).
 *
 * ## Storage layout (chunked)
 *
 * Three parallel arrays-of-primitive-arrays:
 * * `keysHi: Array<LongArray>` — high 8 bytes of each key.
 * * `keysLo: Array<IntArray>` — low 4 bytes of each key.
 * * `values: Array<IntArray>` — the associated `Int` value.
 *
 * Each inner array is one *chunk* of `1 shl chunkShift` slots. The total capacity is `numChunks ×
 * chunkSize`. Both `chunkSize` and `numChunks` are powers of two so that the total capacity is
 * itself a power of two and `index = hash and (capacity - 1)` is a single bitwise AND on `Long`s.
 *
 * The two-level layout lets the table grow past the JVM's `Int.MAX_VALUE` single-array limit while
 * preserving the bitmask hot path: a logical `Long` index `i` decomposes to `(chunkIdx, slotIdx) =
 * (i ushr chunkShift, i.toInt() and slotMask)`, which are pure bitwise operations.
 *
 * Collisions are resolved with linear probing on the logical index. On [remove] the freed slot is
 * filled by backward-shifting subsequent occupied slots; the algorithm crosses chunk boundaries
 * transparently because the logical index wraps at `capacity` rather than at any chunk edge.
 *
 * ## Memory cost
 *
 * Approximately `16 B` per slot (8 + 4 + 4 across the three parallel arrays). At the default `0.75`
 * load factor that is roughly `21 B` per stored entry. Examples:
 * * 360 M entries → ~7 GiB
 * * 1.5 B entries → ~31 GiB
 * * 2 B entries → ~42 GiB
 *
 * Chunked allocation also bounds the *peak* transient memory during [resize]: new chunks are
 * allocated incrementally rather than as a single large primitive array.
 *
 * ## Capacity ceiling
 *
 * Per chunk: up to `2^30` slots (the largest power of two below the JVM array length limit). Total
 * capacity: up to `2^30 × 2^30 = 2^60` slots, far beyond any realistic use. In practice the table
 * is bounded only by heap memory.
 *
 * ## Key assumptions
 *
 * Keys are assumed to be drawn from a uniform distribution (e.g. the leading bytes of a
 * cryptographic hash such as SHA-256). The table applies only a cheap mixing step before reducing
 * to a table index; adversarial or highly-clustered keys can produce pathological probe chains.
 *
 * ## Absent-key sentinel
 *
 * [get] and [remove] return [NOT_PRESENT] (== `-1`) when the requested key is not in the table.
 * Callers that only ever store non-negative values (e.g. ranks, subpool IDs, or a fixed
 * presence-sentinel such as `0`) can rely on this return value to distinguish "missing" from
 * "present" without a separate [containsKey] probe. Callers that may store `-1` as a real value
 * MUST use [containsKey] for presence checks; [containsKey] probes the key arrays directly and is
 * unambiguous for any stored value.
 *
 * ## Concurrency
 *
 * **Not thread-safe.** Concurrent mutation, or mutation concurrent with read, can corrupt the
 * internal arrays via resize races or probe-sequence races, matching the contract of `fastutil`'s
 * `Long2IntOpenHashMap`. Callers that need concurrent access must serialize externally.
 *
 * ## Iteration during mutation
 *
 * Iteration via [forEach] reads the parallel arrays directly with no snapshot or copy. Iteration
 * concurrent with mutation produces undefined behavior.
 *
 * @param initialCapacity initial number of slots. Rounded up to the next power of two and floored
 *   at [MIN_CAPACITY].
 * @param loadFactor maximum fill ratio before a resize is triggered. Must be in `(0, 1)`. Lower
 *   values trade memory for shorter probe chains.
 * @param maxChunkShift `log2` of the MAXIMUM per-chunk slot count. Defaults to
 *   [DEFAULT_CHUNK_SHIFT] (i.e. `2^30` slots per chunk). The actual chunk size starts at
 *   `min(2^maxChunkShift, nextPowerOfTwo(initialCapacity))` so small tables don't allocate a huge
 *   first chunk. Chunk size grows on resize until it reaches the cap, after which further resizes
 *   add chunks. Advanced parameter — almost all callers should leave it at the default; the only
 *   reason to lower it is to force multi-chunk layouts at small total sizes (used by this class's
 *   own unit tests). Must be in `(0, MAX_CHUNK_SHIFT]`.
 */
class Bytes12IntMap(
  initialCapacity: Long = DEFAULT_INITIAL_CAPACITY,
  private val loadFactor: Float = DEFAULT_LOAD_FACTOR,
  private val maxChunkShift: Int = DEFAULT_CHUNK_SHIFT,
) {
  init {
    require(initialCapacity > 0L) { "initialCapacity must be > 0, got $initialCapacity" }
    require(loadFactor > 0f && loadFactor < 1f) { "loadFactor must be in (0, 1), got $loadFactor" }
    require(maxChunkShift in 1..MAX_CHUNK_SHIFT) {
      "maxChunkShift must be in [1, $MAX_CHUNK_SHIFT], got $maxChunkShift"
    }
  }

  // Layout state. All updated together by [installLayout] at construction and on resize.
  private var chunkShift: Int = 0
  private var chunkSize: Int = 0
  private var slotMask: Int = 0
  private var capacity: Long = 0L
  private var mask: Long = 0L
  private var threshold: Long = 0L

  // Storage. Marked @PublishedApi internal so the inline [forEach] can
  // access them from inlined code without exposing them in the public
  // API. The setter is kept private so external callers cannot mutate
  // the layout.
  @PublishedApi
  internal var keysHi: Array<LongArray> = emptyArray()
    private set

  @PublishedApi
  internal var keysLo: Array<IntArray> = emptyArray()
    private set

  @PublishedApi
  internal var values: Array<IntArray> = emptyArray()
    private set

  /**
   * The all-zero key `(keyHi == 0L && keyLo == 0)` cannot be stored in the parallel arrays because
   * that value is the empty-slot sentinel. It is stored out-of-band so non-zero keys can use the
   * cheap sentinel-based empty check.
   *
   * Marked @PublishedApi internal so the inline [forEach] can read it from inlined code; setter
   * remains private.
   */
  @PublishedApi
  internal var containsZeroKey: Boolean = false
    private set

  @PublishedApi
  internal var zeroKeyValue: Int = 0
    private set

  /** Number of entries currently in the table. */
  var size: Long = 0L
    private set

  init {
    val rawCap = initialCapacity.coerceAtLeast(MIN_CAPACITY)
    installLayout(nextPowerOfTwoLong(rawCap))
  }

  /**
   * Allocates fresh parallel-array storage for [newCapacity] slots and updates all dependent state.
   * The chunk size is chosen as `min(2^maxChunkShift, newCapacity)` so small tables allocate small
   * chunks; once `newCapacity` exceeds the chunk-size cap, additional chunks of `2^maxChunkShift`
   * slots are allocated.
   */
  private fun installLayout(newCapacity: Long) {
    val maxChunk = 1L shl maxChunkShift
    val effectiveChunkSize = if (newCapacity < maxChunk) newCapacity.toInt() else maxChunk.toInt()
    val numChunksLong = newCapacity / effectiveChunkSize
    check(numChunksLong <= Int.MAX_VALUE.toLong()) {
      "numChunks ($numChunksLong) would exceed Int.MAX_VALUE; capacity=$newCapacity, " +
        "chunkSize=$effectiveChunkSize"
    }
    capacity = newCapacity
    mask = newCapacity - 1L
    threshold = computeThreshold(newCapacity, loadFactor)
    chunkSize = effectiveChunkSize
    chunkShift = Integer.numberOfTrailingZeros(effectiveChunkSize)
    slotMask = effectiveChunkSize - 1
    val numChunks = numChunksLong.toInt().coerceAtLeast(1)
    keysHi = Array(numChunks) { LongArray(effectiveChunkSize) }
    keysLo = Array(numChunks) { IntArray(effectiveChunkSize) }
    values = Array(numChunks) { IntArray(effectiveChunkSize) }
  }

  /** Returns `true` iff [size] is zero. */
  fun isEmpty(): Boolean = size == 0L

  /** Returns the current table capacity in slots. Exposed for tests / diagnostics. */
  fun capacity(): Long = capacity

  /** Returns the number of chunks in the current layout. Exposed for tests / diagnostics. */
  fun numChunks(): Int = keysHi.size

  /**
   * Inserts or updates the mapping `(keyHi, keyLo) -> value`.
   *
   * @return the previous value associated with the key, or [NOT_PRESENT] if the key was not
   *   present.
   */
  fun put(keyHi: Long, keyLo: Int, value: Int): Int {
    if (keyHi == 0L && keyLo == 0) {
      val previous = if (containsZeroKey) zeroKeyValue else NOT_PRESENT
      zeroKeyValue = value
      if (!containsZeroKey) {
        containsZeroKey = true
        size++
      }
      return previous
    }

    val shift = chunkShift
    val sMask = slotMask
    var pos = indexFor(keyHi, keyLo)
    var currentChunk = (pos ushr shift).toInt()
    var chunkHi = keysHi[currentChunk]
    var chunkLo = keysLo[currentChunk]
    var chunkVal = values[currentChunk]
    while (true) {
      val slot = pos.toInt() and sMask
      val currHi = chunkHi[slot]
      val currLo = chunkLo[slot]
      if (isEmptySlot(currHi, currLo)) {
        chunkHi[slot] = keyHi
        chunkLo[slot] = keyLo
        chunkVal[slot] = value
        size++
        maybeResize()
        return NOT_PRESENT
      }
      if (currHi == keyHi && currLo == keyLo) {
        val previous = chunkVal[slot]
        chunkVal[slot] = value
        return previous
      }
      pos = nextPos(pos)
      val newChunk = (pos ushr shift).toInt()
      if (newChunk != currentChunk) {
        currentChunk = newChunk
        chunkHi = keysHi[currentChunk]
        chunkLo = keysLo[currentChunk]
        chunkVal = values[currentChunk]
      }
    }
  }

  /**
   * Returns the value associated with `(keyHi, keyLo)`, or [NOT_PRESENT] if the key is not present.
   */
  fun get(keyHi: Long, keyLo: Int): Int {
    if (keyHi == 0L && keyLo == 0) {
      return if (containsZeroKey) zeroKeyValue else NOT_PRESENT
    }
    val shift = chunkShift
    val sMask = slotMask
    var pos = indexFor(keyHi, keyLo)
    var currentChunk = (pos ushr shift).toInt()
    var chunkHi = keysHi[currentChunk]
    var chunkLo = keysLo[currentChunk]
    var chunkVal = values[currentChunk]
    while (true) {
      val slot = pos.toInt() and sMask
      val currHi = chunkHi[slot]
      val currLo = chunkLo[slot]
      if (isEmptySlot(currHi, currLo)) return NOT_PRESENT
      if (currHi == keyHi && currLo == keyLo) return chunkVal[slot]
      pos = nextPos(pos)
      val newChunk = (pos ushr shift).toInt()
      if (newChunk != currentChunk) {
        currentChunk = newChunk
        chunkHi = keysHi[currentChunk]
        chunkLo = keysLo[currentChunk]
        chunkVal = values[currentChunk]
      }
    }
  }

  /**
   * Removes the entry for `(keyHi, keyLo)`.
   *
   * @return the previous value, or [NOT_PRESENT] if the key was not present.
   */
  fun remove(keyHi: Long, keyLo: Int): Int {
    if (keyHi == 0L && keyLo == 0) {
      if (!containsZeroKey) return NOT_PRESENT
      val previous = zeroKeyValue
      containsZeroKey = false
      zeroKeyValue = 0
      size--
      return previous
    }
    val shift = chunkShift
    val sMask = slotMask
    var pos = indexFor(keyHi, keyLo)
    var currentChunk = (pos ushr shift).toInt()
    var chunkHi = keysHi[currentChunk]
    var chunkLo = keysLo[currentChunk]
    var chunkVal = values[currentChunk]
    while (true) {
      val slot = pos.toInt() and sMask
      val currHi = chunkHi[slot]
      val currLo = chunkLo[slot]
      if (isEmptySlot(currHi, currLo)) return NOT_PRESENT
      if (currHi == keyHi && currLo == keyLo) {
        val previous = chunkVal[slot]
        shiftKeys(pos)
        size--
        return previous
      }
      pos = nextPos(pos)
      val newChunk = (pos ushr shift).toInt()
      if (newChunk != currentChunk) {
        currentChunk = newChunk
        chunkHi = keysHi[currentChunk]
        chunkLo = keysLo[currentChunk]
        chunkVal = values[currentChunk]
      }
    }
  }

  /**
   * Returns `true` iff `(keyHi, keyLo)` is present in the table.
   *
   * Probes the key arrays directly and never reads the value slot, so the result is correct
   * regardless of what `Int` value the caller stored. This is the only correct way to disambiguate
   * "present with value [NOT_PRESENT]" from "absent".
   */
  fun containsKey(keyHi: Long, keyLo: Int): Boolean {
    if (keyHi == 0L && keyLo == 0) return containsZeroKey
    val shift = chunkShift
    val sMask = slotMask
    var pos = indexFor(keyHi, keyLo)
    var currentChunk = (pos ushr shift).toInt()
    var chunkHi = keysHi[currentChunk]
    var chunkLo = keysLo[currentChunk]
    while (true) {
      val slot = pos.toInt() and sMask
      val currHi = chunkHi[slot]
      val currLo = chunkLo[slot]
      if (isEmptySlot(currHi, currLo)) return false
      if (currHi == keyHi && currLo == keyLo) return true
      pos = nextPos(pos)
      val newChunk = (pos ushr shift).toInt()
      if (newChunk != currentChunk) {
        currentChunk = newChunk
        chunkHi = keysHi[currentChunk]
        chunkLo = keysLo[currentChunk]
      }
    }
  }

  /** Removes all entries. Capacity is preserved. */
  fun clear() {
    if (size == 0L) return
    for (chunk in keysHi.indices) {
      keysHi[chunk].fill(0L)
      keysLo[chunk].fill(0)
      values[chunk].fill(0)
    }
    containsZeroKey = false
    zeroKeyValue = 0
    size = 0L
  }

  /**
   * Invokes [action] once for each entry. Order is unspecified.
   *
   * Iteration concurrent with mutation produces undefined behavior.
   *
   * Inlined into call sites to avoid a per-entry virtual call; critical when iterating tables with
   * hundreds of millions of entries.
   */
  inline fun forEach(action: (keyHi: Long, keyLo: Int, value: Int) -> Unit) {
    if (containsZeroKey) action(0L, 0, zeroKeyValue)
    val hi = keysHi
    val lo = keysLo
    val vs = values
    val nc = hi.size
    var c = 0
    while (c < nc) {
      val chunkHi = hi[c]
      val chunkLo = lo[c]
      val chunkVal = vs[c]
      val len = chunkHi.size
      var s = 0
      while (s < len) {
        val h = chunkHi[s]
        val l = chunkLo[s]
        if (h != 0L || l != 0) action(h, l, chunkVal[s])
        s++
      }
      c++
    }
  }

  /**
   * Inserts or updates the entry whose key is the 12 bytes of [fingerprint]. Equivalent to
   * `put(highOf(fingerprint), lowOf(fingerprint), value)`.
   *
   * @throws IllegalArgumentException if `fingerprint.size != 12`.
   */
  fun put(fingerprint: ByteArray, value: Int): Int {
    requireFingerprintWidth(fingerprint)
    return put(highOf(fingerprint), lowOf(fingerprint), value)
  }

  /** Convenience overload of [get] for 12-byte fingerprints. */
  fun get(fingerprint: ByteArray): Int {
    requireFingerprintWidth(fingerprint)
    return get(highOf(fingerprint), lowOf(fingerprint))
  }

  /** Convenience overload of [remove] for 12-byte fingerprints. */
  fun remove(fingerprint: ByteArray): Int {
    requireFingerprintWidth(fingerprint)
    return remove(highOf(fingerprint), lowOf(fingerprint))
  }

  /** Convenience overload of [containsKey] for 12-byte fingerprints. */
  fun containsKey(fingerprint: ByteArray): Boolean {
    requireFingerprintWidth(fingerprint)
    return containsKey(highOf(fingerprint), lowOf(fingerprint))
  }

  /**
   * Advances [pos] by one logical slot, wrapping at [capacity]. Works transparently across chunk
   * boundaries: the chunk index is implicit in the high bits of the logical position.
   */
  private fun nextPos(pos: Long): Long {
    val p = pos + 1L
    return if (p == capacity) 0L else p
  }

  /**
   * Backward-shift deletion. After freeing slot [initial], walks forward and pulls each subsequent
   * occupied slot back into the gap whenever doing so does not invalidate its lookup path. Operates
   * on the logical (chunk-spanning) index; chunk boundaries do not affect the algorithm because
   * logical-distance arithmetic is performed on the Long index.
   */
  private fun shiftKeys(initial: Long) {
    val shift = chunkShift
    val sMask = slotMask
    var last = initial
    while (true) {
      var pos = nextPos(last)
      var currentChunk = (pos ushr shift).toInt()
      var chunkHi = keysHi[currentChunk]
      var chunkLo = keysLo[currentChunk]
      var chunkVal = values[currentChunk]
      while (true) {
        val slot = pos.toInt() and sMask
        val currHi = chunkHi[slot]
        val currLo = chunkLo[slot]
        if (isEmptySlot(currHi, currLo)) {
          val lastChunk = (last ushr shift).toInt()
          val lastSlot = last.toInt() and sMask
          keysHi[lastChunk][lastSlot] = 0L
          keysLo[lastChunk][lastSlot] = 0
          values[lastChunk][lastSlot] = 0
          return
        }
        val natural = indexFor(currHi, currLo)
        val canShift =
          if (last <= pos) last >= natural || natural > pos else last >= natural && natural > pos
        if (canShift) {
          val lastChunk = (last ushr shift).toInt()
          val lastSlot = last.toInt() and sMask
          keysHi[lastChunk][lastSlot] = currHi
          keysLo[lastChunk][lastSlot] = currLo
          values[lastChunk][lastSlot] = chunkVal[slot]
          last = pos
          break // restart outer loop with new `last`
        }
        pos = nextPos(pos)
        val newChunk = (pos ushr shift).toInt()
        if (newChunk != currentChunk) {
          currentChunk = newChunk
          chunkHi = keysHi[currentChunk]
          chunkLo = keysLo[currentChunk]
          chunkVal = values[currentChunk]
        }
      }
    }
  }

  private fun maybeResize() {
    if (size <= threshold) return
    val newCapacity = capacity * 2L
    check(newCapacity <= MAX_CAPACITY) {
      "Bytes12IntMap would exceed MAX_CAPACITY ($MAX_CAPACITY); refusing to resize."
    }
    resize(newCapacity)
  }

  private fun resize(newCapacity: Long) {
    val oldKeysHi = keysHi
    val oldKeysLo = keysLo
    val oldValues = values
    val numOldChunks = oldKeysHi.size

    installLayout(newCapacity)

    var c = 0
    while (c < numOldChunks) {
      val chunkHi = oldKeysHi[c]
      val chunkLo = oldKeysLo[c]
      val chunkVal = oldValues[c]
      val len = chunkHi.size
      var s = 0
      while (s < len) {
        val hi = chunkHi[s]
        val lo = chunkLo[s]
        if (hi != 0L || lo != 0) {
          insertNoResize(hi, lo, chunkVal[s])
        }
        s++
      }
      c++
    }
  }

  /**
   * Inserts `(hi, lo) -> v` into the current (post-resize) arrays without touching [size] or
   * triggering a re-resize.
   *
   * Preconditions (caller must guarantee):
   * * `(hi, lo) != (0L, 0)` — zero-key is stored out-of-band.
   * * `(hi, lo)` is NOT already present in the table — this method has no key-equality check; a
   *   duplicate would loop forever or silently land in an unrelated empty slot.
   *
   * Used by [resize] to repopulate the new chunks from old contents; both preconditions hold by
   * construction there (zero-key never appears in the data arrays, and old contents have unique
   * keys).
   */
  private fun insertNoResize(hi: Long, lo: Int, v: Int) {
    val shift = chunkShift
    val sMask = slotMask
    var pos = indexFor(hi, lo)
    var currentChunk = (pos ushr shift).toInt()
    var chunkHi = keysHi[currentChunk]
    var chunkLo = keysLo[currentChunk]
    var chunkVal = values[currentChunk]
    while (true) {
      val slot = pos.toInt() and sMask
      if (isEmptySlot(chunkHi[slot], chunkLo[slot])) {
        chunkHi[slot] = hi
        chunkLo[slot] = lo
        chunkVal[slot] = v
        return
      }
      pos = nextPos(pos)
      val newChunk = (pos ushr shift).toInt()
      if (newChunk != currentChunk) {
        currentChunk = newChunk
        chunkHi = keysHi[currentChunk]
        chunkLo = keysLo[currentChunk]
        chunkVal = values[currentChunk]
      }
    }
  }

  /**
   * Maps a 96-bit key to a logical Long slot index in `[0, capacity)`. Combines all 96 bits of the
   * key via XOR folding (no avalanche mixing): the 32-bit low half is XORed into both halves of the
   * 64-bit high, then reduced to range via `and mask`. This is adequate only for
   * uniformly-distributed keys (see the class-level "Key assumptions" section); adversarial or
   * clustered keys can still produce pathological probe chains.
   */
  private fun indexFor(keyHi: Long, keyLo: Int): Long {
    val loAsLong = keyLo.toLong() and 0xFFFFFFFFL
    val mixed = keyHi xor (loAsLong shl 32) xor loAsLong
    return mixed and mask
  }

  /**
   * Sentinel test: a slot is empty iff both halves of its key are zero. The actual zero key `(0L,
   * 0)` is stored out-of-band so this never produces a false positive for the in-array data.
   */
  private fun isEmptySlot(hi: Long, lo: Int): Boolean = hi == 0L && lo == 0

  private fun requireFingerprintWidth(bytes: ByteArray) {
    require(bytes.size == FINGERPRINT_BYTE_WIDTH) {
      "Fingerprint must be exactly $FINGERPRINT_BYTE_WIDTH bytes, got ${bytes.size}"
    }
  }

  companion object {
    /** Width in bytes of a fingerprint key (12 bytes = 96 bits). */
    const val FINGERPRINT_BYTE_WIDTH: Int = 12

    /** Returned by [get], [put] and [remove] to indicate the key was not present. */
    const val NOT_PRESENT: Int = -1

    /** Minimum total capacity (in slots) of the table. */
    const val MIN_CAPACITY: Long = 16L

    /**
     * Per-chunk slot count cap: `2^30` slots per chunk, the largest power of two below the JVM
     * array length limit (`Int.MAX_VALUE - 8`).
     */
    const val MAX_CHUNK_SHIFT: Int = 30

    /** Default chunk shift: `2^30` slots per chunk. */
    const val DEFAULT_CHUNK_SHIFT: Int = MAX_CHUNK_SHIFT

    /**
     * Hard upper bound on total capacity: `2^60` slots. Far beyond realistic use; bounded in
     * practice by heap memory.
     */
    const val MAX_CAPACITY: Long = 1L shl 60

    /** Default initial capacity when none is specified. */
    const val DEFAULT_INITIAL_CAPACITY: Long = MIN_CAPACITY

    /** Default load factor when none is specified. */
    const val DEFAULT_LOAD_FACTOR: Float = 0.75f

    /** Returns the smallest power of two `>= n`. Requires `n >= 1`. */
    private fun nextPowerOfTwoLong(n: Long): Long {
      require(n >= 1L) { "nextPowerOfTwoLong requires n >= 1, got $n" }
      var x = n - 1L
      x = x or (x ushr 1)
      x = x or (x ushr 2)
      x = x or (x ushr 4)
      x = x or (x ushr 8)
      x = x or (x ushr 16)
      x = x or (x ushr 32)
      return x + 1L
    }

    private fun computeThreshold(capacity: Long, loadFactor: Float): Long {
      return (capacity.toDouble() * loadFactor.toDouble()).toLong()
    }

    /** Reads the high 8 bytes (big-endian) from a 12-byte fingerprint. */
    private fun highOf(bytes: ByteArray): Long {
      return ((bytes[0].toLong() and 0xFF) shl 56) or
        ((bytes[1].toLong() and 0xFF) shl 48) or
        ((bytes[2].toLong() and 0xFF) shl 40) or
        ((bytes[3].toLong() and 0xFF) shl 32) or
        ((bytes[4].toLong() and 0xFF) shl 24) or
        ((bytes[5].toLong() and 0xFF) shl 16) or
        ((bytes[6].toLong() and 0xFF) shl 8) or
        (bytes[7].toLong() and 0xFF)
    }

    /** Reads the low 4 bytes (big-endian) from a 12-byte fingerprint. */
    private fun lowOf(bytes: ByteArray): Int {
      return ((bytes[8].toInt() and 0xFF) shl 24) or
        ((bytes[9].toInt() and 0xFF) shl 16) or
        ((bytes[10].toInt() and 0xFF) shl 8) or
        (bytes[11].toInt() and 0xFF)
    }
  }
}
