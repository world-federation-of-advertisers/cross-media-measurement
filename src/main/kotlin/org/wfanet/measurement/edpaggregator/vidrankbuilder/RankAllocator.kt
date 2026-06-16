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

import com.google.protobuf.UnsafeByteOperations
import java.util.BitSet
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * In-heap rank-allocation state for a **single** subpool: the source-of-truth cumulative
 * `(fingerprint -> rank)` map plus an acceleration [BitSet] of taken ranks.
 *
 * Per the design, one subpool is owned by exactly one ranker VM, so this structure is
 * single-threaded and needs no synchronization.
 *
 * Two [Bytes12IntMap]s live here:
 * * [cumulative] — every surviving `(fp, rank)` across all uploads; serialized as the new
 *   `SNAPSHOT` blob. 12-byte key, [Int] rank value.
 * * [dayOnly] — the `(fp, rank)` pairs touched **this** dispatch (newly allocated *and* renewed);
 *   serialized as the `DAY_ONLY` blob for renewal tracking and retention bookkeeping.
 *
 * [taken] is rebuilt from [cumulative]'s rank values at load (never persisted). Allocation finds
 * the next free rank via [BitSet.nextClearBit] starting from a monotonically advancing [cursor];
 * because retention runs (and frees ranks) **before** any allocation, every freed slot is visible
 * to the first allocation pass.
 *
 * @param poolOffset the subpool's `population_offset`.
 * @param rankedSize the subpool's ranked sub-range size; ranks are allocated in `[0, rankedSize)`.
 *   Fingerprints beyond it overflow and are left unranked (Phase-2 hash fallback).
 */
class RankAllocator(
  val poolOffset: Long,
  val rankedSize: Int,
  initialCapacity: Long = Bytes12IntMap.DEFAULT_INITIAL_CAPACITY,
) {
  init {
    require(rankedSize >= 0) { "rankedSize must be >= 0, got $rankedSize" }
  }

  private val cumulative = Bytes12IntMap(initialCapacity)
  private val dayOnly = Bytes12IntMap()
  private val taken = BitSet(rankedSize.coerceAtLeast(1))
  private var cursor = 0

  /** Number of fingerprints currently holding a rank in this subpool. */
  val cumulativeSize: Long
    get() = cumulative.size

  /** Ranks newly allocated to never-before-seen fingerprints this dispatch. */
  var allocated: Long = 0L
    private set

  /** Already-ranked fingerprints re-observed this dispatch (rank preserved). */
  var renewed: Long = 0L
    private set

  /** Fingerprints left unranked because the subpool reached [rankedSize]. */
  var overflow: Long = 0L
    private set

  /** Ranks freed by the retention pass. */
  var freed: Long = 0L
    private set

  /**
   * Loads a single prior `(fp, rank)` entry into [cumulative] and marks its rank taken. Used by
   * [loadFrom]; safe to call directly in tests.
   */
  fun loadEntry(keyHi: Long, keyLo: Int, rank: Int) {
    cumulative.put(keyHi, keyLo, rank)
    if (rank in 0 until rankedSize) taken.set(rank)
  }

  /** Rebuilds [cumulative] + [taken] from a prior `SNAPSHOT` blob's records. */
  suspend fun loadFrom(records: Flow<RankIndexMap>) {
    records.collect { record ->
      val fps = record.fingerprints
      val count = record.ranksCount
      var off = 0
      for (i in 0 until count) {
        loadEntry(
          FingerprintCodec.readHi(fps, off),
          FingerprintCodec.readLo(fps, off + 8),
          record.getRanks(i),
        )
        off += FingerprintCodec.WIDTH
      }
    }
  }

  /** Whether [keyHi]/[keyLo] currently holds a rank. */
  fun contains(keyHi: Long, keyLo: Int): Boolean = cumulative.containsKey(keyHi, keyLo)

  /** The rank of [keyHi]/[keyLo], or [Bytes12IntMap.NOT_PRESENT] if unranked. */
  fun get(keyHi: Long, keyLo: Int): Int = cumulative.get(keyHi, keyLo)

  /**
   * Frees the rank of [keyHi]/[keyLo] if present: removes it from [cumulative] and clears its
   * [taken] bit so the slot is reusable. No-op if the fingerprint is not ranked.
   */
  fun free(keyHi: Long, keyLo: Int) {
    val rank = cumulative.remove(keyHi, keyLo)
    if (rank != Bytes12IntMap.NOT_PRESENT) {
      if (rank in 0 until rankedSize) taken.clear(rank)
      freed++
    }
  }

  /**
   * Assigns a rank to [keyHi]/[keyLo] for this dispatch and records it in [dayOnly]:
   * * already ranked -> reuses the existing rank (renewal; keeps the VID stable),
   * * otherwise -> claims the next free rank via [BitSet.nextClearBit], unless the subpool is full.
   *
   * @return the assigned rank, or `null` if the subpool reached [rankedSize] (overflow — the
   *   fingerprint stays unranked and falls back to the Phase-2 hash path).
   */
  fun assign(keyHi: Long, keyLo: Int): Int? {
    val existing = cumulative.get(keyHi, keyLo)
    if (existing != Bytes12IntMap.NOT_PRESENT) {
      dayOnly.put(keyHi, keyLo, existing)
      renewed++
      return existing
    }
    val rank = taken.nextClearBit(cursor)
    if (rank >= rankedSize) {
      overflow++
      return null
    }
    taken.set(rank)
    cursor = rank + 1
    cumulative.put(keyHi, keyLo, rank)
    dayOnly.put(keyHi, keyLo, rank)
    allocated++
    return rank
  }

  /** Streams the cumulative map as chunked [RankIndexMap] records (the new `SNAPSHOT` blob). */
  fun streamCumulativeChunks(chunkEntries: Int = DEFAULT_CHUNK_ENTRIES): Flow<RankIndexMap> =
    streamChunks(cumulative, chunkEntries)

  /**
   * Streams this dispatch's touched entries as chunked [RankIndexMap] records (`DAY_ONLY` blob).
   */
  fun streamDayOnlyChunks(chunkEntries: Int = DEFAULT_CHUNK_ENTRIES): Flow<RankIndexMap> =
    streamChunks(dayOnly, chunkEntries)

  private fun streamChunks(map: Bytes12IntMap, chunkEntries: Int): Flow<RankIndexMap> = flow {
    val total = map.size
    if (total == 0L) return@flow
    val chunkCount = minOf(chunkEntries.toLong(), total).toInt()
    var fps = ByteArray(chunkCount * FingerprintCodec.WIDTH)
    var ranks = IntArray(chunkCount)
    var n = 0
    var produced = 0L
    // forEach is inline, so the suspend emit() is legal inside the flow block.
    map.forEach { keyHi, keyLo, rank ->
      val base = n * FingerprintCodec.WIDTH
      FingerprintCodec.writeHi(fps, base, keyHi)
      FingerprintCodec.writeLo(fps, base + 8, keyLo)
      ranks[n] = rank
      n++
      produced++
      if (n == ranks.size) {
        emit(buildRecord(fps, ranks, n))
        val remaining = total - produced
        if (remaining > 0) {
          val next = minOf(chunkEntries.toLong(), remaining).toInt()
          fps = ByteArray(next * FingerprintCodec.WIDTH)
          ranks = IntArray(next)
          n = 0
        }
      }
    }
  }

  private fun buildRecord(fps: ByteArray, ranks: IntArray, count: Int): RankIndexMap =
    rankIndexMap {
      poolOffset = this@RankAllocator.poolOffset
      rankedSize = this@RankAllocator.rankedSize
      fingerprints = UnsafeByteOperations.unsafeWrap(fps, 0, count * FingerprintCodec.WIDTH)
      for (i in 0 until count) this.ranks += ranks[i]
    }

  companion object {
    /** ~16M entries (~256 MB of fps+ranks) per record: one buffer in memory at a time. */
    const val DEFAULT_CHUNK_ENTRIES = 16 * 1024 * 1024
  }
}
