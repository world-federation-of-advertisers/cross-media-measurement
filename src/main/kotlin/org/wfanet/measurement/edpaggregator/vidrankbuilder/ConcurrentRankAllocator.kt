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
import java.util.concurrent.ConcurrentLinkedQueue
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.CoroutineDispatcher
import kotlinx.coroutines.coroutineScope
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.launch
import org.jetbrains.annotations.VisibleForTesting
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * Multi-core, striped variant of [RankAllocator] for the **forward** (non-backfill) rank build of a
 * single subpool. It saturates the VM's cores while producing byte-compatible `SNAPSHOT` /
 * `DAY_ONLY` output.
 *
 * ## Design: stripe the maps, share ONE rank pool
 *
 * This is **not** rank-space partitioning. The `(fingerprint -> rank)` space is striped by
 * `stripe(keyHi) = floorMod(keyHi, stripes)` purely so each stripe's maps can be mutated by exactly
 * one coroutine during the assign/free phases (thread-owned, lock-free there). Ranks themselves are
 * drawn from a **single shared pool** across all stripes, so a subpool's ranks stay dense and
 * overflow happens **iff the entire [rankedSize] is exhausted** — identical density to the serial
 * allocator.
 *
 * Per stripe:
 * * [todayFps] — this dispatch's fingerprint set (map-as-set, deduplicated). Written by many parse
 *   workers, so [addToday] guards it with `synchronized(stripeMap)` exactly like
 *   `SubpoolFingerprintsAccumulator.add`. After the parse phase it is thread-owned (read-only
 *   during free, and read via [containsKey] only).
 * * [cumulative] — surviving `(fp, rank)` across all uploads for this stripe; serialized as the new
 *   `SNAPSHOT`. Thread-owned during [loadFrom] (serial), [freeAgedRanks] (one coroutine per
 *   stripe), and [assign] (one coroutine per stripe).
 * * [dayOnly] — the `(fp, rank)` pairs touched this dispatch; serialized as the `DAY_ONLY` blob.
 *
 * Shared rank pool (the whole point — no per-stripe rank band):
 * * [newRankCursor] — the next never-used rank ([AtomicInteger.getAndIncrement], lock-free).
 * * [reclaimed] — ranks freed by retention (or holes below the loaded high-water), reused before
 *   the cursor is extended.
 * * [lastSeen] — `lastSeen[rank]` recency; each rank is owned/written by exactly one coroutine, and
 *   reads are of stable values, so no lock is needed.
 *
 * [allocateRank] returns `reclaimed.poll()` if any, else `newRankCursor.getAndIncrement()` unless
 * it has reached [rankedSize] (overflow). No bulk reservation and no per-stripe capacity, so no
 * rank is ever stranded: overflow occurs **only** when the whole ranked range is simultaneously
 * held.
 *
 * ## Lifecycle
 * 1. [addToday] (parallel parse phase, under the per-stripe lock),
 * 2. [loadFrom] (serial; only on a warm forward dispatch) rebuilds [cumulative] + [lastSeen] and
 *    derives the shared pool (cursor = maxLoadedRank + 1, holes enqueued into [reclaimed]),
 * 3. [freeAgedRanks] (one coroutine per stripe),
 * 4. [assign] (one coroutine per stripe),
 * 5. [streamCumulativeChunks] / [streamDayOnlyChunks] (single serial drain, called by the writer).
 *
 * @param poolOffset the subpool's `population_offset`.
 * @param rankedSize the subpool's ranked sub-range size; ranks are allocated in `[0, rankedSize)`.
 * @param eventDay epoch-day stamped as `last_seen` for every fingerprint touched this dispatch.
 * @param stripes number of stripes (≈ available cores). Must be `>= 1`.
 * @param todayStripeCapacity initial slot count for each stripe's [todayFps] / [dayOnly] map, sized
 *   to this dispatch's per-stripe fingerprint count so the parallel fill does not resize under the
 *   per-stripe monitor.
 * @param cumulativeStripeCapacity initial slot count for each stripe's [cumulative] map, sized to
 *   the per-stripe prior-snapshot-plus-today entry count so [loadFrom] does not resize the
 *   cumulative on a warm dispatch (see
 *   world-federation-of-advertisers/cross-media-measurement#4015).
 * @param estimatedTotalRanks estimated distinct-rank count (prior + today, capped at [rankedSize]);
 *   used only to pre-size the [loadFrom] `taken` [BitSet]. Over- or under-estimating is safe: the
 *   `BitSet` auto-grows and its contents/`nextClearBit` results do not depend on its initial size.
 */
class ConcurrentRankAllocator(
  val poolOffset: Long,
  val rankedSize: Int,
  private val eventDay: Int,
  private val stripes: Int = DEFAULT_STRIPES,
  todayStripeCapacity: Long = Bytes12IntMap.DEFAULT_INITIAL_CAPACITY,
  cumulativeStripeCapacity: Long = todayStripeCapacity,
  private val estimatedTotalRanks: Int = rankedSize,
) {
  init {
    require(rankedSize >= 0) { "rankedSize must be >= 0, got $rankedSize" }
    require(eventDay in 0..LastSeenDayBytes.MAX_DAY) {
      "eventDay $eventDay out of uint16 epoch-day range [0, ${LastSeenDayBytes.MAX_DAY}]"
    }
    require(stripes >= 1) { "stripes must be >= 1, got $stripes" }
  }

  private val todayFps = Array(stripes) { Bytes12IntMap(todayStripeCapacity) }
  private val cumulative = Array(stripes) { Bytes12IntMap(cumulativeStripeCapacity) }
  private val dayOnly = Array(stripes) { Bytes12IntMap(todayStripeCapacity) }

  // Sized to [rankedSize] (not the estimated rank count): a rank is only ever hard-capped at
  // [rankedSize] by the `rank in 0 until rankedSize` guards, but the MAX rank value loaded from a
  // prior snapshot can exceed the live-entry count (retention frees low ranks while a high rank
  // survives), so a shorter array would index out of bounds. Kept fixed-size for safety.
  private val lastSeen = ShortArray(rankedSize)
  private val newRankCursor = AtomicInteger(0)
  private val reclaimed = ConcurrentLinkedQueue<Int>()

  // Per-stripe tallies, summed after the parallel phases (each stripe is written by one coroutine).
  private val allocatedPerStripe = LongArray(stripes)
  private val renewedPerStripe = LongArray(stripes)
  private val overflowPerStripe = LongArray(stripes)
  private val freedPerStripe = LongArray(stripes)

  /** Ranks newly allocated to never-before-seen fingerprints this dispatch. */
  val allocated: Long
    get() = allocatedPerStripe.sum()

  /** Already-ranked fingerprints re-observed this dispatch (rank preserved). */
  val renewed: Long
    get() = renewedPerStripe.sum()

  /** Fingerprints left unranked because the subpool reached [rankedSize]. */
  val overflow: Long
    get() = overflowPerStripe.sum()

  /** Ranks freed by [freeAgedRanks]. */
  val freed: Long
    get() = freedPerStripe.sum()

  /** Number of fingerprints currently holding a rank in this subpool (across all stripes). */
  val cumulativeSize: Long
    get() {
      var total = 0L
      for (map in cumulative) total += map.size
      return total
    }

  /** Number of distinct fingerprints accumulated for this dispatch (across all stripes). */
  @VisibleForTesting
  fun todaySize(): Long {
    var total = 0L
    for (map in todayFps) total += map.size
    return total
  }

  private fun stripe(keyHi: Long): Int = Math.floorMod(keyHi, stripes)

  /**
   * The stripe a fingerprint with high half [keyHi] routes to. Exposed so a parse worker can bucket
   * a whole record's fingerprints per stripe and insert them with a single lock via [addTodayBulk];
   * MUST match the internal [stripe] routing so [assign] finds each renewal in its own stripe.
   */
  fun stripeOf(keyHi: Long): Int = stripe(keyHi)

  /** Number of stripes. Exposed for parse-worker bucketing and tests. */
  @VisibleForTesting fun stripeCount(): Int = stripes

  /** Per-stripe slot capacity of the [cumulative] maps. Exposed to verify pre-sizing in tests. */
  @VisibleForTesting fun cumulativeStripeCapacity(): Long = cumulative[0].capacity()

  /** Per-stripe slot capacity of the [todayFps] maps. Exposed to verify pre-sizing in tests. */
  @VisibleForTesting fun todayStripeCapacity(): Long = todayFps[0].capacity()

  /**
   * Records fingerprint `(keyHi, keyLo)` in its stripe's [todayFps]. Concurrency-safe: the parse
   * phase runs many workers, so the per-stripe map is guarded exactly like
   * `SubpoolFingerprintsAccumulator.add`. The map is used as a set, so duplicates collapse.
   */
  fun addToday(keyHi: Long, keyLo: Int) {
    val map = todayFps[stripe(keyHi)]
    synchronized(map) { map.put(keyHi, keyLo, PRESENT) }
  }

  /**
   * Inserts the first [n] fingerprints of `(hi, lo)` — all of which the caller has already bucketed
   * into stripe [stripeIndex] via [stripeOf] — into that stripe's [todayFps] under a SINGLE monitor
   * acquisition, instead of one `synchronized` per fingerprint as in [addToday]. Set semantics are
   * identical (the map is used as a set; duplicates collapse), so the accumulated fingerprint set —
   * and therefore every downstream result — is unchanged; only the lock granularity differs.
   */
  fun addTodayBulk(stripeIndex: Int, hi: LongArray, lo: IntArray, n: Int) {
    val map = todayFps[stripeIndex]
    synchronized(map) {
      for (i in 0 until n) {
        map.put(hi[i], lo[i], PRESENT)
      }
    }
  }

  /**
   * Rebuilds [cumulative] + [lastSeen] from a prior `SNAPSHOT` blob's records (serial; warm forward
   * only), then derives the shared rank pool so that later allocation stays dense:
   * * [newRankCursor] starts just past the highest loaded rank, and
   * * every CLEAR rank in `[0, maxLoadedRank]` (a hole) is enqueued into [reclaimed] so holes are
   *   reused before the cursor is extended.
   *
   * On a cold start (no records, or all ranks out of range) the cursor stays at 0 and [reclaimed]
   * stays empty. Records that predate `last_seen_days` (length mismatch) default each entry's
   * recency to [eventDay]. Out-of-range ranks are dropped (matching [RankAllocator.loadEntry]).
   */
  suspend fun loadFrom(records: Flow<RankIndexMap>) {
    // [BitSet] auto-grows and its results are independent of the initial size, so this is a pure
    // capacity hint; [estimatedTotalRanks] avoids over-allocating at a huge [rankedSize].
    val taken = BitSet(estimatedTotalRanks.coerceAtLeast(1))
    var maxLoadedRank = -1
    records.collect { record ->
      val fps = record.fingerprints
      val count = record.ranksCount
      val lastSeenDays = record.lastSeenDays
      val hasLastSeen = lastSeenDays.size() == count * LastSeenDayBytes.WIDTH
      var off = 0
      for (i in 0 until count) {
        val keyHi = EventIdDigestBytes.readHi(fps, off)
        val keyLo = EventIdDigestBytes.readLo(fps, off + 8)
        off += EventIdDigestBytes.WIDTH
        val rank = record.getRanks(i)
        // Ignore ranks outside [0, rankedSize) (e.g. a snapshot written when ranked_size was
        // larger); a dropped fingerprint is simply re-ranked into the valid range if seen again.
        if (rank !in 0 until rankedSize) continue
        val day =
          if (hasLastSeen) LastSeenDayBytes.read(lastSeenDays, i * LastSeenDayBytes.WIDTH)
          else eventDay
        cumulative[stripe(keyHi)].put(keyHi, keyLo, rank)
        taken.set(rank)
        lastSeen[rank] = day.toShort()
        if (rank > maxLoadedRank) maxLoadedRank = rank
      }
    }
    newRankCursor.set(maxLoadedRank + 1)
    if (maxLoadedRank >= 0) {
      var r = taken.nextClearBit(0)
      while (r <= maxLoadedRank) {
        reclaimed.add(r)
        r = taken.nextClearBit(r + 1)
      }
    }
  }

  /**
   * Claims the next rank from the shared pool: a [reclaimed] hole if any, else the next never-used
   * rank via [newRankCursor]. Returns [NOT_ALLOCATED] **only** when [reclaimed] is empty AND the
   * cursor has reached [rankedSize] — i.e. the entire ranked range is exhausted (overflow).
   */
  private fun allocateRank(): Int {
    val reused = reclaimed.poll()
    if (reused != null) return reused
    val next = newRankCursor.getAndIncrement()
    return if (next >= rankedSize) NOT_ALLOCATED else next
  }

  /**
   * Frees the ranks of aged-out fingerprints (`last_seen < cutoffEpochDay`, not seen this
   * dispatch), one coroutine per stripe on [dispatcher]. Freed ranks return to the shared
   * [reclaimed] queue so the [assign] phase reuses them first. Must complete before [assign].
   */
  suspend fun freeAgedRanks(cutoffEpochDay: Int, dispatcher: CoroutineDispatcher) {
    coroutineScope {
      for (s in 0 until stripes) {
        launch(dispatcher) { freeAgedRanksStripe(s, cutoffEpochDay) }
      }
    }
  }

  private fun freeAgedRanksStripe(stripeIndex: Int, cutoffEpochDay: Int) {
    val cum = cumulative[stripeIndex]
    val today = todayFps[stripeIndex]
    // Collect the doomed keys first, then remove them: mutating the map mid-iteration is undefined.
    // Growable primitive long[]/int[] (not boxed ArrayList<Long>/ArrayList<Int>); the iteration
    // order within the stripe is unchanged, so the freed set and reclaim order are identical.
    var freeHi = LongArray(INITIAL_FREE_BUFFER)
    var freeLo = IntArray(INITIAL_FREE_BUFFER)
    var freeCount = 0
    cum.forEach { keyHi, keyLo, rank ->
      if (
        rank in 0 until rankedSize &&
          (lastSeen[rank].toInt() and 0xFFFF) < cutoffEpochDay &&
          !today.containsKey(keyHi, keyLo)
      ) {
        if (freeCount == freeHi.size) {
          freeHi = freeHi.copyOf(freeHi.size * 2)
          freeLo = freeLo.copyOf(freeLo.size * 2)
        }
        freeHi[freeCount] = keyHi
        freeLo[freeCount] = keyLo
        freeCount++
      }
    }
    var freedHere = 0L
    for (i in 0 until freeCount) {
      val rank = cum.remove(freeHi[i], freeLo[i])
      if (rank != Bytes12IntMap.NOT_PRESENT) {
        if (rank in 0 until rankedSize) reclaimed.add(rank)
        freedHere++
      }
    }
    freedPerStripe[stripeIndex] = freedHere
  }

  /**
   * Assigns ranks to this dispatch's fingerprints, one coroutine per stripe on [dispatcher]:
   * * already ranked (renewal) -> reuse the existing rank, refresh `last_seen`, record in
   *   [dayOnly],
   * * otherwise -> [allocateRank] from the shared pool; overflow leaves it unranked.
   *
   * Each stripe's [cumulative] / [dayOnly] are thread-owned here; only the shared pool ([reclaimed]
   * / [newRankCursor]) is touched concurrently and it is lock-free.
   */
  suspend fun assign(dispatcher: CoroutineDispatcher) {
    coroutineScope {
      for (s in 0 until stripes) {
        launch(dispatcher) { assignStripe(s) }
      }
    }
  }

  private fun assignStripe(stripeIndex: Int) {
    val today = todayFps[stripeIndex]
    val cum = cumulative[stripeIndex]
    val day = dayOnly[stripeIndex]
    var alloc = 0L
    var renew = 0L
    var over = 0L
    today.forEach { keyHi, keyLo, _ ->
      val existing = cum.get(keyHi, keyLo)
      if (existing != Bytes12IntMap.NOT_PRESENT) {
        if (existing in 0 until rankedSize) lastSeen[existing] = eventDay.toShort()
        day.put(keyHi, keyLo, existing)
        renew++
      } else {
        val rank = allocateRank()
        if (rank == NOT_ALLOCATED) {
          over++
        } else {
          lastSeen[rank] = eventDay.toShort()
          cum.put(keyHi, keyLo, rank)
          day.put(keyHi, keyLo, rank)
          alloc++
        }
      }
    }
    allocatedPerStripe[stripeIndex] = alloc
    renewedPerStripe[stripeIndex] = renew
    overflowPerStripe[stripeIndex] = over
  }

  /**
   * Streams the cumulative maps across ALL stripes as chunked [RankIndexMap] records (the new
   * `SNAPSHOT` blob), each carrying its entries' persisted `last_seen` recency. Single serial
   * drain.
   */
  fun streamCumulativeChunks(chunkEntries: Int = DEFAULT_CHUNK_ENTRIES): Flow<RankIndexMap> =
    streamChunks(cumulative, chunkEntries) { rank -> lastSeen[rank].toInt() and 0xFFFF }

  /**
   * Streams this dispatch's touched entries across ALL stripes as chunked [RankIndexMap] records
   * (`DAY_ONLY` blob); every entry's `last_seen` is [eventDay]. Single serial drain.
   */
  fun streamDayOnlyChunks(chunkEntries: Int = DEFAULT_CHUNK_ENTRIES): Flow<RankIndexMap> =
    streamChunks(dayOnly, chunkEntries) { eventDay }

  private fun streamChunks(
    maps: Array<Bytes12IntMap>,
    chunkEntries: Int,
    lastSeenOf: (rank: Int) -> Int,
  ): Flow<RankIndexMap> = flow {
    var total = 0L
    for (map in maps) total += map.size
    if (total == 0L) return@flow
    val firstChunk = minOf(chunkEntries.toLong(), total).toInt()
    var fps = ByteArray(firstChunk * EventIdDigestBytes.WIDTH)
    var ranks = IntArray(firstChunk)
    var seen = IntArray(firstChunk)
    var n = 0
    var produced = 0L
    // The buffers are sized to exactly what will fill them (initial = min(chunk, total); each
    // resize = min(chunk, remaining)), so the final chunk fills exactly and no trailing partial
    // emit is needed. `n` carries across stripe maps, so a chunk boundary may fall mid-map.
    for (map in maps) {
      // forEach is inline, so the suspend emit() is legal inside the flow block.
      map.forEach { keyHi, keyLo, rank ->
        val base = n * EventIdDigestBytes.WIDTH
        EventIdDigestBytes.writeHi(fps, base, keyHi)
        EventIdDigestBytes.writeLo(fps, base + 8, keyLo)
        ranks[n] = rank
        seen[n] = lastSeenOf(rank)
        n++
        produced++
        if (n == ranks.size) {
          emit(buildRecord(fps, ranks, seen, n))
          val remaining = total - produced
          if (remaining > 0) {
            val next = minOf(chunkEntries.toLong(), remaining).toInt()
            fps = ByteArray(next * EventIdDigestBytes.WIDTH)
            ranks = IntArray(next)
            seen = IntArray(next)
            n = 0
          }
        }
      }
    }
  }

  private fun buildRecord(
    fps: ByteArray,
    ranks: IntArray,
    seen: IntArray,
    count: Int,
  ): RankIndexMap = rankIndexMap {
    poolOffset = this@ConcurrentRankAllocator.poolOffset
    rankedSize = this@ConcurrentRankAllocator.rankedSize
    fingerprints = UnsafeByteOperations.unsafeWrap(fps, 0, count * EventIdDigestBytes.WIDTH)
    val lastSeenBytes = ByteArray(count * LastSeenDayBytes.WIDTH)
    // Bulk-add ranks in O(count). A per-element `+=` on the packed repeated-field DSL
    // list is O(count) each (repeated-field grow/copy), making this loop O(count^2) and stalling
    // large no-overflow rank-index writes; addAll copies once.
    this.ranks += if (count == ranks.size) ranks.asList() else ranks.asList().subList(0, count)
    for (i in 0 until count) {
      LastSeenDayBytes.write(lastSeenBytes, i * LastSeenDayBytes.WIDTH, seen[i])
    }
    lastSeenDays = UnsafeByteOperations.unsafeWrap(lastSeenBytes)
  }

  companion object {
    /** Returned by [allocateRank] when the whole ranked range is exhausted (overflow). */
    private const val NOT_ALLOCATED = -1

    /** Ignored value stored for every fingerprint; [todayFps] is used purely as a set. */
    private const val PRESENT = 1

    /** Initial size of the growable primitive buffer of doomed keys in [freeAgedRanksStripe]. */
    private const val INITIAL_FREE_BUFFER = 16

    /** Default stripe count: ≈ available cores. */
    val DEFAULT_STRIPES: Int = maxOf(1, Runtime.getRuntime().availableProcessors())

    /** ~1M entries per record (same as [RankAllocator]): one buffer in memory at a time. */
    const val DEFAULT_CHUNK_ENTRIES = 1 * 1024 * 1024
  }
}
