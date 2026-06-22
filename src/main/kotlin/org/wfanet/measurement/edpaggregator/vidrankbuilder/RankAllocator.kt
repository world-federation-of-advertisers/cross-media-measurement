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
 * `(fingerprint -> rank)` map, an acceleration [BitSet] of taken ranks, and a per-rank `last_seen`
 * recency index.
 *
 * Per the design, one subpool is owned by exactly one ranker VM, so this structure is
 * single-threaded and needs no synchronization.
 *
 * State held here:
 * * [cumulative] — every surviving `(fp, rank)` across all uploads; serialized as the new
 *   `SNAPSHOT` blob. 12-byte key, [Int] rank value.
 * * [dayOnly] — the `(fp, rank)` pairs touched **this** dispatch (newly allocated *and* renewed);
 *   serialized as the `DAY_ONLY` blob.
 * * [taken] — rank `BitSet`, rebuilt from [cumulative] at load.
 * * [lastSeen] — `lastSeen[rank]` is the epoch-day the fingerprint at that rank was most recently
 *   observed. Indexed by rank (dense in `[0, ranked_size)`), it makes retention an in-heap pass
 *   over [cumulative] rather than a re-scan of every surviving `DAY_ONLY` blob. Held as an unsigned
 *   `uint16` in a [ShortArray] — epoch-days fit `[0, LastSeenDayBytes.MAX_DAY]`, so every read
 *   masks with `and 0xFFFF` — which halves the array's heap footprint (e.g. 720 MB vs 1.4 GB at a
 *   360 M US-scale subpool). Persisted positionally in the `SNAPSHOT`
 *   (`RankIndexMap.last_seen_days`) and rebuilt at load.
 *
 * Allocation finds the next free rank via [BitSet.nextClearBit] from a monotonically advancing
 * [cursor]; because [freeAgedRanks] runs **before** any [assign], freed slots are visible to the
 * first allocation.
 *
 * @param poolOffset the subpool's `population_offset`.
 * @param rankedSize the subpool's ranked sub-range size; ranks are allocated in `[0, rankedSize)`.
 * @param eventDay epoch-day stamped as `last_seen` for every fingerprint touched this dispatch
 *   (typically the dispatch's `max_event_date`). Also the default `last_seen` for entries loaded
 *   from a legacy `SNAPSHOT` that predates the field.
 */
class RankAllocator(
  val poolOffset: Long,
  val rankedSize: Int,
  private val eventDay: Int,
  initialCapacity: Long = Bytes12IntMap.DEFAULT_INITIAL_CAPACITY,
) {
  init {
    require(rankedSize >= 0) { "rankedSize must be >= 0, got $rankedSize" }
    require(eventDay in 0..LastSeenDayBytes.MAX_DAY) {
      "eventDay $eventDay out of uint16 epoch-day range [0, ${LastSeenDayBytes.MAX_DAY}]"
    }
  }

  private val cumulative = Bytes12IntMap(initialCapacity)
  private val dayOnly = Bytes12IntMap()
  private val taken = BitSet(rankedSize.coerceAtLeast(1))
  private val lastSeen = ShortArray(rankedSize)
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

  /** Ranks freed by [freeAgedRanks]. */
  var freed: Long = 0L
    private set

  /** Backfilled fingerprints whose DAY_ONLY label came from the old snapshot's rank. */
  var backfillReusedOldRank: Long = 0L
    private set

  /**
   * Backfilled fingerprints reassigned an old rank that differs from the rank they hold in the
   * latest snapshot — the accepted reach-undercount sizing.
   */
  var backfillRankCollisions: Long = 0L
    private set

  /**
   * Loads a single prior `(fp, rank)` entry with its persisted [lastSeenDay], marking the rank
   * taken and recording its recency. Used by [loadFrom]; safe to call directly in tests.
   */
  fun loadEntry(keyHi: Long, keyLo: Int, rank: Int, lastSeenDay: Int) {
    // Ignore ranks outside [0, rankedSize) (e.g. a prior snapshot written when ranked_size was
    // larger): keeping them would store invalid ranks in the cumulative map (and crash
    // serialization). A dropped fingerprint is simply re-ranked into the valid range if seen again.
    if (rank !in 0 until rankedSize) return
    cumulative.put(keyHi, keyLo, rank)
    taken.set(rank)
    lastSeen[rank] = lastSeenDay.toShort()
  }

  /**
   * Rebuilds [cumulative], [taken], and [lastSeen] from a prior `SNAPSHOT` blob's records. Records
   * that predate `last_seen_days` (length mismatch) default every entry's recency to [eventDay] so
   * a one-time migration never prematurely frees a still-active rank.
   */
  suspend fun loadFrom(records: Flow<RankIndexMap>) {
    records.collect { record ->
      val fps = record.fingerprints
      val count = record.ranksCount
      val lastSeenDays = record.lastSeenDays
      val hasLastSeen = lastSeenDays.size() == count * LastSeenDayBytes.WIDTH
      var off = 0
      for (i in 0 until count) {
        val day =
          if (hasLastSeen) LastSeenDayBytes.read(lastSeenDays, i * LastSeenDayBytes.WIDTH)
          else eventDay
        loadEntry(
          EventIdDigestBytes.readHi(fps, off),
          EventIdDigestBytes.readLo(fps, off + 8),
          record.getRanks(i),
          day,
        )
        off += EventIdDigestBytes.WIDTH
      }
    }
  }

  /** Whether [keyHi]/[keyLo] currently holds a rank. */
  fun contains(keyHi: Long, keyLo: Int): Boolean = cumulative.containsKey(keyHi, keyLo)

  /** The rank of [keyHi]/[keyLo], or [Bytes12IntMap.NOT_PRESENT] if unranked. */
  fun get(keyHi: Long, keyLo: Int): Int = cumulative.get(keyHi, keyLo)

  /** The `last_seen` epoch-day recorded for [rank]. Exposed for tests. */
  fun lastSeenOf(rank: Int): Int = lastSeen[rank].toInt() and 0xFFFF

  /**
   * Frees the ranks of fingerprints that have aged out — `last_seen < cutoffEpochDay` — **unless**
   * the fingerprint appears in [todayFps]. Excluding today's fingerprints is essential: a
   * fingerprint whose persisted recency is old but which is observed again this dispatch must keep
   * its rank (and therefore its VID); [assign] will refresh its `last_seen` afterwards.
   *
   * Runs before any [assign] so the freed slots are reusable by today's allocation. Collects the
   * doomed keys first, then removes them, because mutating [cumulative] mid-iteration is undefined.
   *
   * @return the number of ranks freed.
   */
  fun freeAgedRanks(cutoffEpochDay: Int, todayFps: Bytes12IntMap): Long {
    val freeHi = ArrayList<Long>()
    val freeLo = ArrayList<Int>()
    cumulative.forEach { keyHi, keyLo, rank ->
      if (
        rank in 0 until rankedSize &&
          (lastSeen[rank].toInt() and 0xFFFF) < cutoffEpochDay &&
          !todayFps.containsKey(keyHi, keyLo)
      ) {
        freeHi.add(keyHi)
        freeLo.add(keyLo)
      }
    }
    for (i in freeHi.indices) {
      val rank = cumulative.remove(freeHi[i], freeLo[i])
      if (rank != Bytes12IntMap.NOT_PRESENT) {
        if (rank in 0 until rankedSize) taken.clear(rank)
        freed++
      }
    }
    return freed
  }

  /**
   * Assigns a rank to [keyHi]/[keyLo] for this dispatch, stamping its `last_seen` to [eventDay] and
   * recording it in [dayOnly]:
   * * already ranked -> reuses the existing rank (renewal; keeps the VID stable),
   * * otherwise -> claims the next free rank via [BitSet.nextClearBit], unless the subpool is full.
   *
   * @return the assigned rank, or `null` if the subpool reached [rankedSize] (overflow — the
   *   fingerprint stays unranked and falls back to the Phase-2 hash path).
   */
  fun assign(keyHi: Long, keyLo: Int): Int? {
    val existing = cumulative.get(keyHi, keyLo)
    if (existing != Bytes12IntMap.NOT_PRESENT) {
      if (existing in 0 until rankedSize) lastSeen[existing] = eventDay.toShort()
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
    lastSeen[rank] = eventDay.toShort()
    cumulative.put(keyHi, keyLo, rank)
    dayOnly.put(keyHi, keyLo, rank)
    allocated++
    return rank
  }

  /**
   * Assigns ranks for a fingerprint observed in a **backfill** dispatch, updating both the
   * [dayOnly] delta (the rank used to label the backfilled day) and the cumulative `SNAPSHOT` (the
   * live state carried forward), which may diverge.
   *
   * [oldRank] is the fingerprint's rank in the *old* snapshot (the cumulative just before the
   * backfilled day), or any value outside `[0, rankedSize)` ([Bytes12IntMap.NOT_PRESENT]) if it had
   * none.
   *
   * The **DAY_ONLY label** gives the person back their original rank for that day:
   * * in the old snapshot -> [oldRank],
   * * else already ranked (in the latest snapshot) -> its current rank,
   * * else -> a freshly allocated rank.
   *
   * The **cumulative `SNAPSHOT`** is built forward from the latest snapshot, only *adding* backfill
   * fingerprints (so they are not re-ranked on a later dispatch) — never lowering an existing rank:
   * * already ranked (case 3) -> keep its current rank (a renewal), refreshing `last_seen` only
   *   upward (the backfill date is older, so usually a no-op),
   * * else old rank still free (case 2) -> take it back,
   * * else (case 1, or case 2 where the old rank was already re-handed to someone else) -> a
   *   freshly allocated free slot. In the case-2 collision the cumulative gets a fresh rank while
   *   the DAY_ONLY still labels the day with the (shared) old rank.
   *
   * @return the DAY_ONLY label rank, or `null` if the subpool is full (overflow — unranked).
   */
  fun assignBackfill(keyHi: Long, keyLo: Int, oldRank: Int): Int? {
    val hasOld = oldRank in 0 until rankedSize
    val recentRank = cumulative.get(keyHi, keyLo)

    if (recentRank != Bytes12IntMap.NOT_PRESENT) {
      // case 3: keep the latest rank in the cumulative; never lower last_seen with the older date.
      if (recentRank in 0 until rankedSize) {
        val current = lastSeen[recentRank].toInt() and 0xFFFF
        if (eventDay > current) lastSeen[recentRank] = eventDay.toShort()
      }
      renewed++
      if (hasOld) {
        backfillReusedOldRank++
        if (oldRank != recentRank) backfillRankCollisions++
        dayOnly.put(keyHi, keyLo, oldRank)
        return oldRank
      }
      dayOnly.put(keyHi, keyLo, recentRank)
      return recentRank
    }

    if (hasOld && !taken.get(oldRank)) {
      // case 2 (old rank still free): take it back in both the cumulative and the day-only delta.
      taken.set(oldRank)
      lastSeen[oldRank] = eventDay.toShort()
      cumulative.put(keyHi, keyLo, oldRank)
      dayOnly.put(keyHi, keyLo, oldRank)
      allocated++
      backfillReusedOldRank++
      return oldRank
    }

    // case 1 (new fingerprint) or case 2 with the old rank already taken: allocate a fresh slot for
    // the cumulative. The day-only label still prefers the old rank when there is one.
    val newRank = taken.nextClearBit(cursor)
    if (newRank >= rankedSize) {
      overflow++
      return null
    }
    taken.set(newRank)
    cursor = newRank + 1
    lastSeen[newRank] = eventDay.toShort()
    cumulative.put(keyHi, keyLo, newRank)
    allocated++
    if (hasOld) {
      dayOnly.put(keyHi, keyLo, oldRank)
      backfillReusedOldRank++
      return oldRank
    }
    dayOnly.put(keyHi, keyLo, newRank)
    return newRank
  }

  /**
   * Streams the cumulative map as chunked [RankIndexMap] records (the new `SNAPSHOT` blob), each
   * carrying its entries' persisted `last_seen` recency.
   */
  fun streamCumulativeChunks(chunkEntries: Int = DEFAULT_CHUNK_ENTRIES): Flow<RankIndexMap> =
    streamChunks(cumulative, chunkEntries) { rank -> lastSeen[rank].toInt() and 0xFFFF }

  /**
   * Streams this dispatch's touched entries as chunked [RankIndexMap] records (`DAY_ONLY` blob);
   * every entry's `last_seen` is [eventDay] (they were all observed this dispatch).
   */
  fun streamDayOnlyChunks(chunkEntries: Int = DEFAULT_CHUNK_ENTRIES): Flow<RankIndexMap> =
    streamChunks(dayOnly, chunkEntries) { eventDay }

  private fun streamChunks(
    map: Bytes12IntMap,
    chunkEntries: Int,
    lastSeenOf: (rank: Int) -> Int,
  ): Flow<RankIndexMap> = flow {
    val total = map.size
    if (total == 0L) return@flow
    val chunkCount = minOf(chunkEntries.toLong(), total).toInt()
    var fps = ByteArray(chunkCount * EventIdDigestBytes.WIDTH)
    var ranks = IntArray(chunkCount)
    var seen = IntArray(chunkCount)
    var n = 0
    var produced = 0L
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

  private fun buildRecord(
    fps: ByteArray,
    ranks: IntArray,
    seen: IntArray,
    count: Int,
  ): RankIndexMap = rankIndexMap {
    poolOffset = this@RankAllocator.poolOffset
    rankedSize = this@RankAllocator.rankedSize
    fingerprints = UnsafeByteOperations.unsafeWrap(fps, 0, count * EventIdDigestBytes.WIDTH)
    val lastSeenBytes = ByteArray(count * LastSeenDayBytes.WIDTH)
    for (i in 0 until count) {
      this.ranks += ranks[i]
      LastSeenDayBytes.write(lastSeenBytes, i * LastSeenDayBytes.WIDTH, seen[i])
    }
    lastSeenDays = UnsafeByteOperations.unsafeWrap(lastSeenBytes)
  }

  companion object {
    /** ~16M entries (~288 MB of fps+ranks+last_seen) per record: one buffer in memory at a time. */
    const val DEFAULT_CHUNK_ENTRIES = 16 * 1024 * 1024
  }
}
