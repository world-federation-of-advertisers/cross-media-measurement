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

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap

private const val EVENT_DAY = 100
private const val POOL = 7L

/**
 * Correctness tests for [ConcurrentRankAllocator] — the parallel, striped-map / shared-rank-pool
 * forward allocator. Every parallel phase is driven on [Dispatchers.Default] so the tests exercise
 * real cross-stripe concurrency.
 */
@RunWith(JUnit4::class)
class ConcurrentRankAllocatorTest {

  @Test
  fun `cold build assigns a dense bijection from zero across stripes`() =
    runBlocking<Unit> {
      val allocator =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 8)
      val fps = distinctFps(50)
      fps.forEach { (hi, lo) -> allocator.addToday(hi, lo) }

      allocator.assign(Dispatchers.Default)

      assertThat(allocator.allocated).isEqualTo(50)
      assertThat(allocator.renewed).isEqualTo(0)
      assertThat(allocator.overflow).isEqualTo(0)
      val snapshot = decode(allocator.streamCumulativeChunks().toList())
      // Every fingerprint has exactly one rank...
      assertThat(snapshot.keys).containsExactlyElementsIn(fps)
      // ...ranks are unique and form the contiguous dense set [0, 50).
      assertThat(snapshot.values.toSet()).isEqualTo((0 until 50).toSet())
      // The DAY_ONLY delta mirrors the cumulative on a cold dispatch.
      assertThat(decode(allocator.streamDayOnlyChunks().toList())).isEqualTo(snapshot)
      // Every rank is stamped with the dispatch's event day.
      assertThat(snapshotLastSeen(allocator).values.toSet()).containsExactly(EVENT_DAY)
    }

  @Test
  fun `no rank is shared by two fingerprints across all stripes`() =
    runBlocking<Unit> {
      val allocator =
        ConcurrentRankAllocator(POOL, rankedSize = 1000, eventDay = EVENT_DAY, stripes = 8)
      val fps = distinctFps(500)
      fps.forEach { (hi, lo) -> allocator.addToday(hi, lo) }

      allocator.assign(Dispatchers.Default)

      val ranks = decode(allocator.streamCumulativeChunks().toList()).values.toList()
      assertThat(ranks).hasSize(500)
      assertThat(ranks.toSet()).hasSize(500) // all distinct -> no rank shared
    }

  @Test
  fun `overflow happens only once the whole ranked range is exhausted`() =
    runBlocking<Unit> {
      val allocator =
        ConcurrentRankAllocator(POOL, rankedSize = 10, eventDay = EVENT_DAY, stripes = 4)
      val fps = distinctFps(25)
      fps.forEach { (hi, lo) -> allocator.addToday(hi, lo) }

      allocator.assign(Dispatchers.Default)

      assertThat(allocator.allocated).isEqualTo(10)
      assertThat(allocator.overflow).isEqualTo(15) // exactly #distinct - rankedSize
      // Nothing is stranded: ALL ranks [0, 10) are used.
      val ranks = decode(allocator.streamCumulativeChunks().toList()).values.toSet()
      assertThat(ranks).isEqualTo((0 until 10).toSet())
    }

  @Test
  fun `no overflow when the ranked range exceeds the fingerprint count`() =
    runBlocking<Unit> {
      val allocator =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 4)
      distinctFps(25).forEach { (hi, lo) -> allocator.addToday(hi, lo) }

      allocator.assign(Dispatchers.Default)

      assertThat(allocator.overflow).isEqualTo(0)
      assertThat(allocator.allocated).isEqualTo(25)
    }

  @Test
  fun `a fingerprint seen in multiple records gets exactly one rank`() =
    runBlocking<Unit> {
      val allocator =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 8)
      // F(5) appears three times (as it would across records); plus 9 other distinct fps.
      repeat(3) { allocator.addToday(5L, 0) }
      (10..18).forEach { allocator.addToday(it.toLong(), 0) }

      allocator.assign(Dispatchers.Default)

      val snapshot = decode(allocator.streamCumulativeChunks().toList())
      assertThat(snapshot).hasSize(10) // 1 (deduped) + 9
      assertThat(allocator.allocated).isEqualTo(10)
      assertThat(snapshot.values.toSet()).isEqualTo((0 until 10).toSet())
    }

  @Test
  fun `parallel forward build matches the serial allocator as a rank bijection`() =
    runBlocking<Unit> {
      val fps = distinctFps(64)

      // Serial reference.
      val serial = RankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY)
      fps.forEach { (hi, lo) -> serial.assign(hi, lo) }
      val serialRanks = serial.streamCumulativeChunks().toList().let(::decode)

      // Parallel.
      val parallel =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 8)
      fps.forEach { (hi, lo) -> parallel.addToday(hi, lo) }
      parallel.assign(Dispatchers.Default)
      val parallelRanks = decode(parallel.streamCumulativeChunks().toList())

      // Same fingerprints, both a dense bijection onto the same rank set [0, 64).
      assertThat(parallelRanks.keys).containsExactlyElementsIn(serialRanks.keys)
      assertThat(parallelRanks.values.toSet()).isEqualTo(serialRanks.values.toSet())
      assertThat(parallelRanks.values.toSet()).isEqualTo((0 until 64).toSet())
    }

  @Test
  fun `renewal across two dispatches keeps overlapping ranks and freshly ranks new ones`() =
    runBlocking<Unit> {
      // Dispatch 1 (cold): fingerprints 1..10.
      val first = ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 4)
      val firstFps = (1L..10L).map { it to 0 }
      firstFps.forEach { (hi, lo) -> first.addToday(hi, lo) }
      first.assign(Dispatchers.Default)
      val snapshot = first.streamCumulativeChunks().toList()
      val firstRanks = decode(snapshot)

      // Dispatch 2 (warm): load the snapshot, then observe 5..14 (5..10 overlap, 11..14 new).
      val second =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY + 1, stripes = 4)
      second.loadFrom(snapshot.asFlow())
      (5L..14L).forEach { second.addToday(it, 0) }
      // cutoff below every last_seen, so retention frees nothing.
      second.freeAgedRanks(cutoffEpochDay = 0, Dispatchers.Default)
      second.assign(Dispatchers.Default)

      assertThat(second.renewed).isEqualTo(6) // 5..10
      assertThat(second.allocated).isEqualTo(4) // 11..14
      val secondRanks = decode(second.streamCumulativeChunks().toList())
      // Overlapping fingerprints keep their original ranks.
      for (fp in (5L..10L).map { it to 0 }) {
        assertThat(secondRanks[fp]).isEqualTo(firstRanks[fp])
      }
      // No two fingerprints share a rank after the merge.
      assertThat(secondRanks.values.toList()).hasSize(14)
      assertThat(secondRanks.values.toSet()).hasSize(14)
    }

  @Test
  fun `the rank set is a dense bijection regardless of stripe count`() =
    runBlocking<Unit> {
      val fps = distinctFps(40)

      val one = ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 1)
      fps.forEach { (hi, lo) -> one.addToday(hi, lo) }
      one.assign(Dispatchers.Default)
      val ranksS1 = decode(one.streamCumulativeChunks().toList())

      val eight = ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 8)
      fps.forEach { (hi, lo) -> eight.addToday(hi, lo) }
      eight.assign(Dispatchers.Default)
      val ranksS8 = decode(eight.streamCumulativeChunks().toList())

      // Same fingerprints, and each is a dense bijection onto [0, 40) regardless of S.
      assertThat(ranksS1.keys).containsExactlyElementsIn(fps)
      assertThat(ranksS8.keys).containsExactlyElementsIn(fps)
      assertThat(ranksS1.values.toSet()).isEqualTo((0 until 40).toSet())
      assertThat(ranksS8.values.toSet()).isEqualTo((0 until 40).toSet())
    }

  @Test
  fun `retention frees an aged rank and a new fingerprint reuses the freed slot`() =
    runBlocking<Unit> {
      // Dispatch 1 places fingerprints 1..3 at ranks with last_seen == EVENT_DAY.
      val first = ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 4)
      (1L..3L).forEach { first.addToday(it, 0) }
      first.assign(Dispatchers.Default)
      val snapshot = first.streamCumulativeChunks().toList()
      val firstRanks = decode(snapshot)

      // Dispatch 2 observes only a brand-new fingerprint; fingerprints 1..3 age out (cutoff past
      // EVENT_DAY) and are not seen today, so all three ranks are freed and returned to the pool.
      val second =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY + 50, stripes = 4)
      second.loadFrom(snapshot.asFlow())
      second.addToday(99L, 0)
      second.freeAgedRanks(cutoffEpochDay = EVENT_DAY + 1, Dispatchers.Default)
      second.assign(Dispatchers.Default)

      assertThat(second.freed).isEqualTo(3)
      val secondRanks = decode(second.streamCumulativeChunks().toList())
      // Only the new fingerprint survives, and it reused one of the freed ranks (dense, no new
      // high).
      assertThat(secondRanks.keys).containsExactly(99L to 0)
      assertThat(firstRanks.values.toSet()).contains(secondRanks.getValue(99L to 0))
    }

  @Test
  fun `addTodayBulk collapses duplicates and matches per-fingerprint addToday across stripes`() =
    runBlocking<Unit> {
      val stripes = 4
      val perFp =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = stripes)
      val bulk =
        ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = stripes)
      // 20 distinct fingerprints spread across stripes, plus two duplicates.
      val fps = (1L..20L).map { it to 0 } + listOf(5L to 0, 12L to 0)

      // Reference: one synchronized put per fingerprint.
      fps.forEach { (hi, lo) -> perFp.addToday(hi, lo) }
      // Under test: bucket by stripe, then one addTodayBulk per stripe.
      val byStripe = HashMap<Int, MutableList<Pair<Long, Int>>>()
      fps.forEach { (hi, lo) ->
        byStripe.getOrPut(bulk.stripeOf(hi)) { mutableListOf() }.add(hi to lo)
      }
      for ((s, list) in byStripe) {
        bulk.addTodayBulk(
          s,
          LongArray(list.size) { list[it].first },
          IntArray(list.size) { list[it].second },
          list.size,
        )
      }

      // Identical deduplicated set size.
      assertThat(bulk.todaySize()).isEqualTo(perFp.todaySize())
      assertThat(bulk.todaySize()).isEqualTo(20L)

      // ...and assign yields the same dense rank set.
      perFp.assign(Dispatchers.Default)
      bulk.assign(Dispatchers.Default)
      assertThat(decode(bulk.streamCumulativeChunks().toList()).values.toSet())
        .isEqualTo((0 until 20).toSet())
      assertThat(bulk.allocated).isEqualTo(perFp.allocated)
    }

  @Test
  fun `cumulative is pre-sized for prior plus today and does not resize on a warm dispatch`() =
    runBlocking<Unit> {
      // Dispatch 1 (cold): 10 fingerprints.
      val first = ConcurrentRankAllocator(POOL, rankedSize = 100, eventDay = EVENT_DAY, stripes = 4)
      (1L..10L).forEach { first.addToday(it, 0) }
      first.assign(Dispatchers.Default)
      val snapshot = first.streamCumulativeChunks().toList()
      val firstRanks = decode(snapshot)

      // Dispatch 2 (warm): pre-size the cumulative to comfortably hold prior(10)+today(4) per
      // stripe.
      val second =
        ConcurrentRankAllocator(
          POOL,
          rankedSize = 100,
          eventDay = EVENT_DAY + 1,
          stripes = 4,
          todayStripeCapacity = 16,
          cumulativeStripeCapacity = 64,
          estimatedTotalRanks = 14,
        )
      val capacityBefore = second.cumulativeStripeCapacity()
      second.loadFrom(snapshot.asFlow())
      // Observe 1..14: 1..10 overlap the prior snapshot (renewals), 11..14 are new.
      (1L..14L).forEach { second.addToday(it, 0) }
      second.freeAgedRanks(cutoffEpochDay = 0, Dispatchers.Default)
      second.assign(Dispatchers.Default)

      // No resize occurred at the pre-sized capacity.
      assertThat(second.cumulativeStripeCapacity()).isEqualTo(capacityBefore)
      // Ranks are identical to the unsized path: overlap keeps its rank, new fps are added densely.
      assertThat(second.renewed).isEqualTo(10)
      assertThat(second.allocated).isEqualTo(4)
      val secondRanks = decode(second.streamCumulativeChunks().toList())
      for (fp in (1L..10L).map { it to 0 }) {
        assertThat(secondRanks[fp]).isEqualTo(firstRanks[fp])
      }
      assertThat(secondRanks.values.toSet()).isEqualTo((0 until 14).toSet())
    }

  private companion object {
    /** Distinct fingerprints `(i, 0)` for `i in 1..count`, spread across stripes by `hi`. */
    private fun distinctFps(count: Int): List<Pair<Long, Int>> = (1..count).map { it.toLong() to 0 }

    private fun decode(records: List<RankIndexMap>): Map<Pair<Long, Int>, Int> {
      val out = mutableMapOf<Pair<Long, Int>, Int>()
      for (record in records) {
        val fps = record.fingerprints
        for (i in 0 until record.ranksCount) {
          val hi = EventIdDigestBytes.readHi(fps, i * EventIdDigestBytes.WIDTH)
          val lo = EventIdDigestBytes.readLo(fps, i * EventIdDigestBytes.WIDTH + 8)
          out[hi to lo] = record.getRanks(i)
        }
      }
      return out
    }

    private fun snapshotLastSeen(allocator: ConcurrentRankAllocator): Map<Pair<Long, Int>, Int> =
      runBlocking {
        val out = mutableMapOf<Pair<Long, Int>, Int>()
        for (record in allocator.streamCumulativeChunks().toList()) {
          val fps = record.fingerprints
          for (i in 0 until record.ranksCount) {
            val hi = EventIdDigestBytes.readHi(fps, i * EventIdDigestBytes.WIDTH)
            val lo = EventIdDigestBytes.readLo(fps, i * EventIdDigestBytes.WIDTH + 8)
            out[hi to lo] = LastSeenDayBytes.read(record.lastSeenDays, i * LastSeenDayBytes.WIDTH)
          }
        }
        out
      }
  }
}
