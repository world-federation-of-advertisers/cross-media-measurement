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
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

private const val EVENT_DAY = 100

@RunWith(JUnit4::class)
class RankAllocatorTest {
  @Test
  fun `assign allocates sequential ranks from zero`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)

    assertThat(allocator.assign(1L, 0)).isEqualTo(0)
    assertThat(allocator.assign(2L, 0)).isEqualTo(1)
    assertThat(allocator.assign(3L, 0)).isEqualTo(2)

    assertThat(allocator.allocated).isEqualTo(3)
    assertThat(allocator.cumulativeSize).isEqualTo(3)
  }

  @Test
  fun `assign renews an already-ranked fingerprint and refreshes its last_seen`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    allocator.loadEntry(1L, 0, rank = 5, lastSeenDay = 3)

    val renewedRank = allocator.assign(1L, 0)

    assertThat(renewedRank).isEqualTo(5)
    assertThat(allocator.renewed).isEqualTo(1)
    assertThat(allocator.allocated).isEqualTo(0)
    assertThat(allocator.lastSeenOf(5)).isEqualTo(EVENT_DAY)
  }

  @Test
  fun `assign returns null and counts overflow once the subpool is full`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 2, eventDay = EVENT_DAY)

    assertThat(allocator.assign(1L, 0)).isEqualTo(0)
    assertThat(allocator.assign(2L, 0)).isEqualTo(1)
    assertThat(allocator.assign(3L, 0)).isNull()
    assertThat(allocator.assign(4L, 0)).isNull()

    assertThat(allocator.allocated).isEqualTo(2)
    assertThat(allocator.overflow).isEqualTo(2)
  }

  @Test
  fun `freeAgedRanks frees only aged ranks and a subsequent assign reuses a freed slot`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    // rank 0 + rank 2 are aged (last_seen 1); rank 1 is recent (last_seen 99).
    allocator.loadEntry(1L, 0, rank = 0, lastSeenDay = 1)
    allocator.loadEntry(2L, 0, rank = 1, lastSeenDay = 99)
    allocator.loadEntry(3L, 0, rank = 2, lastSeenDay = 1)

    val freed = allocator.freeAgedRanks(cutoffEpochDay = 50, todayFps = Bytes12IntMap())

    assertThat(freed).isEqualTo(2)
    assertThat(allocator.contains(1L, 0)).isFalse()
    assertThat(allocator.contains(3L, 0)).isFalse()
    assertThat(allocator.contains(2L, 0)).isTrue()
    // The lowest free slot is the just-freed rank 0.
    assertThat(allocator.assign(9L, 0)).isEqualTo(0)
  }

  @Test
  fun `freeAgedRanks never frees a fingerprint observed this dispatch`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    allocator.loadEntry(1L, 0, rank = 0, lastSeenDay = 1) // aged by date...
    val todayFps = Bytes12IntMap().apply { put(1L, 0, 1) } // ...but seen today.

    val freed = allocator.freeAgedRanks(cutoffEpochDay = 50, todayFps = todayFps)

    assertThat(freed).isEqualTo(0)
    assertThat(allocator.contains(1L, 0)).isTrue()
  }

  @Test
  fun `loadFrom rebuilds map, bitset, and last_seen`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    val record =
      buildRecord(
        7L,
        100,
        entries = listOf(Entry(10L, 0, rank = 5, day = 30), Entry(20L, 0, rank = 9, day = 40)),
      )

    allocator.loadFrom(listOf(record).asFlow())

    assertThat(allocator.get(10L, 0)).isEqualTo(5)
    assertThat(allocator.lastSeenOf(5)).isEqualTo(30)
    assertThat(allocator.lastSeenOf(9)).isEqualTo(40)
    // Ranks 5 and 9 are taken; a fresh assign skips them.
    assertThat(allocator.assign(30L, 0)).isEqualTo(0)
  }

  @Test
  fun `loadFrom defaults last_seen to eventDay when the record predates the field`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    // A legacy record: ranks present, last_seen_days absent.
    val legacy =
      RankIndexMap.newBuilder()
        .setPoolOffset(7L)
        .setRankedSize(100)
        .setFingerprints(packFingerprint(10L, 0))
        .addRanks(5)
        .build()

    allocator.loadFrom(listOf(legacy).asFlow())

    assertThat(allocator.lastSeenOf(5)).isEqualTo(EVENT_DAY)
  }

  @Test
  fun `streamCumulativeChunks round-trips ranks and last_seen across chunks`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    allocator.loadEntry(1L, 0, rank = 0, lastSeenDay = 11)
    allocator.loadEntry(2L, 0, rank = 1, lastSeenDay = 22)
    allocator.assign(3L, 0) // new entry -> rank 2, last_seen = EVENT_DAY

    val records = allocator.streamCumulativeChunks(chunkEntries = 2).toList()

    assertThat(records.map { it.ranksCount }).containsExactly(2, 1).inOrder()
    assertThat(records.all { it.poolOffset == 7L && it.rankedSize == 100 }).isTrue()
    // Every record carries a last_seen entry (2 bytes) per rank entry.
    assertThat(records.all { it.lastSeenDays.size() == it.ranksCount * LastSeenDayBytes.WIDTH })
      .isTrue()
    assertThat(decodeLastSeenByRank(records)).containsExactly(0, 11, 1, 22, 2, EVENT_DAY)
    Unit
  }

  @Test
  fun `streamDayOnlyChunks stamps eventDay and contains only this dispatch's entries`() =
    runBlocking {
      val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
      allocator.loadEntry(99L, 0, rank = 42, lastSeenDay = 3) // not touched this dispatch
      allocator.assign(1L, 0) // new
      allocator.loadEntry(2L, 0, rank = 7, lastSeenDay = 3)
      allocator.assign(2L, 0) // renewal

      val records = allocator.streamDayOnlyChunks().toList()

      val ranks = records.flatMap { it.ranksList }.toSet()
      assertThat(ranks).doesNotContain(42)
      assertThat(records.all { record -> decodeLastSeen(record).all { it == EVENT_DAY } }).isTrue()
    }

  @Test
  fun `loadEntry ignores ranks outside the ranked range`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 3, eventDay = EVENT_DAY)
    allocator.loadEntry(1L, 0, rank = 0, lastSeenDay = 5)
    allocator.loadEntry(2L, 0, rank = 9, lastSeenDay = 5) // out of range -> ignored

    assertThat(allocator.contains(1L, 0)).isTrue()
    assertThat(allocator.contains(2L, 0)).isFalse()
    assertThat(allocator.cumulativeSize).isEqualTo(1)
  }

  @Test
  fun `loadFrom drops out-of-range ranks so they are not re-serialized`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 2, eventDay = EVENT_DAY)
    // rank 5 is invalid for rankedSize 2 (e.g. ranked_size shrank); must be dropped on load.
    val record =
      buildRecord(
        7L,
        2,
        entries = listOf(Entry(1L, 0, rank = 0, day = 5), Entry(2L, 0, rank = 5, day = 5)),
      )

    allocator.loadFrom(listOf(record).asFlow())

    assertThat(allocator.contains(1L, 0)).isTrue()
    assertThat(allocator.contains(2L, 0)).isFalse()
    assertThat(allocator.streamCumulativeChunks().toList().flatMap { it.ranksList })
      .containsExactly(0)
    Unit
  }

  @Test
  fun `rankedSize of zero overflows every fingerprint`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 0, eventDay = EVENT_DAY)
    assertThat(allocator.assign(1L, 0)).isNull()
    assertThat(allocator.overflow).isEqualTo(1)
  }

  @Test
  fun `assignBackfill allocates a new rank for a fingerprint in neither snapshot`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)

    val label = allocator.assignBackfill(1L, 0, oldRank = Bytes12IntMap.NOT_PRESENT)

    assertThat(label).isEqualTo(0)
    assertThat(allocator.get(1L, 0)).isEqualTo(0)
    assertThat(allocator.allocated).isEqualTo(1)
    assertThat(allocator.backfillReusedOldRank).isEqualTo(0)
  }

  @Test
  fun `assignBackfill takes a free old rank back into both the cumulative and day-only`() =
    runBlocking<Unit> {
      val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
      allocator.loadEntry(2L, 0, rank = 0, lastSeenDay = 90) // someone else at rank 0; rank 5 free

      val label = allocator.assignBackfill(1L, 0, oldRank = 5)

      assertThat(label).isEqualTo(5)
      assertThat(allocator.get(1L, 0)).isEqualTo(5) // cumulative took the old rank back
      assertThat(allocator.allocated).isEqualTo(1)
      assertThat(allocator.backfillReusedOldRank).isEqualTo(1)
      assertThat(allocator.backfillRankCollisions).isEqualTo(0)
      assertThat(allocator.streamDayOnlyChunks().toList().flatMap { it.ranksList })
        .containsExactly(5)
    }

  @Test
  fun `assignBackfill labels with the old rank but takes a fresh cumulative rank when it is taken`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    allocator.loadEntry(2L, 0, rank = 5, lastSeenDay = 90) // a different fp already holds rank 5

    val label = allocator.assignBackfill(1L, 0, oldRank = 5)

    assertThat(label).isEqualTo(5) // day-only still labels with the original rank
    assertThat(allocator.get(1L, 0)).isEqualTo(0) // cumulative gets a fresh free rank instead
    assertThat(allocator.allocated).isEqualTo(1)
    assertThat(allocator.backfillReusedOldRank).isEqualTo(1)
    assertThat(allocator.backfillRankCollisions).isEqualTo(0) // not in the latest snapshot
  }

  @Test
  fun `assignBackfill keeps the latest rank when the fingerprint has no old rank`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
    allocator.loadEntry(1L, 0, rank = 2, lastSeenDay = 90)

    val label = allocator.assignBackfill(1L, 0, oldRank = Bytes12IntMap.NOT_PRESENT)

    assertThat(label).isEqualTo(2)
    assertThat(allocator.get(1L, 0)).isEqualTo(2)
    assertThat(allocator.renewed).isEqualTo(1)
    assertThat(allocator.backfillReusedOldRank).isEqualTo(0)
    // last_seen is refreshed upward to the dispatch's eventDay (never lowered).
    assertThat(allocator.lastSeenOf(2)).isEqualTo(EVENT_DAY)
  }

  @Test
  fun `assignBackfill keeps the latest rank and labels with the matching old rank`() =
    runBlocking<Unit> {
      val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
      allocator.loadEntry(1L, 0, rank = 5, lastSeenDay = 90) // latest rank == old rank

      val label = allocator.assignBackfill(1L, 0, oldRank = 5)

      assertThat(label).isEqualTo(5)
      assertThat(allocator.get(1L, 0)).isEqualTo(5)
      assertThat(allocator.renewed).isEqualTo(1)
      assertThat(allocator.backfillReusedOldRank).isEqualTo(1)
      assertThat(allocator.backfillRankCollisions).isEqualTo(0) // old == latest, no divergence
      assertThat(allocator.streamDayOnlyChunks().toList().flatMap { it.ranksList })
        .containsExactly(5)
    }

  @Test
  fun `assignBackfill counts a collision when the old rank differs from the latest rank`() =
    runBlocking<Unit> {
      val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100, eventDay = EVENT_DAY)
      allocator.loadEntry(1L, 0, rank = 8, lastSeenDay = 90) // latest rank 8, old rank 5

      val label = allocator.assignBackfill(1L, 0, oldRank = 5)

      assertThat(label).isEqualTo(5) // day-only labels with the old rank
      assertThat(allocator.get(1L, 0)).isEqualTo(8) // cumulative keeps the latest rank
      assertThat(allocator.renewed).isEqualTo(1)
      assertThat(allocator.backfillReusedOldRank).isEqualTo(1)
      assertThat(allocator.backfillRankCollisions).isEqualTo(1)
      assertThat(allocator.streamDayOnlyChunks().toList().flatMap { it.ranksList })
        .containsExactly(5)
    }

  @Test
  fun `assignBackfill overflows when the subpool is full and the fingerprint is new`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 1, eventDay = EVENT_DAY)
    allocator.loadEntry(2L, 0, rank = 0, lastSeenDay = 90) // subpool full

    val label = allocator.assignBackfill(1L, 0, oldRank = Bytes12IntMap.NOT_PRESENT)

    assertThat(label).isNull()
    assertThat(allocator.overflow).isEqualTo(1)
  }

  private data class Entry(val hi: Long, val lo: Int, val rank: Int, val day: Int)

  private fun packFingerprint(hi: Long, lo: Int): com.google.protobuf.ByteString {
    val fp = ByteArray(12)
    EventIdDigestBytes.writeHi(fp, 0, hi)
    EventIdDigestBytes.writeLo(fp, 8, lo)
    return com.google.protobuf.ByteString.copyFrom(fp)
  }

  private fun buildRecord(poolOffset: Long, rankedSize: Int, entries: List<Entry>): RankIndexMap {
    val fps = ByteArray(entries.size * 12)
    val lastSeen = ByteArray(entries.size * LastSeenDayBytes.WIDTH)
    val builder = RankIndexMap.newBuilder().setPoolOffset(poolOffset).setRankedSize(rankedSize)
    entries.forEachIndexed { i, e ->
      EventIdDigestBytes.writeHi(fps, i * 12, e.hi)
      EventIdDigestBytes.writeLo(fps, i * 12 + 8, e.lo)
      builder.addRanks(e.rank)
      LastSeenDayBytes.write(lastSeen, i * LastSeenDayBytes.WIDTH, e.day)
    }
    return builder
      .setFingerprints(com.google.protobuf.ByteString.copyFrom(fps))
      .setLastSeenDays(com.google.protobuf.ByteString.copyFrom(lastSeen))
      .build()
  }

  /** Decodes a record's packed `last_seen_days` into per-entry epoch-days. */
  private fun decodeLastSeen(record: RankIndexMap): List<Int> =
    (0 until record.ranksCount).map {
      LastSeenDayBytes.read(record.lastSeenDays, it * LastSeenDayBytes.WIDTH)
    }

  /** Flattens `[rank0, lastSeen0, rank1, lastSeen1, …]` for order-insensitive assertion. */
  private fun decodeLastSeenByRank(records: List<RankIndexMap>): List<Int> {
    val out = mutableListOf<Int>()
    for (record in records) {
      val lastSeen = decodeLastSeen(record)
      for (i in 0 until record.ranksCount) {
        out.add(record.getRanks(i))
        out.add(lastSeen[i])
      }
    }
    return out
  }
}
