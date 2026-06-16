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

@RunWith(JUnit4::class)
class RankAllocatorTest {
  @Test
  fun `assign allocates sequential ranks from zero`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)

    assertThat(allocator.assign(1L, 0)).isEqualTo(0)
    assertThat(allocator.assign(2L, 0)).isEqualTo(1)
    assertThat(allocator.assign(3L, 0)).isEqualTo(2)

    assertThat(allocator.allocated).isEqualTo(3)
    assertThat(allocator.cumulativeSize).isEqualTo(3)
  }

  @Test
  fun `assign renews an already-ranked fingerprint without consuming a new rank`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)
    val rank = allocator.assign(1L, 0)

    val renewedRank = allocator.assign(1L, 0)

    assertThat(renewedRank).isEqualTo(rank)
    assertThat(allocator.allocated).isEqualTo(1)
    assertThat(allocator.renewed).isEqualTo(1)
    assertThat(allocator.cumulativeSize).isEqualTo(1)
  }

  @Test
  fun `assign returns null and counts overflow once the subpool is full`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 2)

    assertThat(allocator.assign(1L, 0)).isEqualTo(0)
    assertThat(allocator.assign(2L, 0)).isEqualTo(1)
    assertThat(allocator.assign(3L, 0)).isNull()
    assertThat(allocator.assign(4L, 0)).isNull()

    assertThat(allocator.allocated).isEqualTo(2)
    assertThat(allocator.overflow).isEqualTo(2)
  }

  @Test
  fun `free releases a rank and a subsequent assign reuses the freed slot`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)
    // Pre-load three entries (as a snapshot load would), then free the middle rank before any
    // allocation — mirroring the retention-before-allocation ordering.
    allocator.loadEntry(1L, 0, 0)
    allocator.loadEntry(2L, 0, 1)
    allocator.loadEntry(3L, 0, 2)

    allocator.free(2L, 0)
    assertThat(allocator.freed).isEqualTo(1)
    assertThat(allocator.contains(2L, 0)).isFalse()

    // The next allocation claims the lowest free slot, which is the just-freed rank 1.
    assertThat(allocator.assign(4L, 0)).isEqualTo(1)
  }

  @Test
  fun `free is a no-op for an unranked fingerprint`() {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)
    allocator.free(99L, 0)
    assertThat(allocator.freed).isEqualTo(0)
  }

  @Test
  fun `loadFrom rebuilds the map and bitset so taken ranks are not reallocated`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)
    val record =
      buildRecord(
        poolOffset = 7L,
        rankedSize = 100,
        entries = listOf((10L to 0) to 5, (20L to 0) to 9)
      )

    allocator.loadFrom(listOf(record).asFlow())

    assertThat(allocator.get(10L, 0)).isEqualTo(5)
    assertThat(allocator.get(20L, 0)).isEqualTo(9)
    // Ranks 5 and 9 are taken; a fresh assign skips them.
    assertThat(allocator.assign(30L, 0)).isEqualTo(0)
  }

  @Test
  fun `streamCumulativeChunks round-trips every entry across multiple chunks`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)
    val assigned = mutableMapOf<Pair<Long, Int>, Int>()
    for (i in 1..5) {
      val rank = allocator.assign(i.toLong(), i)!!
      assigned[i.toLong() to i] = rank
    }

    val records = allocator.streamCumulativeChunks(chunkEntries = 2).toList()

    // 5 entries at 2 per chunk -> 3 records (2, 2, 1).
    assertThat(records.map { it.ranksCount }).containsExactly(2, 2, 1).inOrder()
    assertThat(records.all { it.poolOffset == 7L && it.rankedSize == 100 }).isTrue()
    assertThat(decode(records)).isEqualTo(assigned)
  }

  @Test
  fun `streamDayOnlyChunks contains only this dispatch's touched entries`() = runBlocking {
    val allocator = RankAllocator(poolOffset = 7L, rankedSize = 100)
    // A pre-existing entry that is NOT touched this dispatch must not appear in the day-only blob.
    allocator.loadEntry(99L, 0, 42)
    val a = allocator.assign(1L, 0)!! // new
    allocator.loadEntry(2L, 0, 7)
    val renewed = allocator.assign(2L, 0)!! // renewal of a loaded entry

    val dayOnly = decode(allocator.streamDayOnlyChunks().toList())

    assertThat(dayOnly).containsExactly(1L to 0, a, 2L to 0, renewed)
    assertThat(dayOnly).doesNotContainKey(99L to 0)
  }

  private fun buildRecord(
    poolOffset: Long,
    rankedSize: Int,
    entries: List<Pair<Pair<Long, Int>, Int>>,
  ): RankIndexMap {
    val fps = ByteArray(entries.size * 12)
    val builder = RankIndexMap.newBuilder().setPoolOffset(poolOffset).setRankedSize(rankedSize)
    entries.forEachIndexed { i, (key, rank) ->
      FingerprintCodec.writeHi(fps, i * 12, key.first)
      FingerprintCodec.writeLo(fps, i * 12 + 8, key.second)
      builder.addRanks(rank)
    }
    return builder.setFingerprints(com.google.protobuf.ByteString.copyFrom(fps)).build()
  }

  /** Decodes a sequence of records into a `(hi, lo) -> rank` map. */
  private fun decode(records: List<RankIndexMap>): Map<Pair<Long, Int>, Int> {
    val out = mutableMapOf<Pair<Long, Int>, Int>()
    for (record in records) {
      val fps = record.fingerprints
      for (i in 0 until record.ranksCount) {
        val hi = FingerprintCodec.readHi(fps, i * 12)
        val lo = FingerprintCodec.readLo(fps, i * 12 + 8)
        out[hi to lo] = record.getRanks(i)
      }
    }
    return out
  }
}
