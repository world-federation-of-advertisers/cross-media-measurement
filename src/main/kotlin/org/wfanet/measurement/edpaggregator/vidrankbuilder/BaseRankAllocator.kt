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
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.flow
import org.wfanet.measurement.edpaggregator.v1alpha.RankIndexMap
import org.wfanet.measurement.edpaggregator.v1alpha.rankIndexMap
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

/**
 * Shared machinery for the two subpool rank allocators: the chunked [RankIndexMap] emission
 * ([streamChunks] + [buildRecord]), which is byte-identical across both. Only [poolOffset] and
 * [rankedSize] are needed to build a record; each subclass keeps its own `last_seen` array and
 * supplies it per call through the `lastSeenOf` lambda.
 *
 * The rank-*allocation* strategy is deliberately NOT shared. [RankAllocator] is serial with
 * old-rank reuse (the backfill path needs stable rank values), while [ConcurrentRankAllocator] is
 * striped/parallel and hands out ranks in arrival order (the forward path, non-reproducible by
 * design). Those `assign` / `loadFrom` / `freeAgedRanks` implementations have different guarantees,
 * so they stay in the subclasses; only the output serialization is common and lives here.
 *
 * @param poolOffset the subpool's `population_offset`, stamped on every emitted record.
 * @param rankedSize the subpool's ranked sub-range size, stamped on every emitted record.
 */
abstract class BaseRankAllocator(val poolOffset: Long, val rankedSize: Int) {
  /**
   * Streams the entries of [maps] as chunked [RankIndexMap] records, at most [chunkEntries] entries
   * per record; [lastSeenOf] supplies each entry's persisted `last_seen` day. A single map (the
   * serial allocator) or one map per stripe (the concurrent allocator) both work. Buffers are sized
   * to exactly what will fill them (initial = `min(chunk, total)`, each resize = `min(chunk,
   * remaining)`), so the final chunk fills exactly and no trailing partial emit is needed. The fill
   * counter carries across maps, so a chunk boundary may fall mid-map.
   */
  protected fun streamChunks(
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
    poolOffset = this@BaseRankAllocator.poolOffset
    rankedSize = this@BaseRankAllocator.rankedSize
    fingerprints = UnsafeByteOperations.unsafeWrap(fps, 0, count * EventIdDigestBytes.WIDTH)
    val lastSeenBytes = ByteArray(count * LastSeenDayBytes.WIDTH)
    // Bulk-add ranks in one call. Per-element `this.ranks += ranks[i]` allocated one boxed Int per
    // entry and paid N virtual dispatches through DslList; addAll skips both, at the cost of the
    // list-view copy in the subList branch.
    this.ranks += if (count == ranks.size) ranks.asList() else ranks.asList().subList(0, count)
    for (i in 0 until count) {
      LastSeenDayBytes.write(lastSeenBytes, i * LastSeenDayBytes.WIDTH, seen[i])
    }
    lastSeenDays = UnsafeByteOperations.unsafeWrap(lastSeenBytes)
  }
}
