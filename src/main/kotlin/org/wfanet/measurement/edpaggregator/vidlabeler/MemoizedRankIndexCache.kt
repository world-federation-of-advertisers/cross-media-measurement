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

package org.wfanet.measurement.edpaggregator.vidlabeler

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock

/**
 * Process-scoped, size-1 cache of the most recently built [MemoizedRankIndex], keyed by its content
 * identity ([MemoizedRankIndex.Key] — `(dataProvider, modelLine)` + the sorted chosen SNAPSHOT blob
 * URIs).
 *
 * The memoized Phase-2 index for a large model line can be tens of GB, and building it dominates a
 * WorkItem's wall-clock. Consecutive WorkItems for the same `(dataProvider, modelLine)` almost
 * always resolve to the SAME chosen blobs (Phase-1 has not produced a new snapshot between them),
 * so rebuilding the identical index each time is pure waste. The caller runs the cheap listing
 * ([MemoizedRankIndex.resolveLatestBlobs]) on EVERY WorkItem — so a new Phase-1 snapshot is always
 * detected — and only the expensive download+decode is skipped on a [Key] match.
 *
 * Reuse is behavior-preserving: a [MemoizedRankIndex] is immutable after construction, its lookups
 * are a pure function of the chosen blobs, and `RankIndexBlob` URIs are per-write-unique over
 * immutable content — so an equal [Key] would build a byte-identical index. A changed snapshot set
 * yields a different [Key] and a rebuild.
 *
 * **Memory.** The cache holds exactly one index. On a miss it drops the old reference BEFORE
 * building the new one, so the old (~tens of GB) index is GC-reclaimable while the new one is built
 * and the peak stays a single index, not two.
 *
 * **Concurrency.** A [Mutex] serializes [getOrBuild], including across the (suspending) build: at
 * most one index is built at a time, and concurrent callers for the same key wait and then observe
 * the freshly cached index rather than each building their own (which would transiently hold two
 * multi-GB indexes). This is the intended trade-off — the labeler is memory-bound on the index, so
 * serializing its (re)build is preferable to duplicating it.
 *
 * **Cross-key serialization.** Callers with DIFFERENT keys also serialize on the mutex — the second
 * waits for the first's build to complete, then evicts it and rebuilds. Two concurrent WorkItems
 * for two different (dataProvider, modelLine) pairs on the same VM therefore run their builds
 * sequentially, not in parallel. Intentional: two multi-GB indexes in heap simultaneously would OOM
 * the VM. (In practice the TEE app leases one WorkItem at a time, so this contention does not arise
 * in production.)
 *
 * One instance per process (wired at `VidLabelerAppRunner`, shared by every WorkItem), NOT one per
 * WorkItem.
 */
class MemoizedRankIndexCache {
  private val mutex = Mutex()
  private var cachedKey: MemoizedRankIndex.Key? = null
  private var cachedIndex: MemoizedRankIndex? = null

  /**
   * Returns the cached [MemoizedRankIndex] when it was built for [key], otherwise builds it via
   * [build] (dropping any previously cached index first so peak memory stays one index) and caches
   * it. Serialized across concurrent callers.
   */
  suspend fun getOrBuild(
    key: MemoizedRankIndex.Key,
    build: suspend () -> MemoizedRankIndex,
  ): MemoizedRankIndex =
    mutex.withLock {
      val existing = cachedIndex
      if (existing != null && cachedKey == key) {
        return@withLock existing
      }
      // Miss: release the old index reference BEFORE building the new one so it can be reclaimed
      // during the build and the peak stays a single index.
      cachedIndex = null
      cachedKey = null
      val built = build()
      cachedIndex = built
      cachedKey = key
      built
    }
}
