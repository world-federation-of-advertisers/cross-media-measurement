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
 * Process-scoped, size-one cache of an upload's saved [MemoizedRankIndex].
 *
 * The caller resolves the exact upload's DAY_ONLY set for every WorkItem. Jobs selecting the same
 * immutable subpool blobs reuse their index; a different upload or selected set rebuilds it.
 *
 * A [Mutex] serializes builds. On a cache miss the old reference is dropped before building the
 * replacement, preserving the existing one-index memory bound. Failed builds are not cached. One
 * instance is shared by every WorkItem in [VidLabelerAppRunner].
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
      if (cachedKey == key) {
        val cached = cachedIndex
        if (cached != null) return@withLock cached
      }
      // Miss: drop the old index — the field AND any local — BEFORE building the new one so it can
      // be reclaimed during the build and the peak stays a single index. Binding the old index to a
      // local that outlives build() would pin it in heap and defeat that (two multi-GB indexes at
      // once).
      cachedIndex = null
      cachedKey = null
      val built = build()
      cachedIndex = built
      cachedKey = key
      built
    }
}
