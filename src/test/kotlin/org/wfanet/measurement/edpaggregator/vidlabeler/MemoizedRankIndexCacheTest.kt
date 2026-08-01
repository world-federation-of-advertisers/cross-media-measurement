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

import com.google.common.truth.Truth.assertThat
import java.util.concurrent.atomic.AtomicInteger
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.edpaggregator.vidlabeler.utils.Bytes12IntMap

@RunWith(JUnit4::class)
class MemoizedRankIndexCacheTest {
  private fun index(rank: Int): MemoizedRankIndex =
    MemoizedRankIndex.fromMaps(mapOf(0L to Bytes12IntMap().apply { put(1L, 1, rank) }))

  private fun key(vararg uris: String): MemoizedRankIndex.Key =
    MemoizedRankIndex.Key("dp", "ml", uris.toList())

  @Test
  fun `getOrBuild builds once and reuses the index on a key match`() =
    runBlocking<Unit> {
      val cache = MemoizedRankIndexCache()
      val builds = AtomicInteger(0)

      val first =
        cache.getOrBuild(key("a")) {
          builds.incrementAndGet()
          index(1)
        }
      val second =
        cache.getOrBuild(key("a")) {
          builds.incrementAndGet()
          index(2)
        }

      assertThat(builds.get()).isEqualTo(1)
      assertThat(second).isSameInstanceAs(first)
    }

  @Test
  fun `getOrBuild rebuilds on a changed key and does not retain the evicted index`() =
    runBlocking<Unit> {
      val cache = MemoizedRankIndexCache()
      val builds = AtomicInteger(0)

      val a1 =
        cache.getOrBuild(key("a")) {
          builds.incrementAndGet()
          index(1)
        }
      val b =
        cache.getOrBuild(key("b")) {
          builds.incrementAndGet()
          index(2)
        }

      assertThat(b).isNotSameInstanceAs(a1)
      assertThat(builds.get()).isEqualTo(2)

      // Size-1: the "a" entry was evicted when "b" was built, so requesting "a" again rebuilds
      // rather than returning the original a1 (the old index is not retained).
      val a2 =
        cache.getOrBuild(key("a")) {
          builds.incrementAndGet()
          index(3)
        }
      assertThat(a2).isNotSameInstanceAs(a1)
      assertThat(builds.get()).isEqualTo(3)
    }

  @Test
  fun `getOrBuild serializes concurrent callers so the same key builds exactly once`() =
    runBlocking<Unit> {
      val cache = MemoizedRankIndexCache()
      val builds = AtomicInteger(0)

      val results =
        (1..16)
          .map {
            async(Dispatchers.Default) {
              cache.getOrBuild(key("a")) {
                builds.incrementAndGet()
                delay(50) // widen the overlap window so an unsynchronized cache would double-build
                index(1)
              }
            }
          }
          .awaitAll()

      // The Mutex serialized the callers: only the first built; the rest observed the cached index.
      assertThat(builds.get()).isEqualTo(1)
      assertThat(results.toSet()).hasSize(1)
    }
}
