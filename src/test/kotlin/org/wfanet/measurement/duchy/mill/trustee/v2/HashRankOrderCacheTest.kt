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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.common.truth.Truth.assertThat
import java.util.concurrent.Callable
import java.util.concurrent.Executors
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.internal.duchy.PopulationSpec
import org.wfanet.measurement.internal.duchy.PopulationSpecKt
import org.wfanet.measurement.internal.duchy.populationSpec

@RunWith(JUnit4::class)
class HashRankOrderCacheTest {
  @Test
  fun `get returns the cached order of an equal PopulationSpec`() {
    val cache = HashRankOrderCache(maxSize = 1)

    val first: HashRankOrder = cache.get(testPopulationSpec(lastVid = 99L))
    val second: HashRankOrder = cache.get(testPopulationSpec(lastVid = 99L))

    assertThat(second).isSameInstanceAs(first)
  }

  @Test
  fun `get rebuilds an order evicted by a more recently used PopulationSpec`() {
    val cache = HashRankOrderCache(maxSize = 1)
    val first: HashRankOrder = cache.get(testPopulationSpec(lastVid = 99L))

    cache.get(testPopulationSpec(lastVid = 199L))
    val rebuilt: HashRankOrder = cache.get(testPopulationSpec(lastVid = 99L))

    assertThat(rebuilt).isNotSameInstanceAs(first)
  }

  @Test
  fun `get keeps maxSize orders`() {
    val cache = HashRankOrderCache(maxSize = 2)
    val first: HashRankOrder = cache.get(testPopulationSpec(lastVid = 99L))

    cache.get(testPopulationSpec(lastVid = 199L))

    assertThat(cache.get(testPopulationSpec(lastVid = 99L))).isSameInstanceAs(first)
  }

  @Test
  fun `get builds an order once for concurrent lookups`() {
    val cache = HashRankOrderCache(maxSize = 1)
    val executor = Executors.newFixedThreadPool(THREAD_COUNT)
    try {
      val orders: List<HashRankOrder> =
        executor
          .invokeAll(List(THREAD_COUNT) { Callable { cache.get(testPopulationSpec(99_999L)) } })
          .map { it.get() }

      assertThat(orders.distinct()).hasSize(1)
    } finally {
      executor.shutdown()
    }
  }

  @Test
  fun `constructor throws for a non-positive maxSize`() {
    assertFailsWith<IllegalArgumentException> { HashRankOrderCache(maxSize = 0) }
  }

  companion object {
    private const val THREAD_COUNT = 8

    private fun testPopulationSpec(lastVid: Long): PopulationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 0L
              endVidInclusive = lastVid
            }
        }
    }
  }
}
