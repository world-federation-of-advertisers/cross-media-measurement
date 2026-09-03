// Copyright 2022 The Cross-Media Measurement Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the \License.

package org.wfanet.virtualpeople.core.model.utils

import kotlin.random.Random
import kotlin.random.nextULong
import kotlin.test.assertEquals
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

private const val KEY_NUMBER = 1000
private const val MAX_BUCKETS = 1000

@RunWith(JUnit4::class)
class ConsistentHashTest {

  private fun isConsistentJumpHashing(values: List<Int>): Boolean {
    if (values.isEmpty()) {
      return true
    }
    if (values[0] != 0) {
      /** First element should be zero. */
      return false
    }
    for (i in 1 until values.size) {
      if (values[i] != values[i - 1] && values[i] != i) {
        return false
      }
    }
    return true
  }

  /**
   * When using the same key, for any number of buckets n > 1, one of the following must be
   * satisfied.
   * 1. JumpConsistentHash(key, n) == JumpConsistentHash(key, n - 1)
   * 2. JumpConsistentHash(key, n) == n - 1
   */
  private fun checkCorrectnessForOneKey(key: ULong, maxBuckets: Int) {
    val values = (1..maxBuckets).map { jumpConsistentHash(key, it) }
    assertTrue(isConsistentJumpHashing(values))
  }

  @Test
  fun `check expected results of some input examples`() {
    assertEquals(93, jumpConsistentHash(1000UL, 1000))
    assertEquals(31613, jumpConsistentHash(1000UL, 1 shl 16))
  }

  @Test
  fun `test correctness with random keys`() {
    repeat(KEY_NUMBER) { checkCorrectnessForOneKey(Random.nextULong(), MAX_BUCKETS) }
  }

  @Test
  fun `test int max bucketss`() {
    repeat(KEY_NUMBER) { assertTrue { jumpConsistentHash(Random.nextULong(), Int.MAX_VALUE) >= 0 } }
  }
}
