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

import kotlin.test.assertEquals
import kotlin.test.assertFailsWith
import kotlin.test.assertTrue
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

private const val SEED_NUMBER = 10000

@RunWith(JUnit4::class)
class DistributedConsistentHashingTest {

  @Test
  fun `empty distribution should fail`() {
    assertFailsWith<IllegalStateException> { DistributedConsistentHashing(listOf()) }
  }

  @Test
  fun `zero probabilities sum should fail`() {
    assertFailsWith<IllegalStateException> {
      DistributedConsistentHashing(listOf(DistributionChoice(0, 0.0), DistributionChoice(1, 0.0)))
    }
  }

  @Test
  fun `negative probability should fail`() {
    assertFailsWith<IllegalStateException> {
      DistributedConsistentHashing(listOf(DistributionChoice(0, -1.0)))
    }
  }

  @Test
  fun `output distribution should match`() {
    // Distribution:
    // choice_id probability
    // 0         0.4
    // 1         0.2
    // 2         0.2
    // 3         0.2
    val distributions =
      listOf(
        DistributionChoice(0, 0.4),
        DistributionChoice(1, 0.2),
        DistributionChoice(2, 0.2),
        DistributionChoice(3, 0.2)
      )
    val hashing = DistributedConsistentHashing(distributions)

    val outputCounts = mutableMapOf<Int, Int>()
    (0 until SEED_NUMBER).forEach {
      val output = hashing.hash(it.toString())
      outputCounts[output] = outputCounts.getOrDefault(output, 0) + 1
    }

    assertEquals(outputCounts.keys, setOf(0, 1, 2, 3))
    /**
     * Compare to the exact results to make sure C++ and Kotlin implementations have same output.
     */
    assertEquals(4032, outputCounts[0]!!)
    assertEquals(2012, outputCounts[1]!!)
    assertEquals(1996, outputCounts[2]!!)
    assertEquals(1960, outputCounts[3]!!)
  }

  @Test
  fun `not normalized should fail`() {
    // Distribution:
    // choice_id probability
    // 0         0.8
    // 1         0.4
    // 2         0.4
    // 3         0.4
    val distributions =
      listOf(
        DistributionChoice(0, 0.8),
        DistributionChoice(1, 0.4),
        DistributionChoice(2, 0.4),
        DistributionChoice(3, 0.4)
      )
    assertFailsWith<IllegalStateException> { DistributedConsistentHashing(distributions) }
  }

  @Test
  fun `zero probability should always choose others`() {
    // Distribution:
    // choice_id probability
    // 0         0.0
    // 1         1.0
    val distributions = listOf(DistributionChoice(0, 0.0), DistributionChoice(1, 1.0))
    val hashing = DistributedConsistentHashing(distributions)

    (0 until SEED_NUMBER).forEach { assertEquals(1, hashing.hash(it.toString())) }
  }

  @Test
  fun `output distribution with non-consecutive choiceIds should match`() {
    // Distribution:
    // choice_id probability
    // 0         0.4
    // 2         0.2
    // 4         0.2
    // 6         0.2
    val distributions =
      listOf(
        DistributionChoice(0, 0.4),
        DistributionChoice(2, 0.2),
        DistributionChoice(4, 0.2),
        DistributionChoice(6, 0.2)
      )
    val hashing = DistributedConsistentHashing(distributions)

    val outputCounts = mutableMapOf<Int, Int>()
    (0 until SEED_NUMBER).forEach {
      val output = hashing.hash(it.toString())
      outputCounts[output] = outputCounts.getOrDefault(output, 0) + 1
    }

    assertEquals(outputCounts.keys, setOf(0, 2, 4, 6))
    /**
     * Compare to the exact results to make sure C++ and Kotlin implementations have same output.
     */
    assertEquals(4049, outputCounts[0]!!)
    assertEquals(1961, outputCounts[2]!!)
    assertEquals(2028, outputCounts[4]!!)
    assertEquals(1962, outputCounts[6]!!)
  }

  @Test
  fun `output count should match`() {
    // Distributions:
    // choice_id probability_1 probability_2
    // 0         0.4           0.2
    // 1         0.2           0.2
    // 2         0.2           0.2
    // 3         0.2           0.4
    val distributions1 =
      listOf(
        DistributionChoice(0, 0.4),
        DistributionChoice(1, 0.2),
        DistributionChoice(2, 0.2),
        DistributionChoice(3, 0.2)
      )
    val hashing1 = DistributedConsistentHashing(distributions1)
    val distributions2 =
      listOf(
        DistributionChoice(0, 0.2),
        DistributionChoice(1, 0.2),
        DistributionChoice(2, 0.2),
        DistributionChoice(3, 0.4)
      )
    val hashing2 = DistributedConsistentHashing(distributions2)

    var diffOutputCount = 0
    (0 until SEED_NUMBER).forEach {
      val output1 = hashing1.hash(it.toString())
      val output2 = hashing2.hash(it.toString())
      if (output1 != output2) {
        ++diffOutputCount
      }
    }
    /**
     * The number of outputs different between 2 hashings is guaranteed to be less than L1 distance
     * of the 2 distributions, which is 40% of total counts here.
     */
    assertTrue(diffOutputCount < SEED_NUMBER * 0.4)
  }
}
