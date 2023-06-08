// Copyright 2021 The Cross-Media Measurement Authors
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
// limitations under the License.

package org.wfanet.panelmatch.client.privatemembership

import com.google.common.truth.Truth.assertThat
import com.google.common.truth.Truth.assertWithMessage
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class QueryIdGeneratorTest {
  @Test
  fun generatesEntireRange() {
    for (size in listOf(0, 1, 2, 10, 100)) {
      assertWithMessage("Range size: $size")
        .that(generateQueryIds(size).asSequence().toList())
        .containsExactlyElementsIn(0 until size)
    }
  }

  @Test
  fun generatesAllSequences() {
    val size = 5
    val numPermutations = 5 * 4 * 3 * 2
    val iterationsPerPermutation = 100
    val numIterations = numPermutations * iterationsPerPermutation

    val histogram =
      (0 until numIterations)
        .map { generateQueryIds(size).asSequence().toList() }
        .groupingBy { it }
        .eachCount()

    assertThat(histogram.size).isEqualTo(numPermutations)
    assertThat(histogram.values.minOrNull()).isAtLeast(iterationsPerPermutation / 2)
    assertThat(histogram.values.maxOrNull()).isAtMost(iterationsPerPermutation * 2)
  }
}
