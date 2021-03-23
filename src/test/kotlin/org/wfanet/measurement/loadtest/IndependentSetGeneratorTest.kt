// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest

import com.google.common.truth.Truth.assertThat
import kotlin.random.Random
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

private const val DEFAULT_SEED = 1L

@RunWith(JUnit4::class)
class IndependentSetGeneratorTest {
  @Test
  fun `set with same seed first value match succeeds`() {
    val random = Random(DEFAULT_SEED)
    val universeSize: Long = 1000000
    val setGenerator = generateIndependentSets(
      universeSize,
      1,
      random
    )

    // Generate new Random object with the same seed to generate same numbers.
    val random2 = Random(DEFAULT_SEED)
    val expected = setOf(random2.nextLong(universeSize))
    assertThat(setGenerator.first()).isEqualTo(expected)
  }

  @Test
  fun `set contains 0 to n values succeeds`() {
    val setGenerator = generateIndependentSets(
      100,
      100
    )
    assertThat(setGenerator.first()).containsExactlyElementsIn(0L..99L)
  }

  @Test
  fun `set size succeeds`() {
    val random = Random(DEFAULT_SEED)
    val setGenerator = generateIndependentSets(
      99,
      33,
      random
    ).take(10)
    var size = 0
    setGenerator.forEach {
      size++
      assertThat(it.size).isEqualTo(33)
    }
    assertThat(size).isEqualTo(10)
  }

  @Test
  fun `set contains no duplicates succeeds`() {
    val random = Random(DEFAULT_SEED)
    val setGenerator = generateIndependentSets(
      1000,
      100,
      random
    ).take(10)
    setGenerator.forEach {
      assertThat(it).containsNoDuplicates()
    }
  }

  @Test
  fun `set none without elements succeeds`() {
    val setGenerator = generateIndependentSets(
      100,
      100
    ).take(0)

    assertThat(setGenerator.none()).isTrue()
  }

  @Test
  fun `set set size out of bounds throws`() {
    assertFailsWith(IllegalArgumentException::class, "SetSize larger than UniverseSize") {
      generateIndependentSets(
        10,
        100
      ).first()
    }
  }

  @Test
  fun `set universe size out of bounds throws`() {
    assertFailsWith(IllegalArgumentException::class, "Universe size less than 1") {
      generateIndependentSets(
        -10,
        1
      ).first()
    }
  }
}
