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

package org.wfanet.measurement.duchy.utils

import com.google.common.truth.Truth.assertThat
import org.junit.Assert.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

// TODO: use assertThat instead of assertEquals
@RunWith(JUnit4::class)
class DuchyOrderTest {

  @Test
  fun `all orders are possible`() {
    assertEquals(
      listOf(SALZBURG, BOHEMIA, AUSTRIA),
      getDuchyOrderByPublicKeysAndComputationId(duchies, "1"),
    )
    assertEquals(
      listOf(SALZBURG, AUSTRIA, BOHEMIA),
      getDuchyOrderByPublicKeysAndComputationId(duchies, "3"),
    )
    assertEquals(
      listOf(BOHEMIA, SALZBURG, AUSTRIA),
      getDuchyOrderByPublicKeysAndComputationId(duchies, "8"),
    )
    assertEquals(
      listOf(BOHEMIA, AUSTRIA, SALZBURG),
      getDuchyOrderByPublicKeysAndComputationId(duchies, "5"),
    )
    assertEquals(
      listOf(AUSTRIA, BOHEMIA, SALZBURG),
      getDuchyOrderByPublicKeysAndComputationId(duchies, "9"),
    )
    assertEquals(
      listOf(AUSTRIA, SALZBURG, BOHEMIA),
      getDuchyOrderByPublicKeysAndComputationId(duchies, "2"),
    )
  }

  @Test
  fun `all orders are equally possible`() {
    val histogram = mutableMapOf<List<String>, Int>()
    val trials = 100000
    for (i in 1..trials) {
      val globalComputationId = Math.random().toString()
      val order = getDuchyOrderByPublicKeysAndComputationId(duchies, globalComputationId)
      histogram.merge(order, 1, Int::plus)
    }
    assertThat(histogram).hasSize(6)

    val frequency: (List<String>) -> Double = { histogram[it]!!.toDouble() / trials }

    assertEquals(1.0 / 6, frequency(listOf(SALZBURG, BOHEMIA, AUSTRIA)), 0.01)
    assertEquals(1.0 / 6, frequency(listOf(SALZBURG, AUSTRIA, BOHEMIA)), 0.01)
    assertEquals(1.0 / 6, frequency(listOf(BOHEMIA, SALZBURG, AUSTRIA)), 0.01)
    assertEquals(1.0 / 6, frequency(listOf(BOHEMIA, AUSTRIA, SALZBURG)), 0.01)
    assertEquals(1.0 / 6, frequency(listOf(AUSTRIA, BOHEMIA, SALZBURG)), 0.01)
    assertEquals(1.0 / 6, frequency(listOf(AUSTRIA, SALZBURG, BOHEMIA)), 0.01)
  }

  @Test
  fun `getNextDuchy returns the next duchy`() {
    assertEquals(getNextDuchy(listOf(BOHEMIA, SALZBURG, AUSTRIA), BOHEMIA), SALZBURG)
    assertEquals(getNextDuchy(listOf(BOHEMIA, SALZBURG, AUSTRIA), SALZBURG), AUSTRIA)
    assertEquals(getNextDuchy(listOf(BOHEMIA, SALZBURG, AUSTRIA), AUSTRIA), BOHEMIA)
  }

  @Test
  fun `getFollowingDuchies returns the following duchies`() {
    assertThat(getFollowingDuchies(listOf(BOHEMIA, SALZBURG, AUSTRIA), BOHEMIA))
      .containsExactly(SALZBURG, AUSTRIA)
    assertThat(getFollowingDuchies(listOf(BOHEMIA, SALZBURG, AUSTRIA), SALZBURG))
      .containsExactly(AUSTRIA)
    assertThat(getFollowingDuchies(listOf(BOHEMIA, SALZBURG, AUSTRIA), AUSTRIA)).isEmpty()
  }

  companion object {
    private const val AUSTRIA = "Austria"
    private const val BOHEMIA = "Bohemia"
    private const val SALZBURG = "Salzburg"
    private val duchies =
      setOf(Duchy(BOHEMIA, "key1"), Duchy(SALZBURG, "key2"), Duchy(AUSTRIA, "key3"))
  }
}
