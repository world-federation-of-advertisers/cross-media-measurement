// Copyright 2020 The Measurement System Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import kotlin.test.assertFalse
import kotlin.test.assertNotNull
import kotlin.test.assertTrue
import kotlin.test.fail
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class FlowKtTest {

  @Test
  fun `withRetriesOnEach no errors thrown`() = runBlocking {
    val successfullyProcessedItems = mutableListOf<Int>()
    var totalTries = 0
    (1..10).asFlow()
      .withRetriesOnEach(3, retryPredicate = { fail("unexpected retry") }) {
        totalTries += 1
      }
      .toList(successfullyProcessedItems)
    assertThat(successfullyProcessedItems).isEqualTo((1..10).toList())
    assertThat(totalTries).isEqualTo(10)
  }

  @Test
  fun `withRetriesOnEach non-retriable error`() = runBlocking {
    val successfullyProcessedItems = mutableListOf<Int>()
    val triesCounter = mutableMapOf<Int, Int>()
    assertFailsWith<IllegalArgumentException> {
      (1..100).asFlow()
        .withRetriesOnEach(3, retryPredicate = { it is NullPointerException }) {
          triesCounter.putIfAbsent(it, 0)
          triesCounter[it] = 1 + (triesCounter[it] ?: 0)
          require(it < 5) { "Simulated error in test..." }
        }
        // The require() will throw an IllegalArgumentException that is not caught in the flow.
        // It will be thrown on the collection of this flow.
        .toList(successfullyProcessedItems)
    }
    assertThat(successfullyProcessedItems).isEqualTo((1..4).toList())
    assertThat(triesCounter).isEqualTo(mapOf(1 to 1, 2 to 1, 3 to 1, 4 to 1, 5 to 1))
  }

  @Test
  fun `withRetriesOnEach retry errors`() = runBlocking {
    val successfullyProcessedItems = mutableListOf<Int>()
    val triesCounter = mutableMapOf<Int, Int>()
    assertFailsWith<IllegalArgumentException> {
      (1..100).asFlow()
        .withRetriesOnEach(3) {
          triesCounter.putIfAbsent(it, 0)
          triesCounter[it] = 1 + (triesCounter[it] ?: 0)
          require(it < 8) { "Simulated error in test..." }
        }
        // The require() will throw an IllegalArgumentException that is caught in the flow, and
        // matches the predicate causing the block to be retried. However, it will be tried 3 times
        // and the exception will be thrown all three times at which point it is rethrown.
        .toList(successfullyProcessedItems)
    }
    assertThat(successfullyProcessedItems).isEqualTo((1..7).toList())
    assertThat(triesCounter).isEqualTo(
      // all items should have been attempted once except the last item which is attempted 3 times.
      mapOf(1 to 1, 2 to 1, 3 to 1, 4 to 1, 5 to 1, 6 to 1, 7 to 1, 8 to 3)
    )
  }

  @Test
  fun `withRetriesOnEach retries and succeeds`() = runBlocking {
    val successfullyProcessedItems = mutableListOf<Int>()
    val triesCounter = mutableMapOf<Int, Int>()
    (1..6).asFlow()
      .withRetriesOnEach(3) {
        val initValue = triesCounter.putIfAbsent(it, 0)
        triesCounter[it] = 1 + (triesCounter[it] ?: 0)
        require(it % 2 == 0 || initValue != null) { "Simulated error in test..." }
      }
      // The require() will throw an IllegalArgumentException that is caught in the flow, and
      // matches the predicate causing the block to be retried. However, it succeeds on retry.
      .toList(successfullyProcessedItems)
    assertThat(successfullyProcessedItems).isEqualTo((1..6).toList())
    // Even items are attempted once and odd items attempted twice.
    assertThat(triesCounter).isEqualTo(mapOf(1 to 2, 2 to 1, 3 to 2, 4 to 1, 5 to 2, 6 to 1))
  }

  @Test
  fun `concurrentMap runs concurrently`() = runBlocking<Unit> {
    val latch = CountDownLatch(100)
    val events = mutableListOf<Int>()
    val result =
      (1..100)
        .asFlow()
        .mapConcurrently(this, 100) {
          events.add(it)
          latch.countDown()
          latch.await()
          -it
        }
        .toList()
    assertThat(events)
      .containsExactlyElementsIn(1..100)
      .inOrder()
    assertThat(result)
      .containsExactlyElementsIn(-1 downTo -100)
      .inOrder()
  }

  @Test
  fun `concurrentMap runs in order`() = runBlocking<Unit> {
    val events = mutableListOf<Int>()
    val result =
      flowOf(1, 2)
        .mapConcurrently(this, 1) {
          events.add(it)
          -it
        }
        .toList()
    assertThat(events)
      .containsExactly(1, 2)
      .inOrder()
    assertThat(result)
      .containsExactly(-1, -2)
      .inOrder()
  }

  @Test
  fun `consumeFirst returns null for empty flow`() = runBlocking {
    assertThat(flowOf<String>().consumeFirst()).isNull()
  }

  @Test
  fun `consumeFirst hasRemaining is false for single-item flow`() = runBlocking {
    assertNotNull(flowOf("foo").consumeFirst()).use { consumed ->
      assertFalse(consumed.hasRemaining)
    }
  }

  @Test
  fun `consumeFirst remaining is empty for single-item flow`() = runBlocking {
    assertNotNull(flowOf("foo").consumeFirst()).use { consumed ->
      assertThat(consumed.remaining.toList()).isEmpty()
    }
  }

  @Test
  fun `consumeFirst item is first item for single-item flow`() = runBlocking {
    val item = "foo"
    assertNotNull(flowOf(item).consumeFirst()).use { consumed ->
      assertThat(consumed.item).isEqualTo(item)
    }
  }

  @Test
  fun `consumeFirst hasRemaining is true for multi-item flow`() = runBlocking {
    val items = listOf("foo", "bar", "baz")
    assertNotNull(items.asFlow().consumeFirst()).use { consumed ->
      assertTrue(consumed.hasRemaining)
    }
  }

  @Test
  fun `consumeFirst item is first item for multi-item flow`() = runBlocking {
    val items = listOf("foo", "bar", "baz")
    assertNotNull(items.asFlow().consumeFirst()).use { consumed ->
      assertThat(consumed.item).isEqualTo(items.first())
    }
  }

  @Test
  fun `consumeFirst remaining contains remaining items`() = runBlocking {
    val items = listOf("foo", "bar", "baz")
    assertNotNull(items.asFlow().consumeFirst()).use { consumed ->
      assertThat(consumed.remaining.toList()).containsExactlyElementsIn(items.drop(1)).inOrder()
    }
  }

  @Test
  fun `consumeFirstOr returns alternate for empty flow`() = runBlocking {
    val alternate = "foo"
    flowOf<String>().consumeFirstOr { alternate }.use { consumed ->
      assertThat(consumed.item).isEqualTo(alternate)
      assertFalse(consumed.hasRemaining)
      assertThat(consumed.remaining.toList()).isEmpty()
    }
  }

  @Test
  fun `consumeFirstOr returns same as consumeFirst for non-empty flow`() = runBlocking {
    val items = listOf("foo", "bar", "baz")
    items.asFlow().consumeFirstOr { "alternate" }.use { consumed1 ->
      assertNotNull(items.asFlow().consumeFirst()).use { consumed2 ->
        assertThat(consumed1.item).isEqualTo(consumed2.item)
        assertThat(consumed1.hasRemaining).isEqualTo(consumed2.hasRemaining)
        assertThat(consumed1.remaining.toList())
          .containsExactlyElementsIn(consumed2.remaining.toList())
          .inOrder()
      }
    }
  }
}
