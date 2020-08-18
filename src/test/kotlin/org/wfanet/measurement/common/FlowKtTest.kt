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
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.toList
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import kotlin.test.assertFailsWith
import kotlin.test.fail

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
}
