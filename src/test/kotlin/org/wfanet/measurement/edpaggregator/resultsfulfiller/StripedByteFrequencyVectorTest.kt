/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.edpaggregator.resultsfulfiller

import com.google.common.truth.Truth.assertThat
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import java.util.concurrent.ThreadLocalRandom

@RunWith(JUnit4::class)
class StripedByteFrequencyVectorTest {

  @Test
  fun `empty vector has all zeros`() {
    val vector = StripedByteFrequencyVector(100)
    val array = vector.getArray()
    
    assertThat(array).hasLength(100)
    assertThat(array.all { it == 0 }).isTrue()
  }

  @Test
  fun `single increment works correctly`() {
    val vector = StripedByteFrequencyVector(100)
    
    vector.incrementByIndex(5)
    
    val array = vector.getArray()
    assertThat(array[5]).isEqualTo(1)
    assertThat(array.filterIndexed { index, value -> index != 5 }.all { it == 0 }).isTrue()
  }

  @Test
  fun `multiple increments at same index accumulate`() {
    val vector = StripedByteFrequencyVector(100)
    val index = 42
    
    repeat(5) {
      vector.incrementByIndex(index)
    }
    
    val array = vector.getArray()
    assertThat(array[index]).isEqualTo(5)
    assertThat(array.filterIndexed { i, _ -> i != index }.all { it == 0 }).isTrue()
  }

  @Test
  fun `multiple indices work correctly`() {
    val vector = StripedByteFrequencyVector(100)
    
    vector.incrementByIndex(10)
    vector.incrementByIndex(20)
    vector.incrementByIndex(30)
    vector.incrementByIndex(10) // Second increment at index 10
    
    val array = vector.getArray()
    assertThat(array[10]).isEqualTo(2)
    assertThat(array[20]).isEqualTo(1)
    assertThat(array[30]).isEqualTo(1)
    assertThat(array.filterIndexed { i, _ -> i != 10 && i != 20 && i != 30 }.all { it == 0 }).isTrue()
  }

  @Test
  fun `out of bounds indices are ignored`() {
    val vector = StripedByteFrequencyVector(10)
    
    vector.incrementByIndex(-1)
    vector.incrementByIndex(10)
    vector.incrementByIndex(100)
    vector.incrementByIndex(5) // Valid index
    
    val array = vector.getArray()
    assertThat(array[5]).isEqualTo(1)
    assertThat(array.filterIndexed { i, _ -> i != 5 }.all { it == 0 }).isTrue()
  }

  @Test
  fun `frequency caps at 255`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5
    
    // Increment beyond 255
    repeat(300) {
      vector.incrementByIndex(index)
    }
    
    val array = vector.getArray()
    assertThat(array[index]).isEqualTo(255)
    assertThat(array.filterIndexed { i, _ -> i != index }.all { it == 0 }).isTrue()
  }

  @Test
  fun `concurrent access works correctly`() = runBlocking {
    val vector = StripedByteFrequencyVector(1000)
    val concurrency = 50
    val incrementsPerThread = 20
    
    // Launch concurrent operations
    val jobs = (0 until concurrency).map { threadId ->
      async {
        repeat(incrementsPerThread) { increment ->
          val index = (threadId * incrementsPerThread + increment) % 1000
          vector.incrementByIndex(index)
        }
      }
    }
    
    jobs.awaitAll()
    
    val totalExpectedIncrements = concurrency * incrementsPerThread
    val array = vector.getArray()
    assertThat(array.sum()).isEqualTo(totalExpectedIncrements)
  }

  @Test
  fun `concurrent access to same indices works correctly`() = runBlocking {
    val vector = StripedByteFrequencyVector(100)
    val concurrency = 10
    val incrementsPerThread = 5
    val targetIndex = 50
    
    // Multiple threads incrementing the same index
    val jobs = (0 until concurrency).map {
      async {
        repeat(incrementsPerThread) {
          vector.incrementByIndex(targetIndex)
        }
      }
    }
    
    jobs.awaitAll()
    
    val expectedTotal = concurrency * incrementsPerThread
    
    val array = vector.getArray()
    assertThat(array[targetIndex]).isEqualTo(expectedTotal)
    assertThat(array.filterIndexed { i, _ -> i != targetIndex }.all { it == 0 }).isTrue()
  }

  @Test
  fun `mixed concurrent operations`() = runBlocking {
    val vector = StripedByteFrequencyVector(1000)
    // Launch multiple threads with mixed access patterns
    val jobs = (0 until 20).map { threadId ->
      async {
        repeat(100) {
          val index = ThreadLocalRandom.current().nextInt(1000)
          vector.incrementByIndex(index)
        }
      }
    }
    
    jobs.awaitAll()
    
    val array = vector.getArray()
    val totalCount = array.sum()
    val reach = array.count { it > 0 }
    
    assertThat(totalCount).isEqualTo(2000) // 20 threads * 100 increments
    assertThat(reach).isAtLeast(1)
    assertThat(reach).isAtMost(1000)
  }

  @Test
  fun `edge case - size 1 vector`() {
    val vector = StripedByteFrequencyVector(1)
    
    vector.incrementByIndex(0)
    vector.incrementByIndex(0)
    
    val array = vector.getArray()
    assertThat(array).hasLength(1)
    assertThat(array[0]).isEqualTo(2)
  }

  @Test
  fun `large vector works correctly`() {
    val size = 100_000
    val vector = StripedByteFrequencyVector(size)
    
    // Increment every 1000th index
    for (i in 0 until size step 1000) {
      vector.incrementByIndex(i)
    }
    
    val array = vector.getArray()
    val expectedReach = size / 1000
    
    // Check that every 1000th index has value 1
    for (i in 0 until size step 1000) {
      assertThat(array[i]).isEqualTo(1)
    }
    // Check total count
    assertThat(array.sum()).isEqualTo(expectedReach)
  }

  @Test
  fun `getArray returns consistent results`() {
    val vector = StripedByteFrequencyVector(50)
    
    // Add some data
    vector.incrementByIndex(10)
    vector.incrementByIndex(20)
    vector.incrementByIndex(20)
    vector.incrementByIndex(30)
    vector.incrementByIndex(30)
    vector.incrementByIndex(30)
    
    // Multiple calls should return same results
    val array1 = vector.getArray()
    val array2 = vector.getArray()
    
    assertThat(array1).isEqualTo(array2)
    assertThat(array1[10]).isEqualTo(1)
    assertThat(array1[20]).isEqualTo(2)
    assertThat(array1[30]).isEqualTo(3)
    assertThat(array1.sum()).isEqualTo(6) // Total increments
  }

  @Test
  fun `interface methods return correct values`() {
    val vector = StripedByteFrequencyVector(100)
    
    // Add various frequencies
    vector.incrementByIndex(10)
    vector.incrementByIndex(10)
    vector.incrementByIndex(20)
    vector.incrementByIndex(20)
    vector.incrementByIndex(20)
    vector.incrementByIndex(30)
    
    // Test interface methods
    val array = vector.getArray()
    assertThat(array[10]).isEqualTo(2)
    assertThat(array[20]).isEqualTo(3)
    assertThat(array[30]).isEqualTo(1)
    assertThat(array.sum()).isEqualTo(6)
  }

  @Test
  fun `interface methods work correctly with empty vector`() {
    val vector = StripedByteFrequencyVector(100)
    
    val array = vector.getArray()
    assertThat(array.all { it == 0 }).isTrue()
    assertThat(array.sum()).isEqualTo(0)
  }

  @Test
  fun `interface methods work correctly with single element`() {
    val vector = StripedByteFrequencyVector(100)
    vector.incrementByIndex(42)
    
    val array = vector.getArray()
    assertThat(array[42]).isEqualTo(1)
    assertThat(array.sum()).isEqualTo(1)
    assertThat(array.count { it > 0 }).isEqualTo(1)
  }
}
