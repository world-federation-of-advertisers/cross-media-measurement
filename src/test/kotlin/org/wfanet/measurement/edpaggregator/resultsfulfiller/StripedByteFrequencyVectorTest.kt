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
import kotlin.random.Random

@RunWith(JUnit4::class)
class StripedByteFrequencyVectorTest {

  @Test
  fun `empty vector has zero statistics`() {
    val vector = StripedByteFrequencyVector(100)
    
    assertThat(vector.getAverageFrequency()).isEqualTo(0.0)
    assertThat(vector.getReach()).isEqualTo(0L)
    assertThat(vector.getTotalCount()).isEqualTo(0L)
  }

  @Test
  fun `single increment works correctly`() {
    val vector = StripedByteFrequencyVector(100)
    
    vector.incrementByIndex(5)
    
    assertThat(vector.getAverageFrequency()).isEqualTo(1.0)
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getTotalCount()).isEqualTo(1L)
  }

  @Test
  fun `multiple increments at same index accumulate`() {
    val vector = StripedByteFrequencyVector(100)
    val index = 42
    
    repeat(5) {
      vector.incrementByIndex(index)
    }
    
    assertThat(vector.getAverageFrequency()).isEqualTo(5.0)
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getTotalCount()).isEqualTo(5L)
  }

  @Test
  fun `multiple indices work correctly`() {
    val vector = StripedByteFrequencyVector(100)
    
    vector.incrementByIndex(10)
    vector.incrementByIndex(20)
    vector.incrementByIndex(30)
    vector.incrementByIndex(10) // Second increment at index 10
    
    assertThat(vector.getReach()).isEqualTo(3L) // Three unique indices
    assertThat(vector.getTotalCount()).isEqualTo(4L) // Four total increments
    assertThat(vector.getAverageFrequency()).isWithin(0.001).of(4.0 / 3.0) // 4 total / 3 unique
  }

  @Test
  fun `out of bounds indices are ignored`() {
    val vector = StripedByteFrequencyVector(10)
    
    vector.incrementByIndex(-1)
    vector.incrementByIndex(10)
    vector.incrementByIndex(100)
    vector.incrementByIndex(5) // Valid index
    
    assertThat(vector.getAverageFrequency()).isEqualTo(1.0)
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getTotalCount()).isEqualTo(1L)
  }

  @Test
  fun `frequency caps at 255`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5
    
    // Increment beyond 255
    repeat(300) {
      vector.incrementByIndex(index)
    }
    
    assertThat(vector.getAverageFrequency()).isEqualTo(255.0)
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getTotalCount()).isEqualTo(255L)
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
    assertThat(vector.getTotalCount()).isEqualTo(totalExpectedIncrements.toLong())
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
    
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getAverageFrequency()).isEqualTo(expectedTotal.toDouble())
    assertThat(vector.getTotalCount()).isEqualTo(expectedTotal.toLong())
  }

  @Test
  fun `mixed concurrent operations`() = runBlocking {
    val vector = StripedByteFrequencyVector(1000)
    val random = Random(42) // Fixed seed for reproducibility
    
    // Launch multiple threads with mixed access patterns
    val jobs = (0 until 20).map { threadId ->
      async {
        repeat(100) {
          val index = random.nextInt(1000)
          vector.incrementByIndex(index)
        }
      }
    }
    
    jobs.awaitAll()
    
    val totalCount = vector.getTotalCount()
    
    assertThat(totalCount).isEqualTo(2000L) // 20 threads * 100 increments
    assertThat(vector.getReach()).isAtLeast(1L)
    assertThat(vector.getReach()).isAtMost(1000L)
    assertThat(vector.getAverageFrequency()).isAtLeast(1.0)
    assertThat(vector.getAverageFrequency()).isAtMost(255.0)
  }

  @Test
  fun `edge case - size 1 vector`() {
    val vector = StripedByteFrequencyVector(1)
    
    vector.incrementByIndex(0)
    vector.incrementByIndex(0)
    
    assertThat(vector.getAverageFrequency()).isEqualTo(2.0)
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getTotalCount()).isEqualTo(2L)
  }

  @Test
  fun `large vector works correctly`() {
    val size = 100_000
    val vector = StripedByteFrequencyVector(size)
    
    // Increment every 1000th index
    for (i in 0 until size step 1000) {
      vector.incrementByIndex(i)
    }
    
    val expectedReach = size / 1000
    
    assertThat(vector.getReach()).isEqualTo(expectedReach.toLong())
    assertThat(vector.getAverageFrequency()).isEqualTo(1.0)
    assertThat(vector.getTotalCount()).isEqualTo(expectedReach.toLong())
  }

  @Test
  fun `statistics computation is consistent`() {
    val vector = StripedByteFrequencyVector(50)
    
    // Add some data
    vector.incrementByIndex(10)
    vector.incrementByIndex(20)
    vector.incrementByIndex(20)
    vector.incrementByIndex(30)
    vector.incrementByIndex(30)
    vector.incrementByIndex(30)
    
    // Multiple calls should return same results
    val reach1 = vector.getReach()
    val reach2 = vector.getReach()
    val avgFreq1 = vector.getAverageFrequency()
    val avgFreq2 = vector.getAverageFrequency()
    val count1 = vector.getTotalCount()
    val count2 = vector.getTotalCount()
    
    assertThat(reach1).isEqualTo(reach2)
    assertThat(avgFreq1).isEqualTo(avgFreq2)
    assertThat(count1).isEqualTo(count2)
    assertThat(count1).isEqualTo(6L) // Total increments
    assertThat(reach1).isEqualTo(3L) // Unique indices
    assertThat(avgFreq1).isWithin(0.001).of(2.0) // 6 total / 3 unique
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
    assertThat(vector.getReach()).isEqualTo(3L)
    assertThat(vector.getTotalCount()).isEqualTo(6L)
    assertThat(vector.getAverageFrequency()).isWithin(0.001).of(2.0)
  }

  @Test
  fun `interface methods work correctly with empty vector`() {
    val vector = StripedByteFrequencyVector(100)
    
    assertThat(vector.getReach()).isEqualTo(0L)
    assertThat(vector.getTotalCount()).isEqualTo(0L)
    assertThat(vector.getAverageFrequency()).isEqualTo(0.0)
  }

  @Test
  fun `interface methods work correctly with single element`() {
    val vector = StripedByteFrequencyVector(100)
    vector.incrementByIndex(42)
    
    assertThat(vector.getReach()).isEqualTo(1L)
    assertThat(vector.getTotalCount()).isEqualTo(1L)
    assertThat(vector.getAverageFrequency()).isEqualTo(1.0)
  }
}