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
import kotlin.test.assertFailsWith

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

  @Test
  fun `merge combines two frequency vectors correctly`() {
    val vector1 = StripedByteFrequencyVector(100)
    val vector2 = StripedByteFrequencyVector(100)
    
    // Add some frequencies to vector1
    vector1.incrementByIndex(10)
    vector1.incrementByIndex(20)
    vector1.incrementByIndex(20) // VID 20 has frequency 2
    
    // Add some frequencies to vector2
    vector2.incrementByIndex(10) // VID 10 appears in both
    vector2.incrementByIndex(30)
    vector2.incrementByIndex(40)
    
    // Merge vectors
    val merged = vector1.merge(vector2)
    
    // Verify results
    assertThat(merged.getReach()).isEqualTo(4L) // VIDs 10, 20, 30, 40
    assertThat(merged.getTotalCount()).isEqualTo(6L) // 2 + 2 + 1 + 1
  }

  @Test
  fun `merge handles frequency capping at 255`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(10)
    
    // Max out frequency for VID 5 in vector1
    repeat(255) {
      vector1.incrementByIndex(5)
    }
    
    // Add more frequency for VID 5 in vector2
    repeat(100) {
      vector2.incrementByIndex(5)
    }
    
    // Merge vectors
    val merged = vector1.merge(vector2)
    
    // Verify frequency is capped at 255
    assertThat(merged.getReach()).isEqualTo(1L)
    assertThat(merged.getTotalCount()).isEqualTo(255L) // Capped at max byte value
  }

  @Test
  fun `merge throws exception for different sized vectors`() {
    val vector1 = StripedByteFrequencyVector(100)
    val vector2 = StripedByteFrequencyVector(200)
    
    assertFailsWith<IllegalArgumentException> {
      vector1.merge(vector2)
    }
  }

  @Test
  fun `merge with empty vectors returns empty result`() {
    val vector1 = StripedByteFrequencyVector(50)
    val vector2 = StripedByteFrequencyVector(50)
    
    val merged = vector1.merge(vector2)
    
    assertThat(merged.getReach()).isEqualTo(0L)
    assertThat(merged.getTotalCount()).isEqualTo(0L)
    assertThat(merged.getAverageFrequency()).isEqualTo(0.0)
  }

  @Test
  fun `merge preserves individual vector data`() {
    val vector1 = StripedByteFrequencyVector(100)
    val vector2 = StripedByteFrequencyVector(100)
    
    vector1.incrementByIndex(5)
    vector1.incrementByIndex(10)
    
    vector2.incrementByIndex(15)
    vector2.incrementByIndex(20)
    
    // Store original values
    val reach1Before = vector1.getReach()
    val reach2Before = vector2.getReach()
    
    // Perform merge
    val merged = vector1.merge(vector2)
    
    // Verify original vectors are unchanged
    assertThat(vector1.getReach()).isEqualTo(reach1Before)
    assertThat(vector2.getReach()).isEqualTo(reach2Before)
    
    // Verify merged result
    assertThat(merged.getReach()).isEqualTo(4L)
  }

  @Test
  fun `merge handles overlapping and non-overlapping VIDs`() {
    val vector1 = StripedByteFrequencyVector(100)
    val vector2 = StripedByteFrequencyVector(100)
    
    // Vector1: VIDs 1, 2, 3 with different frequencies
    vector1.incrementByIndex(1)
    vector1.incrementByIndex(2)
    vector1.incrementByIndex(2)
    vector1.incrementByIndex(3)
    vector1.incrementByIndex(3)
    vector1.incrementByIndex(3)
    
    // Vector2: VIDs 2, 3, 4 with different frequencies
    vector2.incrementByIndex(2)
    vector2.incrementByIndex(2)
    vector2.incrementByIndex(2)
    vector2.incrementByIndex(3)
    vector2.incrementByIndex(4)
    
    val merged = vector1.merge(vector2)
    
    // Expected: VID 1 (freq 1), VID 2 (freq 2+3=5), VID 3 (freq 3+1=4), VID 4 (freq 1)
    assertThat(merged.getReach()).isEqualTo(4L)
    assertThat(merged.getTotalCount()).isEqualTo(11L) // 1 + 5 + 4 + 1
  }

  @Test
  fun `merge of multiple vectors using reduce`() {
    val vectors = listOf(
      StripedByteFrequencyVector(50).apply {
        incrementByIndex(1)
        incrementByIndex(2)
      },
      StripedByteFrequencyVector(50).apply {
        incrementByIndex(2)
        incrementByIndex(3)
      },
      StripedByteFrequencyVector(50).apply {
        incrementByIndex(3)
        incrementByIndex(4)
      }
    )
    
    val merged = vectors.reduce { acc, vector -> acc.merge(vector) }
    
    assertThat(merged.getReach()).isEqualTo(4L) // VIDs 1, 2, 3, 4
    assertThat(merged.getTotalCount()).isEqualTo(6L) // 1 + 2 + 2 + 1
  }
}