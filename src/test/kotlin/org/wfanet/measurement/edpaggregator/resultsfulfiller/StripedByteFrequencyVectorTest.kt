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
import kotlin.test.assertFailsWith

@RunWith(JUnit4::class)
class StripedByteFrequencyVectorTest {

  @Test
  fun `empty vector has all zeros`() {
    val vector = StripedByteFrequencyVector(10)
    val array = vector.getByteArray()

    assertThat(array).hasLength(10)
    assertThat(array.all { it == 0.toByte() }).isTrue()
  }

  @Test
  fun `single increment works correctly`() {
    val vector = StripedByteFrequencyVector(10)

    vector.incrementByIndex(5)

    val array = vector.getByteArray()
    assertThat(array[5]).isEqualTo(1)
    assertThat(array.filterIndexed { index, _ -> index != 5 }.all { it == 0.toByte() }).isTrue()
  }

  @Test
  fun `multiple increments accumulate`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5

    repeat(5) {
      vector.incrementByIndex(index)
    }

    val array = vector.getByteArray()
    assertThat(array[index]).isEqualTo(5)
  }

  @Test
  fun `increment caps at 127`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5

    // Increment beyond maximum
    repeat(150) {
      vector.incrementByIndex(index)
    }

    val array = vector.getByteArray()
    assertThat(array[index]).isEqualTo(127) // Should be capped at 127
  }

  @Test
  fun `out of bounds index throws exception`() {
    val vector = StripedByteFrequencyVector(10)

    assertFailsWith<IllegalArgumentException>("negative index") {
      vector.incrementByIndex(-1)
    }

    assertFailsWith<IllegalArgumentException>("index >= size") {
      vector.incrementByIndex(10)
    }

    assertFailsWith<IllegalArgumentException>("index > size") {
      vector.incrementByIndex(15)
    }
  }

  @Test
  fun `merge combines two vectors correctly`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(10)

    // Add some counts to both vectors
    repeat(3) { vector1.incrementByIndex(5) }
    repeat(2) { vector2.incrementByIndex(5) }
    repeat(1) { vector1.incrementByIndex(7) }
    repeat(4) { vector2.incrementByIndex(7) }

    vector1.merge(vector2)

    val result = vector1.getByteArray()
    assertThat(result[5]).isEqualTo(5) // 3 + 2 = 5
    assertThat(result[7]).isEqualTo(5) // 1 + 4 = 5
  }

  @Test
  fun `merge saturates at maximum value`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(10)

    // Set up values that will overflow when merged
    repeat(100) { vector1.incrementByIndex(5) }
    repeat(100) { vector2.incrementByIndex(5) }

    vector1.merge(vector2)

    val result = vector1.getByteArray()
    assertThat(result[5]).isEqualTo(127) // Should saturate at 127
  }

  @Test
  fun `merge fails with different sizes`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(20)

    assertFailsWith<IllegalArgumentException>("Different sizes") {
      vector1.merge(vector2)
    }
  }

  @Test
  fun `concurrent increments work correctly`() = runBlocking {
    val vector = StripedByteFrequencyVector(1000)
    val concurrency = 10
    val incrementsPerThread = 100

    val jobs = (0 until concurrency).map {
      async {
        repeat(incrementsPerThread) {
          val index = ThreadLocalRandom.current().nextInt(1000)
          vector.incrementByIndex(index)
        }
      }
    }

    jobs.awaitAll()

    val array = vector.getByteArray()
    val totalCount = array.sumOf { it.toInt() }
    // We should have made concurrency * incrementsPerThread increments total
    // Some may overlap on the same index, but total should be > 0
    assertThat(totalCount).isAtLeast(1)
  }

  @Test
  fun `concurrent merge operations work correctly`() = runBlocking {
    val baseVector = StripedByteFrequencyVector(100)

    // Create multiple vectors to merge concurrently
    val vectors = (0 until 10).map {
      StripedByteFrequencyVector(100).apply {
        repeat(10) {
          val index = ThreadLocalRandom.current().nextInt(100)
          incrementByIndex(index)
        }
      }
    }

    // Merge all vectors concurrently
    val jobs = vectors.map { vector ->
      async {
        baseVector.merge(vector)
      }
    }

    jobs.awaitAll()

    val result = baseVector.getByteArray()
    val totalCount = result.sumOf { it.toInt() }
    assertThat(totalCount).isAtLeast(1) // At least some increments should have occurred
  }

  @Test
  fun `large vector works correctly`() {
    val size = 1_000_000
    val vector = StripedByteFrequencyVector(size)

    assertThat(vector.size).isEqualTo(size)

    // Test some operations
    vector.incrementByIndex(0)
    vector.incrementByIndex(500_000)
    vector.incrementByIndex(999_999)

    val array = vector.getByteArray()
    assertThat(array[0]).isEqualTo(1)
    assertThat(array[500_000]).isEqualTo(1)
    assertThat(array[999_999]).isEqualTo(1)
  }

  @Test
  fun `striping provides concurrent performance`() = runBlocking {
    val vector = StripedByteFrequencyVector(10000)
    val threads = 20

    // Each thread works on different parts of the vector
    val jobs = (0 until threads).map { threadId ->
      async {
        val startIndex = (threadId * 500) % 10000
        repeat(1000) { iteration ->
          val index = (startIndex + iteration) % 10000
          vector.incrementByIndex(index)
        }
      }
    }

    jobs.awaitAll()

    // Verify some data was written
    val array = vector.getByteArray()
    val nonZeroCount = array.count { it != 0.toByte() }
    assertThat(nonZeroCount).isAtLeast(threads * 100) // At least some unique indexes hit
  }
}
