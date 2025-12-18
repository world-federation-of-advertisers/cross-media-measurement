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
import java.util.concurrent.ThreadLocalRandom
import kotlin.test.assertFailsWith
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

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

    vector.increment(5)

    val array = vector.getByteArray()
    assertThat(array[5]).isEqualTo(1)
    assertThat(array.filterIndexed { index, _ -> index != 5 }.all { it == 0.toByte() }).isTrue()
  }

  @Test
  fun `multiple increments accumulate`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5

    repeat(5) { vector.increment(index) }

    val array = vector.getByteArray()
    assertThat(array[index]).isEqualTo(5)
  }

  @Test
  fun `increment caps at Byte MAX_VALUE`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5

    // Increment beyond maximum
    repeat(150) { vector.increment(index) }

    val array = vector.getByteArray()
    assertThat(array[index]).isEqualTo(127) // Should be capped at 127
  }

  @Test
  fun `out of bounds index throws exception`() {
    val vector = StripedByteFrequencyVector(10)

    assertFailsWith<IllegalArgumentException>("negative index") { vector.increment(-1) }

    assertFailsWith<IllegalArgumentException>("index >= size") { vector.increment(10) }

    assertFailsWith<IllegalArgumentException>("index > size") { vector.increment(15) }
  }

  @Test
  fun `merge combines two vectors correctly`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(10)

    // Add some counts to both vectors
    repeat(3) { vector1.increment(5) }
    repeat(2) { vector2.increment(5) }
    repeat(1) { vector1.increment(7) }
    repeat(4) { vector2.increment(7) }

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
    repeat(100) { vector1.increment(5) }
    repeat(100) { vector2.increment(5) }

    vector1.merge(vector2)

    val result = vector1.getByteArray()
    assertThat(result[5]).isEqualTo(127) // Should saturate at 127
  }

  @Test
  fun `merge fails with different sizes`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(20)

    assertFailsWith<IllegalArgumentException>("Different sizes") { vector1.merge(vector2) }
  }

  @Test
  fun `concurrent increments work correctly`() = runBlocking {
    val vector = StripedByteFrequencyVector(size = 1000, stripeCount = 1)
    val concurrency = 10
    val incrementsPerThread = 12

    val jobs =
      (0 until concurrency).map {
        async {
          repeat(incrementsPerThread) {
            val index = ThreadLocalRandom.current().nextInt(1000)
            vector.increment(index)
          }
        }
      }

    jobs.awaitAll()

    val array = vector.getByteArray()
    val totalCount = array.sumOf { it.toInt() }
    assertThat(totalCount).isEqualTo(concurrency * incrementsPerThread)
  }

  @Test
  fun `concurrent merge operations work correctly`() = runBlocking {
    val baseVector = StripedByteFrequencyVector(size = 100, stripeCount = 1)

    // Create multiple vectors to merge concurrently
    val vectors =
      (0 until 10).map {
        StripedByteFrequencyVector(100).apply {
          repeat(10) {
            val index = ThreadLocalRandom.current().nextInt(100)
            increment(index)
          }
        }
      }

    // Merge all vectors concurrently
    val jobs = vectors.map { vector -> async { baseVector.merge(vector) } }

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
    vector.increment(0)
    vector.increment(500_000)
    vector.increment(999_999)

    val array = vector.getByteArray()
    assertThat(array[0]).isEqualTo(1)
    assertThat(array[500_000]).isEqualTo(1)
    assertThat(array[999_999]).isEqualTo(1)
  }

  @Test
  fun `getTotalUncappedImpressions returns zero for empty vector`() {
    val vector = StripedByteFrequencyVector(10)
    assertThat(vector.getTotalUncappedImpressions()).isEqualTo(0L)
  }

  @Test
  fun `getTotalUncappedImpressions tracks all increments without capping`() {
    val vector = StripedByteFrequencyVector(10)
    val index = 5

    // Increment beyond the byte cap
    repeat(150) { vector.increment(index) }

    // The byte array should be capped at 127
    val array = vector.getByteArray()
    assertThat(array[index]).isEqualTo(127)

    // But total uncapped impressions should reflect all 150 increments
    assertThat(vector.getTotalUncappedImpressions()).isEqualTo(150L)
  }

  @Test
  fun `getTotalUncappedImpressions counts across multiple indices`() {
    val vector = StripedByteFrequencyVector(10)

    vector.increment(0)
    vector.increment(0)
    vector.increment(5)
    vector.increment(9)

    assertThat(vector.getTotalUncappedImpressions()).isEqualTo(4L)
  }

  @Test
  fun `merge combines uncapped impressions correctly`() {
    val vector1 = StripedByteFrequencyVector(10)
    val vector2 = StripedByteFrequencyVector(10)

    repeat(100) { vector1.increment(5) }
    repeat(50) { vector2.increment(5) }
    repeat(30) { vector2.increment(7) }

    assertThat(vector1.getTotalUncappedImpressions()).isEqualTo(100L)
    assertThat(vector2.getTotalUncappedImpressions()).isEqualTo(80L)

    vector1.merge(vector2)

    // After merge, total uncapped should be sum of both
    assertThat(vector1.getTotalUncappedImpressions()).isEqualTo(180L)
    // The frequency vector itself should be capped
    val result = vector1.getByteArray()
    assertThat(result[5]).isEqualTo(127)
  }

  @Test
  fun `concurrent increments track uncapped impressions correctly`() = runBlocking {
    val vector = StripedByteFrequencyVector(size = 10, stripeCount = 1)
    val concurrency = 100
    val incrementsPerThread = 100

    val jobs =
      (0 until concurrency).map {
        async {
          repeat(incrementsPerThread) {
            // All increments go to the same index to test capping vs uncapped count
            vector.increment(0)
          }
        }
      }

    jobs.awaitAll()

    // Total uncapped should be exactly all increments
    val expectedTotal = concurrency.toLong() * incrementsPerThread
    assertThat(vector.getTotalUncappedImpressions()).isEqualTo(expectedTotal)

    // But the byte array should be capped at 127
    val array = vector.getByteArray()
    assertThat(array[0]).isEqualTo(127)
  }
}
