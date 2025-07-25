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
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec

@RunWith(JUnit4::class)
class StripedByteFrequencyVectorTest {

  companion object {
    private const val STARTING_VID = 100_000L
    private const val SMALL_POPULATION_SIZE = 10

    private val SMALL_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = STARTING_VID
          endVidInclusive = STARTING_VID + SMALL_POPULATION_SIZE - 1
        }
      }
    }

    private val LARGE_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 100_000L
        }
      }
    }

    private val COMPLEX_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = STARTING_VID
          endVidInclusive = STARTING_VID + 4
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = STARTING_VID + 5
          endVidInclusive = STARTING_VID + SMALL_POPULATION_SIZE - 1
        }
      }
    }

    private val FULL_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0f
      width = 1.0f
    }

    private val PARTIAL_NON_WRAPPING_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0.3f
      width = 0.5f
    }

    private val PARTIAL_WRAPPING_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0.8f
      width = 0.5f
    }

    private val FULL_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reach = reach {}
    }

    private val PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = PARTIAL_NON_WRAPPING_SAMPLING_INTERVAL
      reach = reach {}
    }

    private val PARTIAL_WRAPPING_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = PARTIAL_WRAPPING_SAMPLING_INTERVAL
      reach = reach {}
    }

    private val REACH_AND_FREQUENCY_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reachAndFrequency = reachAndFrequency {
        maximumFrequency = 5
      }
    }

    private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      impression = impression {
        maximumFrequencyPerUser = 10
      }
    }
  }

  @Test
  fun `construction fails when sampling interval is invalid`() {
    assertFailsWith<IllegalArgumentException>("expected exception start < 0") {
      StripedByteFrequencyVector(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = -1.0f
            width = 0.5f
          }
        }
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception start > 1.0") {
      StripedByteFrequencyVector(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 2.0f
            width = 0.5f
          }
        }
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception width <= 0") {
      StripedByteFrequencyVector(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 0f
            width = 0f
          }
        }
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception width > 1") {
      StripedByteFrequencyVector(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 0f
            width = 2f
          }
        }
      )
    }
  }

  @Test
  fun `empty vector has all zeros for full interval`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, FULL_REACH_MEASUREMENT_SPEC)
    val array = vector.getArray()
    
    assertThat(array).hasLength(10)
    assertThat(array.all { it == 0 }).isTrue()
  }

  @Test
  fun `single increment works correctly for reach measurement`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, FULL_REACH_MEASUREMENT_SPEC)
    
    vector.incrementByIndex(5)
    
    val array = vector.getArray()
    assertThat(array[5]).isEqualTo(1)
    assertThat(array.filterIndexed { index, value -> index != 5 }.all { it == 0 }).isTrue()
  }

  @Test
  fun `reach measurement caps at 1`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, FULL_REACH_MEASUREMENT_SPEC)
    val index = 5
    
    // Increment multiple times
    repeat(5) {
      vector.incrementByIndex(index)
    }
    
    val array = vector.getArray()
    assertThat(array[index]).isEqualTo(1) // Should be capped at 1 for reach
  }

  @Test
  fun `reach and frequency measurement respects maximum frequency`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, REACH_AND_FREQUENCY_MEASUREMENT_SPEC)
    val index = 5
    
    // Increment beyond maximum frequency
    repeat(10) {
      vector.incrementByIndex(index)
    }
    
    val array = vector.getArray()
    assertThat(array[index]).isEqualTo(5) // Should be capped at 5
  }

  @Test
  fun `impression measurement respects maximum frequency per user`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, IMPRESSION_MEASUREMENT_SPEC)
    val index = 5
    
    // Increment beyond maximum
    repeat(15) {
      vector.incrementByIndex(index)
    }
    
    val array = vector.getArray()
    assertThat(array[index]).isEqualTo(10) // Should be capped at 10
  }

  @Test
  fun `partial non-wrapping interval creates correct size vector`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC)
    
    // Population size is 10, interval is [0.3, 0.8)
    // 10 * 0.3 = 3 (start index)
    // 10 * 0.8 = 8 (end index, exclusive)
    // Size should be 8 - 3 = 5
    assertThat(vector.size).isEqualTo(5)
    
    val array = vector.getArray()
    assertThat(array).hasLength(5)
  }

  @Test
  fun `partial non-wrapping interval handles indexes correctly`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC)
    
    // Population size is 10, interval is [0.3, 0.8)
    // Valid global indexes are 3, 4, 5, 6, 7
    
    // These should work (within range)
    vector.incrementByIndex(3)
    vector.incrementByIndex(7)
    
    // These should be ignored (out of range)
    vector.incrementByIndex(2)
    vector.incrementByIndex(8)
    
    val array = vector.getArray()
    assertThat(array[0]).isEqualTo(1) // Global index 3 maps to local index 0
    assertThat(array[4]).isEqualTo(1) // Global index 7 maps to local index 4
    assertThat(array[1]).isEqualTo(0)
    assertThat(array[2]).isEqualTo(0)
    assertThat(array[3]).isEqualTo(0)
  }

  @Test
  fun `partial wrapping interval creates correct size vector`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_WRAPPING_REACH_MEASUREMENT_SPEC)
    
    // Population size is 10, interval is [0.8, 1.3) which wraps to [0.8, 1.0) + [0, 0.3)
    // Primary range: 10 * 0.8 = 8 to 10 * 1.0 = 10 (exclusive) -> indexes 8, 9
    // Wrapped range: 10 * 0 = 0 to 10 * 0.3 = 3 (exclusive) -> indexes 0, 1, 2
    // Total size = 2 + 3 = 5
    assertThat(vector.size).isEqualTo(5)
  }

  @Test
  fun `partial wrapping interval handles indexes correctly`() {
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_WRAPPING_REACH_MEASUREMENT_SPEC)
    
    // Valid global indexes are 8, 9 (primary) and 0, 1, 2 (wrapped)
    
    // Primary range indexes
    vector.incrementByIndex(8)
    vector.incrementByIndex(9)
    
    // Wrapped range indexes
    vector.incrementByIndex(0)
    vector.incrementByIndex(2)
    
    // Out of range (should be ignored)
    vector.incrementByIndex(3)
    vector.incrementByIndex(7)
    
    val array = vector.getArray()
    assertThat(array[0]).isEqualTo(1) // Global index 8 maps to local index 0
    assertThat(array[1]).isEqualTo(1) // Global index 9 maps to local index 1
    assertThat(array[2]).isEqualTo(1) // Global index 0 maps to local index 2
    assertThat(array[4]).isEqualTo(1) // Global index 2 maps to local index 4
    assertThat(array[3]).isEqualTo(0) // Global index 1 was not incremented
  }

  @Test
  fun `thread-safe merge combines two vectors correctly`() {
    val vector1 = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, REACH_AND_FREQUENCY_MEASUREMENT_SPEC)
    val vector2 = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, REACH_AND_FREQUENCY_MEASUREMENT_SPEC)
    
    // Add some counts to both vectors
    repeat(3) { vector1.incrementByIndex(5) }
    repeat(2) { vector2.incrementByIndex(5) }
    repeat(1) { vector1.incrementByIndex(7) }
    repeat(4) { vector2.incrementByIndex(7) }
    
    // Use thread-safe merge
    vector1.merge(vector2)
    
    val result = vector1.getArray()
    assertThat(result[5]).isEqualTo(5) // 3 + 2, capped at max frequency 5
    assertThat(result[7]).isEqualTo(5) // 1 + 4 = 5
  }

  @Test
  fun `merge fails with incompatible primary ranges`() {
    val vector1 = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, FULL_REACH_MEASUREMENT_SPEC)
    val vector2 = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC)
    
    assertFailsWith<IllegalArgumentException>("Primary ranges incompatible") {
      vector1.merge(vector2)
    }
  }

  @Test
  fun `mergeUnsafe fails with incompatible wrapped ranges`() {
    val vector1 = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC)
    val vector2 = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, PARTIAL_WRAPPING_REACH_MEASUREMENT_SPEC)
    
    assertFailsWith<IllegalArgumentException>("Wrapped ranges incompatible") {
      vector1.merge(vector2)
    }
  }

  @Test
  fun `concurrent merge operations work correctly`() = runBlocking {
    val baseVector = StripedByteFrequencyVector(LARGE_POPULATION_SPEC, REACH_AND_FREQUENCY_MEASUREMENT_SPEC)
    
    // Create multiple vectors to merge concurrently
    val vectors = (0 until 10).map {
      StripedByteFrequencyVector(LARGE_POPULATION_SPEC, REACH_AND_FREQUENCY_MEASUREMENT_SPEC).apply {
        repeat(100) { iter ->
          val index = ThreadLocalRandom.current().nextInt(1000)
          incrementByIndex(index)
        }
      }
    }
    
    // Merge all vectors concurrently using thread-safe merge
    val jobs = vectors.map { vector ->
      async {
        baseVector.merge(vector)
      }
    }
    
    jobs.awaitAll()
    
    val result = baseVector.getArray()
    val totalCount = result.sum()
    assertThat(totalCount).isAtLeast(1) // At least some increments should have occurred
  }

  @Test
  fun `concurrent access with partial intervals`() = runBlocking {
    val vector = StripedByteFrequencyVector(LARGE_POPULATION_SPEC, PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC)
    val concurrency = 20
    
    val jobs = (0 until concurrency).map { threadId ->
      async {
        repeat(50) {
          // Try various indexes, some in range, some out
          val index = ThreadLocalRandom.current().nextInt(100000)
          vector.incrementByIndex(index)
        }
      }
    }
    
    jobs.awaitAll()
    
    val array = vector.getArray()
    val totalCount = array.sum()
    // Only indexes within the partial interval should be counted
    assertThat(totalCount).isAtLeast(1)
    assertThat(array.all { it <= 1 }).isTrue() // Reach measurement caps at 1
  }

  @Test
  fun `complex population spec with full interval`() {
    val vector = StripedByteFrequencyVector(COMPLEX_POPULATION_SPEC, FULL_REACH_MEASUREMENT_SPEC)
    
    // Complex population has two ranges with a gap
    assertThat(vector.size).isEqualTo(10)
    
    // Increment some indexes
    vector.incrementByIndex(0)
    vector.incrementByIndex(4)
    vector.incrementByIndex(5)
    vector.incrementByIndex(9)
    
    val array = vector.getArray()
    assertThat(array[0]).isEqualTo(1)
    assertThat(array[4]).isEqualTo(1)
    assertThat(array[5]).isEqualTo(1)
    assertThat(array[9]).isEqualTo(1)
  }

  @Test
  fun `byte value saturation at 127`() {
    val measurementSpec = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reachAndFrequency = reachAndFrequency {
        maximumFrequency = 127 // At byte max
      }
    }
    
    val vector = StripedByteFrequencyVector(SMALL_POPULATION_SPEC, measurementSpec)
    
    // Try to increment beyond byte capacity
    repeat(150) {
      vector.incrementByIndex(5)
    }
    
    val array = vector.getArray()
    assertThat(array[5]).isEqualTo(127) // Should be capped at 127 (byte limit)
  }

  @Test
  fun `empty population spec creates zero-size vector`() {
    val emptyPopSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 0L // Invalid range - end < start
        }
      }
    }
    
    assertFailsWith<Exception>("Invalid Population Spec") {
      StripedByteFrequencyVector(emptyPopSpec, FULL_REACH_MEASUREMENT_SPEC)
    }
  }

  @Test
  fun `very large population works correctly`() {
    val largePopSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1L
          endVidInclusive = 1_000_000L
        }
      }
    }
    
    val vector = StripedByteFrequencyVector(largePopSpec, FULL_REACH_MEASUREMENT_SPEC)
    
    assertThat(vector.size).isEqualTo(1_000_000)
    
    // Test some operations
    vector.incrementByIndex(0)
    vector.incrementByIndex(500_000)
    vector.incrementByIndex(999_999)
    
    val array = vector.getArray()
    assertThat(array[0]).isEqualTo(1)
    assertThat(array[500_000]).isEqualTo(1)
    assertThat(array[999_999]).isEqualTo(1)
  }
}