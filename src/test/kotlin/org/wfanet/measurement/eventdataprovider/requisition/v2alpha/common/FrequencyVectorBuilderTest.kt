/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.requisition.v2alpha.common

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec

@RunWith(JUnit4::class)
class FrequencyVectorBuilderTest {

  @Test
  fun `construction fails on reach and frequency measurement spec without max frequency`() {
    assertFailsWith<IllegalArgumentException>("expected exception") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reachAndFrequency = reachAndFrequency {}
          vidSamplingInterval = FULL_SAMPLING_INTERVAL
        },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }
  }

  @Test
  fun `construction fails on impression measurement spec without max frequency`() {
    assertFailsWith<IllegalArgumentException>("expected exception") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          impression = impression {}
          vidSamplingInterval = FULL_SAMPLING_INTERVAL
        },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }
  }

  @Test
  fun `construction fails when sampling interval is invalid`() {
    assertFailsWith<IllegalArgumentException>("expected exception start < 0") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = -1.0f
            width = 0.5f
          }
        },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception start > 1.0") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 2.0f
            width = 0.5f
          }
        },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception width <= 0") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 0f
            width = 0f
          }
        },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception width > 1") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 0f
            width = 2f
          }
        },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }
  }

  @Test
  fun `build returns frequency vector for reach over full interval`() {
    // Get the full set of VIDs in the population. Add them then add one duplicate.
    // This shows that the primary range is calculated correctly and that
    // frequency capping for reach works.
    val vids =
      generateSequence({ STARTING_VID }, { current -> current + 1 })
        .take(SMALL_POPULATION_SIZE)
        .toList()

    val frequencyVector =
      FrequencyVectorBuilder.build(SMALL_POPULATION_SPEC, FULL_REACH_MEASUREMENT_SPEC) {
        // Make sure we exercise both "increment" and "incrementAll"
        incrementAll(vids.map { SMALL_POPULATION_VID_INDEX_MAP[it] })
        increment(SMALL_POPULATION_VID_INDEX_MAP[STARTING_VID])
      }

    assertThat(frequencyVector)
      .isEqualTo(
        frequencyVector { data += generateSequence { 1 }.take(SMALL_POPULATION_SIZE).toList() }
      )
  }

  @Test
  fun `build returns frequency vector from previous frequency vector`() {
    val frequencyVector =
      FrequencyVectorBuilder.build(
        SMALL_POPULATION_SPEC,
        FULL_REACH_MEASUREMENT_SPEC,
        frequencyVector { data += listOf<Int>(2, 1, 0, 0, 0, 0, 0, 0, 0, 0) },
      ) {}

    // Note this is not the same vector as above because the initialization will cap
    // inputs that are over the max frequency.
    assertThat(frequencyVector)
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 0, 0, 0, 0, 0, 0, 0, 0) })
  }

  @Test
  fun `builder construction fails when with incompatible frequency vector as input`() {
    assertFailsWith<IllegalArgumentException>("expected exception") {
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        FULL_REACH_MEASUREMENT_SPEC,
        frequencyVector { data += listOf<Int>(2, 1, 0, 0, 0) },
        overrideImpressionMaxFrequencyPerUser = null,
      )
    }
  }

  @Test
  fun `build returns frequency vector when incrementing with another build`() {
    val builder1 =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        FULL_REACH_MEASUREMENT_SPEC,
        frequencyVector { data += listOf<Int>(1, 1, 0, 0, 0, 0, 0, 0, 0, 0) },
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val frequencyVector =
      FrequencyVectorBuilder.build(
        SMALL_POPULATION_SPEC,
        FULL_REACH_MEASUREMENT_SPEC,
        frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0, 0, 0, 0, 0, 1) },
      ) {
        incrementAll(builder1)
      }

    // Note this is note the same vector as above because the initialization will cap
    // inputs that are over the max frequency.
    assertThat(frequencyVector)
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 0, 0, 0, 0, 0, 0, 0, 1) })
  }

  @Test
  fun `increment all fails with an incompatible builder as input`() {
    val builder1 =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        FULL_REACH_MEASUREMENT_SPEC,
        frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0, 0, 0, 0, 0, 0) },
        overrideImpressionMaxFrequencyPerUser = null,
      )

    val builder2 =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC,
        frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0) },
        overrideImpressionMaxFrequencyPerUser = null,
      )

    assertFailsWith<IllegalArgumentException>("expected exception") {
      builder2.incrementAll(builder1)
    }
  }

  @Test
  fun `increment fails with out of range VID in strict mode`() {
    val builder =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // This fails because the partial interval is from 0.3 to 0.8, and therefore does
    // not include the global index zero.
    assertFailsWith<IllegalArgumentException>("expected exception") { builder.increment(0) }
  }

  @Test
  fun `increment ignores out of range VID with strict mode disabled`() {
    val builder =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Since we are not in strict mode this has no effect.
    builder.increment(0)
    assertThat(builder.build()).isEqualTo(frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0) })
  }

  @Test
  fun `incrementBy fails when amount is 0`() {
    val builder =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
      )
    assertFailsWith<IllegalArgumentException>("expected exception") { builder.incrementBy(5, 0) }
  }

  @Test
  fun `build returns a frequency vector for frequency over full interval`() {
    val frequencyMeasurementSpec = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reachAndFrequency = reachAndFrequency { maximumFrequency = 3 }
    }

    val frequencyVector =
      FrequencyVectorBuilder.build(SMALL_POPULATION_SPEC, frequencyMeasurementSpec) {
        // One is capped, one does not require capping, one goes up to cap
        listOf(
            STARTING_VID,
            STARTING_VID,
            STARTING_VID,
            STARTING_VID + 1,
            STARTING_VID + 8,
            STARTING_VID + 8,
          )
          .map { incrementBy(SMALL_POPULATION_VID_INDEX_MAP[it], 2) }
      }

    assertThat(frequencyVector)
      .isEqualTo(frequencyVector { data += listOf<Int>(3, 2, 0, 0, 0, 0, 0, 0, 3, 0) })
  }

  @Test
  fun `build returns a frequency vector for reach over partial interval`() {
    val builder =
      FrequencyVectorBuilder(
        SMALL_COMPLEX_POPULATION_SPEC,
        PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Population size is 10, interval is [0.3, 0.8)
    val expectedSize = 5
    assertThat(builder.size).isEqualTo(expectedSize)

    // Boundaries are: 10 * 0.3 = 3, this is the 4th VID and is contained
    // Boundaries are: 10 * 0.8 = 8, this is the 9th VID and is not contained

    // Increment the contained boundaries
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 3])
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 7])

    // Ensure the non-contained boundaries fail to be added
    assertFailsWith<IllegalArgumentException>("outside of left hand bounds") {
      builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 2])
    }

    assertFailsWith<IllegalArgumentException>("outside of right hand bounds") {
      builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 8])
    }

    val expectedData = IntArray(expectedSize)
    expectedData[0] = 1
    expectedData[4] = 1
    assertThat(builder.build()).isEqualTo(frequencyVector { data += expectedData.toList() })
  }

  @Test
  fun `build returns a frequency vector for reach over partial wrapping interval`() {
    val builder =
      FrequencyVectorBuilder(
        SMALL_COMPLEX_POPULATION_SPEC,
        PARTIAL_WRAPPING_REACH_MEASUREMENT_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Population size is 10 interval is [0.8, 1.0] + [0, 0.3)
    val expectedSize = 5
    assertThat(builder.size).isEqualTo(expectedSize)

    // Boundaries are: 10 * 0.8 = 8, which is contained
    // Boundaries are: 10 * 1.0 = 10, which is not contained, but 9 is
    // Boundaries are: 10 * 0 = 0, which is contained
    // Boundaries are: 10 * 0.3 = 3, which is not contained

    // Increment the contained boundaries
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 8])
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 9])
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 0])
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 2])

    // Ensure the non-contained boundaries fail to be added
    assertFailsWith<IllegalArgumentException>("outside of left hand bounds") {
      builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 7])
    }

    assertFailsWith<IllegalArgumentException>("outside of right hand bounds") {
      builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 3])
    }

    val expectedData = IntArray(expectedSize)
    expectedData[0] = 1
    expectedData[1] = 1
    expectedData[2] = 1
    expectedData[4] = 1
    assertThat(builder.build()).isEqualTo(frequencyVector { data += expectedData.toList() })
  }

  @Test
  fun `build returns a frequency vector for reach over full wrapping interval`() {
    val builder =
      FrequencyVectorBuilder(
        SMALL_POPULATION_SPEC,
        FULL_WRAPPING_REACH_MEASUREMENT_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Population size is 10, interval is [1.0, 1.0) + [0, 1.0)
    val expectedSize = 10
    assertThat(builder.size).isEqualTo(expectedSize)

    // Boundaries are: 10 * 0.3 = 3, this is the 4th VID is contained
    // Boundaries are: 10 * 0.8 = 8, this is the 9th VID and is contained

    // Increment the contained boundaries
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 3])
    builder.increment(SMALL_COMPLEX_POPULATION_VID_INDEX_MAP[STARTING_VID + 8])

    val expectedData = IntArray(expectedSize)
    expectedData[3] = 1
    expectedData[8] = 1
    assertThat(builder.build()).isEqualTo(frequencyVector { data += expectedData.toList() })
  }

  @Test
  fun `ByteArray constructor builds correct frequency vector for reach over full interval`() {
    // Create a byte array with frequency data for each VID
    val frequencyDataBytes = byteArrayOf(1, 2, 3, 0, 1, 0, 0, 5, 1, 0)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = FULL_REACH_MEASUREMENT_SPEC,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // For reach, all frequencies should be capped at 1
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 1, 0, 1, 0, 0, 1, 1, 0) })
  }

  @Test
  fun `ByteArray constructor builds correct frequency vector for frequency measurement`() {
    val frequencyMeasurementSpec = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reachAndFrequency = reachAndFrequency { maximumFrequency = 5 }
    }

    // Create a byte array with frequency data, some values exceed max frequency
    val frequencyDataBytes = byteArrayOf(1, 2, 3, 0, 1, 0, 0, 10, 5, 0)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = frequencyMeasurementSpec,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Frequencies should be capped at maximum of 5
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 2, 3, 0, 1, 0, 0, 5, 5, 0) })
  }

  @Test
  fun `ByteArray constructor handles unsigned bytes correctly`() {
    val frequencyMeasurementSpec = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reachAndFrequency = reachAndFrequency { maximumFrequency = 255 }
    }

    // Test unsigned byte values (200 and 255 would be negative as signed bytes)
    val frequencyDataBytes =
      byteArrayOf(10, 50, 100, (-56).toByte(), 200.toByte(), 255.toByte(), 0, 0, 0, 0)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = frequencyMeasurementSpec,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Verify unsigned conversion: -56 as byte = 200 unsigned, 255 stays 255
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(10, 50, 100, 200, 200, 255, 0, 0, 0, 0) })
  }

  @Test
  fun `ByteArray constructor applies sampling for partial non-wrapping interval`() {
    // Population size is 10, interval is [0.3, 0.8) = indices 3-7 (5 elements)
    val frequencyDataBytes = byteArrayOf(1, 1, 1, 5, 6, 7, 8, 9, 1, 1)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Should only include indices 3-7, and cap at 1 for reach
    assertThat(builder.size).isEqualTo(5)
    assertThat(builder.build()).isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 1, 1, 1) })
  }

  @Test
  fun `ByteArray constructor applies sampling for partial wrapping interval`() {
    // Population size is 10, interval is [0.8, 1.3) = [0.8, 1.0) + [0, 0.3)
    // This should include indices 8-9 and 0-2 (5 elements)
    val frequencyDataBytes = byteArrayOf(10, 20, 30, 0, 0, 0, 0, 0, 80, 90)

    val frequencyMeasurementSpec = measurementSpec {
      vidSamplingInterval = PARTIAL_WRAPPING_SAMPLING_INTERVAL
      reachAndFrequency = reachAndFrequency { maximumFrequency = 100 }
    }

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = frequencyMeasurementSpec,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Output order: indices 8, 9, 0, 1, 2
    assertThat(builder.size).isEqualTo(5)
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(80, 90, 10, 20, 30) })
  }

  @Test
  fun `ByteArray constructor handles empty frequencies`() {
    val frequencyDataBytes = byteArrayOf(0, 0, 0, 0, 0, 0, 0, 0, 0, 0)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = FULL_REACH_MEASUREMENT_SPEC,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0, 0, 0, 0, 0, 0) })
  }

  @Test
  fun `ByteArray constructor handles array smaller than population size`() {
    // Only provide data for first 5 VIDs
    val frequencyDataBytes = byteArrayOf(1, 2, 3, 4, 5)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = FULL_REACH_MEASUREMENT_SPEC,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Remaining VIDs should have frequency 0
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 1, 1, 1, 0, 0, 0, 0, 0) })
  }

  @Test
  fun `ByteArray constructor produces same result as manual increment approach`() {
    val frequencyDataBytes = byteArrayOf(1, 2, 3, 0, 1, 0, 0, 5, 1, 0)
    val frequencyData =
      IntArray(frequencyDataBytes.size) { frequencyDataBytes[it].toInt() and 0xFF }

    // Build using ByteArray constructor
    val builderFromBytes =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = FULL_REACH_MEASUREMENT_SPEC,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
      )

    // Build using manual increment
    val builderManual =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = FULL_REACH_MEASUREMENT_SPEC,
        overrideImpressionMaxFrequencyPerUser = null,
        strict = false,
      )
    for (index in frequencyData.indices) {
      val frequency = frequencyData[index]
      if (frequency > 0) {
        builderManual.incrementBy(index, frequency)
      }
    }

    // Both should produce identical results
    assertThat(builderFromBytes.build()).isEqualTo(builderManual.build())
  }

  @Test
  fun `ByteArray constructor with kAnonymityParams for reach measurement`() {
    val kAnonymityParams =
      org.wfanet.measurement.computation.KAnonymityParams(
        minUsers = 10,
        minImpressions = 10,
        reachMaxFrequencyPerUser = 3,
      )

    val reachMeasurementSpec = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reach = reach {}
    }

    // Create a byte array with various frequency values
    val frequencyDataBytes = byteArrayOf(1, 5, 10, 0, 2, 0, 0, 8, 3, 0)

    val builder =
      FrequencyVectorBuilder(
        populationSpec = SMALL_POPULATION_SPEC,
        measurementSpec = reachMeasurementSpec,
        frequencyDataBytes = frequencyDataBytes,
        strict = false,
        overrideImpressionMaxFrequencyPerUser = null,
        kAnonymityParams = kAnonymityParams,
      )

    // For reach with k-anonymity, frequencies should be capped at reachMaxFrequencyPerUser (3)
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 3, 3, 0, 2, 0, 0, 3, 3, 0) })
  }

  companion object {
    // A hash function to map the VIDs into the frequency vector based on their numeric order
    private val hashFunction = { vid: Long, _: ByteString -> vid - STARTING_VID }

    private const val STARTING_VID = 100_000L

    private const val SMALL_POPULATION_SIZE = 10
    private val SMALL_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          // make the VIDs out of bounds of the frequency vector
          startVid = STARTING_VID
          endVidInclusive = STARTING_VID + SMALL_POPULATION_SIZE - 1
        }
      }
    }
    val SMALL_POPULATION_VID_INDEX_MAP =
      InMemoryVidIndexMap.buildInternal(SMALL_POPULATION_SPEC, hashFunction)

    private val SMALL_COMPLEX_POPULATION_SPEC = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          // make the VIDs out of bounds of the frequency vector
          startVid = STARTING_VID
          endVidInclusive = STARTING_VID + 4
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          // make the VIDs out of bounds of the frequency vector
          startVid = STARTING_VID + 4 + 1
          endVidInclusive = STARTING_VID + SMALL_POPULATION_SIZE - 1
        }
      }
    }
    val SMALL_COMPLEX_POPULATION_VID_INDEX_MAP =
      InMemoryVidIndexMap.buildInternal(SMALL_POPULATION_SPEC, hashFunction)

    private val FULL_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0f
      width = 1.0f
    }
    private val FULL_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = FULL_SAMPLING_INTERVAL
      reach = reach {}
    }

    private val PARTIAL_NON_WRAPPING_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0.3f
      width = 0.5f
    }
    private val PARTIAL_NON_WRAPPING_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = PARTIAL_NON_WRAPPING_SAMPLING_INTERVAL
      reach = reach {}
    }

    private val PARTIAL_WRAPPING_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 0.8f
      width = 0.5f
    }
    private val PARTIAL_WRAPPING_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = PARTIAL_WRAPPING_SAMPLING_INTERVAL
      reach = reach {}
    }

    private val FULL_WRAPPING_SAMPLING_INTERVAL = vidSamplingInterval {
      start = 1.0f
      width = 1.0f
    }
    private val FULL_WRAPPING_REACH_MEASUREMENT_SPEC = measurementSpec {
      vidSamplingInterval = FULL_WRAPPING_SAMPLING_INTERVAL
      reach = reach {}
    }
  }
}
