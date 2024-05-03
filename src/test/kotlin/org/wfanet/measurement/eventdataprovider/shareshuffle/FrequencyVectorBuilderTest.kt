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
package org.wfanet.measurement.eventdataprovider.shareshuffle

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.ByteString
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.frequencycount.frequencyVector
import org.wfanet.measurement.api.v2alpha.MeasurementSpec.VidSamplingInterval
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reach
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.populationSpec

@RunWith(JUnit4::class)
class FrequencyVectorBuilderTest {
  private val startingVid = 100_000L

  private val smallPopulationSize = 10
  private val smallPopulationSpec = populationSpec {
    subpopulations += subPopulation {
      vidRanges += vidRange {
        // make the VIDs out of bounds of the frequency vector
        startVid = startingVid
        endVidInclusive = startingVid + smallPopulationSize - 1
      }
    }
  }

  private val smallComplexPopulationSpec = populationSpec {
    subpopulations += subPopulation {
      vidRanges += vidRange {
        // make the VIDs out of bounds of the frequency vector
        startVid = startingVid
        endVidInclusive = startingVid + 4
      }
    }
    subpopulations += subPopulation {
      vidRanges += vidRange {
        // make the VIDs out of bounds of the frequency vector
        startVid = startingVid + 4 + 1
        endVidInclusive = startingVid + smallPopulationSize - 1
      }
    }
  }

  private val fullSamplingInterval = vidSamplingInterval {
    start = 0f
    width = 1.0f
  }
  private val fullReachMeasurementSpec = measurementSpec {
    vidSamplingInterval = fullSamplingInterval
    reach = reach {}
  }

  private val partialNonWrappingSamplingInterval = vidSamplingInterval {
    start = 0.3f
    width = 0.5f
  }
  private val partialNonWrappingReachMeasurementSpec = measurementSpec {
    vidSamplingInterval = partialNonWrappingSamplingInterval
    reach = reach {}
  }

  private val partialWrappingSamplingInterval = vidSamplingInterval {
    start = 0.8f
    width = 0.5f
  }
  private val partialWrappingReachMeasurementSpec = measurementSpec {
    vidSamplingInterval = partialWrappingSamplingInterval
    reach = reach {}
  }

  private val fullWrappingSamplingInterval = vidSamplingInterval {
    start = 1.0f
    width = 1.0f
  }
  private val fullWrappingReachMeasurementSpec = measurementSpec {
    vidSamplingInterval = fullWrappingSamplingInterval
    reach = reach {}
  }

  // A hash function to map the VIDs into the frequency vector based on their numeric order
  private val hashFunction = { vid: Long, _: ByteString -> vid - startingVid }

  private fun getExpectedFrequencyVectorSize(
    populationSize: Int,
    samplingInterval: VidSamplingInterval,
  ): Int {
    return (populationSize * samplingInterval.width).toInt()
  }

  @Test
  fun `construction fails when measurement spec does not have reach or reach and frequency`() {
    assertFailsWith<IllegalArgumentException>("expected exception") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        measurementSpec { vidSamplingInterval = fullSamplingInterval },
      )
    }
  }

  @Test
  fun `construction fails on reach and frequency measurement spec without max frequency`() {
    assertFailsWith<IllegalArgumentException>("expected exception") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        measurementSpec {
          reachAndFrequency = reachAndFrequency {}
          vidSamplingInterval = fullSamplingInterval
        },
      )
    }
  }

  @Test
  fun `construction fails when sampling interval is invalid`() {
    assertFailsWith<IllegalArgumentException>("expected exception start < 0") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = -1.0f
            width = 0.5f
          }
        },
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception start > 1.0") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 2.0f
            width = 0.5f
          }
        },
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception width <= 0") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 0f
            width = 0f
          }
        },
      )
    }

    assertFailsWith<IllegalArgumentException>("expected exception width > 1") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        measurementSpec {
          reach = reach {}
          vidSamplingInterval = vidSamplingInterval {
            start = 0f
            width = 2f
          }
        },
      )
    }
  }

  @Test
  fun `create frequency vector for reach over full interval`() {
    val vidIndexMap: VidIndexMap = InMemoryVidIndexMap(smallPopulationSpec, hashFunction)
    val builder = FrequencyVectorBuilder(smallPopulationSpec, fullReachMeasurementSpec)

    // Get the full set of VIDs in the population. Add them then add one duplicate.
    // This shows that the primary range is calculated correctly and that
    // frequency capping for reach works.
    val vids =
      generateSequence({ startingVid }, { current -> current + 1 })
        .take(smallPopulationSize)
        .toList()
    // Exercise both "increment" and "incrementAll"
    builder.incrementAll(vids.map { vidIndexMap[it] })
    builder.increment(vidIndexMap[startingVid])

    assertThat(builder.build())
      .isEqualTo(
        frequencyVector { data += generateSequence { 1 }.take(smallPopulationSize).toList() }
      )
  }

  @Test
  fun `create frequency vector from previous frequency vector`() {
    val builder =
      FrequencyVectorBuilder(
        smallPopulationSpec,
        fullReachMeasurementSpec,
        frequencyVector { data += listOf<Int>(2, 1, 0, 0, 0, 0, 0, 0, 0, 0) },
      )

    // Note this is not the same vector as above because the initialization will cap
    // inputs that are over the max frequency.
    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 0, 0, 0, 0, 0, 0, 0, 0) })
  }

  @Test
  fun `create frequency vector from previous incompatible frequency vector fails`() {
    assertFailsWith<IllegalArgumentException>("expected exception") {
      FrequencyVectorBuilder(
        smallPopulationSpec,
        fullReachMeasurementSpec,
        frequencyVector { data += listOf<Int>(2, 1, 0, 0, 0) },
      )
    }
  }

  @Test
  fun `add a builder to another builder`() {
    val builder1 =
      FrequencyVectorBuilder(
        smallPopulationSpec,
        fullReachMeasurementSpec,
        frequencyVector { data += listOf<Int>(1, 1, 0, 0, 0, 0, 0, 0, 0, 0) },
      )

    val builder2 =
      FrequencyVectorBuilder(
        smallPopulationSpec,
        fullReachMeasurementSpec,
        frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0, 0, 0, 0, 0, 1) },
      )

    builder2.incrementAll(builder1)

    // Note this is note the same vector as above because the initialization will cap
    // inputs that are over the max frequency.
    assertThat(builder2.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(1, 1, 0, 0, 0, 0, 0, 0, 0, 1) })
  }

  @Test
  fun `add an incompatible builder to another builder fails`() {
    val builder1 =
      FrequencyVectorBuilder(
        smallPopulationSpec,
        fullReachMeasurementSpec,
        frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0, 0, 0, 0, 0, 0) },
      )

    val builder2 =
      FrequencyVectorBuilder(
        smallPopulationSpec,
        partialNonWrappingReachMeasurementSpec,
        frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0) },
      )

    assertFailsWith<IllegalArgumentException>("expected exception") {
      builder2.incrementAll(builder1)
    }
  }

  @Test
  fun `increment fails with out of range VID in strict mode`() {
    val builder =
      FrequencyVectorBuilder(smallPopulationSpec, partialNonWrappingReachMeasurementSpec)

    // This fails because the partial interval is from 0.3 to 0.8, and therefore does
    // not include the global index zero.
    assertFailsWith<IllegalArgumentException>("expected exception") { builder.increment(0) }
  }

  @Test
  fun `increment out of range VID with strict mode disabled`() {
    val builder =
      FrequencyVectorBuilder(
        smallPopulationSpec,
        partialNonWrappingReachMeasurementSpec,
        strict = false,
      )

    // In strict mode this fails because the partial interval is from 0.3 to 0.8, however
    // since we are not in strict mode this has no effect.
    // not include the global index zero.
    builder.increment(0)
    assertThat(builder.build()).isEqualTo(frequencyVector { data += listOf<Int>(0, 0, 0, 0, 0) })
  }

  @Test
  fun `create frequency vector for frequency over full interval`() {
    val vidIndexMap: VidIndexMap = InMemoryVidIndexMap(smallPopulationSpec, hashFunction)
    val frequencyMeasurementSpec = measurementSpec {
      vidSamplingInterval = fullSamplingInterval
      reachAndFrequency = reachAndFrequency { maximumFrequency = 2 }
    }

    val builder = FrequencyVectorBuilder(smallPopulationSpec, frequencyMeasurementSpec)

    // One is capped, one does not require capping, one goes up to cap
    val vids =
      listOf(
        startingVid,
        startingVid,
        startingVid,
        startingVid + 1,
        startingVid + 8,
        startingVid + 8,
      )
    vids.map { builder.increment(vidIndexMap[it]) }

    assertThat(builder.build())
      .isEqualTo(frequencyVector { data += listOf<Int>(2, 1, 0, 0, 0, 0, 0, 0, 2, 0) })
  }

  @Test
  fun `create frequency vector for reach over partial interval`() {
    val vidIndexMap: VidIndexMap = InMemoryVidIndexMap(smallComplexPopulationSpec, hashFunction)
    val builder =
      FrequencyVectorBuilder(smallComplexPopulationSpec, partialNonWrappingReachMeasurementSpec)

    // Population size is 10, interval is [0.3, 0.8)
    val expectedSize = 5
    assertThat(builder.size).isEqualTo(expectedSize)

    // Boundaries are: 10 * 0.3 = 3, this is the 4th VID and is contained
    // Boundaries are: 10 * 0.8 = 8, this is the 9th VID and is not contained

    // Increment the contained boundaries
    builder.increment(vidIndexMap[startingVid + 3])
    builder.increment(vidIndexMap[startingVid + 7])

    // Ensure the non-contained boundaries fail to be added
    assertFailsWith<IllegalArgumentException>("outside of left hand bounds") {
      builder.increment(vidIndexMap[startingVid + 2])
    }

    assertFailsWith<IllegalArgumentException>("outside of right hand bounds") {
      builder.increment(vidIndexMap[startingVid + 8])
    }

    val expectedData = IntArray(expectedSize)
    expectedData[0] = 1
    expectedData[4] = 1
    assertThat(builder.build()).isEqualTo(frequencyVector { data += expectedData.toList() })
  }

  @Test
  fun `create frequency vector for reach over partial wrapping interval`() {
    val vidIndexMap: VidIndexMap = InMemoryVidIndexMap(smallComplexPopulationSpec, hashFunction)
    val builder =
      FrequencyVectorBuilder(smallComplexPopulationSpec, partialWrappingReachMeasurementSpec)

    // Population size is 10 interval is [0.8, 1.0] + [0, 0.3)
    val expectedSize = 5
    assertThat(builder.size).isEqualTo(expectedSize)

    // Boundaries are: 10 * 0.8 = 8, which is contained
    // Boundaries are: 10 * 1.0 = 10, which is not contained, but 9 is
    // Boundaries are: 10 * 0 = 0, which is contained
    // Boundaries are: 10 * 0.3 = 3, which is not contained

    // Increment the contained boundaries
    builder.increment(vidIndexMap[startingVid + 8])
    builder.increment(vidIndexMap[startingVid + 9])
    builder.increment(vidIndexMap[startingVid + 0])
    builder.increment(vidIndexMap[startingVid + 2])

    // Ensure the non-contained boundaries fail to be added
    assertFailsWith<IllegalArgumentException>("outside of left hand bounds") {
      builder.increment(vidIndexMap[startingVid + 7])
    }

    assertFailsWith<IllegalArgumentException>("outside of right hand bounds") {
      builder.increment(vidIndexMap[startingVid + 3])
    }

    val expectedData = IntArray(expectedSize)
    expectedData[0] = 1
    expectedData[1] = 1
    expectedData[2] = 1
    expectedData[4] = 1
    assertThat(builder.build()).isEqualTo(frequencyVector { data += expectedData.toList() })
  }

  @Test
  fun `create frequency vector for reach over full wrapping interval`() {
    val vidIndexMap: VidIndexMap = InMemoryVidIndexMap(smallPopulationSpec, hashFunction)
    val builder = FrequencyVectorBuilder(smallPopulationSpec, fullWrappingReachMeasurementSpec)

    // Population size is 10, interval is [1.0, 1.0) + [0, 1.0)
    val expectedSize = 10
    assertThat(builder.size).isEqualTo(expectedSize)

    // Boundaries are: 10 * 0.3 = 3, this is the 4th VID is contained
    // Boundaries are: 10 * 0.8 = 8, this is the 9th VID and is contained

    // Increment the contained boundaries
    builder.increment(vidIndexMap[startingVid + 3])
    builder.increment(vidIndexMap[startingVid + 8])

    val expectedData = IntArray(expectedSize)
    expectedData[3] = 1
    expectedData[8] = 1
    assertThat(builder.build()).isEqualTo(frequencyVector { data += expectedData.toList() })
  }
}
