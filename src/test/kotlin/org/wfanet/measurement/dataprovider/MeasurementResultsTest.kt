// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.TypeRegistry
import kotlinx.coroutines.flow.Flow
import kotlinx.coroutines.flow.asFlow
import kotlinx.coroutines.flow.flowOf
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.mockito.kotlin.any
import org.mockito.kotlin.mock
import org.mockito.kotlin.whenever
import org.projectnessie.cel.Program
import org.wfanet.measurement.dataprovider.MeasurementResults.ReachAndFrequency
import org.wfanet.measurement.populationdataprovider.PopulationInfo
import org.wfanet.measurement.populationdataprovider.PopulationRequisitionFulfiller

@RunWith(JUnit4::class)
class MeasurementResultsTest {

  @Test
  fun `computeReachAndFrequency with Flow returns correct reach and frequency distribution`() = runBlocking {
    // Setup - Create 10 VIDs with different frequencies
    val vids = flowOf(
      1L, 1L, 1L,           // VID 1 appears 3 times
      2L, 2L,               // VID 2 appears 2 times
      3L,                   // VID 3 appears 1 time
      4L, 4L, 4L, 4L,       // VID 4 appears 4 times
      5L, 5L, 5L, 5L, 5L,   // VID 5 appears 5 times
      6L, 6L, 6L, 6L, 6L, 6L, // VID 6 appears 6 times (will be capped at 5)
      7L,                   // VID 7 appears 1 time
      8L, 8L,               // VID 8 appears 2 times
      9L, 9L, 9L,           // VID 9 appears 3 times
      10L                   // VID 10 appears 1 time
    )
    val maxFrequency = 5

    // Execute
    val result = MeasurementResults.computeReachAndFrequency(vids, maxFrequency)

    // Verify
    assertThat(result.reach).isEqualTo(10) // 10 distinct VIDs
    assertThat(result.relativeFrequencyDistribution).hasSize(maxFrequency)
    assertThat(result.relativeFrequencyDistribution[1]).isEqualTo(3.0 / 10.0) // 3 VIDs with frequency 1
    assertThat(result.relativeFrequencyDistribution[2]).isEqualTo(2.0 / 10.0) // 2 VIDs with frequency 2
    assertThat(result.relativeFrequencyDistribution[3]).isEqualTo(2.0 / 10.0) // 2 VIDs with frequency 3
    assertThat(result.relativeFrequencyDistribution[4]).isEqualTo(1.0 / 10.0) // 1 VID with frequency 4
    assertThat(result.relativeFrequencyDistribution[5]).isEqualTo(2.0 / 10.0) // 2 VIDs with frequency 5 (including capped)
  }

  @Test
  fun `computeReachAndFrequency with Flow handles empty input`() = runBlocking {
    // Setup
    val vids = flowOf<Long>()
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeReachAndFrequency(vids, maxFrequency)

    // Verify
    assertThat(result.reach).isEqualTo(0)
    assertThat(result.relativeFrequencyDistribution).hasSize(maxFrequency)
    assertThat(result.relativeFrequencyDistribution[1]).isEqualTo(0.0)
    assertThat(result.relativeFrequencyDistribution[2]).isEqualTo(0.0)
    assertThat(result.relativeFrequencyDistribution[3]).isEqualTo(0.0)
  }

  @Test
  fun `computeReachAndFrequency with Flow caps frequencies at maxFrequency`() = runBlocking {
    // Setup
    val vids = flowOf(1L, 1L, 1L, 1L, 1L, 2L, 2L) // VID 1 has frequency 5, VID 2 has frequency 2
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeReachAndFrequency(vids, maxFrequency)

    // Verify
    assertThat(result.reach).isEqualTo(2)
    assertThat(result.relativeFrequencyDistribution).hasSize(maxFrequency)
    assertThat(result.relativeFrequencyDistribution[1]).isEqualTo(0.0) // No VIDs with frequency 1
    assertThat(result.relativeFrequencyDistribution[2]).isEqualTo(0.5) // 1 VID with frequency 2
    assertThat(result.relativeFrequencyDistribution[3]).isEqualTo(0.5) // 1 VID with frequency 3 (capped from 5)
  }

  @Test
  fun `computeReachAndFrequency with Iterable returns correct reach and frequency distribution`() {
    // Setup - Create 10 VIDs with different frequencies
    val vids = listOf(
      1L, 1L, 1L,           // VID 1 appears 3 times
      2L, 2L,               // VID 2 appears 2 times
      3L,                   // VID 3 appears 1 time
      4L, 4L, 4L, 4L,       // VID 4 appears 4 times
      5L, 5L, 5L, 5L, 5L,   // VID 5 appears 5 times
      6L, 6L, 6L, 6L, 6L, 6L, // VID 6 appears 6 times (will be capped at 5)
      7L,                   // VID 7 appears 1 time
      8L, 8L,               // VID 8 appears 2 times
      9L, 9L, 9L,           // VID 9 appears 3 times
      10L                   // VID 10 appears 1 time
    )
    val maxFrequency = 5

    // Execute
    val result = MeasurementResults.computeReachAndFrequency(vids, maxFrequency)

    // Verify
    assertThat(result.reach).isEqualTo(10) // 10 distinct VIDs
    assertThat(result.relativeFrequencyDistribution).hasSize(maxFrequency)
    assertThat(result.relativeFrequencyDistribution[1]).isEqualTo(3.0 / 10.0) // 3 VIDs with frequency 1
    assertThat(result.relativeFrequencyDistribution[2]).isEqualTo(2.0 / 10.0) // 2 VIDs with frequency 2
    assertThat(result.relativeFrequencyDistribution[3]).isEqualTo(2.0 / 10.0) // 2 VIDs with frequency 3
    assertThat(result.relativeFrequencyDistribution[4]).isEqualTo(1.0 / 10.0) // 1 VID with frequency 4
    assertThat(result.relativeFrequencyDistribution[5]).isEqualTo(2.0 / 10.0) // 2 VIDs with frequency 5 (including capped)
  }

  @Test
  fun `computeReachAndFrequency with Iterable handles empty input`() {
    // Setup
    val vids = emptyList<Long>()
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeReachAndFrequency(vids, maxFrequency)

    // Verify
    assertThat(result.reach).isEqualTo(0)
    assertThat(result.relativeFrequencyDistribution).hasSize(maxFrequency)
    assertThat(result.relativeFrequencyDistribution[1]).isEqualTo(0.0)
    assertThat(result.relativeFrequencyDistribution[2]).isEqualTo(0.0)
    assertThat(result.relativeFrequencyDistribution[3]).isEqualTo(0.0)
  }

  @Test
  fun `computeReachAndFrequency with Iterable caps frequencies at maxFrequency`() {
    // Setup
    val vids = listOf(1L, 1L, 1L, 1L, 1L, 2L, 2L) // VID 1 has frequency 5, VID 2 has frequency 2
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeReachAndFrequency(vids, maxFrequency)

    // Verify
    assertThat(result.reach).isEqualTo(2)
    assertThat(result.relativeFrequencyDistribution).hasSize(maxFrequency)
    assertThat(result.relativeFrequencyDistribution[1]).isEqualTo(0.0) // No VIDs with frequency 1
    assertThat(result.relativeFrequencyDistribution[2]).isEqualTo(0.5) // 1 VID with frequency 2
    assertThat(result.relativeFrequencyDistribution[3]).isEqualTo(0.5) // 1 VID with frequency 3 (capped from 5)
  }

  @Test
  fun `computeReach with Flow returns correct count of distinct VIDs`() = runBlocking {
    // Setup - Create 10 VIDs with some duplicates
    val vids = flowOf(
      1L, 1L, 1L,           // VID 1 appears 3 times
      2L, 2L,               // VID 2 appears 2 times
      3L,                   // VID 3 appears 1 time
      4L, 4L, 4L, 4L,       // VID 4 appears 4 times
      5L, 5L, 5L, 5L, 5L,   // VID 5 appears 5 times
      6L, 6L, 6L, 6L, 6L, 6L, // VID 6 appears 6 times
      7L,                   // VID 7 appears 1 time
      8L, 8L,               // VID 8 appears 2 times
      9L, 9L, 9L,           // VID 9 appears 3 times
      10L                   // VID 10 appears 1 time
    )

    // Execute
    val result = MeasurementResults.computeReach(vids)

    // Verify - Should count 10 distinct VIDs regardless of frequency
    assertThat(result).isEqualTo(10)
  }

  @Test
  fun `computeReach with Flow handles empty input`() = runBlocking {
    // Setup
    val vids = flowOf<Long>()

    // Execute
    val result = MeasurementResults.computeReach(vids)

    // Verify
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `computeReach with Iterable returns correct count of distinct VIDs`() {
    // Setup - Create 10 VIDs with some duplicates
    val vids = listOf(
      1L, 1L, 1L,           // VID 1 appears 3 times
      2L, 2L,               // VID 2 appears 2 times
      3L,                   // VID 3 appears 1 time
      4L, 4L, 4L, 4L,       // VID 4 appears 4 times
      5L, 5L, 5L, 5L, 5L,   // VID 5 appears 5 times
      6L, 6L, 6L, 6L, 6L, 6L, // VID 6 appears 6 times
      7L,                   // VID 7 appears 1 time
      8L, 8L,               // VID 8 appears 2 times
      9L, 9L, 9L,           // VID 9 appears 3 times
      10L                   // VID 10 appears 1 time
    )

    // Execute
    val result = MeasurementResults.computeReach(vids)

    // Verify - Should count 10 distinct VIDs regardless of frequency
    assertThat(result).isEqualTo(10)
  }

  @Test
  fun `computeReach with Iterable handles empty input`() {
    // Setup
    val vids = emptyList<Long>()

    // Execute
    val result = MeasurementResults.computeReach(vids)

    // Verify
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `computeImpression with Flow returns correct impression count`() = runBlocking {
    // Setup - Create 10 VIDs with different frequencies
    val vids = flowOf(
      1L, 1L, 1L,           // VID 1 appears 3 times
      2L, 2L,               // VID 2 appears 2 times
      3L,                   // VID 3 appears 1 time
      4L, 4L, 4L, 4L,       // VID 4 appears 4 times
      5L, 5L, 5L, 5L, 5L,   // VID 5 appears 5 times
      6L, 6L, 6L, 6L, 6L, 6L, // VID 6 appears 6 times (will be capped at 5)
      7L,                   // VID 7 appears 1 time
      8L, 8L,               // VID 8 appears 2 times
      9L, 9L, 9L,           // VID 9 appears 3 times
      10L                   // VID 10 appears 1 time
    )
    val maxFrequency = 5

    // Execute
    val result = MeasurementResults.computeImpression(vids, maxFrequency)

    // Verify
    // Expected impressions:
    // VID 1: 3, VID 2: 2, VID 3: 1, VID 4: 4, VID 5: 5, 
    // VID 6: 5 (capped from 6), VID 7: 1, VID 8: 2, VID 9: 3, VID 10: 1
    // Total: 3 + 2 + 1 + 4 + 5 + 5 + 1 + 2 + 3 + 1 = 27
    assertThat(result).isEqualTo(27)
  }

  @Test
  fun `computeImpression with Flow caps frequencies at maxFrequency`() = runBlocking {
    // Setup
    val vids = flowOf(1L, 1L, 1L, 1L, 1L, 2L, 2L) // VID 1 appears 5 times, VID 2 appears 2 times
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeImpression(vids, maxFrequency)

    // Verify
    // VID 1 appears 5 times but capped at 3, VID 2 appears 2 times
    // Total impression: 3 + 2 = 5
    assertThat(result).isEqualTo(5)
  }

  @Test
  fun `computeImpression with Flow handles empty input`() = runBlocking {
    // Setup
    val vids = flowOf<Long>()
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeImpression(vids, maxFrequency)

    // Verify
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `computeImpression with Iterable returns correct impression count`() {
    // Setup - Create 10 VIDs with different frequencies
    val vids = listOf(
      1L, 1L, 1L,           // VID 1 appears 3 times
      2L, 2L,               // VID 2 appears 2 times
      3L,                   // VID 3 appears 1 time
      4L, 4L, 4L, 4L,       // VID 4 appears 4 times
      5L, 5L, 5L, 5L, 5L,   // VID 5 appears 5 times
      6L, 6L, 6L, 6L, 6L, 6L, // VID 6 appears 6 times (will be capped at 5)
      7L,                   // VID 7 appears 1 time
      8L, 8L,               // VID 8 appears 2 times
      9L, 9L, 9L,           // VID 9 appears 3 times
      10L                   // VID 10 appears 1 time
    )
    val maxFrequency = 5

    // Execute
    val result = MeasurementResults.computeImpression(vids, maxFrequency)

    // Verify
    // Expected impressions:
    // VID 1: 3, VID 2: 2, VID 3: 1, VID 4: 4, VID 5: 5, 
    // VID 6: 5 (capped from 6), VID 7: 1, VID 8: 2, VID 9: 3, VID 10: 1
    // Total: 3 + 2 + 1 + 4 + 5 + 5 + 1 + 2 + 3 + 1 = 27
    assertThat(result).isEqualTo(27)
  }

  @Test
  fun `computeImpression with Iterable caps frequencies at maxFrequency`() {
    // Setup
    val vids = listOf(1L, 1L, 1L, 1L, 1L, 2L, 2L) // VID 1 appears 5 times, VID 2 appears 2 times
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeImpression(vids, maxFrequency)

    // Verify
    // VID 1 appears 5 times but capped at 3, VID 2 appears 2 times
    // Total impression: 3 + 2 = 5
    assertThat(result).isEqualTo(5)
  }

  @Test
  fun `computeImpression with Iterable handles empty input`() {
    // Setup
    val vids = emptyList<Long>()
    val maxFrequency = 3

    // Execute
    val result = MeasurementResults.computeImpression(vids, maxFrequency)

    // Verify
    assertThat(result).isEqualTo(0)
  }

  // Note: We can't easily test the computePopulation method directly because it delegates to
  // PopulationRequisitionFulfiller.computePopulation, which is a static method in a Kotlin object.
  // In a real-world scenario, we would use a library like PowerMockito to mock static methods,
  // but for simplicity, we'll skip this test.
}
