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

package org.wfanet.measurement.duchy.mill.trustee.processor

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams

@RunWith(JUnit4::class)
class TrusTeeProcessorImplTest {
  @Test
  fun `constructor throws for invalid maxFrequency of zero`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = 0,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid max frequency")
  }

  @Test
  fun `constructor throws for invalid negative maxFrequency`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = -1,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid max frequency")
  }

  @Test
  fun `constructor throws for invalid negative vid sampling interval width`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
        vidSamplingIntervalWidth = -0.2f,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid vid sampling interval width")
  }

  @Test
  fun `constructor throws for invalid vid sampling interval width that is larger than 1`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
        vidSamplingIntervalWidth = 1.1f,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid vid sampling interval width")
  }

  @Test
  fun `addFrequencyVector initializes and aggregates a single vector`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val vector = byteArrayOf(1, 1, 0, 2)

    processor.addFrequencyVector(vector)
    val result = processor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isGreaterThan(0)
    assertThat(result.frequency.size).isEqualTo(MAX_FREQUENCY)
  }

  @Test
  fun `addFrequencyVector throws for empty vector`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val exception =
      assertFailsWith<IllegalArgumentException> { processor.addFrequencyVector(byteArrayOf()) }
    assertThat(exception.message).contains("Input frequency vector cannot be empty")
  }

  @Test
  fun `addFrequencyVector correctly sums and caps multiple vectors`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val vector1 = byteArrayOf(1, 4, 1, 3)
    val vector2 = byteArrayOf(0, 1, 1, 1) // Sums: [1, 5, 2, 4]
    val vector3 = byteArrayOf(1, 1, 0, 3) // Sums: [2, 6, 2, 7], capped at 5 -> [2, 5, 2, 5]

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachAndFrequencyResult

    // The exact result is subject to noise. We just check for reasonable properties.
    assertThat(result.reach).isGreaterThan(0)
    assertThat(result.frequency.size).isEqualTo(MAX_FREQUENCY)
    assertThat(result.frequency.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }

  @Test
  fun `addFrequencyVector throws for mismatched vector sizes`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    processor.addFrequencyVector(byteArrayOf(1, 2, 3))

    val exception =
      assertFailsWith<IllegalArgumentException> { processor.addFrequencyVector(byteArrayOf(1, 2)) }
    assertThat(exception.message).contains("size")
  }

  @Test
  fun `computeResult throws IllegalStateException if no vectors are added`() {
    val processor = TrusTeeProcessorImpl(REACH_ONLY_PARAMS)
    assertFailsWith<IllegalStateException> { processor.computeResult() }
  }

  @Test
  fun `computeResult with all-zero frequency vectors returns near-zero results`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    processor.addFrequencyVector(byteArrayOf(0, 0, 0, 0))
    val result = processor.computeResult() as ReachAndFrequencyResult

    // With noise, reach might not be exactly 0, but should be small.
    // The frequency distribution should be all zeros if the noised total is zero.
    assertThat(result.reach).isAtMost(5)
    if (result.reach == 0L) {
      val expectedDistribution = (1L..MAX_FREQUENCY).associateWith { 0.0 }
      assertThat(result.frequency).isEqualTo(expectedDistribution)
    }
  }

  @Test
  fun `computeResult for Reach-Only returns correct result type`() {
    val params = TrusTeeReachParams(vidSamplingIntervalWidth = 0.5f, dpParams = DEFAULT_DP_PARAMS)
    val processor = TrusTeeProcessorImpl(params)
    processor.addFrequencyVector(byteArrayOf(1, 0, 1, 0, 1, 0))
    val result = processor.computeResult() as ReachResult

    assertThat(result).isInstanceOf(ReachResult::class.java)
    // Raw reach is 3. Scaled reach is 6. Noised result should be around 6.
    // A more precise check is in the calculator test.
    assertThat(result.reach).isGreaterThan(0)
  }

  @Test
  fun `computeResult for R&F returns correct result type and values`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        vidSamplingIntervalWidth = 0.25f,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
      )
    val processor = TrusTeeProcessorImpl(params)
    processor.addFrequencyVector(byteArrayOf(1, 2, 0, 1, 3))
    val result = processor.computeResult()

    assertThat(result).isInstanceOf(ReachAndFrequencyResult::class.java)
    result as ReachAndFrequencyResult

    // Raw reach is 4. Scaled reach is 16. Noised result should be around 16.
    assertThat(result.reach).isGreaterThan(0)
    // Frequency distribution should be for freqs 1-5 and sum to 1.0.
    assertThat(result.frequency.keys).containsExactlyElementsIn(1L..MAX_FREQUENCY)
    assertThat(result.frequency.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }

  @Test
  fun `computeResult for R&F caps noised reach at theoretical maximum`() {
    val samplingWidth = 0.5f
    // Low noise
    val dpParams = differentialPrivacyParams {
      epsilon = 100.0
      delta = 0.99
    }
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        vidSamplingIntervalWidth = samplingWidth,
        reachDpParams = dpParams,
        frequencyDpParams = dpParams,
      )
    val processor = TrusTeeProcessorImpl(params)

    val vector = ByteArray(200) { 1 } // 200 VIDs, all reached
    val vectorSize = vector.size

    processor.addFrequencyVector(vector)
    val result = processor.computeResult() as ReachAndFrequencyResult

    val maxPossibleScaledReach = (vectorSize / samplingWidth).toLong() // 200 / 0.5 = 400

    // The result should be at most the theoretical maximum.
    assertThat(result.reach).isAtMost(maxPossibleScaledReach)
  }

  companion object {
    private const val MAX_FREQUENCY = 5
    private const val FLOAT_COMPARISON_TOLERANCE = 1e-9

    private val DEFAULT_DP_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 0.99
    }

    private const val FULL_SAMPLING_RATE = 1.0f

    private val REACH_ONLY_PARAMS =
      TrusTeeReachParams(
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
        dpParams = DEFAULT_DP_PARAMS,
      )
    private val REACH_AND_FREQUENCY_PARAMS =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
      )
  }
}
