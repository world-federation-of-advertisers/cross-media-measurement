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
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.duchy.utils.ReachAndFrequencyResult
import org.wfanet.measurement.duchy.utils.ReachResult
import org.wfanet.measurement.internal.duchy.DifferentialPrivacyParams
import org.wfanet.measurement.internal.duchy.differentialPrivacyParams

private const val MAX_FREQUENCY = 5
private const val FLOAT_COMPARISON_TOLERANCE = 1e-9

private val DEFAULT_DP_PARAMS = differentialPrivacyParams {
  epsilon = 1.0
  delta = 0.99
}

private const val FULL_SAMPLING_RATE = 1.0f

private val REACH_ONLY_PARAMS =
  TrusTeeReachParams(vidSamplingIntervalWidth = FULL_SAMPLING_RATE, dpParams = null)
private val REACH_AND_FREQUENCY_PARAMS =
  TrusTeeReachAndFrequencyParams(
    maximumFrequency = MAX_FREQUENCY,
    reachDpParams = null,
    frequencyDpParams = null,
    vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
  )

/**
 * Calculates a test tolerance for a noised value.
 *
 * The standard deviation of the Gaussian noise is `sqrt(2 * ln(1.25 / delta)) / epsilon`. We return
 * a tolerance of 6 standard deviations, which means a correct implementation should pass this check
 * with near-certainty.
 *
 * @param l2Sensitivity The L2 sensitivity of the query (1 for a simple count).
 */
private fun getNoiseTolerance(
  dpParams: DifferentialPrivacyParams,
  l2Sensitivity: Double = 1.0,
): Long {
  val stddev = sqrt(2 * ln(1.25 / dpParams.delta)) * l2Sensitivity / dpParams.epsilon
  return (6 * stddev).toLong()
}

@RunWith(JUnit4::class)
class TrusTeeProcessorImplTest {
  @Test
  fun `constructor throws for invalid maxFrequency of zero`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = 0,
        reachDpParams = null,
        frequencyDpParams = null,
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
        reachDpParams = null,
        frequencyDpParams = null,
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid")
  }

  @Test
  fun `constructor throws for invalid negative vid sampling interval width`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = -1,
        reachDpParams = null,
        frequencyDpParams = null,
        vidSamplingIntervalWidth = -0.2f,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid")
  }

  @Test
  fun `constructor throws for invalid vid sampling interval width that is larger than 1`() {
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = -1,
        reachDpParams = null,
        frequencyDpParams = null,
        vidSamplingIntervalWidth = 1.1f,
      )
    val exception = assertFailsWith<IllegalArgumentException> { TrusTeeProcessorImpl(params) }
    assertThat(exception.message).contains("Invalid")
  }

  @Test
  fun `addFrequencyVector initializes and aggregates a single vector`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val vector = intArrayOf(1, 1, 0, 2)

    processor.addFrequencyVector(vector)
    val result = processor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isEqualTo(3)
    val expectedDistribution =
      mapOf(0L to 0.25, 1L to 0.5, 2L to 0.25, 3L to 0.0, 4L to 0.0, 5L to 0.0)
    assertThat(result.frequency).isEqualTo(expectedDistribution)
  }

  @Test
  fun `addFrequencyVector throws for empty vector`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val exception =
      assertFailsWith<IllegalArgumentException> { processor.addFrequencyVector(intArrayOf()) }
    assertThat(exception.message).contains("Input frequency vector cannot be empty")
  }

  @Test
  fun `addFrequencyVector correctly sums multiple vectors`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val vector1 = intArrayOf(1, 0, 1, 3)
    val vector2 = intArrayOf(0, 1, 1, 1)
    val vector3 = intArrayOf(1, 1, 0, 0)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 2, 2, 4].
    assertThat(result.reach).isEqualTo(4)
    val expectedDistribution =
      mapOf(0L to 0.0, 1L to 0.0, 2L to 0.75, 3L to 0.0, 4L to 0.25, 5L to 0.0)
    assertThat(result.frequency).isEqualTo(expectedDistribution)
  }

  @Test
  fun `addFrequencyVector caps frequencies at the specified maxFrequency`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val vector1 = intArrayOf(3, 4, 5)
    val vector2 = intArrayOf(3, 0, 1)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    val result = processor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [5, 4, 5].
    assertThat(result.reach).isEqualTo(3)
    val expectedDistribution =
      mapOf(0L to 0.0, 1L to 0.0, 2L to 0.0, 3L to 0.0, 4L to (1.0 / 3.0), 5L to (2.0 / 3.0))

    assertThat(result.frequency.keys).isEqualTo(expectedDistribution.keys)
    for ((freq, expectedValue) in expectedDistribution) {
      assertThat(result.frequency[freq]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expectedValue)
    }
  }

  @Test
  fun `addFrequencyVector throws for mismatched vector sizes`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    processor.addFrequencyVector(intArrayOf(1, 2, 3))

    val exception =
      assertFailsWith<IllegalArgumentException> { processor.addFrequencyVector(intArrayOf(1, 2)) }
    assertThat(exception.message).contains("size")
  }

  @Test
  fun `computeResult throws IllegalStateException if no vectors are added`() {
    val processor = TrusTeeProcessorImpl(REACH_ONLY_PARAMS)
    assertFailsWith<IllegalStateException> { processor.computeResult() }
  }

  @Test
  fun `computeResult with all-zero frequency vectors returns zero results`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    processor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    processor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    processor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    val result = processor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isEqualTo(0)
    val expectedDistribution = (0L..MAX_FREQUENCY).associateWith { 0.0 }.toMutableMap()
    expectedDistribution[0L] = 1.0
    assertThat(result.frequency).isEqualTo(expectedDistribution)
  }

  @Test
  fun `computeResult for Reach computes correct raw reach without DP`() {
    val processor = TrusTeeProcessorImpl(REACH_ONLY_PARAMS)
    val vector1 = intArrayOf(1, 0, 1, 0, 1)
    val vector2 = intArrayOf(0, 1, 1, 0, 0)
    val vector3 = intArrayOf(1, 0, 0, 0, 1)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)

    // Aggregated frequencies: [1, 1, 1, 0, 1]
    val result = processor.computeResult() as ReachResult
    assertThat(result.reach).isEqualTo(4)
  }

  @Test
  fun `computeResult for Reach scales raw reach by sampling width`() {
    val samplingWidth = 0.5f
    val params = TrusTeeReachParams(vidSamplingIntervalWidth = samplingWidth, dpParams = null)
    val processor = TrusTeeProcessorImpl(params)
    val vector1 = intArrayOf(1, 0, 1, 0, 1, 0)
    val vector2 = intArrayOf(0, 1, 0, 0, 1, 1)
    val vector3 = intArrayOf(1, 0, 1, 0, 0, 0)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachResult

    // Aggregated vector (capped at 1): [1, 1, 1, 0, 1, 1]. Raw reach is 5.
    val rawReach = 5L
    val expectedScaledReach = (rawReach / samplingWidth).toLong() // 10
    assertThat(result.reach).isEqualTo(expectedScaledReach)
  }

  @Test
  fun `computeResult for Reach computes noised reach with DP`() {
    val samplingRate = 0.5f
    val params =
      TrusTeeReachParams(
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
        dpParams = DEFAULT_DP_PARAMS,
      )
    val processor = TrusTeeProcessorImpl(params)
    val vector1 = intArrayOf(1, 0, 1, 0, 1)
    val vector2 = intArrayOf(0, 1, 1, 0, 0)
    val vector3 = intArrayOf(1, 0, 0, 0, 1)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachResult

    // Aggregated frequencies: [1, 1, 1, 0, 1]
    val rawReach = 4L
    val tolerance = getNoiseTolerance(DEFAULT_DP_PARAMS)
    assertThat(result.reach).isAtMost(((rawReach + tolerance) / samplingRate).toLong())
    assertThat(result.reach).isAtLeast((max(0L, rawReach - tolerance) / samplingRate).toLong())
  }

  // region Test Cases for Reach and Frequency Computations
  @Test
  fun `computeResult for R&F computes correct raw results without DP`() {
    val processor = TrusTeeProcessorImpl(REACH_AND_FREQUENCY_PARAMS)
    val vector1 = intArrayOf(1, 2, 0, 1, 3)
    val vector2 = intArrayOf(0, 1, 1, 0, 1)
    val vector3 = intArrayOf(1, 0, 0, 1, 1)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 3, 1, 2, 5].
    // Histogram: [0, 1, 2, 1, 0, 1].
    assertThat(result.reach).isEqualTo(5)
    val expected =
      mapOf(
        0L to 0.0,
        1L to 1.0 / 5.0,
        2L to 2.0 / 5.0,
        3L to 1.0 / 5.0,
        4L to 0.0,
        5L to 1.0 / 5.0,
      )
    assertThat(result.frequency.keys).isEqualTo(expected.keys)
    for ((k, v) in result.frequency) {
      assertThat(v).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expected[k]!!)
    }
  }

  @Test
  fun `computeResult for R&F scales raw reach by sampling width without DP`() {
    val samplingWidth = 0.25f
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        vidSamplingIntervalWidth = samplingWidth,
        reachDpParams = null,
        frequencyDpParams = null,
      )
    val processor = TrusTeeProcessorImpl(params)
    val vector1 = intArrayOf(1, 2, 0, 1, 3)
    val vector2 = intArrayOf(1, 0, 2, 0, 1)
    val vector3 = intArrayOf(0, 1, 1, 2, 0)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 3, 3, 3, 4]. Raw reach is 5.
    val rawReach = 5L
    val expectedScaledReach = (rawReach / samplingWidth).toLong() // 20
    assertThat(result.reach).isEqualTo(expectedScaledReach)

    // Frequency should be unaffected by sampling width.
    // Raw histogram is [0, 0, 1, 3, 1, 0], total 5.
    val expectedFrequency = mapOf(0L to 0.0, 1L to 0.0, 2L to 0.2, 3L to 0.6, 4L to 0.2, 5L to 0.0)
    assertThat(result.frequency.keys).isEqualTo(expectedFrequency.keys)
    for ((k, v) in result.frequency) {
      assertThat(v).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expectedFrequency[k]!!)
    }
  }

  @Test
  fun `computeResult for R&F computes results with DP params`() {
    val samplingWidth = 0.25f
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        vidSamplingIntervalWidth = samplingWidth,
        reachDpParams = DEFAULT_DP_PARAMS,
        frequencyDpParams = DEFAULT_DP_PARAMS,
      )
    val processor = TrusTeeProcessorImpl(params)
    val vector1 = intArrayOf(1, 2, 0, 1, 3)
    val vector2 = intArrayOf(0, 1, 1, 0, 1)
    val vector3 = intArrayOf(1, 0, 0, 1, 1)

    processor.addFrequencyVector(vector1)
    processor.addFrequencyVector(vector2)
    processor.addFrequencyVector(vector3)
    val result = processor.computeResult() as ReachAndFrequencyResult

    // Aggregated vector: [2, 3, 1, 2, 5].
    // Histogram: [0, 1, 2, 1, 0, 1].
    val rawReach = 5L
    val rawHistogram = longArrayOf(0, 1, 2, 1, 0, 1)
    val totalUsers = rawHistogram.sum()

    val reachTolerance = getNoiseTolerance(DEFAULT_DP_PARAMS)
    assertThat(result.reach).isAtMost(((rawReach + reachTolerance) / samplingWidth).toLong())
    assertThat(result.reach)
      .isAtLeast((max(0L, rawReach - reachTolerance) / samplingWidth).toLong())

    assertThat(result.frequency.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
    assertThat(result.frequency.keys).containsExactlyElementsIn(0L..MAX_FREQUENCY)

    // Check that each frequency probability is within a plausible range.
    val binCountTolerance = getNoiseTolerance(DEFAULT_DP_PARAMS)
    val numBins = (MAX_FREQUENCY + 1).toDouble()
    val totalCountTolerance = getNoiseTolerance(DEFAULT_DP_PARAMS, l2Sensitivity = sqrt(numBins))

    val minTotalNoisedCount = max(1.0, (totalUsers - totalCountTolerance).toDouble())
    val maxTotalNoisedCount = (totalUsers + totalCountTolerance).toDouble()

    for (i in 0..MAX_FREQUENCY) {
      val rawCount = rawHistogram[i]
      val minBinNoisedCount = max(0.0, (rawCount - binCountTolerance).toDouble())
      val maxBinNoisedCount = (rawCount + binCountTolerance).toDouble()

      val minProbability = minBinNoisedCount / maxTotalNoisedCount
      val maxProbability = maxBinNoisedCount / minTotalNoisedCount

      assertThat(result.frequency[i.toLong()]).isAtLeast(minProbability)
      assertThat(result.frequency[i.toLong()]).isAtMost(maxProbability)
    }
  }

  @Test
  fun `computeResult with all-zero frequency vectors returns zero results with dp params`() {
    val dpParams = differentialPrivacyParams {
      epsilon = 100.0
      delta = 0.99
    }
    val params =
      TrusTeeReachAndFrequencyParams(
        maximumFrequency = MAX_FREQUENCY,
        vidSamplingIntervalWidth = FULL_SAMPLING_RATE,
        reachDpParams = dpParams,
        frequencyDpParams = dpParams,
      )
    val processor = TrusTeeProcessorImpl(params)
    processor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    processor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    processor.addFrequencyVector(intArrayOf(0, 0, 0, 0))
    val result = processor.computeResult() as ReachAndFrequencyResult

    assertThat(result.reach).isEqualTo(0)
    assertThat(result.frequency[0L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }
}
