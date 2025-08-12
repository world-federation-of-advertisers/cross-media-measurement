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

package org.wfanet.measurement.computation

import com.google.common.truth.Truth
import kotlin.collections.iterator
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.sqrt
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ReachAndFrequencyComputationsTest {
  @Test
  fun `computeReach calculates raw reach correctly`() {
    val rawHistogram = longArrayOf(10, 5, 1) // Frequencies 1, 2, 3
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vectorSize = 20,
        vidSamplingIntervalWidth = 1.0f,
        dpParams = null,
      )
    Truth.assertThat(reach).isEqualTo(16)
  }

  @Test
  fun `computeReach scales raw reach by sampling width`() {
    val rawHistogram = longArrayOf(10, 5, 1) // Frequencies 1, 2, 3
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vectorSize = 40,
        vidSamplingIntervalWidth = 0.5f,
        dpParams = null,
      )
    Truth.assertThat(reach).isEqualTo(32)
  }

  @Test
  fun `computeReach with noise`() {
    val rawHistogram = longArrayOf(100, 50, 20) // Reach in sample = 170
    val tolerance = getNoiseTolerance(DP_PARAMS)
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vectorSize = 200,
        vidSamplingIntervalWidth = 1.0f,
        dpParams = DP_PARAMS,
      )
    Truth.assertThat(reach).isAtMost(170 + tolerance)
    Truth.assertThat(reach).isAtLeast(max(0L, 170 - tolerance))
  }

  @Test
  fun `computeFrequencyDistribution calculates raw distribution`() {
    val rawHistogram = longArrayOf(10, 30, 60) // Frequencies 1, 2, 3
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
      )
    val expected = mapOf(1L to 0.1, 2L to 0.3, 3L to 0.6)
    Truth.assertThat(distribution.keys).isEqualTo(expected.keys)
    for ((k, v) in distribution) {
      Truth.assertThat(v).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expected[k]!!)
    }
  }

  @Test
  fun `computeFrequencyDistribution with zero-only histogram returns map of zeros`() {
    val rawHistogram = longArrayOf(0, 0, 0)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
      )
    val expected = mapOf(1L to 0.0, 2L to 0.0, 3L to 0.0)
    Truth.assertThat(distribution).isEqualTo(expected)
  }

  @Test
  fun `computeFrequencyDistribution with noise`() {
    val rawHistogram = longArrayOf(100, 200, 300, 400, 0) // Frequencies 1-5
    val totalReached = rawHistogram.sum()
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 5,
        dpParams = DP_PARAMS,
      )

    Truth.assertThat(distribution.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)

    val binCountTolerance = getNoiseTolerance(DP_PARAMS)
    val totalCountTolerance = getNoiseTolerance(DP_PARAMS, l2Sensitivity = sqrt(5.0))
    val minTotalNoisedCount = max(1.0, (totalReached - totalCountTolerance).toDouble())
    val maxTotalNoisedCount = (totalReached + totalCountTolerance).toDouble()

    for (i in 0 until MAX_FREQUENCY) {
      val rawCount = rawHistogram[i]
      val minBinNoisedCount = max(0.0, (rawCount - binCountTolerance).toDouble())
      val maxBinNoisedCount = (rawCount + binCountTolerance).toDouble()
      val minProbability = minBinNoisedCount / maxTotalNoisedCount
      val maxProbability = maxBinNoisedCount / minTotalNoisedCount

      Truth.assertThat(distribution[i + 1L]).isAtLeast(minProbability)
      Truth.assertThat(distribution[i + 1L]).isAtMost(maxProbability)
    }
  }

  @Test
  fun `computeFrequencyDistribution throws for mismatched histogram size`() {
    val rawHistogram = longArrayOf(1, 1)
    val exception =
      assertFailsWith<IllegalArgumentException> {
        ReachAndFrequencyComputations.computeFrequencyDistribution(
          rawHistogram,
          maxFrequency = 3,
          dpParams = null,
        )
      }
    Truth.assertThat(exception.message).contains("Invalid histogram size")
  }

  companion object {
    private const val MAX_FREQUENCY = 5
    private const val FLOAT_COMPARISON_TOLERANCE = 1e-9

    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 1.0, delta = 0.99)

    /**
     * Calculates a test tolerance for a noised value.
     *
     * The standard deviation of the Gaussian noise is `sqrt(2 * ln(1.25 / delta)) / epsilon`. We
     * return a tolerance of 6 standard deviations, which means a correct implementation should pass
     * this check with near-certainty.
     */
    private fun getNoiseTolerance(
      dpParams: DifferentialPrivacyParams,
      l2Sensitivity: Double = 1.0,
    ): Long {
      val stddev = sqrt(2 * ln(1.25 / dpParams.delta)) * l2Sensitivity / dpParams.epsilon
      return (6 * stddev).toLong()
    }
  }
}
