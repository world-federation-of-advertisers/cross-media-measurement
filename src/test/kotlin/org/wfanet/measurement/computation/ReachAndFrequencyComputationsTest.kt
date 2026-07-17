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

import com.google.common.truth.Truth.assertThat
import kotlin.collections.iterator
import kotlin.math.ln
import kotlin.math.max
import kotlin.math.min
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
        vidSamplingIntervalWidth = 1.0,
        vectorSize = 20,
        dpParams = null,
        resultMinimumThresholds = null,
      )
    assertThat(reach).isEqualTo(16)
  }

  @Test
  fun `computeReach scales raw reach by sampling width`() {
    val rawHistogram = longArrayOf(10, 5, 1) // Frequencies 1, 2, 3
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 0.5,
        vectorSize = 40,
        dpParams = null,
        resultMinimumThresholds = null,
      )
    assertThat(reach).isEqualTo(32)
  }

  @Test
  fun `computeReach scales raw reach by sampling width to non-zero with small-cell suppression`() {
    val rawHistogram = longArrayOf(10, 5, 1) // Frequencies 1, 2, 3
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 0.5,
        vectorSize = 40,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 20, minImpressions = 20),
      )
    assertThat(reach).isEqualTo(32)
  }

  @Test
  fun `computeReach scales raw reach by sampling width to zero with small-cell suppression for insufficient users`() {
    val rawHistogram = longArrayOf(10, 5, 1) // Frequencies 1, 2, 3
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 0.5,
        vectorSize = 40,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 35, minImpressions = 30),
      )
    assertThat(reach).isEqualTo(0)
  }

  @Test
  fun `computeReach scales raw reach by sampling width to zero with small-cell suppression for insufficient impressions`() {
    val rawHistogram = longArrayOf(10, 5, 1) // Frequencies 1, 2, 3
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 0.5,
        vectorSize = 40,
        dpParams = null,
        resultMinimumThresholds =
          ResultMinimumThresholds(minUsers = 30, minImpressions = 50, reachMaxFrequencyPerUser = 3),
      )
    assertThat(reach).isEqualTo(0)
  }

  @Test
  fun `computeReach with noise`() {
    val rawHistogram = longArrayOf(100, 50, 20) // Reach in sample = 170
    val tolerance = getNoiseTolerance(DP_PARAMS)
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 1.0,
        vectorSize = 200,
        dpParams = DP_PARAMS,
        resultMinimumThresholds = null,
      )
    assertThat(reach).isAtMost(min(200, 170 + tolerance))
    assertThat(reach).isAtLeast(max(0L, 170 - tolerance))
  }

  @Test
  fun `computeReach with noise and small-cell suppression`() {
    val rawHistogram = longArrayOf(100, 50, 20) // Reach in sample = 170
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 1.0,
        vectorSize = 200,
        dpParams = DP_PARAMS,
        resultMinimumThresholds =
          ResultMinimumThresholds(minUsers = 30, minImpressions = 50, reachMaxFrequencyPerUser = 3),
      )
    val tolerance = getNoiseTolerance(DP_PARAMS)
    assertThat(reach).isAtLeast(170 - tolerance)
    assertThat(reach).isAtMost(170 + tolerance)
  }

  @Test
  fun `computeReach with noise and small-cell suppression goes to zero`() {
    val rawHistogram = longArrayOf(100, 50, 20) // Reach in sample = 170
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 1.0,
        vectorSize = 200,
        dpParams = DP_PARAMS,
        resultMinimumThresholds =
          ResultMinimumThresholds(
            minUsers = 200,
            minImpressions = 200,
            reachMaxFrequencyPerUser = 3,
          ),
      )
    assertThat(reach).isEqualTo(0)
  }

  @Test
  fun `computeFrequencyDistribution calculates raw distribution`() {
    val rawHistogram = longArrayOf(10, 30, 60) // Frequencies 1, 2, 3
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
        resultMinimumThresholds = null,
        vidSamplingIntervalWidth = null,
      )
    val expected = mapOf(1L to 0.1, 2L to 0.3, 3L to 0.6)
    assertThat(distribution.keys).isEqualTo(expected.keys)
    for ((k, v) in distribution) {
      assertThat(v).isWithin(FLOAT_COMPARISON_TOLERANCE).of(expected[k]!!)
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
        resultMinimumThresholds = null,
        vidSamplingIntervalWidth = null,
      )
    val expected = mapOf(1L to 0.0, 2L to 0.0, 3L to 0.0)
    assertThat(distribution).isEqualTo(expected)
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
        resultMinimumThresholds = null,
        vidSamplingIntervalWidth = null,
      )

    assertThat(distribution.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)

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

      assertThat(distribution[i + 1L]).isAtLeast(minProbability)
      assertThat(distribution[i + 1L]).isAtMost(maxProbability)
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
          resultMinimumThresholds = null,
          vidSamplingIntervalWidth = null,
        )
      }
    assertThat(exception.message).contains("Invalid histogram size")
  }

  @Test
  fun `computeFrequencyDistribution zeroes only freq-1 bucket when it fails threshold`() {
    val rawHistogram = longArrayOf(10, 30, 70) // Frequencies 1, 2, 3
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 11, minImpressions = 5),
        vidSamplingIntervalWidth = 1.0,
      )
    // With fold-down: freq1 (10 users) fails minUsers=11, cannot fold lower.
    // freq2 (30) and freq3 (70) pass, so only freq1 is suppressed.
    val totalAfterFold = 30.0 + 70.0
    val expected = mapOf(1L to 0.0, 2L to 30.0 / totalAfterFold, 3L to 70.0 / totalAfterFold)
    assertThat(distribution).isEqualTo(expected)
  }

  @Test
  fun `computeFrequencyDistribution without noise folds all users to freq 1 when higher buckets fail`() {
    // histogram=[10,30,70], minUsers=110. Fold: freq3->freq2->freq1 (110>=110 passes)
    val rawHistogram = longArrayOf(10, 30, 70)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 110, minImpressions = 5),
        vidSamplingIntervalWidth = 1.0,
      )
    val expected = mapOf(1L to 1.0, 2L to 0.0, 3L to 0.0)
    assertThat(distribution).isEqualTo(expected)
  }

  @Test
  fun `computeFrequencyDistribution with noise and small-cell suppression`() {
    val rawHistogram = longArrayOf(10, 30, 70) // Frequencies 1, 2, 3
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = DP_PARAMS,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 11, minImpressions = 5),
        vidSamplingIntervalWidth = 1.0,
      )
    // With fold-down + noise, outcome is non-deterministic. Either all zeros or sums to 1.
    val sum = distribution.values.sum()
    assertThat(sum == 0.0 || Math.abs(sum - 1.0) < FLOAT_COMPARISON_TOLERANCE).isTrue()
  }

  @Test
  fun `computeFrequencyDistribution with noise folds all to freq 1`() {
    // After fold-down, freq2 and freq3 are always zero. freq1 may pass or fail with noise.
    val rawHistogram = longArrayOf(10, 30, 70)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = DP_PARAMS,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 110, minImpressions = 5),
        vidSamplingIntervalWidth = 1.0,
      )
    assertThat(distribution[2L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
    assertThat(distribution[3L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
  }

  @Test
  fun `computeReach with no noise suppresses when impressions below threshold but users above`() {
    // 10 users at frequency 1, 5 at frequency 2, 1 at frequency 3 = 16 users, 23 impressions
    val rawHistogram = longArrayOf(10, 5, 1)
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 1.0,
        vectorSize = 20,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 10, minImpressions = 50),
      )
    assertThat(reach).isEqualTo(0)
  }

  @Test
  fun `computeFrequencyDistribution suppresses bins where impression threshold not met`() {
    // Frequency 1: 5 users * 1 impression = 5 impressions
    // Frequency 2: 10 users * 2 impressions = 20 impressions
    // Frequency 3: 20 users * 3 impressions = 60 impressions
    // With minImpressions=15: freq 1 (5 < 15) suppressed, freq 2 and 3 pass.
    val rawHistogram = longArrayOf(5, 10, 20)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 1, minImpressions = 15),
        vidSamplingIntervalWidth = 1.0,
      )
    // freq1: 5*1=5 < 15 fails impressions, cannot fold lower. Only freq1 is suppressed.
    // freq2 (10) and freq3 (20) pass, so they remain.
    val totalAfterFold = 10.0 + 20.0
    val expected = mapOf(1L to 0.0, 2L to 10.0 / totalAfterFold, 3L to 20.0 / totalAfterFold)
    assertThat(distribution).isEqualTo(expected)
  }

  @Test
  fun `computeFrequencyDistribution folds down buckets that fail threshold`() {
    // freq1=2758, freq2=1925, freq3=336, freq4=311, freq5=0
    // With minUsers=500: freq5 folds to freq4 (311), freq4 folds to freq3 (647)
    // After fold: freq1=2758, freq2=1925, freq3=647, freq4=0, freq5=0
    val rawHistogram = longArrayOf(2758, 1925, 336, 311, 0)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 5,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 500, minImpressions = 10),
        vidSamplingIntervalWidth = 1.0,
      )
    val totalAfterFold = 2758 + 1925 + 647
    assertThat(distribution[1L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(2758.0 / totalAfterFold)
    assertThat(distribution[2L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1925.0 / totalAfterFold)
    assertThat(distribution[3L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(647.0 / totalAfterFold)
    assertThat(distribution[4L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
    assertThat(distribution[5L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
    // Total users preserved after fold-down
    assertThat(distribution.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }

  @Test
  fun `computeFrequencyDistribution fold-down with fractional vidSamplingIntervalWidth scales thresholds correctly`() {
    // freq1=10, freq2=30, freq3=70
    // At vidSamplingIntervalWidth=0.5, estimated counts are doubled (count / 0.5):
    //   freq3: 70/0.5=140 >= 50 -> passes
    //   freq2: 30/0.5=60 >= 50 -> passes
    //   freq1: 10/0.5=20 < 50 -> fails, zeroed (no lower bucket)
    // At vidSamplingIntervalWidth=1.0, freq2 (30 < 50) would also fail and fold down.
    val rawHistogram = longArrayOf(10, 30, 70)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 3,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 50, minImpressions = 5),
        vidSamplingIntervalWidth = 0.5,
      )
    val totalAfterFold = 30.0 + 70.0
    assertThat(distribution[1L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
    assertThat(distribution[2L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(30.0 / totalAfterFold)
    assertThat(distribution[3L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(70.0 / totalAfterFold)
    assertThat(distribution.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }

  @Test
  fun `computeFrequencyDistribution fold-down preserves total user count`() {
    val rawHistogram = longArrayOf(100, 80, 40, 20, 5)
    val totalUsers = rawHistogram.sum()
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 5,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 30, minImpressions = 10),
        vidSamplingIntervalWidth = 1.0,
      )
    // Distribution sums to 1.0 means all users are accounted for
    assertThat(distribution.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
    // Verify folded buckets contribute to lower ones (freq5=5 and freq4=20 fold to freq3)
    assertThat(distribution[5L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
    assertThat(distribution[4L]).isWithin(FLOAT_COMPARISON_TOLERANCE).of(0.0)
    assertThat(distribution[3L]).isGreaterThan(0.0)
  }

  @Test
  fun `computeFrequencyDistribution zeros everything when 1+ bucket fails after fold-down`() {
    // All users are at high frequencies — after fold-down to freq1, it still fails min_users
    val rawHistogram = longArrayOf(3, 5, 2, 1, 1)
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 5,
        dpParams = null,
        resultMinimumThresholds = ResultMinimumThresholds(minUsers = 50, minImpressions = 10),
        vidSamplingIntervalWidth = 1.0,
      )
    // All buckets should be zero because even after folding everything to 1+,
    // total users (12) < minUsers (50)
    assertThat(distribution.values.all { it == 0.0 }).isTrue()
  }

  @Test
  fun `computeReach returns zero when 1+ bucket fails threshold`() {
    // Same data as above — total reach is 12, below min_users=50
    val rawHistogram = longArrayOf(3, 5, 2, 1, 1)
    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = 1.0,
        vectorSize = 20,
        dpParams = null,
        resultMinimumThresholds =
          ResultMinimumThresholds(minUsers = 50, minImpressions = 10, reachMaxFrequencyPerUser = 5),
      )
    assertThat(reach).isEqualTo(0)
  }

  @Test
  fun `computeReach and computeFrequencyDistribution are consistent when 1+ bucket fails`() {
    val rawHistogram = longArrayOf(3, 5, 2, 1, 1)
    val thresholds =
      ResultMinimumThresholds(minUsers = 50, minImpressions = 10, reachMaxFrequencyPerUser = 5)
    val vidSamplingIntervalWidth = 1.0

    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = vidSamplingIntervalWidth,
        vectorSize = 20,
        dpParams = null,
        resultMinimumThresholds = thresholds,
      )
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 5,
        dpParams = null,
        resultMinimumThresholds = thresholds,
        vidSamplingIntervalWidth = vidSamplingIntervalWidth,
      )

    assertThat(reach).isEqualTo(0)
    assertThat(distribution.values.all { it == 0.0 }).isTrue()
  }

  @Test
  fun `computeReach and computeFrequencyDistribution are consistent when thresholds pass`() {
    val rawHistogram = longArrayOf(2758, 1925, 336, 311, 0)
    val thresholds =
      ResultMinimumThresholds(minUsers = 500, minImpressions = 10, reachMaxFrequencyPerUser = 5)
    val vidSamplingIntervalWidth = 1.0

    val reach =
      ReachAndFrequencyComputations.computeReach(
        rawHistogram,
        vidSamplingIntervalWidth = vidSamplingIntervalWidth,
        vectorSize = 10000,
        dpParams = null,
        resultMinimumThresholds = thresholds,
      )
    val distribution =
      ReachAndFrequencyComputations.computeFrequencyDistribution(
        rawHistogram,
        maxFrequency = 5,
        dpParams = null,
        resultMinimumThresholds = thresholds,
        vidSamplingIntervalWidth = vidSamplingIntervalWidth,
      )

    assertThat(reach).isGreaterThan(0)
    assertThat(distribution.values.sum()).isWithin(FLOAT_COMPARISON_TOLERANCE).of(1.0)
  }

  companion object {
    private const val MAX_FREQUENCY = 5
    private const val FLOAT_COMPARISON_TOLERANCE = 1e-9

    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 1.0, delta = 0.99)

    /**
     * Calculates a test tolerance for a noised value.
     *
     * The standard deviation of the Gaussian noise is `sqrt(2 * ln(1.25 / delta)) * l2Sensitivity /
     * epsilon`. We return a tolerance of 6 standard deviations, which means a correct
     * implementation should pass this check with near-certainty.
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
