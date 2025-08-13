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
import kotlin.math.ln
import kotlin.math.min
import kotlin.math.sqrt
import kotlin.test.assertFailsWith
import org.junit.Test

class ImpressionComputationsTest {

  @Test
  fun `raw impression count calculation without noise`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        maxFrequency = null,
        dpParams = null,
        kAnonymityParams = null,
      )
    assertThat(result).isEqualTo(57L)
  }

  @Test
  fun `caps raw impression count with maximum frequency`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        maxFrequency = 4,
        dpParams = null,
        kAnonymityParams = null,
      )
    assertThat(result).isEqualTo(50L) // 2*5 + 4*3 + 4*7
  }

  @Test
  fun `impression count is scaled by vidSamplingIntervalWidth`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val scale = 0.5f
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = scale,
        maxFrequency = null,
        dpParams = null,
        kAnonymityParams = null,
      )
    assertThat(result).isEqualTo((57L / scale).toLong())
  }

  @Test
  fun `impression count with DP noise is within expected tolerance`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val maxFrequency = 4L
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        maxFrequency = maxFrequency,
        dpParams = DP_PARAMS,
        kAnonymityParams = null,
      )
    val rawImpressionCount =
      1 * 2 +
        min(maxFrequency, 2) * 4 +
        min(maxFrequency, 4) * 8 +
        min(maxFrequency, 5) * 10 +
        min(maxFrequency, 7) * 2
    val tolerance = calculateNoiseTolerance(DP_PARAMS, 1, maxFrequency.toDouble())
    check(rawImpressionCount > tolerance) {
      "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
    }
    assertThat(result).isAtLeast((rawImpressionCount - tolerance).coerceAtLeast(0))
    assertThat(result).isAtMost((rawImpressionCount + tolerance))
  }

  @Test
  fun `impression count with DP noise is within expected tolerance with smaller interval`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val maxFrequency = 4L
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 0.5f,
        maxFrequency = maxFrequency,
        dpParams = DP_PARAMS,
        kAnonymityParams = null,
      )
    val rawImpressionCount =
      1 * 2 +
        min(maxFrequency, 2) * 4 +
        min(maxFrequency, 4) * 8 +
        min(maxFrequency, 5) * 10 +
        min(maxFrequency, 7) * 2
    val tolerance = calculateNoiseTolerance(DP_PARAMS, 1, maxFrequency.toDouble())
    check(rawImpressionCount * 2 > tolerance) {
      "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
    }
    assertThat(result).isAtLeast((rawImpressionCount * 2 - tolerance).coerceAtLeast(0))
    assertThat(result).isAtMost((rawImpressionCount * 2 + tolerance))
  }

  @Test
  fun `throws error if maxFrequency is not set but dp params are set`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    assertFailsWith<IllegalStateException> {
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        maxFrequency = null,
        dpParams = DP_PARAMS,
        kAnonymityParams = null,
      )
    }
  }

  @Test
  fun `impression count with K Anonymity is zero for too few unique users`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val kAnonymityParams = KAnonymityParams(minUsers = 28, minImpressions = 50)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        dpParams = null,
        maxFrequency = null,
        kAnonymityParams = kAnonymityParams,
      )
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `impression count with K Anonymity is zero for too few impressions`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val kAnonymityParams = KAnonymityParams(minUsers = 28, minImpressions = 100)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        dpParams = null,
        maxFrequency = null,
        kAnonymityParams = kAnonymityParams,
      )
    assertThat(result).isEqualTo(0)
  }

  @Test
  fun `impression count with K Anonymity not changed for sufficient impressions + users`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val kAnonymityParams = KAnonymityParams(minUsers = 24, minImpressions = 50)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        dpParams = null,
        maxFrequency = null,
        kAnonymityParams = kAnonymityParams,
      )
    assertThat(result).isEqualTo(130)
  }

  @Test
  fun `scaled impression count with K Anonymity not changed`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val kAnonymityParams = KAnonymityParams(minUsers = 48, minImpressions = 100)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 0.5f,
        dpParams = null,
        maxFrequency = null,
        kAnonymityParams = kAnonymityParams,
      )
    assertThat(result).isEqualTo(260)
  }

  @Test
  fun `scaled impression count with K Anonymity is zero for too few unique users`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val kAnonymityParams = KAnonymityParams(minUsers = 56, minImpressions = 100)
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 0.5f,
        dpParams = null,
        maxFrequency = null,
        kAnonymityParams = kAnonymityParams,
      )
    assertThat(result).isEqualTo(0)
  }

  companion object {
    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 2.0, delta = 1e-5)

    private fun getL2Sensitivity(l0Sensitivity: Int, lInfSensitivity: Double): Double {
      return sqrt(l0Sensitivity.toDouble()) * lInfSensitivity
    }

    /**
     * Returns an interval (tolerance) of ±6 standard deviations for the DP noise added. This
     * follows the convention to allow for expected fluctuation in noisy outputs for tests.
     */
    fun calculateNoiseTolerance(
      differentialPrivacyParams: DifferentialPrivacyParams,
      l0Sensitivity: Int = 1,
      lInfSensitivity: Double,
    ): Int {
      // Based on DP with Gaussian noise,
      // stddev = sqrt(2 * ln(1.25/delta)) * l2Sensitivity / epsilon
      // Per Google.privacy.differentialprivacy.GaussianNoise docs
      val stddev =
        sqrt(2.0 * ln(1.25 / differentialPrivacyParams.delta)) *
          getL2Sensitivity(l0Sensitivity, lInfSensitivity) / differentialPrivacyParams.epsilon
      return (6 * stddev).toInt() + 1 // ±6 sigma and round-up
    }
  }
}
