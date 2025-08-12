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
import kotlin.math.sqrt
import kotlin.test.assertFailsWith
import org.junit.Test

class ImpressionComputationsTest {

  @Test
  fun `raw impression count calculation without noise`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val result =
      ImpressionComputations(l0Sensitivity = null, lInfiniteSensitivity = null)
        .computeImpressionCount(
          rawHistogram = histogram,
          vidSamplingIntervalWidth = 1.0f,
          dpParams = null,
        )
    assertThat(result).isEqualTo(57L)
  }

  @Test
  fun `impression count is scaled by vidSamplingIntervalWidth`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val scale = 0.5f
    val result =
      ImpressionComputations(l0Sensitivity = null, lInfiniteSensitivity = null)
        .computeImpressionCount(
          rawHistogram = histogram,
          vidSamplingIntervalWidth = scale,
          dpParams = null,
        )
    assertThat(result).isEqualTo((57L / scale).toLong())
  }

  @Test
  fun `impression count with DP noise is within expected tolerance`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    val result =
      ImpressionComputations(l0Sensitivity = 2, lInfiniteSensitivity = 2L)
        .computeImpressionCount(
          rawHistogram = histogram,
          vidSamplingIntervalWidth = 1.0f,
          dpParams = DP_PARAMS,
        )
    val rawImpressionCount = 1 * 2 + 2 * 4 + 4 * 8 + 7 * 10 + 7 * 2
    val tolerance = calculateNoiseTolerance(DP_PARAMS, 2.0, 2.0)
    check(rawImpressionCount > tolerance) {
      "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
    }
    assertThat(result).isAtLeast((rawImpressionCount - tolerance).coerceAtLeast(0).toLong())
    assertThat(result).isAtMost((rawImpressionCount + tolerance).toLong())
  }

  @Test
  fun `throws error is sensitivity is not set but dp params are set`() {
    val histogram = longArrayOf(2L, 4L, 0L, 8L, 0L, 0L, 10L, 0L, 2L) // 1*2 + 2*4 + 4*8 + 7*10 + 7*2
    assertFailsWith<IllegalStateException> {
      ImpressionComputations(l0Sensitivity = null, lInfiniteSensitivity = null)
        .computeImpressionCount(
          rawHistogram = histogram,
          vidSamplingIntervalWidth = 1.0f,
          dpParams = DP_PARAMS,
        )
    }
  }

  companion object {
    private val DP_PARAMS = DifferentialPrivacyParams(epsilon = 1.0, delta = 1e-5)

    /**
     * Returns an interval (tolerance) of ±6 standard deviations for the DP noise added. This
     * follows the convention to allow for expected fluctuation in noisy outputs for tests.
     */
    fun calculateNoiseTolerance(
      differentialPrivacyParams: DifferentialPrivacyParams,
      l0Sensitivity: Double = 1.0,
      linfSensitivity: Double = 1.0,
    ): Int {
      // Based on DP with Gaussian noise, stddev = sqrt(2 * ln(1.25/delta)) / epsilon
      // Per Google.privacy.differentialprivacy.GaussianNoise docs
      val stddev =
        sqrt(2.0 * ln(1.25 / differentialPrivacyParams.delta)) * linfSensitivity * l0Sensitivity /
          differentialPrivacyParams.epsilon
      return (6 * stddev).toInt() + 1 // ±6 sigma and round-up
    }
  }
}
