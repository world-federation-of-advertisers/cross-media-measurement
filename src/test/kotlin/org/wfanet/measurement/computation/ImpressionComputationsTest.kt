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
import org.junit.Test

class ImpressionComputationsTest {

  @Test
  fun `raw impression count calculation without noise`() {
    val histogram = longArrayOf(0L, 5L, 0L, 3L, 7L, 0L) // 2*5 + 4*3 + 5*7
    val result =
      ImpressionComputations.computeImpressionCount(
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
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = scale,
        dpParams = null,
      )
    assertThat(result).isEqualTo((57L / scale).toLong())
  }

  @Test
  fun `impression count with DP noise is within expected tolerance`() {
    val histogram = longArrayOf(1L, 2L, 0L, 4L, 0L, 0L, 5L, 0L, 1L) // 1*1 + 2*2 + 4*4 + 5*7 + 7*1
    val result =
      ImpressionComputations.computeImpressionCount(
        rawHistogram = histogram,
        vidSamplingIntervalWidth = 1.0f,
        dpParams = DP_PARAMS,
      )
    val rawImpressionCount = 1 * 1 + 2 * 2 + 4 * 4 + 5 * 7 + 7 * 1
    val tolerance = calculateNoiseTolerance(DP_PARAMS)
    assertThat(result).isAtLeast((rawImpressionCount - tolerance).coerceAtLeast(0).toLong())
    assertThat(result).isAtMost((rawImpressionCount + tolerance).toLong())
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
