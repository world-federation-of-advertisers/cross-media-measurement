/*
 * Copyright 2025 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.resultsfulfiller.compute.protocols.direct

import com.google.common.truth.Truth.assertThat
import kotlin.math.ln
import kotlin.math.sqrt
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.DifferentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DirectReachResultBuilderTest {

  @Test
  fun `buildMeasurementResult returns non-noisy reach result when noise mechanism is set to NONE`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directReachResultBuilder =
        DirectReachResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          frequencyData = frequencyData,
          maxPopulation = null,
          kAnonymityParams = null,
        )

      val result = directReachResultBuilder.buildMeasurementResult()

      // Verify the result has the expected structure
      assertThat(result.hasReach()).isTrue()
      assertThat(result.reach.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
      assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()
      assertThat(result.reach.value).isEqualTo(100)
    }

  @Test
  fun `buildMeasurementResult returns noisy reach-and-frequency result within acceptable range noise mechanism is set to CONTINUOUS_GAUSSIAN`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directReachResultBuilder =
        DirectReachResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
          frequencyData = frequencyData,
          maxPopulation = null,
          kAnonymityParams = null,
        )

      val result = directReachResultBuilder.buildMeasurementResult().reach.value
      val tolerance = calculateNoiseTolerance(REACH_PRIVACY_PARAMS, 1, 1.0)
      val rawImpressionCount = 110
      check(rawImpressionCount > tolerance) {
        "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
      }
      assertThat(result).isAtLeast((rawImpressionCount - tolerance).coerceAtLeast(0))
      assertThat(result).isAtMost((rawImpressionCount + tolerance))
    }

  companion object {
    private val MAX_FREQUENCY = 10
    private val REACH_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-9
    }

    private val SAMPLING_RATE = 1.0f

    private val NOISE_MECHANISM = NoiseMechanism.CONTINUOUS_GAUSSIAN

    private val DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      deterministicCountDistinct =
        ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
    }

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
