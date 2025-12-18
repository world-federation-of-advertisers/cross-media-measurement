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
class DirectImpressionResultBuilderTest {

  @Test
  fun `buildMeasurementResult returns non-noisy impression result when noise mechanism is set to NONE`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directImpressionResultBuilder =
        DirectImpressionResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          frequencyData = frequencyData,
          privacyParams = PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          maxPopulation = null,
          maxFrequencyFromSpec = MAX_FREQUENCY,
          kAnonymityParams = null,
          impressionMaxFrequencyPerUser = null,
          totalUncappedImpressions = 9999L, // Bogus value to verify it's not used
        )

      val result = directImpressionResultBuilder.buildMeasurementResult()

      // Verify the result has the expected structure
      // frequencyData: 90 users with freq 1, 10 users with freq 2 = 90*1 + 10*2 = 110
      assertThat(result.hasImpression()).isTrue()
      assertThat(result.impression.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
      assertThat(result.impression.hasDeterministicCount()).isTrue()
      assertThat(result.impression.value).isEqualTo(110)
    }

  @Test
  fun `buildMeasurementResult returns noisy impression result within acceptable range when noise mechanism is set to CONTINUOUS_GAUSSIAN`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directImpressionResultBuilder =
        DirectImpressionResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          frequencyData = frequencyData,
          privacyParams = PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
          maxPopulation = null,
          maxFrequencyFromSpec = MAX_FREQUENCY,
          kAnonymityParams = null,
          impressionMaxFrequencyPerUser = null,
          totalUncappedImpressions = 9999L, // Bogus value to verify it's not used
        )

      val result = directImpressionResultBuilder.buildMeasurementResult().impression.value
      val tolerance = calculateNoiseTolerance(PRIVACY_PARAMS, 1, MAX_FREQUENCY.toDouble())
      val rawImpressionCount = 110
      check(rawImpressionCount > tolerance) {
        "Test must be set up such that raw impression count $rawImpressionCount is greater than tolerance $tolerance"
      }
      assertThat(result).isAtLeast((rawImpressionCount - tolerance).coerceAtLeast(0))
      assertThat(result).isAtMost((rawImpressionCount + tolerance))
    }

  @Test
  fun `buildMeasurementResult uses totalUncappedImpressions when impressionMaxFrequencyPerUser is -1`() =
    runBlocking {
      // frequencyData would compute to 110 impressions (90 * 1 + 10 * 2 = 110)
      // but with impressionMaxFrequencyPerUser = -1, totalUncappedImpressions of 500 should be used
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directImpressionResultBuilder =
        DirectImpressionResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          frequencyData = frequencyData,
          privacyParams = PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          maxPopulation = null,
          maxFrequencyFromSpec = MAX_FREQUENCY,
          kAnonymityParams = null,
          impressionMaxFrequencyPerUser = -1,
          totalUncappedImpressions = 500L,
        )

      val result = directImpressionResultBuilder.buildMeasurementResult()

      // Verify totalUncappedImpressions is used instead of histogram computation
      assertThat(result.hasImpression()).isTrue()
      assertThat(result.impression.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
      assertThat(result.impression.value).isEqualTo(500)
    }

  @Test
  fun `buildMeasurementResult uses histogram when impressionMaxFrequencyPerUser is not -1`() =
    runBlocking {
      // frequencyData computes to 110 impressions (90 * 1 + 10 * 2 = 110)
      // Even with totalUncappedImpressions = 500, histogram should be used
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directImpressionResultBuilder =
        DirectImpressionResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          frequencyData = frequencyData,
          privacyParams = PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          maxPopulation = null,
          maxFrequencyFromSpec = MAX_FREQUENCY,
          kAnonymityParams = null,
          impressionMaxFrequencyPerUser = MAX_FREQUENCY,
          totalUncappedImpressions = 500L,
        )

      val result = directImpressionResultBuilder.buildMeasurementResult()

      // Verify histogram computation is used (not totalUncappedImpressions)
      assertThat(result.hasImpression()).isTrue()
      assertThat(result.impression.value).isEqualTo(110)
    }

  companion object {
    private val MAX_FREQUENCY = 2
    private val PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-9
    }

    private val SAMPLING_RATE = 1.0f

    private val NOISE_MECHANISM = NoiseMechanism.CONTINUOUS_GAUSSIAN

    private val DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      deterministicCount = ProtocolConfig.Direct.DeterministicCount.getDefaultInstance()
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
