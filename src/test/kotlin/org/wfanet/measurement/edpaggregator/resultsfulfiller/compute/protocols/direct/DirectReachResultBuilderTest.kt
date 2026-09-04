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
import org.wfanet.measurement.computation.ResultMinimumThresholds
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
          resultMinimumThresholds = null,
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
          resultMinimumThresholds = null,
        )

      val result = directReachResultBuilder.buildMeasurementResult()
      val tolerance = calculateNoiseTolerance(REACH_PRIVACY_PARAMS, 1, 1.0)
      val rawReach = 100
      check(rawReach > tolerance) {
        "Test must be set up such that raw reach $rawReach is greater than tolerance $tolerance"
      }
      assertThat(result.reach.value).isAtLeast((rawReach - tolerance).coerceAtLeast(0))
      assertThat(result.reach.value).isAtMost(rawReach + tolerance)
      assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()
    }

  @Test
  fun `buildMeasurementResult is reproducible when noise mechanism is DETERMINISTIC_TRUNCATED_LAPLACE`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      fun build() =
        DirectReachResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE,
          frequencyData = frequencyData,
          maxPopulation = null,
          resultMinimumThresholds = null,
        )

      val first = build().buildMeasurementResult()
      val second = build().buildMeasurementResult()

      assertThat(first.reach.noiseMechanism)
        .isEqualTo(NoiseMechanism.DETERMINISTIC_TRUNCATED_LAPLACE)
      assertThat(second.reach.value).isEqualTo(first.reach.value)
      assertThat(first.reach.value).isWithin(TRUNCATION_BOUND.toLong()).of(100L)
    }

  @Test
  fun `buildMeasurementResult reports variance when reach is thresholded`() = runBlocking {
    val thresholds = ResultMinimumThresholds(minUsers = 100, minImpressions = 1)
    val result =
      DirectReachResultBuilder(
          directProtocolConfig = CUSTOM_DIRECT_PROTOCOL,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          frequencyData = IntArray(99) { 1 },
          maxPopulation = null,
          resultMinimumThresholds = thresholds,
        )
        .buildMeasurementResult()

    assertThat(result.reach.value).isEqualTo(0)
    assertThat(result.reach.hasCustomDirectMethodology()).isTrue()
    assertThat(result.reach.customDirectMethodology.variance.scalar).isEqualTo(10000.0)
  }

  @Test
  fun `buildMeasurementResult includes noise variance when reach is thresholded`() = runBlocking {
    val threshold = 1000
    val result =
      DirectReachResultBuilder(
          directProtocolConfig = CUSTOM_DIRECT_PROTOCOL,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
          frequencyData = IntArray(100) { 1 },
          maxPopulation = null,
          resultMinimumThresholds =
            ResultMinimumThresholds(minUsers = threshold, minImpressions = 1),
        )
        .buildMeasurementResult()

    assertThat(result.reach.value).isEqualTo(0)
    assertThat(result.reach.customDirectMethodology.variance.scalar)
      .isGreaterThan(threshold.toDouble() * threshold)
  }

  @Test
  fun `buildMeasurementResult does not report variance for true zero reach`() = runBlocking {
    val result =
      DirectReachResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          frequencyData = IntArray(99),
          maxPopulation = null,
          resultMinimumThresholds = ResultMinimumThresholds(minUsers = 100, minImpressions = 1),
        )
        .buildMeasurementResult()

    assertThat(result.reach.value).isEqualTo(0)
    assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()
  }

  companion object {
    private val MAX_FREQUENCY = 10
    private val REACH_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-9
    }

    private val SAMPLING_RATE = 1.0f

    /** ceil of the compiled unit-sensitivity bound, 6.7571. */
    private const val TRUNCATION_BOUND = 7

    private val NOISE_MECHANISM = NoiseMechanism.CONTINUOUS_GAUSSIAN

    private val CUSTOM_DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
    }

    private val DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      customDirectMethodology = ProtocolConfig.Direct.CustomDirectMethodology.getDefaultInstance()
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
