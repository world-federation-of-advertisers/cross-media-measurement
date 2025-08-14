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
import kotlin.math.absoluteValue
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
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
          maxFrequency = MAX_FREQUENCY,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          frequencyData = frequencyData,
          maxPopulation = null,
        )

      val result = directReachResultBuilder.buildMeasurementResult()

      // Verify the result has the expected structure
      assertThat(result.hasReach()).isTrue()
      assertThat(result.reach.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
      assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()
      assertThat(result.reach.value).isEqualTo(100)
    }

  @Test
  fun `buildMeasurementResult returns noisy reach-and-frequency result with respect to variance when noise mechanism is set to CONTINUOUS_GAUSSIAN`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val directReachResultBuilder =
        DirectReachResultBuilder(
          directProtocolConfig = DIRECT_PROTOCOL,
          maxFrequency = MAX_FREQUENCY,
          reachPrivacyParams = REACH_PRIVACY_PARAMS,
          samplingRate = SAMPLING_RATE,
          directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
          frequencyData = frequencyData,
          maxPopulation = null,
        )

      val result = directReachResultBuilder.buildMeasurementResult()

      // Verify the result has the expected structure
      assertThat(result.hasReach()).isTrue()
      assertThat(result.reach.noiseMechanism).isEqualTo(NoiseMechanism.CONTINUOUS_GAUSSIAN)
      assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()
    }

  @Test
  fun `buildMeasurementResult returns noisy reach-and-frequency result within acceptable range noise mechanism is set to CONTINUOUS_GAUSSIAN`() =
    runBlocking {
      val frequencyData = IntArray(100) { if (it < 90) 1 else 2 }

      val reachResults = mutableListOf<Long>()

      for (round in 1..100) {
        val directReachResultBuilder =
          DirectReachResultBuilder(
            directProtocolConfig = DIRECT_PROTOCOL,
            maxFrequency = MAX_FREQUENCY,
            reachPrivacyParams = REACH_PRIVACY_PARAMS,
            samplingRate = SAMPLING_RATE,
            directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
            frequencyData = frequencyData,
            maxPopulation = null,
          )

        val result = directReachResultBuilder.buildMeasurementResult()

        reachResults.add(result.reach.value)
      }

      val averageReach = reachResults.map { it }.average()

      // Test that average reach size is within acceptable range of +/- 5 when compared to actual
      // reach
      val reachDifference = (100 - averageReach).absoluteValue
      assertThat(reachDifference).isLessThan(5)
    }

  companion object {
    private val MAX_FREQUENCY = 10
    private val REACH_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    private val SAMPLING_RATE = 1.0f

    private val NOISE_MECHANISM = NoiseMechanism.CONTINUOUS_GAUSSIAN

    private val DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      deterministicCountDistinct =
        ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
    }
  }
}
