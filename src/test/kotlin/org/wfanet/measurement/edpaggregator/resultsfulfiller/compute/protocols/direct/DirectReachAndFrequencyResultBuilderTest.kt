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
import java.security.SecureRandom
import kotlinx.coroutines.flow.flow
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DirectReachAndFrequencyResultBuilderTest {

  @Test
  fun `buildMeasurementResult returns non-noisy result`() = runBlocking {
    val sampledVids = flow {
        for (i in 1..100) {
        emit(i.toLong())
        }
    }

    val directReachAndFrequencyResultBuilder = DirectReachAndFrequencyResultBuilder(
      directProtocolConfig = DIRECT_PROTOCOL,
      sampledVids = sampledVids,
      maxFrequency = MAX_FREQUENCY,
      reachPrivacyParams = REACH_PRIVACY_PARAMS,
      frequencyPrivacyParams = FREQUENCY_PRIVACY_PARAMS,
      samplingRate = SAMPLING_RATE,
      directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
    )

    val result = directReachAndFrequencyResultBuilder.buildMeasurementResult()

    // Verify the result has the expected structure
    assertThat(result.hasReach()).isTrue()
    assertThat(result.reach.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
    assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()

    assertThat(result.hasFrequency()).isTrue()
    assertThat(result.frequency.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
    assertThat(result.frequency.hasDeterministicDistribution()).isTrue()
    assertThat(result.frequency.relativeFrequencyDistributionMap).isNotEmpty()
  }

  @Test
  fun `buildNoisyMeasurementResult returns noisy result`() = runBlocking {
    val sampledVids = flow {
        for (i in 1..100) {
        emit(i.toLong())
        }
    }

    val directReachAndFrequencyResultBuilder = DirectReachAndFrequencyResultBuilder(
      directProtocolConfig = DIRECT_PROTOCOL,
      sampledVids = sampledVids,
      maxFrequency = MAX_FREQUENCY,
      reachPrivacyParams = REACH_PRIVACY_PARAMS,
      frequencyPrivacyParams = FREQUENCY_PRIVACY_PARAMS,
      samplingRate = SAMPLING_RATE,
      directNoiseMechanism = DirectNoiseMechanism.CONTINUOUS_GAUSSIAN,
      random = SecureRandom()
    )

    val result = directReachAndFrequencyResultBuilder.buildNoisyMeasurementResult()

    // Verify the result has the expected structure
    assertThat(result.hasReach()).isTrue()
    assertThat(result.reach.noiseMechanism).isEqualTo(NoiseMechanism.CONTINUOUS_GAUSSIAN)
    assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()

    assertThat(result.hasFrequency()).isTrue()
    assertThat(result.frequency.noiseMechanism).isEqualTo(NoiseMechanism.CONTINUOUS_GAUSSIAN)
    assertThat(result.frequency.hasDeterministicDistribution()).isTrue()
    assertThat(result.frequency.relativeFrequencyDistributionMap).isNotEmpty()
  }

  companion object {
    private val MAX_FREQUENCY = 10
    private val REACH_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val FREQUENCY_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    private val SAMPLING_RATE = 1.0f

    private val NOISE_MECHANISM = NoiseMechanism.CONTINUOUS_GAUSSIAN

    private val DIRECT_PROTOCOL = direct {
      noiseMechanisms += NOISE_MECHANISM
      deterministicCountDistinct =
        ProtocolConfig.Direct.DeterministicCountDistinct.getDefaultInstance()
      deterministicDistribution =
        ProtocolConfig.Direct.DeterministicDistribution.getDefaultInstance()
    }
  }
}
