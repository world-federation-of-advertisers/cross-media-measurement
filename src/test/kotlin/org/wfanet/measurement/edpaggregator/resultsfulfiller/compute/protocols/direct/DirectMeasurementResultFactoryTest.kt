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
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.ProtocolConfig
import org.wfanet.measurement.api.v2alpha.ProtocolConfig.NoiseMechanism
import org.wfanet.measurement.api.v2alpha.ProtocolConfigKt.direct
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.eventdataprovider.noiser.DirectNoiseMechanism

@RunWith(JUnit4::class)
class DirectMeasurementResultFactoryTest {

  @Test
  fun `buildMeasurementResult returns reach and frequency result for REACH_AND_FREQUENCY measurement type`() =
    runBlocking {
      // Setup
      val distinctVids = 100
      val sampledVids = flow {
        for (i in 1..distinctVids) {
          emit(i.toLong())
          // Duplicating the VID for every 10th entry to simulate frequency of 2 for 10 users
          if (i % 10 == 0) {
            emit(i.toLong())
          }
        }
      }

      val measurementSpec = measurementSpec {
        reachAndFrequency =
          MeasurementSpecKt.reachAndFrequency {
            reachPrivacyParams = REACH_PRIVACY_PARAMS
            frequencyPrivacyParams = FREQUENCY_PRIVACY_PARAMS
            maximumFrequency = MAX_FREQUENCY
          }
        vidSamplingInterval =
          MeasurementSpecKt.vidSamplingInterval {
            start = 0.0f
            width = SAMPLING_RATE
          }
      }

      // Execute
      val result =
        DirectMeasurementResultFactory.buildMeasurementResult(
          directProtocolConfig = DIRECT_PROTOCOL,
          directNoiseMechanism = DirectNoiseMechanism.NONE,
          measurementSpec = measurementSpec,
          sampledVids = sampledVids,
          random = SecureRandom(),
        )

      // Verify
      assertThat(result.hasReach()).isTrue()
      assertThat(result.reach.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
      assertThat(result.reach.hasDeterministicCountDistinct()).isTrue()
      assertThat(result.reach.value).isEqualTo(distinctVids)

      assertThat(result.hasFrequency()).isTrue()
      assertThat(result.frequency.noiseMechanism).isEqualTo(NoiseMechanism.NONE)
      assertThat(result.frequency.hasDeterministicDistribution()).isTrue()
      assertThat(result.frequency.relativeFrequencyDistributionMap).isNotEmpty()
      // Since every 10th VID was duplicated during the creation of sampledVids, 90% of users saw ad
      // once and 10% saw it twice
      assertThat(result.frequency.relativeFrequencyDistributionMap[1]).isEqualTo(0.9)
      assertThat(result.frequency.relativeFrequencyDistributionMap[2]).isEqualTo(0.1)
    }

  companion object {
    private const val MAX_FREQUENCY = 10
    private val REACH_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }
    private val FREQUENCY_PRIVACY_PARAMS = differentialPrivacyParams {
      epsilon = 1.0
      delta = 1E-12
    }

    private const val SAMPLING_RATE = 1.0f

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
