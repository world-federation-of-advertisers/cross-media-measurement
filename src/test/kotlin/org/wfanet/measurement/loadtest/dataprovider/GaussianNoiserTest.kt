// Copyright 2023 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import java.util.Random
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt
import org.wfanet.measurement.api.v2alpha.copy
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape

private const val RANDOM_SEED: Long = 1

@RunWith(JUnit4::class)
class GaussianNoiserTest {
  @Test
  fun `calculate Gaussian noised direct reach and frequency correctly with epsilon equals to 1`() {
    val random = Random(RANDOM_SEED)
    val gaussianNoiser = GaussianNoiser(MEASUREMENT_SPEC.reachAndFrequency, random)

    val (noisedReachValue, noisedFrequencyMap) =
      gaussianNoiser.addPublisherNoise(
        reachValue,
        frequencyMap,
      )

    val expectedNoisedReachValue = 512
    val expectedNoisedFrequencyMap =
      mapOf(1L to 0.590035536760728, 2L to 0.18212132343148613, 3L to 0.18976753501501636)

    assertThat(noisedReachValue).isEqualTo(expectedNoisedReachValue)
    noisedFrequencyMap.forEach { (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedNoisedFrequencyMap[frequency])
    }
  }
  @Test
  fun `calculate Gaussian noised direct reach and frequency correctly with epsilon equals to 1E-4`() {
    val measurementSpec =
      MEASUREMENT_SPEC.copy {
        reachAndFrequency =
          reachAndFrequency.copy {
            reachPrivacyParams = reachPrivacyParams.copy { epsilon = 1E-4 }
            frequencyPrivacyParams = frequencyPrivacyParams.copy { epsilon = 1E-4 }
          }
      }

    val random = Random(RANDOM_SEED)
    val gaussianNoiser = GaussianNoiser(measurementSpec.reachAndFrequency, random)

    val (noisedReachValue, noisedFrequencyMap) =
      gaussianNoiser.addPublisherNoise(
        reachValue,
        frequencyMap,
      )

    val expectedNoisedReachValue = 52897
    val expectedNoisedFrequencyMap =
      mapOf(1L to -40.21444386138276, 2L to -73.03106359061003, 3L to -41.71217907726343)

    assertThat(noisedReachValue).isEqualTo(expectedNoisedReachValue)
    noisedFrequencyMap.forEach { (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedNoisedFrequencyMap[frequency])
    }
  }
  companion object {
    private val MEASUREMENT_SPEC = measurementSpec {
      reachAndFrequency =
        MeasurementSpecKt.reachAndFrequency {
          reachPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1E-12
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1E-12
          }
        }
      vidSamplingInterval =
        MeasurementSpecKt.vidSamplingInterval {
          start = 0.0f
          width = PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        }
    }
    private val reachValue = 500L
    private val frequencyMap = mapOf(1L to 0.6, 2L to 0.2, 3L to 0.2)
  }
}
