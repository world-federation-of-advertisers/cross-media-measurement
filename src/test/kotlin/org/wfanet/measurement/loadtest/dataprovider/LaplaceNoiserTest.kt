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
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyLandscape

private const val RANDOM_SEED: Long = 1

@RunWith(JUnit4::class)
class LaplaceNoiserTest {
  @Test
  fun `calculate Laplace noised direct reach and frequency correctly`() {
    val reachValue = 500L
    val frequencyMap = mapOf(1L to 0.6, 2L to 0.2, 3L to 0.2)

    val random = Random(RANDOM_SEED)
    val laplaceNoiser = LaplaceNoiser(MEASUREMENT_SPEC.reachAndFrequency, random)
    val (noisedReachValue, noisedFrequencyMap) =
      laplaceNoiser.addReachAndFrequencyPublisherNoise(
        reachValue,
        frequencyMap,
      )

    val expectedNoisedReachValue = 500
    val expectedNoisedFrequencyMap =
      mapOf(1L to 0.5996034922861095, 2L to 0.1982431161708369, 3L to 0.19918536869714157)

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
            delta = 1.0
          }
          frequencyPrivacyParams = differentialPrivacyParams {
            epsilon = 1.0
            delta = 1.0
          }
        }
      vidSamplingInterval =
        MeasurementSpecKt.vidSamplingInterval {
          start = 0.0f
          width = PrivacyLandscape.PRIVACY_BUCKET_VID_SAMPLE_WIDTH
        }
    }
  }
}
