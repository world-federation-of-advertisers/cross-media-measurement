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
import kotlin.math.abs
import kotlin.math.pow
import kotlin.math.sqrt
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
  fun `calculate Gaussian noised direct reach and frequency correctly with seeded random number generator and epsilon equals to 1`() {
    val random = Random(RANDOM_SEED)
    val gaussianNoiser = GaussianNoiser(MEASUREMENT_SPEC.reachAndFrequency, random)

    val (noisedReachValue, noisedFrequencyMap) =
      gaussianNoiser.addReachAndFrequencyPublisherNoise(
        reachValue,
        frequencyMap,
      )

    val expectedNoisedReachValue = 510
    val expectedNoisedFrequencyMap =
      mapOf(1L to 0.592023309152474, 2L to 0.1856878717573189, 3L to 0.19180877004275923)

    assertThat(noisedReachValue).isEqualTo(expectedNoisedReachValue)
    noisedFrequencyMap.forEach { (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedNoisedFrequencyMap[frequency])
    }
  }
  @Test
  fun `calculate Gaussian noised direct reach and frequency correctly with seeded random number generator and epsilon equals to 1E-4`() {
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
      gaussianNoiser.addReachAndFrequencyPublisherNoise(
        reachValue,
        frequencyMap,
      )

    val expectedNoisedReachValue = 78766
    // Frequency values can be negative due to large variance
    val expectedNoisedFrequencyMap =
      mapOf(1L to -60.364636044457335, 2L to -109.18542135016607, 3L to -62.40432585957863)

    assertThat(noisedReachValue).isEqualTo(expectedNoisedReachValue)
    noisedFrequencyMap.forEach { (frequency, percentage) ->
      assertThat(percentage).isEqualTo(expectedNoisedFrequencyMap[frequency])
    }
  }
  @Test
  fun `standard deviation from noisedReachValues is close to the theoretical sigma`() {
    val random = Random(RANDOM_SEED)
    val gaussianNoiser = GaussianNoiser(MEASUREMENT_SPEC.reachAndFrequency, random)
    val noisedReachValues =
      List(1000) {
        gaussianNoiser
          .addReachAndFrequencyPublisherNoise(
            reachValue,
            frequencyMap,
          )
          .reach
      }

    val sigma = calculateStandardDeviation(noisedReachValues)
    // Sigma value with pre-set epsilon and delta
    val expectedSigma = 6.557822067460045
    val diffRatio = abs((expectedSigma - sigma) / expectedSigma)

    assertThat(diffRatio).isLessThan(0.2)
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
    private const val reachValue = 500L
    private val frequencyMap = mapOf(1L to 0.6, 2L to 0.2, 3L to 0.2)
    private fun calculateStandardDeviation(nums: List<Long>): Double {
      val mean = nums.average()
      val standardDeviation = nums.fold(0.0) { acc, num -> acc + (num - mean).pow(2.0) }

      return sqrt(standardDeviation / nums.size)
    }
  }
}
