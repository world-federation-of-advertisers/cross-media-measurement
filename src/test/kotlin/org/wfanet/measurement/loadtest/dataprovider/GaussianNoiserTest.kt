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
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec

private const val RANDOM_SEED: Long = 1

@RunWith(JUnit4::class)
class GaussianNoiserTest {
  @Test
  fun `Gaussian noiser with random seed returns expected samples`() {
    val random = Random(RANDOM_SEED)
    val gaussianNoiser =
      GaussianNoiser(MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams, random)
    val samples = List(5) { gaussianNoiser.sample() }
    val expectedSamples =
      listOf(
        10.240550327678935,
        -3.9883454237630307,
        -7.156064121340545,
        -4.0956149786203895,
        -7.333487792497553
      )

    assertThat(expectedSamples).isEqualTo(samples)
  }
  @Test
  fun `standard deviation from samples is close to the theoretical sigma`() {
    val random = Random(RANDOM_SEED)
    val gaussianNoiser =
      GaussianNoiser(MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams, random)
    val samples = List(1000) { gaussianNoiser.sample() }

    val sigma = calculateStandardDeviation(samples)
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
        }
    }
    private fun calculateStandardDeviation(nums: List<Double>): Double {
      val mean = nums.average()
      val standardDeviation = nums.fold(0.0) { acc, num -> acc + (num - mean).pow(2.0) }

      return sqrt(standardDeviation / nums.size)
    }
  }
}
