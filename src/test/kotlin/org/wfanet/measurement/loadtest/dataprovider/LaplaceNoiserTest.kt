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

private const val RANDOM_SEED: Long = 1

@RunWith(JUnit4::class)
class LaplaceNoiserTest {
  @Test
  fun `Laplace noiser with random seed returns expected samples`() {
    val random = Random(RANDOM_SEED)
    val laplaceNoiser = LaplaceNoiser(MEASUREMENT_SPEC.reachAndFrequency.reachPrivacyParams, random)
    val samples = List(5) { laplaceNoiser.sample() }
    val expectedSamples =
      listOf(
        0.6194439986492494,
        -0.198253856945236,
        -0.878441914581552,
        -0.40731565142921,
        2.741273309927767
      )

    assertThat(expectedSamples).isEqualTo(samples)
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
  }
}
