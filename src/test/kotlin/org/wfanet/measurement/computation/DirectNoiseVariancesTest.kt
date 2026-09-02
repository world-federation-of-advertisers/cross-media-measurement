// Copyright 2026 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.computation

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DirectNoiseVariancesTest {
  @Test
  fun `continuous Laplace variance scales with squared sensitivity`() {
    val unitVariance = DirectNoiseVariances.continuousLaplace(epsilon = 1.0, sensitivity = 1.0)

    assertThat(DirectNoiseVariances.continuousLaplace(epsilon = 1.0, sensitivity = 3.0))
      .isEqualTo(9.0 * unitVariance)
  }

  @Test
  fun `continuous Gaussian variance scales with squared sensitivity`() {
    val unitVariance =
      DirectNoiseVariances.continuousGaussian(epsilon = 1.0, delta = 1E-9, sensitivity = 1.0)

    assertThat(
        DirectNoiseVariances.continuousGaussian(epsilon = 1.0, delta = 1E-9, sensitivity = 3.0)
      )
      .isWithin(1E-12)
      .of(9.0 * unitVariance)
  }

  @Test
  fun `deterministic truncated Laplace variance scales with squared sensitivity`() {
    val unitVariance = DirectNoiseVariances.deterministicTruncatedLaplace(sensitivity = 1.0)

    assertThat(DirectNoiseVariances.deterministicTruncatedLaplace(sensitivity = 3.0))
      .isWithin(1E-12)
      .of(9.0 * unitVariance)
  }
}
