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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DeterministicTruncatedLaplaceParamsTest {
  @Test
  fun `variance matches the truncated Laplace formula`() {
    assertThat(DeterministicTruncatedLaplaceParams.variance(scale = 1.0, bound = 1.0))
      .isWithin(TOLERANCE)
      .of(0.25406987939202064)
    assertThat(DeterministicTruncatedLaplaceParams.variance(scale = 2.0, bound = 10.0))
      .isWithin(TOLERANCE)
      .of(7.050288313117408)
  }

  @Test
  fun `variance tends to the untruncated variance for a wide bound`() {
    assertThat(DeterministicTruncatedLaplaceParams.variance(scale = 1.0, bound = 50.0))
      .isWithin(TOLERANCE)
      .of(2.0)
  }

  @Test
  fun `variance throws for a non-positive scale or bound`() {
    assertFailsWith<IllegalArgumentException> {
      DeterministicTruncatedLaplaceParams.variance(scale = 0.0, bound = 1.0)
    }
    assertFailsWith<IllegalArgumentException> {
      DeterministicTruncatedLaplaceParams.variance(scale = 1.0, bound = 0.0)
    }
  }

  companion object {
    private const val TOLERANCE = 1e-12
  }
}
