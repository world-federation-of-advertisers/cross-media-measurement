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
class TruncatedLaplaceNoiseDistributionTest {
  private val distribution = TruncatedLaplaceNoiseDistribution(EPSILON, SENSITIVITY, BOUND)

  @Test
  fun `inverseCdf at zero is the lower bound`() {
    assertThat(distribution.inverseCdf(0.0)).isWithin(1e-9).of(-BOUND.toDouble())
  }

  @Test
  fun `inverseCdf at one half is zero by symmetry`() {
    assertThat(distribution.inverseCdf(0.5)).isWithin(1e-9).of(0.0)
  }

  @Test
  fun `inverseCdf near one stays within the upper bound`() {
    val draw = distribution.inverseCdf(0.999999999)
    assertThat(draw).isGreaterThan(0.0)
    assertThat(draw).isAtMost(BOUND.toDouble())
  }

  @Test
  fun `inverseCdf is monotonically increasing`() {
    var previous = distribution.inverseCdf(0.0)
    var u = 0.01
    while (u < 1.0) {
      val current = distribution.inverseCdf(u)
      assertThat(current).isGreaterThan(previous)
      previous = current
      u += 0.01
    }
  }

  @Test
  fun `inverseCdf stays within the truncation bound`() {
    var u = 0.0
    while (u < 1.0) {
      val draw = distribution.inverseCdf(u)
      assertThat(draw).isAtLeast(-BOUND.toDouble())
      assertThat(draw).isAtMost(BOUND.toDouble())
      u += 0.001
    }
  }

  @Test
  fun `inverseCdf golden points pin the interior`() {
    // Reference values reimplemented from the same inverse-CDF formula in Python (not by calling
    // this code), so a transcription or composition error here fails. Matched to 1e-9 rather than
    // exactly because Python's libm and the code's StrictMath can differ by ~1 ulp on log/exp.
    assertThat(distribution.inverseCdf(0.1)).isWithin(1e-9).of(-1.6080969613993354)
    assertThat(distribution.inverseCdf(0.25)).isWithin(1e-9).of(-0.6928117741870494)
    assertThat(distribution.inverseCdf(0.75)).isWithin(1e-9).of(0.6928117741870496)
    assertThat(distribution.inverseCdf(0.9)).isWithin(1e-9).of(1.6080969613993368)
    assertThat(distribution.inverseCdf(0.999)).isWithin(1e-9).of(6.059832598415747)
  }

  @Test
  fun `rejects non-positive epsilon`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution(epsilon = 0.0, SENSITIVITY, BOUND)
    }
  }

  @Test
  fun `rejects non-positive sensitivity`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution(EPSILON, sensitivity = 0.0, BOUND)
    }
  }

  @Test
  fun `rejects non-positive truncation bound`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution(EPSILON, SENSITIVITY, truncationBound = 0)
    }
  }

  @Test
  fun `inverseCdf rejects a uniform outside the unit interval`() {
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(1.0) }
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(-0.1) }
  }

  companion object {
    private const val EPSILON = 1.0
    private const val SENSITIVITY = 1.0
    private const val BOUND = 8
  }
}
