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
  private val distribution = TruncatedLaplaceNoiseDistribution(SCALE, BOUND)

  @Test
  fun `inverseCdf at zero is the lower bound`() {
    assertThat(distribution.inverseCdf(0.0)).isWithin(1e-9).of(-BOUND)
  }

  @Test
  fun `inverseCdf at one half is zero by symmetry`() {
    assertThat(distribution.inverseCdf(0.5)).isWithin(1e-9).of(0.0)
  }

  @Test
  fun `inverseCdf near one stays within the upper bound`() {
    val draw = distribution.inverseCdf(0.999999999)
    assertThat(draw).isGreaterThan(0.0)
    assertThat(draw).isAtMost(BOUND)
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
      assertThat(draw).isAtLeast(-BOUND)
      assertThat(draw).isAtMost(BOUND)
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
  fun `rejects non-positive scale`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution(scale = 0.0, bound = BOUND)
    }
  }

  @Test
  fun `rejects non-positive bound`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution(scale = SCALE, bound = 0.0)
    }
  }

  @Test
  fun `inverseCdf rejects a uniform outside the unit interval`() {
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(1.0) }
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(-0.1) }
  }

  @Test
  fun `forDifferentialPrivacy sets the bound from epsilon, delta and sensitivity`() {
    // inverseCdf(0) equals -bound, so it reveals the calibrated bound. At epsilon 1, delta 1/1000,
    // sensitivity 1, bound = ln(1 + (e - 1) / (2 * delta)).
    val dp = TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(1.0, 1.0 / 1000, 1.0)
    assertThat(dp.inverseCdf(0.0)).isWithin(1e-9).of(-6.7570962295802515)
  }

  @Test
  fun `forDifferentialPrivacy scales scale and bound with sensitivity`() {
    // Scale and bound are both proportional to sensitivity, so doubling it doubles both. inverseCdf
    // is linear in scale, so every quantile doubles too.
    val unit = TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(1.0, 1.0 / 1000, 1.0)
    val doubled = TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(1.0, 1.0 / 1000, 2.0)
    assertThat(doubled.inverseCdf(0.0)).isWithin(1e-9).of(-13.514192459160503)
    assertThat(doubled.inverseCdf(0.3)).isWithin(1e-9).of(2.0 * unit.inverseCdf(0.3))
  }

  @Test
  fun `forDifferentialPrivacy rejects non-positive epsilon`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(0.0, 1.0 / 1000, 1.0)
    }
  }

  @Test
  fun `forDifferentialPrivacy rejects delta outside the open unit interval`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(1.0, 0.0, 1.0)
    }
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(1.0, 1.0, 1.0)
    }
  }

  @Test
  fun `forDifferentialPrivacy rejects non-positive sensitivity`() {
    assertFailsWith<IllegalArgumentException> {
      TruncatedLaplaceNoiseDistribution.forDifferentialPrivacy(1.0, 1.0 / 1000, 0.0)
    }
  }

  companion object {
    private const val SCALE = 1.0
    private const val BOUND = 8.0
  }
}
