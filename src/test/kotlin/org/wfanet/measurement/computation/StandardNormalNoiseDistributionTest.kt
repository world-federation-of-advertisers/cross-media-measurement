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
class StandardNormalNoiseDistributionTest {
  private val distribution = StandardNormalNoiseDistribution()

  @Test
  fun `median maps to zero`() {
    assertThat(distribution.inverseCdf(0.5)).isEqualTo(0.0)
  }

  @Test
  fun `known quantiles match the standard normal`() {
    // Pins the mapping against the distribution it claims to be, so a change of approximation that
    // moved the values would fail here. The tolerance covers last-bit differences between
    // implementations of the inverse error function, not a change of algorithm.
    assertThat(distribution.inverseCdf(0.975)).isWithin(TOLERANCE).of(1.9599639845400536)
    assertThat(distribution.inverseCdf(0.025)).isWithin(TOLERANCE).of(-1.9599639845400538)
    assertThat(distribution.inverseCdf(0.8413447460685429)).isWithin(TOLERANCE).of(1.0)
    assertThat(distribution.inverseCdf(0.15865525393145707)).isWithin(TOLERANCE).of(-1.0)
  }

  @Test
  fun `draw is symmetric about the median`() {
    for (u in listOf(0.6, 0.75, 0.9, 0.99, 0.999)) {
      assertThat(distribution.inverseCdf(u))
        .isWithin(TOLERANCE)
        .of(-distribution.inverseCdf(1.0 - u))
    }
  }

  @Test
  fun `draw increases with the uniform`() {
    var previous = distribution.inverseCdf(0.0)
    for (step in 1 until 1000) {
      val draw = distribution.inverseCdf(step / 1000.0)
      assertThat(draw).isGreaterThan(previous)
      previous = draw
    }
  }

  @Test
  fun `zero maps to a finite draw`() {
    // The uniform is in [0, 1), so it reaches 0 and the quantile there would otherwise diverge.
    val draw = distribution.inverseCdf(0.0)
    assertThat(draw).isFinite()
    assertThat(draw).isLessThan(-8.0)
  }

  @Test
  fun `uniform outside its range is rejected`() {
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(1.0) }
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(-0.1) }
    assertFailsWith<IllegalArgumentException> { distribution.inverseCdf(Double.NaN) }
  }

  companion object {
    private const val TOLERANCE = 1.0e-9
  }
}
