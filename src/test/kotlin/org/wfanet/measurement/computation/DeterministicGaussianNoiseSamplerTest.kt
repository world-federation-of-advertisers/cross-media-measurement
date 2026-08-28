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
import kotlin.math.abs
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DeterministicGaussianNoiseSamplerTest {
  private val uniformSampler = DeterministicUniformSampler()
  private val sampler = DeterministicGaussianNoiseSampler(uniformSampler = uniformSampler)

  private val fingerprint = "frequency-vector-fingerprint".toByteArray()

  @Test
  fun `same parts draw the same value`() {
    assertThat(sampler.sample(fingerprint, BAR_0)).isEqualTo(sampler.sample(fingerprint, BAR_0))
  }

  @Test
  fun `a fresh sampler draws the same value`() {
    // No hidden state carries between instances: the draw is a pure function of the parts.
    assertThat(DeterministicGaussianNoiseSampler().sample(fingerprint, BAR_0))
      .isEqualTo(sampler.sample(fingerprint, BAR_0))
  }

  @Test
  fun `different labels draw different values`() {
    assertThat(sampler.sample(fingerprint, BAR_0)).isNotEqualTo(sampler.sample(fingerprint, BAR_1))
  }

  @Test
  fun `different fingerprints draw different values`() {
    assertThat(sampler.sample("other-fingerprint".toByteArray(), BAR_0))
      .isNotEqualTo(sampler.sample(fingerprint, BAR_0))
  }

  @Test
  fun `golden vector pins the seed derivation`() {
    // The uniform is this repository's own construction, so it is pinned exactly. Any change to the
    // digest, the length prefixing or the bit extraction must update it and is expected to be
    // scrutinized: it changes every draw the deterministic path takes.
    assertThat(uniformSampler.sample(fingerprint, BAR_0)).isEqualTo(0.7572690749247485)
  }

  @Test
  fun `golden vector pins the draw`() {
    // Tolerance rather than exact bits, because the quantile comes from the pinned commons-math
    // inverse error function rather than from this repository. Tight enough to fail on a change of
    // approximation.
    assertThat(sampler.sample(fingerprint, BAR_0)).isWithin(TOLERANCE).of(0.6975449004043838)
    assertThat(sampler.sample(fingerprint, BAR_1)).isWithin(TOLERANCE).of(-0.03172334082792348)
  }

  @Test
  fun `draws over many labels look standard normal`() {
    val draws = (0 until SAMPLE_COUNT).map { sampler.sample(fingerprint, "bar-$it".toByteArray()) }

    val mean = draws.average()
    val variance = draws.sumOf { (it - mean) * (it - mean) } / (SAMPLE_COUNT - 1)
    assertThat(abs(mean)).isLessThan(0.05)
    assertThat(variance).isWithin(0.1).of(1.0)
  }

  companion object {
    private val BAR_0 = "bar-0".toByteArray()
    private val BAR_1 = "bar-1".toByteArray()
    private const val SAMPLE_COUNT = 10000
    private const val TOLERANCE = 1.0e-9
  }
}
