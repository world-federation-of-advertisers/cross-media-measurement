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
class DeterministicTruncatedLaplaceNoiseSamplerTest {
  private val distribution = TruncatedLaplaceNoiseDistribution(EPSILON, SENSITIVITY, BOUND)
  private val sampler = DeterministicTruncatedLaplaceNoiseSampler(distribution)

  private val fingerprint = "frequency-vector-fingerprint".toByteArray()
  private val label = "reach".toByteArray()

  @Test
  fun `same parts draw the same value`() {
    assertThat(sampler.sample(fingerprint, label)).isEqualTo(sampler.sample(fingerprint, label))
  }

  @Test
  fun `golden vector pins the draw for cross-build reproducibility`() {
    // Exact StrictMath output, so this pins bit-for-bit cross-JVM reproducibility (no tolerance).
    // Any change to the seed derivation or sampler must update it and is expected to be
    // scrutinized.
    assertThat(sampler.sample(fingerprint, label)).isEqualTo(0.5856028728045781)
  }

  @Test
  fun `draw stays within the truncation bound`() {
    for (i in 0 until 1000) {
      val draw = sampler.sample("fv-$i".toByteArray(), label)
      assertThat(draw).isAtLeast(-BOUND.toDouble())
      assertThat(draw).isAtMost(BOUND.toDouble())
    }
  }

  @Test
  fun `distinct output labels draw independently`() {
    assertThat(sampler.sample(fingerprint, "reach".toByteArray()))
      .isNotEqualTo(sampler.sample(fingerprint, "bucket-1".toByteArray()))
  }

  @Test
  fun `distinct fingerprints draw independently`() {
    assertThat(sampler.sample("fv-a".toByteArray(), label))
      .isNotEqualTo(sampler.sample("fv-b".toByteArray(), label))
  }

  @Test
  fun `higher sensitivity yields a larger-magnitude draw for the same parts`() {
    val narrow = DeterministicTruncatedLaplaceNoiseSampler(EPSILON, sensitivity = 1.0, BOUND)
    val wide = DeterministicTruncatedLaplaceNoiseSampler(EPSILON, sensitivity = 4.0, BOUND)
    assertThat(abs(wide.sample(fingerprint, label)))
      .isGreaterThan(abs(narrow.sample(fingerprint, label)))
  }

  @Test
  fun `draws are roughly centered`() {
    val mean = (0 until 20000).map { sampler.sample("fv-$it".toByteArray(), label) }.average()
    assertThat(mean).isWithin(0.1).of(0.0)
  }

  companion object {
    private const val EPSILON = 1.0
    private const val SENSITIVITY = 1.0
    private const val BOUND = 8
  }
}
