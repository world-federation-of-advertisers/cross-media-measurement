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
  private val uniformSampler = DeterministicUniformSampler()
  private val sampler = DeterministicTruncatedLaplaceNoiseSampler(distribution, uniformSampler)

  private val fingerprint = "frequency-vector-fingerprint".toByteArray()
  private val label = "reach".toByteArray()

  @Test
  fun `same parts draw the same value`() {
    assertThat(sampler.sampleRounded(fingerprint, label))
      .isEqualTo(sampler.sampleRounded(fingerprint, label))
  }

  @Test
  fun `golden vector pins the continuous draw for cross-build reproducibility`() {
    // Pin the pre-rounding draw, not the rounded output: rounding collapses a bit-level drift into
    // the same integer, so only the exact StrictMath double catches it. This is the same
    // computation sampleRounded performs before rint. Any change to the seed derivation or
    // distribution must update it and is expected to be scrutinized.
    assertThat(distribution.inverseCdf(uniformSampler.sample(fingerprint, label)))
      .isEqualTo(0.5856028728045781)
  }

  @Test
  fun `golden vector pins the rounded draw`() {
    // 0.5856... rounds to 1. Guards the rint + toLong step and the round-half-to-even convention.
    assertThat(sampler.sampleRounded(fingerprint, label)).isEqualTo(1L)
  }

  @Test
  fun `draw stays within the truncation bound`() {
    for (i in 0 until 1000) {
      val draw = sampler.sampleRounded("fv-$i".toByteArray(), label)
      assertThat(draw).isAtLeast(-BOUND.toLong())
      assertThat(draw).isAtMost(BOUND.toLong())
    }
  }

  @Test
  fun `distinct output labels draw independently`() {
    // Compare the continuous draws: the rounded outputs can collide by chance on the small
    // integer support, but the underlying draws are independent.
    assertThat(distribution.inverseCdf(uniformSampler.sample(fingerprint, "reach".toByteArray())))
      .isNotEqualTo(
        distribution.inverseCdf(uniformSampler.sample(fingerprint, "bucket-1".toByteArray()))
      )
  }

  @Test
  fun `distinct fingerprints draw independently`() {
    assertThat(distribution.inverseCdf(uniformSampler.sample("fv-a".toByteArray(), label)))
      .isNotEqualTo(distribution.inverseCdf(uniformSampler.sample("fv-b".toByteArray(), label)))
  }

  @Test
  fun `higher sensitivity yields a larger-magnitude draw for the same parts`() {
    val narrow = TruncatedLaplaceNoiseDistribution(EPSILON, sensitivity = 1.0, BOUND)
    val wide = TruncatedLaplaceNoiseDistribution(EPSILON, sensitivity = 4.0, BOUND)
    val u = uniformSampler.sample(fingerprint, label)
    assertThat(abs(wide.inverseCdf(u))).isGreaterThan(abs(narrow.inverseCdf(u)))
  }

  @Test
  fun `draws are roughly centered`() {
    val mean =
      (0 until 20000).map { sampler.sampleRounded("fv-$it".toByteArray(), label) }.average()
    assertThat(mean).isWithin(0.1).of(0.0)
  }

  companion object {
    private const val EPSILON = 1.0
    private const val SENSITIVITY = 1.0
    private const val BOUND = 8
  }
}
