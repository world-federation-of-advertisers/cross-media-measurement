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
import java.nio.ByteBuffer
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DeterministicTruncatedLaplaceNoiseTest {
  @Test
  fun `fingerprint is deterministic`() {
    val vector = intArrayOf(0, 1, 2, 3, 0, 5)
    assertThat(DeterministicTruncatedLaplaceNoise.fingerprint(vector, CONTRIBUTION_COUNT))
      .isEqualTo(
        DeterministicTruncatedLaplaceNoise.fingerprint(vector.copyOf(), CONTRIBUTION_COUNT)
      )
  }

  @Test
  fun `fingerprint changes with vector contents`() {
    assertThat(
        DeterministicTruncatedLaplaceNoise.fingerprint(intArrayOf(1, 2, 3), CONTRIBUTION_COUNT)
      )
      .isNotEqualTo(
        DeterministicTruncatedLaplaceNoise.fingerprint(intArrayOf(1, 2, 4), CONTRIBUTION_COUNT)
      )
  }

  @Test
  fun `fingerprint changes with contribution count`() {
    // Same aggregate vector, different contribution count: reseeds. This is what closes the
    // fully-contained-contribution differencing case, where the aggregate is byte-identical.
    val vector = intArrayOf(0, 1, 2, 3, 0, 5)
    assertThat(DeterministicTruncatedLaplaceNoise.fingerprint(vector, 2))
      .isNotEqualTo(DeterministicTruncatedLaplaceNoise.fingerprint(vector, 3))
  }

  @Test
  fun `noise is deterministic in the fingerprint and params`() {
    val sampled = ReachAndFrequency(15, longArrayOf(10, 4, 1))
    val fingerprint = DeterministicTruncatedLaplaceNoise.fingerprint(COMBINED, CONTRIBUTION_COUNT)

    val first =
      DeterministicTruncatedLaplaceNoise.noise(
        sampled,
        fingerprint,
        REACH_EPSILON,
        FREQUENCY_EPSILON,
        SENSITIVITY,
        BOUND,
      )
    val second =
      DeterministicTruncatedLaplaceNoise.noise(
        sampled,
        fingerprint,
        REACH_EPSILON,
        FREQUENCY_EPSILON,
        SENSITIVITY,
        BOUND,
      )

    assertThat(first.sampledReach).isEqualTo(second.sampledReach)
    assertThat(first.frequencyHistogram).isEqualTo(second.frequencyHistogram)
  }

  @Test
  fun `noise adds one reach draw and one draw per frequency bucket`() {
    val rawHistogram = longArrayOf(10, 4, 1)
    val sampledReach = 15L
    val sampled = ReachAndFrequency(sampledReach, rawHistogram)
    val fingerprint = DeterministicTruncatedLaplaceNoise.fingerprint(COMBINED, CONTRIBUTION_COUNT)

    val result =
      DeterministicTruncatedLaplaceNoise.noise(
        sampled,
        fingerprint,
        REACH_EPSILON,
        FREQUENCY_EPSILON,
        SENSITIVITY,
        BOUND,
      )

    // Reconstruct the expected draws from the same sampler: reach uses label 0 with the reach
    // epsilon; each bucket b uses label b with the frequency epsilon.
    val reachSampler = DeterministicTruncatedLaplaceNoiseSampler(REACH_EPSILON, SENSITIVITY, BOUND)
    val frequencySampler =
      DeterministicTruncatedLaplaceNoiseSampler(FREQUENCY_EPSILON, SENSITIVITY, BOUND)
    val expectedReach =
      (sampledReach + reachSampler.sampleRounded(fingerprint, label(0))).coerceAtLeast(0L)
    val expectedHistogram =
      LongArray(rawHistogram.size) { index ->
        (rawHistogram[index] + frequencySampler.sampleRounded(fingerprint, label(index + 1)))
          .coerceAtLeast(0L)
      }

    assertThat(result.sampledReach).isEqualTo(expectedReach)
    assertThat(result.frequencyHistogram).isEqualTo(expectedHistogram)
  }

  @Test
  fun `noise clamps counts to non-negative`() {
    // Zero counts with noise that can be negative must never produce a negative result.
    val sampled = ReachAndFrequency(0, longArrayOf(0, 0, 0))
    val fingerprint = DeterministicTruncatedLaplaceNoise.fingerprint(COMBINED, CONTRIBUTION_COUNT)

    val result =
      DeterministicTruncatedLaplaceNoise.noise(
        sampled,
        fingerprint,
        REACH_EPSILON,
        FREQUENCY_EPSILON,
        SENSITIVITY,
        BOUND,
      )

    assertThat(result.sampledReach).isAtLeast(0L)
    for (count in result.frequencyHistogram) {
      assertThat(count).isAtLeast(0L)
    }
  }

  companion object {
    private const val REACH_EPSILON = 1.0
    private const val FREQUENCY_EPSILON = 2.0
    private const val SENSITIVITY = 1.0
    private const val BOUND = 8
    private const val CONTRIBUTION_COUNT = 3
    private val COMBINED = intArrayOf(0, 1, 2, 1, 3, 0, 2)

    private fun label(value: Int): ByteArray =
      ByteBuffer.allocate(Int.SIZE_BYTES).putInt(value).array()
  }
}
