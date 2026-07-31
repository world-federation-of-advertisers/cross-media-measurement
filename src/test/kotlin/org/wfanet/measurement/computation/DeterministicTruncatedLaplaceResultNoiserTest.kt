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
class DeterministicTruncatedLaplaceResultNoiserTest {
  @Test
  fun `draws are deterministic in the seed and params`() {
    assertThat(noiser().noiseReach(15)).isEqualTo(noiser().noiseReach(15))
    assertThat(noiser().noiseFrequencyBucket(0, 10)).isEqualTo(noiser().noiseFrequencyBucket(0, 10))
  }

  @Test
  fun `reach and bucket draws match golden`() {
    // Goldens computed outside this codebase from the documented construction: SHA-256 over the
    // length-prefixed parts, top 53 bits as the uniform, then inverseCdf and round-half-to-even.
    // The draws are +1, -2, -1, 0, so this fails against a noiser that returns zero.
    val noiser = noiser()

    assertThat(noiser.noiseReach(15)).isEqualTo(16L)
    assertThat(noiser.noiseFrequencyBucket(0, 10)).isEqualTo(8L)
    assertThat(noiser.noiseFrequencyBucket(1, 4)).isEqualTo(3L)
    assertThat(noiser.noiseFrequencyBucket(2, 1)).isEqualTo(1L)
  }

  @Test
  fun `impression count is capped and drawn on its own label`() {
    // Capped sum over [10, 4, 1] at 3 per user is 1*10 + 2*4 + 3*1 = 21; the draw on the impression
    // label at sensitivity 3 is -4. Distinct from the bucket labels, so it is not the weighted sum
    // of the bucket draws.
    assertThat(noiser().impressionCountForThreshold(longArrayOf(10, 4, 1), MAX_FREQUENCY_PER_USER))
      .isEqualTo(17L)
  }

  @Test
  fun `bucket draws are clamped to non-negative`() {
    val noiser = noiser()
    for (index in 0 until 3) {
      assertThat(noiser.noiseFrequencyBucket(index, 0)).isAtLeast(0L)
    }
  }

  @Test
  fun `a different contribution count gives a different draw`() {
    // Same aggregate vector, different contribution count: reseeds. This is what closes the
    // fully-contained-contribution differencing case, where the aggregate is byte-identical.
    assertThat(noiser(CONTRIBUTION_COUNT + 1).noiseReach(15)).isNotEqualTo(noiser().noiseReach(15))
  }

  @Test
  fun `fingerprint matches golden`() {
    // Golden digest over BE32(count) || BE32(vector). Pins the encoding: a change to the field
    // order, width or endianness reseeds every deployed measurement.
    assertThat(
        DeterministicTruncatedLaplaceResultNoiser.fingerprint(COMBINED, CONTRIBUTION_COUNT).toHex()
      )
      .isEqualTo("ddff6bed977d8001cf37dd15dc88025d1c4f264c82263af73b2f8ced93cf2312")
  }

  @Test
  fun `fingerprint changes with vector contents`() {
    assertThat(
        DeterministicTruncatedLaplaceResultNoiser.fingerprint(
          intArrayOf(1, 2, 3),
          CONTRIBUTION_COUNT,
        )
      )
      .isNotEqualTo(
        DeterministicTruncatedLaplaceResultNoiser.fingerprint(
          intArrayOf(1, 2, 4),
          CONTRIBUTION_COUNT,
        )
      )
  }

  companion object {
    private const val REACH_EPSILON = 1.0
    private const val FREQUENCY_EPSILON = 2.0
    private const val BOUND = 8
    private const val CONTRIBUTION_COUNT = 3
    private const val MAX_FREQUENCY_PER_USER = 3
    private val COMBINED = intArrayOf(0, 1, 2, 1, 3, 0, 2)

    private fun noiser(contributionCount: Int = CONTRIBUTION_COUNT) =
      DeterministicTruncatedLaplaceResultNoiser(
        COMBINED,
        contributionCount,
        REACH_EPSILON,
        FREQUENCY_EPSILON,
        BOUND,
      )

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it.toInt() and 0xFF) }
  }
}
