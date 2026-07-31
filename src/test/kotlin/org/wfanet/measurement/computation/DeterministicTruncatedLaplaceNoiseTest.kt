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
  fun `fingerprint matches golden`() {
    // Golden digest over BE32(count) || BE32(vector). Pins the encoding: a change to the field
    // order, width or endianness reseeds every deployed measurement.
    assertThat(DeterministicTruncatedLaplaceNoise.fingerprint(COMBINED, CONTRIBUTION_COUNT).toHex())
      .isEqualTo("ddff6bed977d8001cf37dd15dc88025d1c4f264c82263af73b2f8ced93cf2312")
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

  companion object {
    private const val CONTRIBUTION_COUNT = 3
    private val COMBINED = intArrayOf(0, 1, 2, 1, 3, 0, 2)

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it.toInt() and 0xFF) }
  }
}
