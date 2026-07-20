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
class DeterministicUniformSamplerTest {
  private val sampler = DeterministicUniformSampler()

  @Test
  fun `same parts sample the same value`() {
    assertThat(sampler.sample("fv".toByteArray(), "reach".toByteArray()))
      .isEqualTo(sampler.sample("fv".toByteArray(), "reach".toByteArray()))
  }

  @Test
  fun `golden value pins the SHA-256 to uniform mapping`() {
    // External anchor: SHA-256 of the length-framed single part "x", first 8 bytes as a big-endian
    // long, top 53 bits over 2^53.
    assertThat(sampler.sample("x".toByteArray())).isEqualTo(0.9033610689879378)
  }

  @Test
  fun `every sample lies in the unit interval`() {
    // Covers the unsigned shift: a signed shift would make the ~half of inputs whose digest has its
    // high bit set produce a negative value.
    for (i in 0 until 5000) {
      val u = sampler.sample("input-$i".toByteArray())
      assertThat(u).isAtLeast(0.0)
      assertThat(u).isLessThan(1.0)
    }
  }

  @Test
  fun `parts are domain-separated`() {
    // Without framing, ("ab", "c") and ("a", "bc") would share the preimage "abc".
    assertThat(sampler.sample("ab".toByteArray(), "c".toByteArray()))
      .isNotEqualTo(sampler.sample("a".toByteArray(), "bc".toByteArray()))
  }

  @Test
  fun `framing separates parts that themselves contain the boundary`() {
    // A single-byte separator would collide here; length-prefix framing does not: the parts
    // (0x61, 0x00) + (0x62) and (0x61) + (0x00, 0x62) share the concatenation 0x61 0x00 0x62.
    val one = sampler.sample(byteArrayOf(0x61, 0x00), byteArrayOf(0x62))
    val two = sampler.sample(byteArrayOf(0x61), byteArrayOf(0x00, 0x62))
    assertThat(one).isNotEqualTo(two)
  }
}
