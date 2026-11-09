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
import com.sun.management.ThreadMXBean
import java.lang.management.ManagementFactory
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
    // The draws are +1, -4, -2, 0, so this fails against a noiser that returns zero.
    val noiser = noiser()

    assertThat(noiser.noiseReach(15)).isEqualTo(16L)
    assertThat(noiser.noiseFrequencyBucket(0, 10)).isEqualTo(6L)
    assertThat(noiser.noiseFrequencyBucket(1, 4)).isEqualTo(2L)
    assertThat(noiser.noiseFrequencyBucket(2, 1)).isEqualTo(1L)
  }

  @Test
  fun `impression count is capped and drawn on its own label`() {
    // Capped sum over [10, 4, 1] at 3 per user is 1*10 + 2*4 + 3*1 = 21; the draw on the impression
    // label at sensitivity 3 is -5, using the bound derived for that sensitivity (20.27, not the
    // 6.76 of the unit-sensitivity draws). Distinct from the bucket labels, so it is not the
    // weighted sum of the bucket draws.
    assertThat(noiser().noiseImpressionsFromFrequencyHistogram(longArrayOf(10, 4, 1)))
      .isEqualTo(16L)
  }

  @Test
  fun `bucket draws are clamped to non-negative`() {
    val noiser = noiser()
    for (index in 0 until 3) {
      assertThat(noiser.noiseFrequencyBucket(index, 0)).isAtLeast(0L)
    }
  }

  @Test
  fun `fingerprint changes with contribution count`() {
    // Same aggregate vector, different contribution count: reseeds. This is what closes the
    // fully-contained-contribution differencing case, where the aggregate is byte-identical.
    // Asserted on the seed, not a draw: draws round into a small integer range and collide.
    assertThat(DeterministicTruncatedLaplaceResultNoiser.fingerprint(COMBINED, CONTRIBUTION_COUNT))
      .isNotEqualTo(
        DeterministicTruncatedLaplaceResultNoiser.fingerprint(COMBINED, CONTRIBUTION_COUNT + 1)
      )
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

  @Test
  fun `byte frequencies preserve the existing fingerprint and noise draws`() {
    val frequencies = byteArrayOf(0, 1, 2, 1, 3, 0, 2)
    assertThat(DeterministicTruncatedLaplaceResultNoiser.fingerprint(frequencies, 3).toHex())
      .isEqualTo("ddff6bed977d8001cf37dd15dc88025d1c4f264c82263af73b2f8ced93cf2312")
    val noiser = DeterministicTruncatedLaplaceResultNoiser(frequencies, 3, 3)
    assertThat(noiser.noiseReach(15)).isEqualTo(16L)
    assertThat(noiser.noiseFrequencyBucket(0, 10)).isEqualTo(6L)
    assertThat(noiser.noiseFrequencyBucket(1, 4)).isEqualTo(2L)
    assertThat(noiser.noiseImpressionsFromFrequencyHistogram(longArrayOf(10, 4, 1))).isEqualTo(16L)
  }

  @Test
  fun `fingerprint encodes unsigned byte frequencies as four-byte integers`() {
    val frequencies = byteArrayOf(0, 1, 127, 128.toByte(), 200.toByte(), 255.toByte())
    assertThat(DeterministicTruncatedLaplaceResultNoiser.fingerprint(frequencies, 3).toHex())
      .isEqualTo("27b36ae7773260e5f3104e665f195aaea38fd924782016e687e0f8f1d4c14c54")
  }

  @Test
  fun `fingerprint preserves encoding across buffer boundaries`() {
    val goldens =
      mapOf(
        16382 to "76dff2b400b739d836119485f833ff2e21393c12b116ceffe8229e0f0e188177",
        16383 to "15baf833dc37074947728c89ec8c791b901541e73252e5f40aed3d24c2919456",
        16384 to "58af1d405cfc4c43721442d47290d5eff4e18d97f41434c6c3fc6f33b5d4c689",
        32785 to "362836ba7f1a39875ded7daf7e01bcb2d938c628ec6814b3352a985cbbef5dc5",
      )
    for ((size, expected) in goldens) {
      val frequencies = ByteArray(size) { (it % 256).toByte() }
      assertThat(DeterministicTruncatedLaplaceResultNoiser.fingerprint(frequencies, 3).toHex())
        .isEqualTo(expected)
    }
  }

  @Test
  fun `fingerprint uses bounded working memory for a large byte vector`() {
    val bean = ManagementFactory.getThreadMXBean() as ThreadMXBean
    bean.isThreadAllocatedMemoryEnabled = true
    DeterministicTruncatedLaplaceResultNoiser.fingerprint(byteArrayOf(1), 1)
    val frequencies = ByteArray(16777216) { 1 }
    val threadId = Thread.currentThread().id
    val before = bean.getThreadAllocatedBytes(threadId)
    val actual = DeterministicTruncatedLaplaceResultNoiser.fingerprint(frequencies, 1)
    val allocated = bean.getThreadAllocatedBytes(threadId) - before
    assertThat(actual.toHex())
      .isEqualTo("f6465292b75c48188b576e9b332f6ac4b064c1ce3bc75335b2f3044c8cb505ed")
    assertThat(allocated).isLessThan(1024 * 1024L)
  }

  @Test
  fun `fingerprint preserves integer frequencies outside the byte range`() {
    val frequencies = intArrayOf(0, 1, 256, 65536, Int.MAX_VALUE, -1)
    assertThat(DeterministicTruncatedLaplaceResultNoiser.fingerprint(frequencies, 3).toHex())
      .isEqualTo("6f134664a620e69cc8eaaea410e605634252eb49eb00682ee1cc1a5537c950fb")
  }

  @Test
  fun `array overloads produce identical fingerprints and noise draws`() {
    val bytes = byteArrayOf(0, 1, 127, 128.toByte(), 200.toByte(), 255.toByte())
    val integers = IntArray(bytes.size) { bytes[it].toInt() and 0xFF }
    assertThat(DeterministicTruncatedLaplaceResultNoiser.fingerprint(bytes, 3))
      .isEqualTo(DeterministicTruncatedLaplaceResultNoiser.fingerprint(integers, 3))
    val byteNoiser = DeterministicTruncatedLaplaceResultNoiser(bytes, 3)
    val intNoiser = DeterministicTruncatedLaplaceResultNoiser(integers, 3)
    assertThat(byteNoiser.noiseReach(123)).isEqualTo(intNoiser.noiseReach(123))
    assertThat(byteNoiser.noiseImpressionsFromFrequencyHistogram(longArrayOf(1, 2, 3)))
      .isEqualTo(intNoiser.noiseImpressionsFromFrequencyHistogram(longArrayOf(1, 2, 3)))
    for (bucket in 0..4) {
      assertThat(byteNoiser.noiseFrequencyBucket(bucket, 12))
        .isEqualTo(intNoiser.noiseFrequencyBucket(bucket, 12))
    }
  }

  companion object {
    private const val CONTRIBUTION_COUNT = 3
    private const val MAX_FREQUENCY_PER_USER = 3
    private val COMBINED = intArrayOf(0, 1, 2, 1, 3, 0, 2)

    private fun noiser() =
      DeterministicTruncatedLaplaceResultNoiser(
        COMBINED,
        CONTRIBUTION_COUNT,
        MAX_FREQUENCY_PER_USER,
      )

    private fun ByteArray.toHex(): String = joinToString("") { "%02x".format(it.toInt() and 0xFF) }
  }
}
