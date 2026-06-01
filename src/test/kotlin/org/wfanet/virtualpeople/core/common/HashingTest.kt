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

package org.wfanet.virtualpeople.core.common

import java.nio.ByteOrder
import kotlin.random.Random
import kotlin.test.assertEquals
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.toLong

private const val FUZZ_ITERATIONS = 100_000
private const val FUZZ_MAX_LEN = 200
private const val FUZZ_SEED = 42L

/**
 * Verifies that [Hashing.hashFingerprint64Long] (the new direct-Long helper used by the optimized
 * `Labeler.setFingerprints`) produces the same 64-bit value as the previous extraction path,
 * `Hashing.hashFingerprint64(input).toLong(ByteOrder.LITTLE_ENDIAN)`.
 *
 * Both paths feed the same UTF-8 bytes into the same Guava `farmHashFingerprint64` hash function;
 * they only differ in how the resulting `HashCode` is converted to a `Long`. Per Guava's contract,
 * `HashCode.asLong()` returns the first eight bytes interpreted in little-endian order — exactly
 * what the old `byte[] -> ByteString -> toLong(LITTLE_ENDIAN)` chain reconstructed. This test pins
 * that contract so the two paths cannot silently diverge.
 */
@RunWith(JUnit4::class)
class HashingTest {

  /** Returns the Long value the pre-optimization Labeler would have computed for [input]. */
  private fun hashViaByteString(input: String): Long =
    Hashing.hashFingerprint64(input).toLong(ByteOrder.LITTLE_ENDIAN)

  @Test
  fun `hashFingerprint64Long matches hashFingerprint64 toLong for hand-picked inputs`() {
    val cases =
      listOf(
        "",
        " ",
        "a",
        "ab",
        "abc",
        "user_0",
        "user_29999999",
        "evt_12345",
        "email_user@example.com",
        "hello world",
        // unicode (UTF-8 multi-byte)
        "café",
        "日本語",
        "🦄🌈",
        // long string
        "x".repeat(1000),
        // production-shaped fingerprintable ids
        "0123456789abcdef0123456789abcdef0123456789abcdef0123456789abcdef",
      )
    for (input in cases) {
      val viaBytes = hashViaByteString(input)
      val direct = Hashing.hashFingerprint64Long(input)
      assertEquals(viaBytes, direct, "mismatch for input=\"$input\"")
    }
  }

  @Test
  fun `hashFingerprint64Long matches hashFingerprint64 toLong for $FUZZ_ITERATIONS random inputs`() {
    val rng = Random(FUZZ_SEED)
    var mismatches = 0
    var firstMismatch: String? = null
    repeat(FUZZ_ITERATIONS) {
      val len = rng.nextInt(0, FUZZ_MAX_LEN)
      val sb = StringBuilder(len)
      // Printable-ish ASCII keeps the test output legible if it ever fails.
      repeat(len) { sb.append(('!' + rng.nextInt(94)).toChar()) }
      val input = sb.toString()
      val viaBytes = hashViaByteString(input)
      val direct = Hashing.hashFingerprint64Long(input)
      if (viaBytes != direct) {
        mismatches++
        if (firstMismatch == null) firstMismatch = input
      }
    }
    assertEquals(
      0,
      mismatches,
      "Fuzz mismatches: $mismatches / $FUZZ_ITERATIONS. First failing input: \"$firstMismatch\"",
    )
  }
}
