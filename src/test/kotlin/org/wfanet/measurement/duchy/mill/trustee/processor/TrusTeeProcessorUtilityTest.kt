// Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.duchy.mill.trustee.processor

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class TrusTeeProcessorUtilityTest {

  @Test
  fun `toIntArray successfully converts valid byte array`() {
    val bytes = byteArrayOf(0, 1, 10, 126)
    val expected = intArrayOf(0, 1, 10, 126)
    assertThat(bytes.toIntArray()).isEqualTo(expected)
  }

  @Test
  fun `toIntArray on empty array returns empty array`() {
    val bytes = byteArrayOf()
    assertThat(bytes.toIntArray()).isEmpty()
  }

  @Test
  fun `toIntArray throws for negative frequency`() {
    val bytes = byteArrayOf(1, -1, 2)
    val exception = assertFailsWith<IllegalArgumentException> { bytes.toIntArray() }
    assertThat(exception.message).contains("Invalid frequency value")
  }

  @Test
  fun `toIntArray throws for frequency of 127`() {
    val bytes = byteArrayOf(1, 127, 2)
    val exception = assertFailsWith<IllegalArgumentException> { bytes.toIntArray() }
    assertThat(exception.message).contains("Invalid frequency value")
  }

  @Test
  fun `toIntArray throws for frequency greater than 127`() {
    // Byte values > 127 are represented as negative numbers in Kotlin/JVM.
    // For example, 128.toByte() is -128.
    val bytes = byteArrayOf(1, 128.toByte(), 2)
    val exception = assertFailsWith<IllegalArgumentException> { bytes.toIntArray() }
    assertThat(exception.message).contains("Invalid frequency value")
  }

  @Test
  fun `buildHistogram creates correct histogram`() {
    val vector = intArrayOf(1, 2, 0, 1, 3, 2, 1, 0, 0)
    val maxFrequency = 3
    val expected = longArrayOf(3, 3, 2, 1) // Counts for frequencies 0, 1, 2, 3
    assertThat(buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram with empty vector returns histogram of zeros`() {
    val vector = intArrayOf()
    val maxFrequency = 5
    val expected = longArrayOf(0, 0, 0, 0, 0, 0)
    assertThat(buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram with all zero frequencies`() {
    val vector = intArrayOf(0, 0, 0, 0)
    val maxFrequency = 3
    val expected = longArrayOf(4, 0, 0, 0)
    assertThat(buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram with frequencies up to maxFrequency`() {
    val vector = intArrayOf(1, 2, 3, 4, 5)
    val maxFrequency = 5
    val expected = longArrayOf(0, 1, 1, 1, 1, 1)
    assertThat(buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }
}
