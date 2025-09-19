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

package org.wfanet.measurement.computation

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class HistogramComputationsTest {

  @Test
  fun `buildHistogram creates correct histogram excluding zeros`() {
    val vector = intArrayOf(1, 2, 0, 1, 3, 2, 1, 0, 0)
    val maxFrequency = 3
    val expected = longArrayOf(3, 2, 1)
    assertThat(HistogramComputations.buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram with empty vector returns histogram of zeros`() {
    val vector = intArrayOf()
    val maxFrequency = 5
    val expected = longArrayOf(0, 0, 0, 0, 0)
    assertThat(HistogramComputations.buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram with all zero frequencies returns histogram of zeros`() {
    val vector = intArrayOf(0, 0, 0, 0)
    val maxFrequency = 3
    val expected = longArrayOf(0, 0, 0)
    assertThat(HistogramComputations.buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram with frequencies up to maxFrequency`() {
    val vector = intArrayOf(1, 2, 3, 4, 5)
    val maxFrequency = 5
    val expected = longArrayOf(1, 1, 1, 1, 1)
    assertThat(HistogramComputations.buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }

  @Test
  fun `buildHistogram caps frequencies greater than maxFrequency`() {
    val vector = intArrayOf(1, 2, 3, 4, 5, 6)
    val maxFrequency = 4
    val expected = longArrayOf(1, 1, 1, 3)
    assertThat(HistogramComputations.buildHistogram(vector, maxFrequency)).isEqualTo(expected)
  }
}
