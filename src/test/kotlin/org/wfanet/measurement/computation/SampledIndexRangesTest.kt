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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class SampledIndexRangesTest {
  @Test
  fun `of returns one range for an interval that does not wrap`() {
    val ranges = SampledIndexRanges.of(populationSize = 100, start = 0.25f, width = 0.5f)

    assertThat(ranges).isEqualTo(SampledIndexRanges(25..74, IntRange.EMPTY))
    assertThat(ranges.size).isEqualTo(50)
  }

  @Test
  fun `of covers the whole population for the full interval`() {
    val ranges = SampledIndexRanges.of(populationSize = 100, start = 0.0f, width = 1.0f)

    assertThat(ranges).isEqualTo(SampledIndexRanges(0..99, IntRange.EMPTY))
  }

  @Test
  fun `of splits an interval that wraps around`() {
    val ranges = SampledIndexRanges.of(populationSize = 100, start = 0.75f, width = 0.5f)

    assertThat(ranges).isEqualTo(SampledIndexRanges(75..99, 0..24))
    assertThat(ranges.size).isEqualTo(50)
  }

  @Test
  fun `of throws for a width outside (0, 1]`() {
    assertFailsWith<IllegalArgumentException> { SampledIndexRanges.of(100, 0.0f, 0.0f) }
    assertFailsWith<IllegalArgumentException> { SampledIndexRanges.of(100, 0.0f, 1.5f) }
  }

  @Test
  fun `of throws for a start outside [0, 1]`() {
    assertFailsWith<IllegalArgumentException> { SampledIndexRanges.of(100, -0.1f, 0.5f) }
    assertFailsWith<IllegalArgumentException> { SampledIndexRanges.of(100, 1.1f, 0.5f) }
  }
}
