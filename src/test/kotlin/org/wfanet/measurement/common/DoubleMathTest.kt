// Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class DoubleMathTest {
  @Test
  fun `fuzzyLessThanOrEqualTo with negative tolerance throws IllegalArgumentException`() {
    val value = 0.5
    val exception =
      assertFailsWith<IllegalArgumentException> { value.fuzzyLessThanOrEqualTo(1.0, -TOLERANCE) }

    assertThat(exception).hasMessageThat().contains("negative")
  }

  @Test
  fun `fuzzyLessThanOrEqualTo returns true when the value is less than the reference one`() {
    val value = 0.5
    assertThat(value.fuzzyLessThanOrEqualTo(1.0, TOLERANCE)).isEqualTo(true)
  }

  @Test
  fun `fuzzyLessThanOrEqualTo returns true when the value is greater than the reference one but is within the tolerance`() {
    val value = 1.0 + TOLERANCE
    assertThat(value.fuzzyLessThanOrEqualTo(1.0, TOLERANCE)).isEqualTo(true)
  }

  @Test
  fun `fuzzyLessThanOrEqualTo returns false when the value is greater than the reference plus the tolerance`() {
    val value = 1.0 + 1.1*TOLERANCE
    assertThat(value.fuzzyLessThanOrEqualTo(1.0, TOLERANCE)).isEqualTo(false)
  }

  companion object {
    private const val TOLERANCE = 1E-6
  }
}
