/*
 * Copyright 2026 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.wfanet.measurement.edpaggregator.vidlabeler.utils

import com.google.common.truth.Truth.assertThat
import java.time.Instant
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ActiveWindowTest {
  @Test
  fun `rejects non-positive width`() {
    assertFailsWith<IllegalArgumentException> { ActiveWindow(100L, 100L) } // equal bounds
    assertFailsWith<IllegalArgumentException> { ActiveWindow(100L, 50L) } // inverted bounds
    assertFailsWith<IllegalArgumentException> {
      ActiveWindow.of(Instant.ofEpochSecond(10), Instant.ofEpochSecond(10))
    }
  }

  @Test
  fun `contains is half-open`() {
    val window = ActiveWindow(10_000_000L, 20_000_000L)
    assertThat(window.contains(9_999_999L)).isFalse()
    assertThat(window.contains(10_000_000L)).isTrue() // inclusive start
    assertThat(window.contains(19_999_999L)).isTrue()
    assertThat(window.contains(20_000_000L)).isFalse() // exclusive end
  }

  @Test
  fun `of with null end is open-ended`() {
    val window = ActiveWindow.of(Instant.ofEpochSecond(10), null)
    assertThat(window.endMicros).isEqualTo(ActiveWindow.OPEN_ENDED)
    assertThat(window.contains(9_999_999L)).isFalse()
    assertThat(window.contains(Long.MAX_VALUE - 1)).isTrue()
  }

  @Test
  fun `instant micros conversions round-trip`() {
    assertThat(instantToEpochMicros(Instant.ofEpochSecond(1, 500_000_000))).isEqualTo(1_500_000L)
    // Sub-microsecond nanos are truncated.
    assertThat(instantToEpochMicros(Instant.ofEpochSecond(0, 1_999))).isEqualTo(1L)
    assertThat(epochMicrosToInstant(1_500_000L)).isEqualTo(Instant.ofEpochSecond(1, 500_000_000))
  }
}
