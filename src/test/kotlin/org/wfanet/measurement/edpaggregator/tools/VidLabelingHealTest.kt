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

package org.wfanet.measurement.edpaggregator.tools

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class VidLabelingHealTest {
  @Test
  fun `isAffirmative accepts y and yes case-insensitively, ignoring surrounding whitespace`() {
    assertThat(isAffirmative("yes")).isTrue()
    assertThat(isAffirmative("y")).isTrue()
    assertThat(isAffirmative("YES")).isTrue()
    assertThat(isAffirmative("Yes")).isTrue()
    assertThat(isAffirmative("  yes  ")).isTrue()
    assertThat(isAffirmative(" Y ")).isTrue()
  }

  @Test
  fun `isAffirmative rejects anything that is not exactly y or yes`() {
    assertThat(isAffirmative("no")).isFalse()
    assertThat(isAffirmative("n")).isFalse()
    assertThat(isAffirmative("yep")).isFalse()
    assertThat(isAffirmative("yesss")).isFalse()
    assertThat(isAffirmative("ye")).isFalse()
    assertThat(isAffirmative("")).isFalse()
    assertThat(isAffirmative("   ")).isFalse()
  }

  @Test
  fun `isAffirmative treats null (no stdin or EOF) as a decline`() {
    assertThat(isAffirmative(null)).isFalse()
  }
}
