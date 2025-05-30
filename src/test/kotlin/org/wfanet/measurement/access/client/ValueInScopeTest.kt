/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.access.client

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class ValueInScopeTest {
  @Test
  fun `test returns true for matching scopes`() {
    val value = "foo.bar"
    assertThat(ValueInScope(value).test(value)).isTrue()
    assertThat(ValueInScope("foo.*").test(value)).isTrue()
    assertThat(ValueInScope("*").test(value)).isTrue()
  }

  @Test
  fun `test returns true if any scope matches`() {
    assertThat(ValueInScope("foo.*", "bar").test("foo.bar")).isTrue()
  }

  @Test
  fun `test returns false for non-matching scopes`() {
    assertThat(ValueInScope("bar", "foo*", "foo.bar.*").test("foo.bar")).isFalse()
  }
}
