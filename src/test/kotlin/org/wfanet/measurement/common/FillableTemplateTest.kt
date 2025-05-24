/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.common

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4

@RunWith(JUnit4::class)
class FillableTemplateTest {
  @Test
  fun `fill replaces placeholders`() {
    val template = FillableTemplate("a {{foo}} is a {{bar}} with style")

    val result = template.fill(mapOf("foo" to "fooz", "bar" to "barz"))

    assertThat(result).isEqualTo("a fooz is a barz with style")
  }

  @Test
  fun `fill replaces placeholders at boundaries`() {
    val template = FillableTemplate("{{foo}} is {{bar}}")

    val result = template.fill(mapOf("foo" to "fooz", "bar" to "barz"))

    assertThat(result).isEqualTo("fooz is barz")
  }

  @Test
  fun `fill replaces unspecified placeholders with empty string`() {
    val template = FillableTemplate("a {{foo}} is a {{bar}} with style")

    val result = template.fill(mapOf("foo" to "fooz"))

    assertThat(result).isEqualTo("a fooz is a  with style")
  }

  @Test
  fun `fill returns empty string if template is empty`() {
    val template = FillableTemplate("")

    val result = template.fill(mapOf("foo" to "fooz"))

    assertThat(result).isEqualTo("")
  }

  @Test
  fun `fill throws if placeholder start does not have matching end`() {
    val template = FillableTemplate("a {{foo is unfinished")

    val exception = assertFailsWith<IllegalStateException> { template.fill(mapOf("foo" to "fooz")) }

    // Verify that exception message contains index of placeholder.
    assertThat(exception).hasMessageThat().contains("2")
  }
}
