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

package org.wfanet.measurement.reporting.service.api.v2alpha

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.reporting.service.api.InvalidFieldValueException

@RunWith(JUnit4::class)
class CelFilterValidationTest {
  private val env = buildCelEnvironment(TestEvent.getDescriptor())

  @Test
  fun `validateCelBooleanFilter accepts empty filter`() {
    validateCelBooleanFilter(env, "", "field")
  }

  @Test
  fun `validateCelBooleanFilter accepts well-formed boolean`() {
    validateCelBooleanFilter(env, "banner_ad.viewable == true", "field")
  }

  @Test
  fun `validateCelBooleanFilter rejects every bad-CEL case with field-anchored message`() {
    for (case in BAD_CEL_CASES) {
      val exception =
        assertFailsWith<InvalidFieldValueException>("case: ${case.label} filter='${case.filter}'") {
          validateCelBooleanFilter(env, case.filter, "my.field.path")
        }
      assertThat(exception.message).contains("my.field.path")
      assertThat(exception.message).contains(case.fieldSuffix)
    }
  }

  @Test
  fun `validateCelBoolean accepts empty filter`() {
    validateCelBoolean(env, "") { "should not be called" }
  }

  @Test
  fun `validateCelBoolean accepts well-formed boolean`() {
    validateCelBoolean(env, "banner_ad.viewable == true") { "should not be called" }
  }

  @Test
  fun `validateCelBoolean rejects every bad-CEL case via IllegalStateException with caller-built message`() {
    for (case in BAD_CEL_CASES) {
      val exception =
        assertFailsWith<IllegalStateException>("case: ${case.label} filter='${case.filter}'") {
          validateCelBoolean(env, case.filter) { issue -> "tagged: $issue [end]" }
        }
      assertThat(exception.message).startsWith("tagged: ")
      assertThat(exception.message).endsWith(" [end]")
      assertThat(exception.message).contains(case.diagnostic)
    }
  }

  companion object {
    /**
     * One-liner CEL filter strings that must be rejected by validation, covering each
     * `CelValidationIssue` branch in [CelFilterValidation].
     *
     * Mirrors `BasicReportTransformationsTest.BAD_CEL_CASES`; keep the two in sync so a future
     * regression in either entry point surfaces in both test classes.
     *
     * Each case carries both:
     * - [diagnostic]: the standalone clause `validateCelBoolean` interpolates into its caller-built
     *   message.
     * - [fieldSuffix]: the field-suffix clause `validateCelBooleanFilter` uses to build the
     *   user-facing message (`"$fieldPath $fieldSuffix"`).
     */
    private data class BadCelCase(
      val label: String,
      val filter: String,
      val diagnostic: String,
      val fieldSuffix: String,
    )

    private val BAD_CEL_CASES =
      listOf(
        BadCelCase(
          label = "trailing operator (parse error)",
          filter = "banner_ad.viewable ==",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "unknown top-level identifier",
          filter = "nonexistent_field == true",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "unknown nested field",
          filter = "banner_ad.bogus_field == true",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "type-mismatched operand (bool + int)",
          filter = "banner_ad.viewable + 1 == 2",
          diagnostic = "not a valid CEL expression",
          fieldSuffix = "is not a valid CEL expression",
        ),
        BadCelCase(
          label = "non-boolean result (float field on its own)",
          filter = "video_ad.viewed_fraction",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
        BadCelCase(
          label = "non-boolean result (string literal)",
          filter = "'just a string'",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
        BadCelCase(
          label = "non-boolean result (int literal)",
          filter = "42",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
        BadCelCase(
          label = "non-boolean result (message field access)",
          filter = "banner_ad",
          diagnostic = "does not evaluate to a boolean",
          fieldSuffix = "does not evaluate to a boolean",
        ),
      )
  }
}
