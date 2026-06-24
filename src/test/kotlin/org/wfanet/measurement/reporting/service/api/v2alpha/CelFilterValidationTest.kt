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
  fun `empty filter is a no-op`() {
    validateCelBooleanFilter(env, "", "field")
  }

  @Test
  fun `valid boolean filter passes`() {
    validateCelBooleanFilter(env, "banner_ad.viewable == true", "field")
  }

  @Test
  fun `syntax error throws InvalidFieldValueException`() {
    val exception =
      assertFailsWith<InvalidFieldValueException> {
        validateCelBooleanFilter(env, "banner_ad.viewable ==", "field")
      }
    assertThat(exception.message).contains("not a valid CEL expression")
  }

  @Test
  fun `non-boolean filter throws InvalidFieldValueException`() {
    val exception =
      assertFailsWith<InvalidFieldValueException> {
        validateCelBooleanFilter(env, "video_ad.viewed_fraction", "field")
      }
    assertThat(exception.message).contains("does not evaluate to a boolean")
  }

  @Test
  fun `unknown field throws InvalidFieldValueException`() {
    val exception =
      assertFailsWith<InvalidFieldValueException> {
        validateCelBooleanFilter(env, "nonexistent.field == true", "field")
      }
    assertThat(exception.message).contains("not a valid CEL expression")
  }
}
