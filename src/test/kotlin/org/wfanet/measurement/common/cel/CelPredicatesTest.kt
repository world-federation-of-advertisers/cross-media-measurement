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

package org.wfanet.measurement.common.cel

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent

@RunWith(JUnit4::class)
class CelPredicatesTest {
  private val env = CelPredicates.buildEnvironment(TestEvent.getDefaultInstance())

  @Test
  fun `validate is no-op on empty filter`() {
    CelPredicates.validate(env, "")
  }

  @Test
  fun `validate accepts a valid boolean filter`() {
    CelPredicates.validate(env, "person.age_group == 1")
  }

  @Test
  fun `validate rejects a syntax error with 'not a valid CEL expression'`() {
    val e =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(env, "person.age_group ==")
      }
    assertThat(e.message).contains("not a valid CEL expression")
  }

  @Test
  fun `validate rejects an unknown identifier`() {
    val e =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(env, "person.nonexistent == 1")
      }
    assertThat(e.message).contains("not a valid CEL expression")
  }

  @Test
  fun `validate rejects a non-boolean result type`() {
    val e =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(env, "person.age_group")
      }
    assertThat(e.message).contains("does not evaluate to a boolean")
  }

  @Test
  fun `validate rejects a filter using a non-CEL operator`() {
    // Non-CEL operators cause the CEL library to throw NPE internally; the validator swallows
    // that and reports it as a syntax error.
    val e =
      assertFailsWith<CelValidationException> {
        CelPredicates.validate(env, "person.age_group === 1")
      }
    assertThat(e.message).contains("not a valid CEL expression")
  }
}
