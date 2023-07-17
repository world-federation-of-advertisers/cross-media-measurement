// Copyright 2022 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.eventdataprovider.eventfiltration

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Message
import java.time.Duration
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video
import org.wfanet.measurement.common.toProtoDuration
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters.compileProgram
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException

@RunWith(JUnit4::class)
class EventFiltersTest {
  private fun exampleEventWithAge(): Message {
    return testEvent {
      person = person { ageGroup = Person.AgeGroup.YEARS_18_TO_34 }
      videoAd = video { length = Duration.ofMinutes(5).withSeconds(11).toProtoDuration() }
    }
  }

  @Test
  fun `matches returns true when filter expression matches event`() {
    val program = compileProgram(TestEvent.getDescriptor(), "person.age_group == 1")
    val event = exampleEventWithAge()
    assertThat(EventFilters.matches(event, program)).isTrue()
  }

  @Test
  fun `matches returns false when filter expression does not match event`() {
    val program = compileProgram(TestEvent.getDescriptor(), "person.age_group == 2")
    val event = exampleEventWithAge()
    assertThat(EventFilters.matches(event, program)).isFalse()
  }

  @Test
  fun `matches returns true for default value when evaluated field is not set`() {
    val program = compileProgram(TestEvent.getDescriptor(), "person.age_group == 0")
    val event = TestEvent.getDefaultInstance()
    assertThat(EventFilters.matches(event, program)).isTrue()
  }

  @Test
  fun `matches returns true for has macro when evaluated field is set`() {
    val program = compileProgram(TestEvent.getDescriptor(), "has(person.age_group)")
    val event = exampleEventWithAge()
    assertThat(EventFilters.matches(event, program)).isTrue()
  }

  @Test
  fun `matches returns true when expression is empty`() {
    val program = compileProgram(TestEvent.getDescriptor(), "")
    val event = exampleEventWithAge()
    assertThat(EventFilters.matches(event, program)).isTrue()
  }

  @Test
  fun `compileProgram throws error when expression is not a conditional`() {
    val e =
      assertFailsWith(EventFilterValidationException::class) {
        compileProgram(TestEvent.getDescriptor(), "person.age_group")
      }
    assertThat(e.code).isEqualTo(EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL)
  }
}
