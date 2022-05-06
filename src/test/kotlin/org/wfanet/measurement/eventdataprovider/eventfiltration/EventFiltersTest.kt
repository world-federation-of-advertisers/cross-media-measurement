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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplate.Gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestBannerTemplateKt.gender
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplateKt.ageRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testBannerTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testVideoTemplate
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException

@RunWith(JUnit4::class)
class EventFiltersTest {
  private fun exampleEventWithAge(): Message {
    return testEvent {
      videoAd = testVideoTemplate { age = ageRange { value = AgeRange.Value.AGE_18_TO_24 } }
    }
  }

  private fun exampleEventsWithAgeGender(): List<Message> {
    return listOf(
      testVideoTemplate { age = ageRange { value = AgeRange.Value.AGE_18_TO_24 } },
      testBannerTemplate { gender = gender { value = Gender.Value.GENDER_MALE } }
    )
  }

  @Test
  fun `keeps event when condition matches`() {
    val program =
      EventFilters.compileProgram(
        " video_ad.age.value == 1",
        listOf(testEvent {}),
      )
    val event = exampleEventWithAge()
    assert(EventFilters.matches(listOf(event), program))
  }

  @Test
  fun `keeps multiple events when condition matches`() {
    val program =
      EventFilters.compileProgram(
        " age.value == 1 && gender.value == 1",
        listOf(testVideoTemplate {}, testBannerTemplate {}),
      )
    assert(EventFilters.matches(exampleEventsWithAgeGender(), program))
  }

  @Test
  fun `filters even when condition does not match`() {
    val program =
      EventFilters.compileProgram(
        " video_ad.age.value != 1",
        listOf(TestEvent.getDefaultInstance())
      )
    val event = exampleEventWithAge()
    assert(!EventFilters.matches(listOf(event), program))
  }

  @Test
  fun `keeps multiple events when condition does not match`() {
    val program =
      EventFilters.compileProgram(
        " age.value == 0 && gender.value == 1",
        listOf(testVideoTemplate {}, testBannerTemplate {}),
      )
    assert(!EventFilters.matches(exampleEventsWithAgeGender(), program))
  }

  private inline fun assertFailsWithCode(
    code: EventFilterException.Code,
    block: () -> Unit,
  ) {
    val e = assertFailsWith(EventFilterException::class, block)
    assertThat(e.code).isEqualTo(code)
  }

  @Test
  fun `throws error when field is not filled`() {
    val program =
      EventFilters.compileProgram(
        " video_ad.age.value == 1",
        listOf(testEvent {}),
      )
    val event = testEvent {}
    assertFailsWithCode(EventFilterException.Code.EVALUATION_ERROR) {
      EventFilters.matches(listOf(event), program)
    }
  }

  @Test
  fun `throws error when result is not boolean`() {
    val e =
      assertFailsWith(EventFilterValidationException::class) {
        EventFilters.compileProgram(
          "video_ad.age.value",
          listOf(testEvent {}),
        )
      }
    assertThat(e.code).isEqualTo(EventFilterValidationException.Code.EXPRESSION_IS_NOT_CONDITIONAL)
  }
}
