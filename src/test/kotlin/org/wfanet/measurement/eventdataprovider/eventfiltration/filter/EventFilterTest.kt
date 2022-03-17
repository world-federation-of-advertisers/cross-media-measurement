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

package org.wfanet.measurement.eventdataprovider.eventfiltration.filter

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Message
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent

private const val TEMPLATE_PREFIX = "org.wfa.measurement.api.v2alpha.event_templates.testing"

@RunWith(JUnit4::class)
class EventFilterTest {
  private fun exampleEventWithAge(): Message {
    val eventBuilder = TestEvent.newBuilder()
    val videoAd =
      eventBuilder
        .videoAdBuilder
        .setAge(AgeRange.newBuilder().setValue(AgeRange.Value.AGE_18_TO_24).build())
        .build()
    eventBuilder.videoAd = videoAd
    return eventBuilder.build()
  }

  @Test
  fun `keeps event when condition matches`() {
    val filter =
      EventFilter(
        " video_ad.age.value == 1",
        TestEvent.getDefaultInstance(),
      )
    val event = exampleEventWithAge()
    assert(filter.matches(event))
  }

  @Test
  fun `filters even when condition does not match`() {
    val filter = EventFilter(" video_ad.age.value != 1", TestEvent.getDefaultInstance())
    val event = exampleEventWithAge()
    assert(!filter.matches(event))
  }

  private fun assertThatFailsWithCode(
    code: EventFilterException.Code,
    block: () -> Unit,
  ) {
    val e = assertFailsWith(EventFilterException::class, block)
    assertThat(e.code).isEqualTo(code)
  }

  @Test
  fun `throws error when field is not filled`() {
    val filter =
      EventFilter(
        " video_ad.age.value == 1",
        TestEvent.getDefaultInstance(),
      )
    val event = TestEvent.newBuilder().build()
    assertThatFailsWithCode(EventFilterException.Code.EVALUATION_ERROR) { filter.matches(event) }
  }

  @Test
  fun `throws error when expression is invalid`() {
    assertThatFailsWithCode(EventFilterException.Code.INVALID_CEL_EXPRESSION) {
      EventFilter(
        "+",
        TestEvent.getDefaultInstance(),
      )
    }
  }

  @Test
  fun `throws error when result is not boolean`() {
    val filter =
      EventFilter(
        "video_ad.age.value",
        TestEvent.getDefaultInstance(),
      )
    val event = exampleEventWithAge()
    assertThatFailsWithCode(EventFilterException.Code.INVALID_EVALUATION_RESULT) {
      filter.matches(event)
    }
  }
}
