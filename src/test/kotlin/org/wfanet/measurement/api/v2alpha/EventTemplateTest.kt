// Copyright 2020 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.api.v2alpha

import com.google.common.truth.Truth.assertThat
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestVideoTemplate.ViewDuration

@RunWith(JUnit4::class)
class EventTemplateTest {
  @Test
  fun `populated fields contain correct values`() {
    val eventTemplate = EventTemplate(TestVideoTemplate.getDescriptor())

    assertThat(eventTemplate.name).isEqualTo("test_video_template")
    assertThat(eventTemplate.displayName).isEqualTo("Video Ad")
    assertThat(eventTemplate.description).isEqualTo("A simple Event Template for a video ad.")
    assertThat(eventTemplate.eventFields).contains(EventField(AgeRange.getDescriptor()))
    assertThat(eventTemplate.eventFields).contains(EventField(ViewDuration.getDescriptor()))
  }

  @Test
  fun `init throws exception if EventTemplate annotation missing`() {
    assertFailsWith(
      IllegalArgumentException::class,
      "Descriptor does not have EventTemplate annotation"
    ) {
      EventTemplate(AgeRange.getDescriptor())
    }
  }
}
