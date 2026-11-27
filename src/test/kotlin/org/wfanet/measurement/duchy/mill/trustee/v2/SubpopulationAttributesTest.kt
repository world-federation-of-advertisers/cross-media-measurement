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

package org.wfanet.measurement.duchy.mill.trustee.v2

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.Message
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.common.ProtoReflection
import org.wfanet.measurement.internal.duchy.PopulationSpec
import org.wfanet.measurement.internal.duchy.PopulationSpecKt
import org.wfanet.measurement.internal.duchy.populationSpec
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2
import org.wfanet.measurement.internal.duchy.protocol.TrusTeeV2Kt
import org.wfanet.measurement.internal.duchy.protocol.copy

@RunWith(JUnit4::class)
class SubpopulationAttributesTest {
  @Test
  fun `build resolves the enum symbols of each subpopulation`() {
    val attributes =
      SubpopulationAttributes.build(
        populationSpecOf(
          person {
            ageGroup = Person.AgeGroup.YEARS_18_TO_34
            gender = Person.Gender.MALE
          },
          person { ageGroup = Person.AgeGroup.YEARS_35_TO_54 },
        ),
        EVENT_DESCRIPTOR,
      )

    assertThat(attributes.subpopulationCount).isEqualTo(2)
    assertThat(attributes.attributeValue(0, "person.age_group")).isEqualTo("YEARS_18_TO_34")
    assertThat(attributes.attributeValue(0, "person.gender")).isEqualTo("MALE")
    assertThat(attributes.attributeValue(1, "person.age_group")).isEqualTo("YEARS_35_TO_54")
  }

  @Test
  fun `attributeValue returns null for a field left at its default`() {
    val attributes =
      SubpopulationAttributes.build(
        populationSpecOf(person { ageGroup = Person.AgeGroup.YEARS_55_PLUS }),
        EVENT_DESCRIPTOR,
      )

    assertThat(attributes.attributeValue(0, "person.gender")).isNull()
  }

  @Test
  fun `attributeValue returns null for a path that is not a population attribute`() {
    val attributes =
      SubpopulationAttributes.build(
        populationSpecOf(person { ageGroup = Person.AgeGroup.YEARS_55_PLUS }),
        EVENT_DESCRIPTOR,
      )

    assertThat(attributes.attributeValue(0, "video_ad.viewed_fraction")).isNull()
  }

  @Test
  fun `build throws when the event message type is not in the descriptor set`() {
    val eventDescriptor = EVENT_DESCRIPTOR.copy { eventMessageType = "wfa.NotAnEvent" }

    assertFailsWith<IllegalArgumentException> {
      SubpopulationAttributes.build(populationSpecOf(person {}), eventDescriptor)
    }
  }

  @Test
  fun `build throws when an attribute is not an event template`() {
    assertFailsWith<IllegalArgumentException> {
      SubpopulationAttributes.build(
        populationSpecOf(TestEvent.getDefaultInstance()),
        EVENT_DESCRIPTOR,
      )
    }
  }

  companion object {
    /**
     * The [TestEvent] descriptor, after a serialization round trip that drops the event annotation
     * extensions to unknown fields, as when it is read back from storage.
     */
    private val EVENT_DESCRIPTOR: TrusTeeV2.EventDescriptor =
      TrusTeeV2.EventDescriptor.parseFrom(
        TrusTeeV2Kt.eventDescriptor {
            descriptorSet = ProtoReflection.buildFileDescriptorSet(TestEvent.getDescriptor())
            eventMessageType = TestEvent.getDescriptor().fullName
          }
          .toByteString()
      )

    /** Returns a [PopulationSpec] with one subpopulation per attribute in [attributes]. */
    private fun populationSpecOf(vararg attributes: Message): PopulationSpec = populationSpec {
      for (attribute in attributes) {
        subpopulations +=
          PopulationSpecKt.subPopulation {
            this.attributes += ProtoAny.pack(attribute).toByteString()
          }
      }
    }
  }
}
