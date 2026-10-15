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

package org.wfanet.measurement.edpaggregator.vidlabeler

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException
import org.wfanet.measurement.api.v2alpha.event_templates.testing.DuplicatePersonEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.video
import org.wfanet.measurement.api.v2alpha.populationSpec

@RunWith(JUnit4::class)
class PopulationAttributeWriterTest {
  private val eventDescriptor = TestEvent.getDescriptor()

  /**
   * `v1.Common` carries three population attributes (gender, age_group, us_state), so a spec that
   * sets fewer than all three is rejected rather than silently zeroing the omitted one.
   */
  private fun subPopulation(
    gender: Common.Gender,
    ageGroup: Common.AgeGroup,
    usState: Common.UsState,
    startVid: Long,
    endVid: Long,
  ): PopulationSpec.SubPopulation =
    PopulationSpecKt.subPopulation {
      attributes +=
        ProtoAny.pack(
          common {
            this.gender = gender
            this.ageGroup = ageGroup
            this.usState = usState
          }
        )
      vidRanges +=
        PopulationSpecKt.vidRange {
          this.startVid = startVid
          endVidInclusive = endVid
        }
    }

  private val spec: PopulationSpec = populationSpec {
    subpopulations +=
      subPopulation(
        Common.Gender.MALE,
        Common.AgeGroup.YEARS_18_TO_34,
        Common.UsState.CALIFORNIA,
        1L,
        100L,
      )
    subpopulations +=
      subPopulation(
        Common.Gender.FEMALE,
        Common.AgeGroup.YEARS_35_TO_54,
        Common.UsState.NEW_YORK,
        101L,
        200L,
      )
    subpopulations +=
      subPopulation(
        Common.Gender.MALE,
        Common.AgeGroup.YEARS_55_PLUS,
        Common.UsState.TEXAS,
        201L,
        300L,
      )
  }

  private fun uploadedEvent(): TestEvent = testEvent {
    common = common {
      gender = Common.Gender.MALE
      ageGroup = Common.AgeGroup.YEARS_18_TO_34
      usState = Common.UsState.CALIFORNIA
    }
  }

  @Test
  fun `apply writes the assigned VID's population attributes over the uploaded ones`() {
    val writer = PopulationAttributeWriter(eventDescriptor, spec)

    val event = writer.apply(uploadedEvent(), 150L).unpack(TestEvent::class.java)

    assertThat(event.common.gender).isEqualTo(Common.Gender.FEMALE)
    assertThat(event.common.ageGroup).isEqualTo(Common.AgeGroup.YEARS_35_TO_54)
    // Every population attribute is taken from the subpopulation, not just gender and age.
    assertThat(event.common.usState).isEqualTo(Common.UsState.NEW_YORK)
  }

  @Test
  fun `apply resolves each VID to its own subpopulation`() {
    val writer = PopulationAttributeWriter(eventDescriptor, spec)

    // Range boundaries are inclusive on both ends.
    assertThat(writer.apply(uploadedEvent(), 1L).unpack(TestEvent::class.java).common.ageGroup)
      .isEqualTo(Common.AgeGroup.YEARS_18_TO_34)
    assertThat(writer.apply(uploadedEvent(), 100L).unpack(TestEvent::class.java).common.ageGroup)
      .isEqualTo(Common.AgeGroup.YEARS_18_TO_34)
    assertThat(writer.apply(uploadedEvent(), 201L).unpack(TestEvent::class.java).common.ageGroup)
      .isEqualTo(Common.AgeGroup.YEARS_55_PLUS)
  }

  @Test
  fun `apply leaves non-population fields untouched`() {
    val writer = PopulationAttributeWriter(eventDescriptor, spec)
    val uploaded =
      uploadedEvent().toBuilder().apply { video = video { viewableFraction = 0.75f } }.build()

    val event = writer.apply(uploaded, 150L).unpack(TestEvent::class.java)

    // Only population attributes are overwritten; genuine event properties survive.
    assertThat(event.video.viewableFraction).isEqualTo(0.75f)
    assertThat(event.common.gender).isEqualTo(Common.Gender.FEMALE)
  }

  @Test
  fun `apply throws for a VID outside every range`() {
    val writer = PopulationAttributeWriter(eventDescriptor, spec)

    // A VID the spec does not describe is a model/spec mismatch: reporting would bucket it by the
    // uploaded demo, so fail rather than pass the event through.
    val exception =
      assertFailsWith<IllegalArgumentException> { writer.apply(uploadedEvent(), 500L) }
    assertThat(exception).hasMessageThat().contains("500")
  }

  @Test
  fun `constructor rejects a spec that omits a population attribute`() {
    // us_state unset: copying it would write US_STATE_UNSPECIFIED over the uploaded value on every
    // impression, which is the corruption this class exists to avoid.
    val incomplete = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          attributes +=
            ProtoAny.pack(
              common {
                gender = Common.Gender.MALE
                ageGroup = Common.AgeGroup.YEARS_18_TO_34
              }
            )
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 100L
            }
        }
    }

    assertFailsWith<PopulationSpecValidationException> {
      PopulationAttributeWriter(eventDescriptor, incomplete)
    }
  }

  @Test
  fun `constructor rejects overlapping VID ranges`() {
    // The per-impression lookup is a binary search, which is only correct on disjoint ranges.
    val overlapping = populationSpec {
      subpopulations +=
        subPopulation(
          Common.Gender.MALE,
          Common.AgeGroup.YEARS_18_TO_34,
          Common.UsState.CALIFORNIA,
          1L,
          100L,
        )
      subpopulations +=
        subPopulation(
          Common.Gender.FEMALE,
          Common.AgeGroup.YEARS_35_TO_54,
          Common.UsState.NEW_YORK,
          50L,
          200L,
        )
    }

    assertFailsWith<PopulationSpecValidationException> {
      PopulationAttributeWriter(eventDescriptor, overlapping)
    }
  }

  @Test
  fun `constructor rejects an event message with two fields of the same template type`() {
    // PopulationSpec attributes are keyed by template type, so the lookup would be ambiguous.
    val exception =
      assertFailsWith<IllegalArgumentException> {
        PopulationAttributeWriter(DuplicatePersonEvent.getDescriptor(), spec)
      }
    assertThat(exception).hasMessageThat().contains("more than one field")
  }

  @Test
  fun `constructor rejects a subpopulation with two attributes of the same template type`() {
    // The mirror of the check above, on the spec side. PopulationSpecValidator does not reject
    // this: getUnsetPopulationFields inspects only the first attribute of each type via `find`, so
    // the first (complete) entry satisfies validation and the duplicate reaches this class
    // unchecked. Taking the last one silently would write values nothing ever validated.
    val duplicated = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          attributes +=
            ProtoAny.pack(
              common {
                gender = Common.Gender.MALE
                ageGroup = Common.AgeGroup.YEARS_18_TO_34
                usState = Common.UsState.CALIFORNIA
              }
            )
          attributes +=
            ProtoAny.pack(
              common {
                gender = Common.Gender.FEMALE
                ageGroup = Common.AgeGroup.YEARS_35_TO_54
                usState = Common.UsState.NEW_YORK
              }
            )
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 100L
            }
        }
    }

    val exception =
      assertFailsWith<IllegalArgumentException> {
        PopulationAttributeWriter(eventDescriptor, duplicated)
      }
    assertThat(exception).hasMessageThat().contains("more than one attribute")
  }
}
