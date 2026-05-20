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

package org.wfanet.measurement.loadtest.dataprovider

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import com.google.protobuf.DynamicMessage
import com.google.protobuf.TypeRegistry
import com.google.type.date
import java.nio.file.Paths
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneId
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.FieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.fieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.DuplicatePersonEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Common as MarketCommon
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.Edp1
import org.wfanet.measurement.api.v2alpha.event_templates.testing.market.v1.MarketEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toOpenEndInstantRange
import org.wfanet.measurement.common.toProtoDuration

/** Tests for the v2alpha [PopulationSpec] overload of [SyntheticDataGeneration.generateEvents]. */
@RunWith(JUnit4::class)
class SyntheticDataGenerationTest {

  @Test
  fun `generateEvents with PopulationSpec returns expected sequence of TestEvents`() {
    val populationSpec = TWO_SUBPOP_POPULATION_SPEC
    val eventGroupSpec = TWO_SUBPOP_EVENT_GROUP_SPEC

    val labeledEvents: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    val expectedMaleEvent = testEvent {
      person = person {
        gender = Person.Gender.MALE
        ageGroup = Person.AgeGroup.YEARS_18_TO_34
      }
      bannerAd = banner { viewable = true }
      videoAd = video { viewedFraction = 0.5 }
    }
    val expectedFemaleEvent = testEvent {
      person = person {
        gender = Person.Gender.FEMALE
        ageGroup = Person.AgeGroup.YEARS_18_TO_34
      }
      bannerAd = banner { viewable = true }
      videoAd = video { viewedFraction = 0.8 }
    }

    val timestamp = LocalDate.of(2023, 6, 27).atStartOfDay().toInstant(ZoneOffset.UTC)
    val expectedEvents = mutableListOf<LabeledEvent<TestEvent>>()
    for (vid in 0L until 25L) {
      expectedEvents.add(LabeledEvent(timestamp, vid, expectedMaleEvent))
    }
    for (vid in 50L until 75L) {
      expectedEvents.add(LabeledEvent(timestamp, vid, expectedFemaleEvent))
    }
    assertThat(
        labeledEvents.map {
          val zdt = it.timestamp.atZone(ZoneId.of("UTC"))
          val startOfDay = zdt.withHour(0).withMinute(0).withSecond(0)
          LabeledEvent(startOfDay.toInstant(), it.vid, it.message)
        }
      )
      .containsExactlyElementsIn(expectedEvents)
  }

  @Test
  fun `generateEvents with PopulationSpec is deterministic`() {
    val labeledEvents1: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          TWO_SUBPOP_POPULATION_SPEC,
          TWO_SUBPOP_EVENT_GROUP_SPEC,
        )
        .toEventsList()
    val labeledEvents2: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          TWO_SUBPOP_POPULATION_SPEC,
          TWO_SUBPOP_EVENT_GROUP_SPEC,
        )
        .toEventsList()
    assertThat(labeledEvents1).containsExactlyElementsIn(labeledEvents2)
  }

  @Test
  fun `generateEvents with PopulationSpec works with DynamicMessage event type`() {
    // Use the TestEvent descriptor but route generation through DynamicMessage to exercise the
    // descriptor-based code path that supports arbitrary event message types.
    val dynamicInstance = DynamicMessage.getDefaultInstance(TestEvent.getDescriptor())

    val dynamicEvents =
      SyntheticDataGeneration.generateEvents(
          dynamicInstance,
          TWO_SUBPOP_POPULATION_SPEC,
          TWO_SUBPOP_EVENT_GROUP_SPEC,
        )
        .toEventsList()

    val compiledEvents: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          TWO_SUBPOP_POPULATION_SPEC,
          TWO_SUBPOP_EVENT_GROUP_SPEC,
        )
        .toEventsList()

    // Re-parse each DynamicMessage as a TestEvent to compare structurally; the wire bytes must
    // match exactly between the dynamic and compiled paths.
    val dynamicAsTestEvents =
      dynamicEvents.map {
        LabeledEvent(it.timestamp, it.vid, TestEvent.parseFrom(it.message.toByteString()))
      }
    assertThat(dynamicAsTestEvents).containsExactlyElementsIn(compiledEvents)
  }

  @Test
  fun `generateEvents with PopulationSpec respects nested non-population fields`() {
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 10L
            }
          attributes +=
            ProtoAny.pack(
              person {
                gender = Person.Gender.FEMALE
                ageGroup = Person.AgeGroup.YEARS_35_TO_54
              }
            )
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 1L
                    endExclusive = 11L
                  }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.42
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                }
            }
        }
    }

    val testEvents: List<TestEvent> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
        .map { it.message }

    assertThat(testEvents).hasSize(10)
    val expected = testEvent {
      person = person {
        gender = Person.Gender.FEMALE
        ageGroup = Person.AgeGroup.YEARS_35_TO_54
      }
      videoAd = video { viewedFraction = 0.42 }
      bannerAd = banner { viewable = true }
    }
    assertThat(testEvents.toSet()).containsExactly(expected)
  }

  @Test
  fun `generateEvents with PopulationSpec throws when no subpopulation contains the VID range`() {
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 10L
            }
          attributes += ProtoAny.pack(person { gender = Person.Gender.MALE })
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  // 100..200 is not contained by any sub-population.
                  vidRange = vidRange {
                    start = 100L
                    endExclusive = 200L
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec throws when attribute type does not match a template field`() {
    // Pack a TestEvent itself as the attribute. Because TestEvent's type URL does not match any
    // *template* field of TestEvent, the prototype builder must fail.
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 10L
            }
          attributes += ProtoAny.pack(testEvent {})
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 1L
                    endExclusive = 11L
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalArgumentException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec throws when nonPopulation field path resolves to a message`() {
    // "banner_ad" resolves to a BannerAd *message* on TestEvent, not a scalar leaf. The engine
    // wraps the resulting IllegalArgumentException from setField as IllegalStateException.
    val populationSpec = singleSubPopWithPerson()
    val eventGroupSpec =
      eventGroupSpecWithNonPopulationField("banner_ad", fieldValue { boolValue = true })

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec throws when nonPopulation field type does not match value type`() {
    // "banner_ad.viewable" is a bool field on TestEvent, but the FieldValue carries a string.
    val populationSpec = singleSubPopWithPerson()
    val eventGroupSpec =
      eventGroupSpecWithNonPopulationField(
        "banner_ad.viewable",
        fieldValue { stringValue = "true" },
      )

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec throws when nonPopulation field path does not exist`() {
    // No such field as "banner_ad.does_not_exist" on TestEvent.
    val populationSpec = singleSubPopWithPerson()
    val eventGroupSpec =
      eventGroupSpecWithNonPopulationField(
        "banner_ad.does_not_exist",
        fieldValue { boolValue = true },
      )

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec throws when vid ranges within a frequency spec overlap`() {
    // Two vidRangeSpecs in the same FrequencySpec overlap (1..25 and 20..50). The engine validates
    // VidRangeSpecs are non-overlapping within a FrequencySpec and surfaces this as
    // IllegalStateException.
    val populationSpec = TWO_SUBPOP_POPULATION_SPEC
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 27
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 28
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 25L
                  }
                }
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    // 20 sits inside the previous range [0, 25), creating an overlap.
                    start = 20L
                    endExclusive = 50L
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec produces expected per-(gender, age_group) counts`() {
    // Four subpopulations covering disjoint VID ranges, each with a distinct (gender, age_group)
    // tuple. The event group spec spans every subpopulation with varying frequencies, so the
    // expected per-tuple counts are exact and deterministic.
    val populationSpec = populationSpec {
      subpopulations +=
        subPopWithPerson(1L, 100L, Person.Gender.MALE, Person.AgeGroup.YEARS_18_TO_34)
      subpopulations +=
        subPopWithPerson(101L, 200L, Person.Gender.MALE, Person.AgeGroup.YEARS_35_TO_54)
      subpopulations +=
        subPopWithPerson(201L, 300L, Person.Gender.FEMALE, Person.AgeGroup.YEARS_18_TO_34)
      subpopulations +=
        subPopWithPerson(301L, 400L, Person.Gender.FEMALE, Person.AgeGroup.YEARS_55_PLUS)
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 4
              }
            }
          // 100 VIDs * frequency 1 = 100 impressions for (MALE, 18-34)
          frequencySpecs += freqSpec(frequency = 1L, start = 1L, endExclusive = 101L)
          // 100 VIDs * frequency 2 = 200 impressions for (MALE, 35-54)
          frequencySpecs += freqSpec(frequency = 2L, start = 101L, endExclusive = 201L)
          // 100 VIDs * frequency 3 = 300 impressions for (FEMALE, 18-34)
          frequencySpecs += freqSpec(frequency = 3L, start = 201L, endExclusive = 301L)
          // 100 VIDs * frequency 1 = 100 impressions for (FEMALE, 55+)
          frequencySpecs += freqSpec(frequency = 1L, start = 301L, endExclusive = 401L)
        }
    }

    val events: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    val countsByGenderAndAge: Map<Pair<Person.Gender, Person.AgeGroup>, Int> =
      events.groupingBy { it.message.person.gender to it.message.person.ageGroup }.eachCount()

    assertThat(countsByGenderAndAge)
      .containsExactly(
        Person.Gender.MALE to Person.AgeGroup.YEARS_18_TO_34,
        100,
        Person.Gender.MALE to Person.AgeGroup.YEARS_35_TO_54,
        200,
        Person.Gender.FEMALE to Person.AgeGroup.YEARS_18_TO_34,
        300,
        Person.Gender.FEMALE to Person.AgeGroup.YEARS_55_PLUS,
        100,
      )
    assertThat(events).hasSize(700)

    // Independently confirm per-VID attributes match the subpopulation that contains the VID.
    for (event in events) {
      val expected: Pair<Person.Gender, Person.AgeGroup> =
        when (event.vid) {
          in 1L..100L -> Person.Gender.MALE to Person.AgeGroup.YEARS_18_TO_34
          in 101L..200L -> Person.Gender.MALE to Person.AgeGroup.YEARS_35_TO_54
          in 201L..300L -> Person.Gender.FEMALE to Person.AgeGroup.YEARS_18_TO_34
          in 301L..400L -> Person.Gender.FEMALE to Person.AgeGroup.YEARS_55_PLUS
          else -> error("Unexpected VID ${event.vid}")
        }
      assertThat(event.message.person.gender to event.message.person.ageGroup).isEqualTo(expected)
    }
  }

  @Test
  fun `generateEvents with PopulationSpec handles SubPopulations with multiple disjoint vid_ranges`() {
    // Single SubPopulation with two disjoint VID ranges (1..49 and 1000..1049). Both ranges share
    // the same Person attribute, so events generated from VIDs in either range must carry the
    // same gender/age tuple.
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 49L
            }
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1000L
              endVidInclusive = 1049L
            }
          attributes +=
            ProtoAny.pack(
              person {
                gender = Person.Gender.MALE
                ageGroup = Person.AgeGroup.YEARS_55_PLUS
              }
            )
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          // VidRangeSpecs target each of the two physical ranges independently, with frequencies
          // 1 and 2 respectively. The engine's `findSubPopulationIndex` must locate the same
          // SubPopulation for both VidRangeSpecs.
          frequencySpecs += freqSpec(frequency = 1L, start = 1L, endExclusive = 50L)
          frequencySpecs += freqSpec(frequency = 2L, start = 1000L, endExclusive = 1050L)
        }
    }

    val events: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    // 49 VIDs * 1 + 50 VIDs * 2 = 149.
    assertThat(events).hasSize(149)
    for (event in events) {
      assertThat(event.message.person.gender).isEqualTo(Person.Gender.MALE)
      assertThat(event.message.person.ageGroup).isEqualTo(Person.AgeGroup.YEARS_55_PLUS)
      assertThat(event.vid).isIn((1L..49L) + (1000L..1049L))
    }
  }

  @Test
  fun `generateEvents with PopulationSpec produces messages with a Duration nonPopulation field`() {
    // Exercises the FieldValue.durationValue branch of the engine: video_ad.length is a
    // google.protobuf.Duration field on TestEvent. Each generated event should carry the same
    // duration value as configured in the vidRangeSpec.
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 10L
            }
          attributes += ProtoAny.pack(person { gender = Person.Gender.FEMALE })
        }
    }
    val videoLength = Duration.ofMinutes(5).toProtoDuration()
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 7
                day = 30
              }
              endExclusive = date {
                year = 2023
                month = 7
                day = 31
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 1L
                    endExclusive = 11L
                  }
                  nonPopulationFieldValues["video_ad.length"] = fieldValue {
                    durationValue = videoLength
                  }
                }
            }
        }
    }

    val events: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    assertThat(events).hasSize(10)
    for (event in events) {
      assertThat(event.message.person.gender).isEqualTo(Person.Gender.FEMALE)
      assertThat(event.message.videoAd.length).isEqualTo(videoLength)
    }
  }

  @Test
  fun `generateEvents with PopulationSpec filters by time range`() {
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 100L
            }
          attributes += ProtoAny.pack(person { gender = Person.Gender.MALE })
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 5
              }
            }
          frequencySpecs += freqSpec(frequency = 1L, start = 1L, endExclusive = 101L)
        }
    }
    val timeRange = (LocalDate.of(2024, 1, 2)..LocalDate.of(2024, 1, 3)).toOpenEndInstantRange()

    val unfilteredEvents =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    val filteredEvents =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
          timeRange,
        )
        .toEventsList()

    // Filter must drop strictly more than zero events but keep at least one (the date range
    // straddles the timeRange boundary).
    assertThat(filteredEvents).isNotEmpty()
    assertThat(filteredEvents.size).isLessThan(unfilteredEvents.size)
    val outOfRange = filteredEvents.firstOrNull { it.timestamp !in timeRange }
    assertThat(outOfRange).isNull()
  }

  @Test
  fun `generateEvents with PopulationSpec returns sampled events when sampling rate is set`() {
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 1000L
            }
          attributes += ProtoAny.pack(person { gender = Person.Gender.FEMALE })
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      samplingNonce = 42L
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 1L
                    endExclusive = 1001L
                  }
                  samplingRate = 0.2
                }
            }
        }
    }

    val sampled: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    // The deterministic fingerprint-based sampler will not yield exactly 200 events but will be
    // close. Allow ample slack so the assertion is not flaky if the hashing changes.
    assertThat(sampled.size).isGreaterThan(100)
    assertThat(sampled.size).isLessThan(300)
    // All sampled events must come from the FEMALE subpopulation.
    for (event in sampled) {
      assertThat(event.message.person.gender).isEqualTo(Person.Gender.FEMALE)
    }
    // Sampling must be a strict subset of the unsampled output.
    val unsampledCount = (1L..1000L).count() // frequency 1 over 1000 VIDs
    assertThat(sampled.size).isLessThan(unsampledCount)
  }

  @Test
  fun `generateEvents with PopulationSpec throws when sampling rate is invalid`() {
    // samplingRate must lie in [0.0, 1.0]. A value > 1.0 is rejected at iteration time.
    val populationSpec = TWO_SUBPOP_POPULATION_SPEC
    val eventGroupSpec = syntheticEventGroupSpec {
      samplingNonce = 42L
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 27
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 28
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 25L
                  }
                  samplingRate = 2.0 // out of range
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec throws when sampling nonce required but missing`() {
    // A vidRangeSpec with a non-trivial samplingRate (0 < r < 1) requires the EventGroupSpec to
    // carry a non-zero samplingNonce. Otherwise the sampler cannot deterministically partition VIDs
    // and the engine raises IllegalStateException.
    val populationSpec = TWO_SUBPOP_POPULATION_SPEC
    val eventGroupSpec = syntheticEventGroupSpec {
      // No samplingNonce set (default 0L).
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 27
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 28
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 25L
                    endExclusive = 50L
                  }
                  samplingRate = 0.4
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec works for a different compiled event template (MarketEvent)`() {
    // This test exercises the engine layer with a *different* compiled event-template class
    // (MarketEvent) than the default TestEvent. It is the engine-level analogue of the
    // GenerateAndVerifySyntheticDataTest CLI MarketEvent case: it proves the generator handles
    // arbitrary user-defined event templates without going through DynamicMessage. It also
    // exercises non-population fields whose paths and types differ from anything on TestEvent
    // (e.g. boolean engagement-bucket fields under `video.*`).
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 10L
            }
          attributes +=
            ProtoAny.pack(
              MarketCommon.newBuilder()
                .setSex(MarketCommon.Sex.MALE)
                .setAgeGroup(MarketCommon.AgeGroup.YEARS_16_TO_34)
                .build()
            )
        }
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 11L
              endVidInclusive = 20L
            }
          attributes +=
            ProtoAny.pack(
              MarketCommon.newBuilder()
                .setSex(MarketCommon.Sex.FEMALE)
                .setAgeGroup(MarketCommon.AgeGroup.YEARS_55_PLUS)
                .build()
            )
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              // One vidRangeSpec per subpopulation: each must be fully contained within a single
              // subpopulation's vid_ranges. Path resolves on
              // MarketEvent.video.completed_50_percent_plus, a bool field that has no analogue on
              // TestEvent.
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 1L
                    endExclusive = 11L
                  }
                  nonPopulationFieldValues["video.completed_50_percent_plus"] = fieldValue {
                    boolValue = true
                  }
                }
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 11L
                    endExclusive = 21L
                  }
                  nonPopulationFieldValues["video.completed_50_percent_plus"] = fieldValue {
                    boolValue = true
                  }
                }
            }
        }
    }

    val events: List<LabeledEvent<MarketEvent>> =
      SyntheticDataGeneration.generateEvents(
          MarketEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    assertThat(events).hasSize(20)
    for (event in events) {
      val expectedSex = if (event.vid <= 10L) MarketCommon.Sex.MALE else MarketCommon.Sex.FEMALE
      val expectedAgeGroup =
        if (event.vid <= 10L) MarketCommon.AgeGroup.YEARS_16_TO_34
        else MarketCommon.AgeGroup.YEARS_55_PLUS
      assertThat(event.message.common.sex).isEqualTo(expectedSex)
      assertThat(event.message.common.ageGroup).isEqualTo(expectedAgeGroup)
      assertThat(event.message.video.completed50PercentPlus).isTrue()
    }
  }

  @Test
  fun `generateEvents with PopulationSpec selects oneof arm via nested nonPopulation field path`() {
    // MarketEvent.common contains `oneof edp_specific { Edp1 edp1 = 3; }` modeled after the
    // Aquila US Common.edp_specific oneof. Setting a leaf scalar on a oneof arm via a nested
    // field path (`common.edp1.placement`) must auto-select that arm without any explicit
    // oneof-handling logic in the engine.
    val populationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 1L
              endVidInclusive = 10L
            }
          attributes +=
            ProtoAny.pack(
              MarketCommon.newBuilder()
                .setSex(MarketCommon.Sex.FEMALE)
                .setAgeGroup(MarketCommon.AgeGroup.YEARS_35_TO_54)
                .build()
            )
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 1L
                    endExclusive = 11L
                  }
                  nonPopulationFieldValues["common.edp1.placement"] = fieldValue {
                    enumValue = Edp1.Placement.HOMEPAGE_VALUE
                  }
                }
            }
        }
    }

    val events: List<LabeledEvent<MarketEvent>> =
      SyntheticDataGeneration.generateEvents(
          MarketEvent.getDefaultInstance(),
          populationSpec,
          eventGroupSpec,
        )
        .toEventsList()

    assertThat(events).hasSize(10)
    for (event in events) {
      // The oneof arm `edp_specific.edp1` must be selected.
      assertThat(event.message.common.edpSpecificCase).isEqualTo(MarketCommon.EdpSpecificCase.EDP1)
      // And its leaf scalar must carry the configured value.
      assertThat(event.message.common.edp1.placement).isEqualTo(Edp1.Placement.HOMEPAGE)
    }
  }

  @Test
  fun `generateEvents with PopulationSpec handles multiple date specs with frequency greater than 1`() {
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 27
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 28
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 25L
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.5
                  }
                }
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 25L
                    endExclusive = 50L
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 50L
                    endExclusive = 75L
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.8
                  }
                }
            }
        }
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 28
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 29
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 75L
                    endExclusive = 100L
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.9
                  }
                }
            }
        }
    }

    val shards: List<LabeledEventDateShard<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          TWO_SUBPOP_POPULATION_SPEC,
          eventGroupSpec,
        )
        .toList()

    assertThat(shards).hasSize(2)
    assertThat(shards[0].localDate).isEqualTo(LocalDate.of(2023, 6, 27))
    assertThat(shards[1].localDate).isEqualTo(LocalDate.of(2023, 6, 28))

    val day1Events = shards[0].labeledEvents.toList()
    val day2Events = shards[1].labeledEvents.toList()

    // Date 1: 25 VIDs * freq 2 + 25 VIDs * freq 2 + 25 VIDs * freq 1 = 125
    assertThat(day1Events).hasSize(125)
    // Date 2: 25 VIDs * freq 1 = 25
    assertThat(day2Events).hasSize(25)

    // Frequency 2 VIDs appear twice on day 1.
    val freq2Vids = day1Events.filter { it.vid in 0L..24L }
    assertThat(freq2Vids).hasSize(50)
    assertThat(freq2Vids.map { it.vid }.distinct()).hasSize(25)

    // Population attributes are correctly assigned across date specs.
    for (event in day1Events.filter { it.vid in 0L..49L }) {
      assertThat(event.message.person.gender).isEqualTo(Person.Gender.MALE)
    }
    for (event in day1Events.filter { it.vid in 50L..74L }) {
      assertThat(event.message.person.gender).isEqualTo(Person.Gender.FEMALE)
    }
    for (event in day2Events) {
      assertThat(event.message.person.gender).isEqualTo(Person.Gender.FEMALE)
    }
  }

  @Test
  fun `generateEvents with PopulationSpec rejects event message with duplicate template type URLs`() {
    // PopulationSpec attributes are keyed by type URL, so an event message with two top-level
    // fields of the same template message type (e.g. two Person fields) is ambiguous: there is
    // no way for the spec to assign different attribute values to each field. Verify that
    // generateEvents fails fast rather than silently merging the same attribute into one
    // arbitrary field.
    val populationSpec = populationSpec {
      subpopulations +=
        subPopWithPerson(1L, 10L, Person.Gender.MALE, Person.AgeGroup.YEARS_18_TO_34)
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2024
                month = 1
                day = 1
              }
              endExclusive = date {
                year = 2024
                month = 1
                day = 2
              }
            }
          frequencySpecs += freqSpec(frequency = 1L, start = 1L, endExclusive = 11L)
        }
    }

    val exception =
      assertFailsWith<IllegalStateException> {
        SyntheticDataGeneration.generateEvents(
            DuplicatePersonEvent.getDefaultInstance(),
            populationSpec,
            eventGroupSpec,
          )
          .toEventsList()
      }
    assertThat(exception).hasMessageThat().contains("Duplicate template type URL")
  }

  @Test
  fun `generateEvents with PopulationSpec spreads data correctly across days`() {
    // Drives the engine end-to-end against the small_population_spec.textproto and
    // small_data_spec.textproto fixtures used by EDP simulator integration tests. Confirms the
    // fixture pair yields exactly one labeled-event shard per day across the 2021-03-15..2021-03-21
    // window and that the total impression count matches the spec.
    // PopulationSpec embeds Person attributes inside google.protobuf.Any, so we must register the
    // Person descriptor with the TypeRegistry used by parseTextProto.
    val typeRegistry = TypeRegistry.newBuilder().add(Person.getDescriptor()).build()
    val populationSpec: PopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        PopulationSpec.getDefaultInstance(),
        typeRegistry,
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )

    val shards: List<LabeledEventDateShard<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          messageInstance = TestEvent.getDefaultInstance(),
          populationSpec = populationSpec,
          syntheticEventGroupSpec = syntheticEventGroupSpec,
        )
        .toList()

    assertThat(shards.map { it.localDate.toString() })
      .isEqualTo(
        listOf(
          "2021-03-15",
          "2021-03-16",
          "2021-03-17",
          "2021-03-18",
          "2021-03-19",
          "2021-03-20",
          "2021-03-21",
        )
      )
    // ~8000 total impressions split across 7 days; allow ample slack on per-day distribution.
    for (shard in shards) {
      assertThat(shard.labeledEvents.toList().size).isWithin(100).of(8000 / 7)
    }
    assertThat(shards.flatMap { it.labeledEvents.toList() }.size).isEqualTo(8001)
  }

  private fun subPopWithPerson(
    startVid: Long,
    endVidInclusive: Long,
    gender: Person.Gender,
    ageGroup: Person.AgeGroup,
  ): PopulationSpec.SubPopulation =
    PopulationSpecKt.subPopulation {
      vidRanges +=
        PopulationSpecKt.vidRange {
          this.startVid = startVid
          this.endVidInclusive = endVidInclusive
        }
      attributes +=
        ProtoAny.pack(
          person {
            this.gender = gender
            this.ageGroup = ageGroup
          }
        )
    }

  /** A trivial single-subpopulation [PopulationSpec] covering VIDs 1..10. */
  private fun singleSubPopWithPerson(): PopulationSpec = populationSpec {
    subpopulations += subPopWithPerson(1L, 10L, Person.Gender.MALE, Person.AgeGroup.YEARS_18_TO_34)
  }

  /**
   * A single-day, single-frequency [SyntheticEventGroupSpec] for VIDs 1..10 carrying exactly one
   * non-population field entry. Used by negative-path tests for `nonPopulationFieldValuesMap`
   * validation.
   */
  private fun eventGroupSpecWithNonPopulationField(
    path: String,
    value: FieldValue,
  ): SyntheticEventGroupSpec = syntheticEventGroupSpec {
    dateSpecs +=
      SyntheticEventGroupSpecKt.dateSpec {
        dateRange =
          SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
            start = date {
              year = 2024
              month = 1
              day = 1
            }
            endExclusive = date {
              year = 2024
              month = 1
              day = 2
            }
          }
        frequencySpecs +=
          SyntheticEventGroupSpecKt.frequencySpec {
            frequency = 1
            vidRangeSpecs +=
              SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                vidRange = vidRange {
                  start = 1L
                  endExclusive = 11L
                }
                nonPopulationFieldValues[path] = value
              }
          }
      }
  }

  private fun freqSpec(
    frequency: Long,
    start: Long,
    endExclusive: Long,
  ): SyntheticEventGroupSpec.FrequencySpec =
    SyntheticEventGroupSpecKt.frequencySpec {
      this.frequency = frequency
      vidRangeSpecs +=
        SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
          vidRange = vidRange {
            this.start = start
            this.endExclusive = endExclusive
          }
        }
    }

  private fun <T : com.google.protobuf.Message> Sequence<LabeledEventDateShard<T>>.toEventsList():
    List<LabeledEvent<T>> = flatMap { it.labeledEvents }.toList()

  companion object {
    private val TEST_DATA_PATH =
      Paths.get(
        "wfa_measurement_system",
        "src",
        "main",
        "proto",
        "wfa",
        "measurement",
        "loadtest",
        "dataprovider",
      )
    private val TEST_DATA_RUNTIME_PATH = getRuntimePath(TEST_DATA_PATH)!!

    /**
     * A two-subpopulation [PopulationSpec] covering VIDs 0..99 with Person attributes set per
     * subpopulation. Mirrors the legacy two-subpopulation spec used in
     * [SyntheticDataGenerationTest].
     */
    private val TWO_SUBPOP_POPULATION_SPEC: PopulationSpec = populationSpec {
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 0L
              endVidInclusive = 49L
            }
          attributes +=
            ProtoAny.pack(
              person {
                gender = Person.Gender.MALE
                ageGroup = Person.AgeGroup.YEARS_18_TO_34
              }
            )
        }
      subpopulations +=
        PopulationSpecKt.subPopulation {
          vidRanges +=
            PopulationSpecKt.vidRange {
              startVid = 50L
              endVidInclusive = 99L
            }
          attributes +=
            ProtoAny.pack(
              person {
                gender = Person.Gender.FEMALE
                ageGroup = Person.AgeGroup.YEARS_18_TO_34
              }
            )
        }
    }

    /** A simple single-day event group spec covering both subpopulations of the spec above. */
    private val TWO_SUBPOP_EVENT_GROUP_SPEC: SyntheticEventGroupSpec = syntheticEventGroupSpec {
      description = "two-subpop event group"
      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 27
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 28
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 25L
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.5
                  }
                }
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 50L
                    endExclusive = 75L
                  }
                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.8
                  }
                }
            }
        }
    }
  }
}
