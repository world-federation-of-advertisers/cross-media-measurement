/*
 * Copyright 2023 The Cross-Media Measurement Authors
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
import com.google.type.date
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.fieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video

@RunWith(JUnit4::class)
class SyntheticDataGenerationTest {
  @Test
  fun `generateEvents returns a sequence of dynamic event messages`() {
    val population = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 0L
        endExclusive = 100L
      }

      populationFields += "person.gender"
      populationFields += "person.age_group"

      nonPopulationFields += "banner_ad.viewable"
      nonPopulationFields += "video_ad.viewed_fraction"

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 0L
            endExclusive = 50L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.MALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 50L
            endExclusive = 100L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      description = "event group 1"

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

    val eventSequence =
      SyntheticDataGeneration.generateEvents(TEST_EVENT_DESCRIPTOR, population, eventGroupSpec)

    val parsedLabeledEvents =
      eventSequence
        .map {
          SyntheticDataGeneration.LabeledEvent(
            it.vid,
            it.timestamp,
            TestEvent.parseFrom(it.event.toByteString())
          )
        }
        .toList()

    val subPopulationPerson = person {
      gender = Person.Gender.MALE
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
    }

    val subPopulationPerson2 = person {
      gender = Person.Gender.FEMALE
      ageGroup = Person.AgeGroup.YEARS_18_TO_34
    }

    val expectedTestEvent = testEvent {
      person = subPopulationPerson
      bannerAd = banner { viewable = true }
      videoAd = video { viewedFraction = 0.5 }
    }

    val expectedTestEvent2 = testEvent {
      person = subPopulationPerson
      bannerAd = banner { viewable = false }
      videoAd = video { viewedFraction = 0.7 }
    }

    val expectedTestEvent3 = testEvent {
      person = subPopulationPerson2
      bannerAd = banner { viewable = true }
      videoAd = video { viewedFraction = 0.8 }
    }

    val expectedTestEvent4 = testEvent {
      person = subPopulationPerson2
      bannerAd = banner { viewable = true }
      videoAd = video { viewedFraction = 0.9 }
    }

    val timestamp = LocalDate.of(2023, 6, 27).atStartOfDay().toInstant(ZoneOffset.UTC)
    val timestamp2 = LocalDate.of(2023, 6, 28).atStartOfDay().toInstant(ZoneOffset.UTC)

    val expectedTestEvents = mutableListOf<SyntheticDataGeneration.LabeledEvent>()
    repeat(2) {
      for (vid in 0L until 25L) {
        expectedTestEvents.add(
          SyntheticDataGeneration.LabeledEvent(vid, timestamp, expectedTestEvent)
        )
      }
      for (vid in 25L until 50L) {
        expectedTestEvents.add(
          SyntheticDataGeneration.LabeledEvent(vid, timestamp, expectedTestEvent2)
        )
      }
    }
    for (vid in 50L until 75L) {
      expectedTestEvents.add(
        SyntheticDataGeneration.LabeledEvent(vid, timestamp, expectedTestEvent3)
      )
    }
    for (vid in 75L until 100L) {
      expectedTestEvents.add(
        SyntheticDataGeneration.LabeledEvent(vid, timestamp2, expectedTestEvent4)
      )
    }

    assertThat(parsedLabeledEvents).containsExactlyElementsIn(expectedTestEvents)
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when vidrange not in subpop`() {
    val population = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 0L
        endExclusive = 100L
      }

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 0L
            endExclusive = 50L
          }
        }
      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 50L
            endExclusive = 100L
          }
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      description = "event group"

      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 7
                day = 1
              }
              endExclusive = date {
                year = 2023
                month = 7
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 49L
                    endExclusive = 100L
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalArgumentException> {
      SyntheticDataGeneration.generateEvents(TEST_EVENT_DESCRIPTOR, population, eventGroupSpec)
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when field is message`() {
    val population = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 0L
        endExclusive = 100L
      }

      nonPopulationFields += "banner_ad"

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 0L
            endExclusive = 50L
          }
        }
      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 50L
            endExclusive = 100L
          }
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      description = "event group"

      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 7
                day = 1
              }
              endExclusive = date {
                year = 2023
                month = 7
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 50L
                    endExclusive = 100L
                  }

                  nonPopulationFieldValues["banner_ad"] = fieldValue { boolValue = true }
                }
            }
        }
    }

    assertFailsWith<IllegalArgumentException> {
      SyntheticDataGeneration.generateEvents(TEST_EVENT_DESCRIPTOR, population, eventGroupSpec)
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when field type wrong`() {
    val population = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 0L
        endExclusive = 100L
      }

      nonPopulationFields += "banner_ad.viewable"

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 0L
            endExclusive = 50L
          }
        }
      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 50L
            endExclusive = 100L
          }
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      description = "event group"

      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 7
                day = 1
              }
              endExclusive = date {
                year = 2023
                month = 7
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 50L
                    endExclusive = 100L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { int32Value = 5 }
                }
            }
        }
    }

    assertFailsWith<IllegalArgumentException> {
      SyntheticDataGeneration.generateEvents(TEST_EVENT_DESCRIPTOR, population, eventGroupSpec)
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when field doesn't exist`() {
    val population = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 0L
        endExclusive = 100L
      }

      nonPopulationFields += "banner_ad.value"

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 0L
            endExclusive = 50L
          }
        }
      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 50L
            endExclusive = 100L
          }
        }
    }
    val eventGroupSpec = syntheticEventGroupSpec {
      description = "event group"

      dateSpecs +=
        SyntheticEventGroupSpecKt.dateSpec {
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 7
                day = 1
              }
              endExclusive = date {
                year = 2023
                month = 7
                day = 2
              }
            }
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 50L
                    endExclusive = 100L
                  }

                  nonPopulationFieldValues["banner_ad.value"] = fieldValue { boolValue = true }
                }
            }
        }
    }

    assertFailsWith<IllegalArgumentException> {
      SyntheticDataGeneration.generateEvents(TEST_EVENT_DESCRIPTOR, population, eventGroupSpec)
        .toList()
    }
  }

  companion object {
    private val TEST_EVENT_DESCRIPTOR = TestEvent.getDescriptor()
  }
}
