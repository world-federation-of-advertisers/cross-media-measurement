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
import java.time.Duration
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipeKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.CartesianSyntheticEventGroupSpecRecipeKt.NonPopulationDimensionSpecKt.fieldValueRatio
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.cartesianSyntheticEventGroupSpecRecipe
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
import org.wfanet.measurement.common.toProtoDuration

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

    val labeledEvents: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
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

    val expectedTestEvents = mutableListOf<LabeledEvent<TestEvent>>()
    repeat(2) {
      for (vid in 0L until 25L) {
        expectedTestEvents.add(LabeledEvent(timestamp, vid, expectedTestEvent))
      }
      for (vid in 25L until 50L) {
        expectedTestEvents.add(LabeledEvent(timestamp, vid, expectedTestEvent2))
      }
    }
    for (vid in 50L until 75L) {
      expectedTestEvents.add(LabeledEvent(timestamp, vid, expectedTestEvent3))
    }
    for (vid in 75L until 100L) {
      expectedTestEvents.add(LabeledEvent(timestamp2, vid, expectedTestEvent4))
    }

    assertThat(labeledEvents).containsExactlyElementsIn(expectedTestEvents)
  }

  @Test
  fun `generateEvents returns a sequence of sampled events when sample size specified`() {
    val population = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 0L
        endExclusive = 1000L
      }

      populationFields += "person.gender"
      populationFields += "person.age_group"

      nonPopulationFields += "banner_ad.viewable"
      nonPopulationFields += "video_ad.viewed_fraction"

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = vidRange {
            start = 0L
            endExclusive = 500L
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
            start = 500L
            endExclusive = 1000L
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
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 250L
                  }
                  samplingRate = 0.2

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.5
                  }
                }
              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 250L
                    endExclusive = 500L
                  }
                  samplingRate = 0.4

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
                    start = 500L
                    endExclusive = 750L
                  }
                  samplingRate = 0.08

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.8
                  }
                }
            }
        }
    }

    val labeledEvents: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()

    // Since the sampling is relying on the uniform distribution of a fingerprinting function, it
    // will not output the exact expected sample size of 250 * 0.2 + 2 * (250 * 0.4 + 250 * 0.08) =
    // 350.
    assertThat(labeledEvents.size).isEqualTo(355)
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when sampling rate is invalid`() {
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
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 25L
                  }
                  samplingRate = 2.0

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.5
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when sampling nonce required but missing`() {
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
                  samplingRate = 0.4

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
                  samplingRate = 0.08

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.8
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when vid ranges overlap`() {
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
                    // 20 is in between 0 and 25, the previous range.
                    start = 20L
                    endExclusive = 50L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                }
            }
        }
    }

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `generateEvents returns a sequence of messages with a Duration field`() {
    val populationSpec = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 1L
        endExclusive = 11L
      }
      populationFields += "person.gender"
      nonPopulationFields += "video_ad.length"

      subPopulations +=
        SyntheticPopulationSpecKt.subPopulation {
          vidSubRange = this@syntheticPopulationSpec.vidRange
          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
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
                  vidRange = populationSpec.vidRange
                  nonPopulationFieldValues["video_ad.length"] = fieldValue {
                    durationValue = videoLength
                  }
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
        .map { TestEvent.parseFrom(it.message.toByteString()) }
        .toList()

    assertThat(testEvents).hasSize(10)
    assertThat(testEvents)
      .contains(
        testEvent {
          person = person { gender = Person.Gender.FEMALE }
          videoAd = video { length = videoLength }
        }
      )
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when vidrange not in subpop`() {
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

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when field is message`() {
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

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when field type wrong`() {
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

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalStateException when field doesn't exist`() {
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

    assertFailsWith<IllegalStateException> {
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toList()
    }
  }

  @Test
  fun `toSyntheticEventGroupSpec returns correct SyntheticEventGroupSpec`() {

    val populationSpec = syntheticPopulationSpec {
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
            endExclusive = 5000L
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
            start = 5000L
            endExclusive = 10000L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
    }

    val cartesianSyntheticEventGroupSpecRecipe = cartesianSyntheticEventGroupSpecRecipe {
      description = "event group 1"
      samplingNonce = 42L
      dateSpecs +=
        CartesianSyntheticEventGroupSpecRecipeKt.dateSpec {
          totalReach = 100
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
          frequencyRatios[1] = 0.5f
          frequencyRatios[2] = 0.5f

          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "banner_ad.viewable"
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = true }
                ratio = 0.5f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = false }
                ratio = 0.5f
              }
            }
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "video_ad.viewed_fraction"

              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.3 }
                ratio = 0.5f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.7 }
                ratio = 0.5f
              }
            }
        }
    }

    val convertedSyntheticEventGroupSpec =
      cartesianSyntheticEventGroupSpecRecipe.toSyntheticEventGroupSpec(populationSpec)

    val totalNumVids = 10000L // 5000 + 5000 from population spec.

    // All enums for both non population dimensions (viewed_fraction, viewable) has 0.5f ratio as
    // well as both frequency dimensions
    val bucketFraction = (0.5f * 0.5f * 0.5f).toDouble()
    val expectedSamplingRate = (bucketFraction * 100) / totalNumVids

    val expectedSyntheticEventGroupSpec = syntheticEventGroupSpec {
      description = "event group 1"
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
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }

          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }

          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 1

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = true }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.3
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = MALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 0L
                    endExclusive = 5000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
          // For gender = FEMALE and age_group = 18_TO_34
          frequencySpecs +=
            SyntheticEventGroupSpecKt.frequencySpec {
              frequency = 2

              vidRangeSpecs +=
                SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                  vidRange = vidRange {
                    start = 5000L
                    endExclusive = 10000L
                  }

                  nonPopulationFieldValues["banner_ad.viewable"] = fieldValue { boolValue = false }
                  nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                    doubleValue = 0.7
                  }
                  samplingRate = expectedSamplingRate
                }
            }
        }
    }
    // There should be only 1 date spec.
    assertThat(convertedSyntheticEventGroupSpec.dateSpecsList.size).isEqualTo(1)
    // There should be 16 frequencySpecs since there are 2 non population fields (viewable and
    // viewed_fraction) each with 2 possible values and there are 2 frequencies (1,2). so 2*2*2 = 8.
    // These are then crossed with the population spec which defines 2 subPopulations. So 8*2*2 = 16
    assertThat(convertedSyntheticEventGroupSpec.dateSpecsList.get(0).frequencySpecsList.size)
      .isEqualTo(16)
    assertThat(convertedSyntheticEventGroupSpec).isEqualTo(expectedSyntheticEventGroupSpec)
  }

  @Test
  fun `toSyntheticEventGroupSpec converts correctly for multiple dateSpecs`() {

    val populationSpec = syntheticPopulationSpec {
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
            endExclusive = 5000L
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
            start = 5000L
            endExclusive = 10000L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
    }

    val cartesianSyntheticEventGroupSpecRecipe = cartesianSyntheticEventGroupSpecRecipe {
      description = "event group 1"
      samplingNonce = 42L
      dateSpecs +=
        CartesianSyntheticEventGroupSpecRecipeKt.dateSpec {
          totalReach = 100
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

          frequencyRatios[1] = 0.5f
          frequencyRatios[2] = 0.5f

          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "banner_ad.viewable"
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = true }
                ratio = 0.5f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = false }
                ratio = 0.5f
              }
            }
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "video_ad.viewed_fraction"

              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.3 }
                ratio = 0.5f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.7 }
                ratio = 0.5f
              }
            }
        }
      dateSpecs +=
        CartesianSyntheticEventGroupSpecRecipeKt.dateSpec {
          totalReach = 100
          dateRange =
            SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
              start = date {
                year = 2023
                month = 6
                day = 29
              }
              endExclusive = date {
                year = 2023
                month = 6
                day = 30
              }
            }

          frequencyRatios[1] = 0.5f
          frequencyRatios[2] = 0.5f

          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "banner_ad.viewable"
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = true }
                ratio = 0.5f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = false }
                ratio = 0.5f
              }
            }
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "video_ad.viewed_fraction"

              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.3 }
                ratio = 0.5f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.7 }
                ratio = 0.5f
              }
            }
        }
    }

    val convertedSyntheticEventGroupSpec =
      cartesianSyntheticEventGroupSpecRecipe.toSyntheticEventGroupSpec(populationSpec)

    // There should be only 2 date specs as defined in the
    assertThat(convertedSyntheticEventGroupSpec.dateSpecsList.size).isEqualTo(2)
    // For all the date specs, there should be 16 frequencySpecs since there are 2 non population
    // fields (viewable and viewed_fraction) each with 2 possible values and there are 2 frequencies
    // (1,2). so 2*2*2 = 8.These are then crossed with the population spec which defines 2
    // subPopulations. So 8*2*2 = 16.
    for (dateSpec in convertedSyntheticEventGroupSpec.dateSpecsList) {
      assertThat(dateSpec.frequencySpecsList.size).isEqualTo(16)
    }
  }

  @Test
  fun `toSyntheticEventGroupSpec throws when samplingNonce not specified`() {

    val populationSpec = syntheticPopulationSpec {
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
            endExclusive = 5000L
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
            start = 5000L
            endExclusive = 10000L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
    }

    val cartesianSyntheticEventGroupSpecRecipe = cartesianSyntheticEventGroupSpecRecipe {
      description = "event group 1"
      dateSpecs +=
        CartesianSyntheticEventGroupSpecRecipeKt.dateSpec {
          totalReach = 100
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

          frequencyRatios[1] = 1.0f

          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "banner_ad.viewable"
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = true }
                ratio = 1.0f
              }
            }
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "video_ad.viewed_fraction"

              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.3 }
                ratio = 1.0f
              }
            }
        }
    }
    assertFailsWith<IllegalStateException> {
      cartesianSyntheticEventGroupSpecRecipe.toSyntheticEventGroupSpec(populationSpec)
    }
  }

  @Test
  fun `toSyntheticEventGroupSpec throws when dimension values don't sum up to 1`() {

    val populationSpec = syntheticPopulationSpec {
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
            endExclusive = 5000L
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
            start = 5000L
            endExclusive = 10000L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
    }

    val cartesianSyntheticEventGroupSpecRecipe = cartesianSyntheticEventGroupSpecRecipe {
      description = "event group 1"
      samplingNonce = 42L
      dateSpecs +=
        CartesianSyntheticEventGroupSpecRecipeKt.dateSpec {
          totalReach = 100
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

          frequencyRatios[1] = 1.0f

          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "banner_ad.viewable"
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = true }
                ratio = 0.8f
              }
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = false }
                ratio = 0.5f
              }
            }
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "video_ad.viewed_fraction"

              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.3 }
                ratio = 1.0f
              }
            }
        }
    }
    assertFailsWith<IllegalStateException> {
      cartesianSyntheticEventGroupSpecRecipe.toSyntheticEventGroupSpec(populationSpec)
    }
  }

  @Test
  fun `toSyntheticEventGroupSpec throws when total reach is larger than avaliable vids`() {

    val populationSpec = syntheticPopulationSpec {
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
            endExclusive = 5000L
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
            start = 5000L
            endExclusive = 10000L
          }

          populationFieldsValues["person.gender"] = fieldValue {
            enumValue = Person.Gender.FEMALE_VALUE
          }
          populationFieldsValues["person.age_group"] = fieldValue {
            enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
          }
        }
    }

    val cartesianSyntheticEventGroupSpecRecipe = cartesianSyntheticEventGroupSpecRecipe {
      description = "event group 1"
      samplingNonce = 42L
      dateSpecs +=
        CartesianSyntheticEventGroupSpecRecipeKt.dateSpec {
          totalReach = 100_000L
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

          frequencyRatios[1] = 1.0f
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "banner_ad.viewable"
              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { boolValue = true }
                ratio = 1.0f
              }
            }
          nonPopulationDimensionSpecs +=
            CartesianSyntheticEventGroupSpecRecipeKt.nonPopulationDimensionSpec {
              fieldName = "video_ad.viewed_fraction"

              fieldValueRatios += fieldValueRatio {
                fieldValue = fieldValue { doubleValue = 0.3 }
                ratio = 1.0f
              }
            }
        }
    }
    assertFailsWith<IllegalStateException> {
      cartesianSyntheticEventGroupSpecRecipe.toSyntheticEventGroupSpec(populationSpec)
    }
  }
}
