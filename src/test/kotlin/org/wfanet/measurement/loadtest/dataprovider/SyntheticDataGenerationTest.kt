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
import com.google.protobuf.Message
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
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
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
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.common.getRuntimePath
import org.wfanet.measurement.common.parseTextProto
import org.wfanet.measurement.common.toProtoDuration

@RunWith(JUnit4::class)
class SyntheticDataGenerationTest {
  @Test
  fun `multiple calls to generateEvents return identical results`() {
    // This test ensures that event generation is deterministic
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

    val labeledEvents1: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toEventsList()

    val labeledEvents2: List<LabeledEvent<TestEvent>> =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
        )
        .toEventsList()
    assertThat(labeledEvents1).containsExactlyElementsIn(labeledEvents2)
  }

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
        .toEventsList()

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

    assertThat(
        labeledEvents.map {
          val zdt = it.timestamp.atZone(ZoneId.of("UTC"))
          val startOfDay = zdt.withHour(0).withMinute(0).withSecond(0)
          LabeledEvent(startOfDay.toInstant(), it.vid, it.message)
        }
      )
      .containsExactlyElementsIn(expectedTestEvents)
  }

  @Test
  fun `generateEvents returns sequence of events filtered by time range`() {
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
    val timeRange =
      OpenEndTimeRange.fromClosedDateRange(LocalDate.of(2023, 6, 27)..LocalDate.of(2023, 6, 27))

    val events =
      SyntheticDataGeneration.generateEvents(
          TestEvent.getDefaultInstance(),
          population,
          eventGroupSpec,
          timeRange,
        )
        .toEventsList()

    val eventOutsideRange = events.firstOrNull { it.timestamp !in timeRange }
    assertThat(eventOutsideRange).isNull()
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
        .toEventsList()

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
        .toEventsList()
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
        .toEventsList()
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
        .toEventsList()
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
        .toEventsList()
        .map { TestEvent.parseFrom(it.message.toByteString()) }

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
        .toEventsList()
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
        .toEventsList()
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
        .toEventsList()
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
        .toEventsList()
    }
  }

  @Test
  fun `verifies that data is correctly spread across days`() {
    val syntheticPopulationSpec: SyntheticPopulationSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_population_spec.textproto").toFile(),
        SyntheticPopulationSpec.getDefaultInstance(),
      )
    val syntheticEventGroupSpec: SyntheticEventGroupSpec =
      parseTextProto(
        TEST_DATA_RUNTIME_PATH.resolve("small_data_spec.textproto").toFile(),
        SyntheticEventGroupSpec.getDefaultInstance(),
      )
    val events =
      SyntheticDataGeneration.generateEvents(
        messageInstance = TestEvent.getDefaultInstance(),
        populationSpec = syntheticPopulationSpec,
        syntheticEventGroupSpec = syntheticEventGroupSpec,
      )
    val eventsList = events.toList()
    assertThat(eventsList.map { it.localDate.toString() })
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
    // 8000 total reach / 7 days
    eventsList.map { assertThat(it.labeledEvents.toList().size).isWithin(100).of(8000 / 7) }
    assertThat(eventsList.flatMap { it.labeledEvents.toList() }.size).isEqualTo(8001)
  }

  private fun <T : Message> Sequence<LabeledEventDateShard<T>>.toEventsList():
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
  }
}
