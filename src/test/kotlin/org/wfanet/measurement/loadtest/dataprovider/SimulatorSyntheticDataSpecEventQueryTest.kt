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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.simulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.BannerKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.PersonKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.VideoKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.banner
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.video

@RunWith(JUnit4::class)
class SimulatorSyntheticDataSpecEventQueryTest {
  @Test
  fun `generateEvents returns a sequence of dynamic event messages`() {
    val simulatorSyntheticDataSpec = simulatorSyntheticDataSpec {
      population = syntheticPopulationSpec {
        vidRange = vidRange {
          start = 0L
          endExclusive = 100L
        }

        populationFields += "person.gender.value"
        populationFields += "person.age_group.value"

        nonPopulationFields += "banner_ad.viewable.value"
        nonPopulationFields += "video_ad.viewed_fraction.value"

        subPopulations +=
          SyntheticPopulationSpecKt.subPopulation {
            vidSubRange = vidRange {
              start = 0L
              endExclusive = 50L
            }

            populationFieldsValues["person.gender.value"] = "male"
            populationFieldsValues["person.age_group.value"] = "years_18_to_34"
          }
        subPopulations +=
          SyntheticPopulationSpecKt.subPopulation {
            vidSubRange = vidRange {
              start = 50L
              endExclusive = 100L
            }

            populationFieldsValues["person.gender.value"] = "female"
            populationFieldsValues["person.age_group.value"] = "years_18_to_34"
          }
      }

      eventGroupSpec += syntheticEventGroupSpec {
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

                    nonPopulationFieldValues["banner_ad.viewable.value"] = "true"
                    nonPopulationFieldValues["video_ad.viewed_fraction.value"] = "0.5"
                  }
                vidRangeSpecs +=
                  SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                    vidRange = vidRange {
                      start = 25L
                      endExclusive = 50L
                    }

                    nonPopulationFieldValues["banner_ad.viewable.value"] = "false"
                    nonPopulationFieldValues["video_ad.viewed_fraction.value"] = "0.7"
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

                    nonPopulationFieldValues["banner_ad.viewable.value"] = "true"
                    nonPopulationFieldValues["video_ad.viewed_fraction.value"] = "0.8"
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

                    nonPopulationFieldValues["banner_ad.viewable.value"] = "true"
                    nonPopulationFieldValues["video_ad.viewed_fraction.value"] = "0.9"
                  }
              }
          }
      }
      eventGroupSpec += syntheticEventGroupSpec {
        description = "event group 2"

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
                      start = 75L
                      endExclusive = 100L
                    }

                    nonPopulationFieldValues["banner_ad.viewable.value"] = "true"
                    nonPopulationFieldValues["video_ad.viewed_fraction.value"] = "0.9"
                  }
              }
          }
      }
    }

    val simulatorSyntheticDataSpecEventQuery = SimulatorSyntheticDataSpecEventQuery()

    val eventSequence =
      simulatorSyntheticDataSpecEventQuery.generateEvents(
        TEST_EVENT_DESCRIPTOR,
        simulatorSyntheticDataSpec
      )

    val testEventList = eventSequence.map { TestEvent.parseFrom(it.toByteString()) }.toList()

    val subPopulationPerson = person {
      gender = PersonKt.genderField { value = Person.Gender.MALE }
      ageGroup = PersonKt.ageGroupField { value = Person.AgeGroup.YEARS_18_TO_34 }
    }

    val subPopulationPerson2 = person {
      gender = PersonKt.genderField { value = Person.Gender.FEMALE }
      ageGroup = PersonKt.ageGroupField { value = Person.AgeGroup.YEARS_18_TO_34 }
    }

    val expectedTestEvent = testEvent {
      person = subPopulationPerson
      bannerAd = banner { viewable = BannerKt.viewableField { value = true } }
      videoAd = video { viewedFraction = VideoKt.viewedFractionField { value = 0.5 } }
    }

    val expectedTestEvent2 = testEvent {
      person = subPopulationPerson
      bannerAd = banner { viewable = BannerKt.viewableField { value = false } }
      videoAd = video { viewedFraction = VideoKt.viewedFractionField { value = 0.7 } }
    }

    val expectedTestEvent3 = testEvent {
      person = subPopulationPerson2
      bannerAd = banner { viewable = BannerKt.viewableField { value = true } }
      videoAd = video { viewedFraction = VideoKt.viewedFractionField { value = 0.8 } }
    }

    val expectedTestEvent4 = testEvent {
      person = subPopulationPerson2
      bannerAd = banner { viewable = BannerKt.viewableField { value = true } }
      videoAd = video { viewedFraction = VideoKt.viewedFractionField { value = 0.9 } }
    }

    val numExpectedTestEvent = 50
    val numExpectedTestEvent2 = 50
    val numExpectedTestEvent3 = 25
    val numExpectedTestEvent4 = 50

    val expectedTestEvents = mutableListOf<TestEvent>()
    for (i in 1..numExpectedTestEvent) {
      expectedTestEvents.add(expectedTestEvent)
    }
    for (i in 1..numExpectedTestEvent2) {
      expectedTestEvents.add(expectedTestEvent2)
    }
    for (i in 1..numExpectedTestEvent3) {
      expectedTestEvents.add(expectedTestEvent3)
    }
    for (i in 1..numExpectedTestEvent4) {
      expectedTestEvents.add(expectedTestEvent4)
    }

    assertThat(testEventList).containsExactlyElementsIn(expectedTestEvents).inOrder()
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when vidrange not in subpop`() {
    val simulatorSyntheticDataSpec = simulatorSyntheticDataSpec {
      population = syntheticPopulationSpec {
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
      eventGroupSpec += syntheticEventGroupSpec {
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
    }

    val simulatorSyntheticDataSpecEventQuery = SimulatorSyntheticDataSpecEventQuery()

    assertFailsWith<IllegalArgumentException> {
      simulatorSyntheticDataSpecEventQuery
        .generateEvents(TEST_EVENT_DESCRIPTOR, simulatorSyntheticDataSpec)
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when field is message`() {
    val simulatorSyntheticDataSpec = simulatorSyntheticDataSpec {
      population = syntheticPopulationSpec {
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
      eventGroupSpec += syntheticEventGroupSpec {
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

                    nonPopulationFieldValues["banner_ad"] = "true"
                  }
              }
          }
      }
    }

    val simulatorSyntheticDataSpecEventQuery = SimulatorSyntheticDataSpecEventQuery()

    assertFailsWith<IllegalArgumentException> {
      simulatorSyntheticDataSpecEventQuery
        .generateEvents(TEST_EVENT_DESCRIPTOR, simulatorSyntheticDataSpec)
        .toList()
    }
  }

  @Test
  fun `sequence from generateEvents throws IllegalArgumentException when field doesn't exist`() {
    val simulatorSyntheticDataSpec = simulatorSyntheticDataSpec {
      population = syntheticPopulationSpec {
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
      eventGroupSpec += syntheticEventGroupSpec {
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

                    nonPopulationFieldValues["banner_ad.value"] = "true"
                  }
              }
          }
      }
    }

    val simulatorSyntheticDataSpecEventQuery = SimulatorSyntheticDataSpecEventQuery()

    assertFailsWith<IllegalArgumentException> {
      simulatorSyntheticDataSpecEventQuery
        .generateEvents(TEST_EVENT_DESCRIPTOR, simulatorSyntheticDataSpec)
        .toList()
    }
  }

  companion object {
    private val TEST_EVENT_DESCRIPTOR = TestEvent.getDescriptor()
  }
}
