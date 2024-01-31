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

package org.wfanet.measurement.loadtest.validation

import com.google.common.truth.Truth.assertThat
import com.google.type.date
import java.util.logging.Logger
import org.junit.Assert.assertThrows
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.DateSpecKt.dateRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.dateSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt.frequencySpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.simulatorSyntheticDataSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange

private val TEST_SUBPOPULATION_1 = subPopulation {
  vidSubRange = vidRange {
    start = 50
    endExclusive = 100
  }
  populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                 "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                 "PERSON.GENDER" to "MALE"))
}
private val TEST_SUBPOPULATION_2 = subPopulation {
  vidSubRange = vidRange {
    start = 100
    endExclusive = 200
  }
  populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                 "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                 "PERSON.GENDER" to "FEMALE"))
}

private val TEST_SYNTHETIC_POPULATION_SPEC = syntheticPopulationSpec {
  vidRange = vidRange {
    start = 1
    endExclusive = 200
  }
  populationFields += listOf("PERSON.AGE_GROUP", "PERSON.SOCIAL_GRADE_GROUP", "PERSON.GENDER")
  nonPopulationFields += listOf("LOCATION, DEVICE")
  subPopulations += listOf(TEST_SUBPOPULATION_1, TEST_SUBPOPULATION_2)
}

private val TEST_VID_RANGE_SPEC = vidRangeSpec {
  vidRange = vidRange {
    start = 50
    endExclusive = 65
  }
  nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US",
                                        "DEVICE" to "MOBILE",
                                        ))
}
private val TEST_FREQUENCY_SPEC = frequencySpec {
  frequency = 1
  vidRangeSpecs += listOf(TEST_VID_RANGE_SPEC)
}

private val TEST_DATE_SPEC = dateSpec {
  dateRange = dateRange {
    start = date {
      day = 1
      month = 1
      year = 2020
    }
    endExclusive = date {
      day = 7
      month = 1
      year = 2020
    }
  }
  frequencySpecs += listOf(TEST_FREQUENCY_SPEC)
}

private val TEST_SYNTHETIC_EVENTGROUP_SPEC = syntheticEventGroupSpec {
  description = "Test Synthetic EventGroup"
  dateSpecs += listOf(TEST_DATE_SPEC)
}

private val TEST_SIMULATOR_SYNTHETIC_DATA_SPEC = simulatorSyntheticDataSpec {
  eventGroupSpec += listOf(TEST_SYNTHETIC_EVENTGROUP_SPEC)
  population = TEST_SYNTHETIC_POPULATION_SPEC
}
@RunWith(JUnit4::class)
class SimulatorSyntheticDataSpecValidatorTest {
  // @Test
  // fun `simulatorSyntheticDataSpec validate passes for valid configuration`() {
  // }

  @Test
  fun `simulatorSyntheticDataSpec validate throws SyntheticDataSpecValidationException when missing Synthetic Population Spec`() {
    val simulatorSyntheticDataSpec = simulatorSyntheticDataSpec {
      eventGroupSpec += listOf(TEST_SYNTHETIC_EVENTGROUP_SPEC)
    }
    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        simulatorSyntheticDataSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("simulatorSyntheticDataSpec")
  }

  @Test
  fun `simulatorSyntheticDataSpec validate throws SyntheticDataSpecValidationException when empty Synthetic EventGroup Spec`() {
    val simulatorSyntheticDataSpec = simulatorSyntheticDataSpec {
      population = TEST_SYNTHETIC_POPULATION_SPEC
    }
    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        simulatorSyntheticDataSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("simulatorSyntheticDataSpec")
  }

  @Test
  fun `syntheticPopulationSpec validate throws SyntheticDataSpecValidationException when the same subpopulation fields values set`() {
    val subpopulation1 = subPopulation {
      vidSubRange = vidRange {
        start = 50
        endExclusive = 100
      }
      populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                          "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                          "PERSON.GENDER" to "MALE"))
    }

    val subpopulation2 = subPopulation {
      vidSubRange = vidRange {
        start = 100
        endExclusive = 150
      }
      populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                          "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                          "PERSON.GENDER" to "MALE"))
    }

    val syntheticPopulationSpec = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 1
        endExclusive = 200
      }
      populationFields += listOf("PERSON.AGE_GROUP", "PERSON.SOCIAL_GRADE_GROUP", "PERSON.GENDER")
      nonPopulationFields += listOf("LOCATION, DEVICE")
      subPopulations += listOf(subpopulation1, subpopulation2)
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        syntheticPopulationSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("syntheticPopulationSpec")
  }

  @Test
  fun `syntheticPopulationSpec validate throws SyntheticDataSpecValidationException when overlapping subPopulations`() {
    val subpopulation1 = subPopulation {
      vidSubRange = vidRange {
        start = 50
        endExclusive = 100
      }
      populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                          "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                          "PERSON.GENDER" to "MALE"))
    }

    val subpopulation2 = subPopulation {
      vidSubRange = vidRange {
        start = 75
        endExclusive = 125
      }
      populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                          "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                          "PERSON.GENDER" to "FEMALE"))
    }

    val syntheticPopulationSpec = syntheticPopulationSpec {
      vidRange = vidRange {
        start = 1
        endExclusive = 200
      }
      populationFields += listOf("PERSON.AGE_GROUP", "PERSON.SOCIAL_GRADE_GROUP", "PERSON.GENDER")
      nonPopulationFields += listOf("LOCATION, DEVICE")
      subPopulations += listOf(subpopulation1, subpopulation2)
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        syntheticPopulationSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("syntheticPopulationSpec")
  }

  // @Test
  // fun `Population fields validate throws SyntheticDataSpecValidationException when non-CEL syntax`() {
  // }
  //
  // @Test
  // fun `Non-population fields validate throws SyntheticDataSpecValidationException when non-CEL syntax`() {
  // }

  @Test
  fun `subpopulation validate throws SyntheticDataSpecValidationException when population field values not set for every population field`() {
    val subpopulation = subPopulation {
      vidSubRange = vidRange {
        start = 50
        endExclusive = 100
      }
      populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                          "PERSON.GENDER" to "MALE"))
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        subpopulation.validate(TEST_SYNTHETIC_POPULATION_SPEC)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("subpopulation")
  }

  @Test
  fun `subpopulation validate throws SyntheticDataSpecValidationException when undefined population field set`() {
    val subpopulation = subPopulation {
      vidSubRange = vidRange {
        start = 50
        endExclusive = 100
      }
      populationFieldsValues.putAll(mapOf("PERSON.AGE_GROUP" to "18_34",
                                          "PERSON.SOCIAL_GRADE_GROUP" to "ABC1",
                                          "PERSON.ID" to "1234"))
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        subpopulation.validate(TEST_SYNTHETIC_POPULATION_SPEC)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("subpopulation")
  }

  @Test
  fun `syntheticEventGroupSpec throws SyntheticDataSpecValidationException when empty dateSpec`() {
    val eventGroupSpec = syntheticEventGroupSpec {
      description = "Test Synthetic EventGroup"
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        eventGroupSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("syntheticEventGroupSpec")  }

  @Test
  fun `syntheticEventGroupSpec validate throws SyntheticDataSpecValidationException when overlapping DateSpec`() {
    val dateSpec1 = dateSpec {
      dateRange = dateRange {
        start = date {
          day = 1
          month = 1
          year = 2020
        }
        endExclusive = date {
          day = 31
          month = 1
          year = 2020
        }
      }
      frequencySpecs += listOf(TEST_FREQUENCY_SPEC)
    }

    val dateSpec2 = dateSpec {
      dateRange = dateRange {
        start = date {
          day = 5
          month = 1
          year = 2020
        }
        endExclusive = date {
          day = 5
          month = 2
          year = 2020
        }
      }
      frequencySpecs += listOf(TEST_FREQUENCY_SPEC)
    }

    val dateSpec3 = dateSpec {
      dateRange = dateRange {
        start = date {
          day = 5
          month = 1
          year = 2019
        }
        endExclusive = date {
          day = 5
          month = 2
          year = 2019
        }
      }
      frequencySpecs += listOf(TEST_FREQUENCY_SPEC)
    }

    val eventGroupSpec = syntheticEventGroupSpec {
      description = "Test Synthetic EventGroup"
      dateSpecs += listOf(dateSpec1, dateSpec2, dateSpec3)
    }
    logger.info {eventGroupSpec.dateSpecsList.toString()}
    val sortedDateSpecs = eventGroupSpec.dateSpecsList.sortedWith(compareBy({it.dateRange.start.year}, {it.dateRange.start.month}, {it.dateRange.start.day}))
    logger.info {sortedDateSpecs.toString()}

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        eventGroupSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("syntheticEventGroupSpec")
  }

  @Test
  fun `dateSpec validate throws SyntheticDataSpecValidationException with empty Frequency Spec`() {
    val dateSpec = dateSpec {
      dateRange = dateRange {
        start = date {
          day = 1
          month = 1
          year = 2020
        }
        endExclusive = date {
          day = 7
          month = 1
          year = 2020
        }
      }
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        dateSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("dateSpec")
  }

  @Test
  fun `dateSpec validate throws SyntheticDataSpecValidationException with Frequency Spec with repeated Frequency`() {
    val dateSpec = dateSpec {
      dateRange = dateRange {
        start = date {
          day = 1
          month = 1
          year = 2020
        }
        endExclusive = date {
          day = 7
          month = 1
          year = 2020
        }
      }
      frequencySpecs += listOf(TEST_FREQUENCY_SPEC, TEST_FREQUENCY_SPEC)
      }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        dateSpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("dateSpec")
  }

  @Test
  fun `vidRangeSpec validate throws SyntheticDataSpecValidationException when vid range spans multiple subpopulations`() {
    val vidRangeSpec = vidRangeSpec {
      vidRange = vidRange {
        start = 75
        endExclusive = 150
      }
      nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US", "DEVICE" to "MOBILE"))
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        vidRangeSpec.validate(TEST_SYNTHETIC_POPULATION_SPEC)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("vidRangeSpec")
  }

  @Test
  fun `vidRangeSpec validate throws SyntheticDataSpecValidationException when undefined nonpopulation field values`() {
    val vidRangeSpec = vidRangeSpec {
      vidRange = vidRange {
        start = 50
        endExclusive = 60
      }
      nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US", "DURATION" to "5M"))
    }


    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        vidRangeSpec.validate(TEST_SYNTHETIC_POPULATION_SPEC)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("vidRangeSpec")
  }

  @Test
  fun `vidRangeSpec validate throws SyntheticDataSpecValidationException when not nonpopulation field values for every nonpopulation fields `() {
    val vidRangeSpec = vidRangeSpec {
      vidRange = vidRange {
        start = 50
        endExclusive = 60
      }
      nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US"))

    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        vidRangeSpec.validate(TEST_SYNTHETIC_POPULATION_SPEC)
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("vidRangeSpec")
  }

  @Test
  fun `frequencySpec validate throws SyntheticDataSpecValidationException when overlapping vid ranges`() {
    val vidRangeSpec1 = vidRangeSpec {
      vidRange = vidRange {
        start = 1
        endExclusive = 100
      }
      nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US", "DEVICE" to "MOBILE"))
    }

    val vidRangeSpec2 = vidRangeSpec {
      vidRange = vidRange {
        start = 80
        endExclusive = 120
      }
      nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US", "DEVICE" to "MOBILE"))
    }

    val vidRangeSpec3 = vidRangeSpec {
      vidRange = vidRange {
        start = 50
        endExclusive = 90
      }
      nonPopulationFieldValues.putAll(mapOf("LOCATION" to "US", "DEVICE" to "MOBILE"))
    }

    val frequencySpec =  frequencySpec {
      frequency = 1
      vidRangeSpecs += listOf(vidRangeSpec1, vidRangeSpec2, vidRangeSpec3)
    }

    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        frequencySpec.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("frequencySpec")
  }

  @Test
  fun `dateRange validate throws SyntheticDataSpecValidationException when start date is after end date`()
  {
    val invalidDateRange = dateRange {
      start = date {
        year = 2021
        month = 2
        day = 1
      }
      endExclusive = date {
        year = 2021
        month = 1
        day = 1
      }
    }
    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        invalidDateRange.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("dateRange")
    assertThat(exception.cause).hasMessageThat().contains("start cannot be after end")
  }
  @Test
  fun `vidRange validate throws SyntheticDataSpecValidationException when start is less than 1`() {
    val invalidVidRange = vidRange {
      start = 0
      endExclusive = 100
    }
    val exception = assertThrows(SyntheticDataSpecValidationException::class.java) {
        invalidVidRange.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("vidRange")
    assertThat(exception.cause).hasMessageThat().contains("less than 1")
  }

  @Test
  fun `vidRange validate throws SyntheticDataSpecValidationException when start greater than end`()
  {
    val invalidVidRange = vidRange {
      start = 201
      endExclusive = 200
    }
    val exception =
      assertThrows(SyntheticDataSpecValidationException::class.java) {
        invalidVidRange.validate()
      }
    assertThat(exception).hasCauseThat().isInstanceOf(IllegalArgumentException::class.java)
    assertThat(exception).hasMessageThat().contains("vidRange")
    assertThat(exception.cause).hasMessageThat().contains("start cannot be greater than end")
  }
  companion object {
    private val logger = Logger.getLogger(this::class.java.name)
  }
}

