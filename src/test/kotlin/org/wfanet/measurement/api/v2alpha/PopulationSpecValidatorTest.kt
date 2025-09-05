/*
 * Copyright 2024 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.api.v2alpha

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Any as ProtoAny
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.EndVidInclusiveLessThanVidStartDetail
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.StartVidNotPositiveDetail
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.VidRangeIndex
import org.wfanet.measurement.api.v2alpha.PopulationSpecValidationException.VidRangesNotDisjointDetail
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person

@RunWith(JUnit4::class)
class PopulationSpecValidatorTest {
  @Test
  fun `validateVidRangesList returns exception when vidRange start greater than end`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 1
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.details.size).isEqualTo(1)
    val details = exception.details[0] as EndVidInclusiveLessThanVidStartDetail
    assertThat(details.index).isEqualTo(VidRangeIndex(0, 0))
  }

  @Test
  fun `validateVidRangesList returns exception when vidRange has non-positive start`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 0
          endVidInclusive = 1
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.details.size).isEqualTo(1)
    val details = exception.details[0] as StartVidNotPositiveDetail
    assertThat(details.index).isEqualTo(VidRangeIndex(0, 0))
  }

  @Test
  fun `validateVidRangesList returns exception when vidRange has non-disjoint vidRanges`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 2
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 3
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.details.size).isEqualTo(1)
    val details = exception.details[0] as VidRangesNotDisjointDetail
    assertThat(details.firstIndex).isEqualTo(VidRangeIndex(0, 0))
    assertThat(details.secondIndex).isEqualTo(VidRangeIndex(1, 0))
  }

  @Test
  fun `validateVidRangesList returns exception with two details`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        // This range is invalid.
        vidRanges += vidRange {
          startVid = 0
          endVidInclusive = 1
        }
        // This one is non-disjoint with the one right below
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 3
        }
      }
      subpopulations += subPopulation {
        // This one is non-disjoint with the one right above
        vidRanges += vidRange {
          startVid = 3
          endVidInclusive = 4
        }
        // This one is valid and disjoint!
        vidRanges += vidRange {
          startVid = 300
          endVidInclusive = 400
        }
      }
    }
    val result = PopulationSpecValidator.validateVidRangesList(testPopulationSpec)
    val exception = result.exceptionOrNull() as PopulationSpecValidationException
    assertThat(exception).isNotNull()
    assertThat(exception.details.size).isEqualTo(2)

    val details0 = exception.details[0] as StartVidNotPositiveDetail
    assertThat(details0.index).isEqualTo(VidRangeIndex(0, 0))

    val details1 = exception.details[1] as VidRangesNotDisjointDetail
    assertThat(details1.firstIndex).isEqualTo(VidRangeIndex(0, 1))
    assertThat(details1.secondIndex).isEqualTo(VidRangeIndex(1, 0))
  }

  @Test
  fun `valid vidRange is valid`() {
    val testPopulationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 1
        }
        vidRanges += vidRange {
          startVid = 2
          endVidInclusive = 3
        }
      }
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 4
          endVidInclusive = 5
        }
      }
    }
    assertThat(PopulationSpecValidator.validateVidRangesList(testPopulationSpec).getOrNull())
      .isTrue()
  }

  @Test
  fun `validate throws if required template is missing from attributes`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 100
        }
      }
    }

    val exception =
      assertFailsWith<PopulationSpecValidationException> {
        PopulationSpecValidator.validate(populationSpec, TestEvent.getDescriptor())
      }

    val personDescriptor = Person.getDescriptor()
    assertThat(exception.details)
      .containsExactly(
        PopulationSpecValidationException.PopulationFieldNotSetDetail(
          personDescriptor.findFieldByNumber(Person.GENDER_FIELD_NUMBER),
          0,
        ),
        PopulationSpecValidationException.PopulationFieldNotSetDetail(
          personDescriptor.findFieldByNumber(Person.AGE_GROUP_FIELD_NUMBER),
          0,
        ),
        PopulationSpecValidationException.PopulationFieldNotSetDetail(
          personDescriptor.findFieldByNumber(Person.SOCIAL_GRADE_GROUP_FIELD_NUMBER),
          0,
        ),
      )
  }

  @Test
  fun `validate throws if population field is missing from attributes`() {
    val populationSpec = populationSpec {
      subpopulations += subPopulation {
        vidRanges += vidRange {
          startVid = 1
          endVidInclusive = 100
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

    val exception =
      assertFailsWith<PopulationSpecValidationException> {
        PopulationSpecValidator.validate(populationSpec, TestEvent.getDescriptor())
      }

    val personDescriptor = Person.getDescriptor()
    assertThat(exception.details)
      .containsExactly(
        PopulationSpecValidationException.PopulationFieldNotSetDetail(
          personDescriptor.findFieldByNumber(Person.SOCIAL_GRADE_GROUP_FIELD_NUMBER),
          0,
        )
      )
  }
}
