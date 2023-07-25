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

package org.wfanet.measurement.loadtest.config

import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticEventGroupSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpecKt
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.fieldValue
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticEventGroupSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.syntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.testMetadataMessage
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.vidRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.common.toProtoDate

/** EventGroup metadata for testing. */
object EventGroupMetadata {
  /**
   * UK population for [Person] from 2011 census data.
   *
   * https://www.ons.gov.uk/peoplepopulationandcommunity/birthsdeathsandmarriages/families/adhocs/005317ct05672011censussexbyagesyoabyapproximatedsocialgradenationaltocountry
   *
   * TODO(@SanjayVas): Read this from a config file.
   */
  val UK_POPULATION = syntheticPopulationSpec {
    vidRange = vidRange {
      start = 1
      endExclusive = 34_335_355 // Age 18-64 with known social grade
    }
    populationFields += "person.gender"
    populationFields += "person.age_group"
    populationFields += "person.social_grade_group"
    nonPopulationFields += "video_ad.viewed_fraction"
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 1
          endExclusive = 3_274_022
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.MALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.A_B_C1_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 3_274_022
          endExclusive = 6_151_245
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.MALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.C2_D_E_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 6_151_245
          endExclusive = 10_177_155
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.MALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_35_TO_54_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.A_B_C1_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 10_177_155
          endExclusive = 13_782_705
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.MALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_35_TO_54_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.C2_D_E_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 13_782_705
          endExclusive = 15_341_499
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.MALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_55_PLUS_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.A_B_C1_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 15_341_499
          endExclusive = 17_038_163
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.MALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_55_PLUS_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.C2_D_E_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 17_038_163
          endExclusive = 20_356_531
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.FEMALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.A_B_C1_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 20_356_531
          endExclusive = 23_213_118
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.FEMALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_18_TO_34_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.C2_D_E_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 23_213_118
          endExclusive = 27_540_955
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.FEMALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_35_TO_54_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.A_B_C1_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 27_540_955
          endExclusive = 31_020_333
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.FEMALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_35_TO_54_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.C2_D_E_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 31_020_333
          endExclusive = 32_726_015
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.FEMALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_55_PLUS_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.A_B_C1_VALUE
        }
      }
    subPopulations +=
      SyntheticPopulationSpecKt.subPopulation {
        vidSubRange = vidRange {
          start = 32_726_015
          endExclusive = 34_335_356
        }
        populationFieldsValues["person.gender"] = fieldValue {
          enumValue = Person.Gender.FEMALE_VALUE
        }
        populationFieldsValues["person.age_group"] = fieldValue {
          enumValue = Person.AgeGroup.YEARS_55_PLUS_VALUE
        }
        populationFieldsValues["person.social_grade_group"] = fieldValue {
          enumValue = Person.SocialGradeGroup.C2_D_E_VALUE
        }
      }
  }

  fun testMetadata(publisherId: Int) = testMetadataMessage { this.publisherId = publisherId }

  /**
   * Synthetic data specification for an EventGroup.
   *
   * TODO(@SanjayVas): Read this from a config file.
   */
  val SYNTHETIC_DATA_SPEC = syntheticEventGroupSpec {
    dateSpecs +=
      SyntheticEventGroupSpecKt.dateSpec {
        dateRange =
          SyntheticEventGroupSpecKt.DateSpecKt.dateRange {
            start = LocalDate.of(2021, 3, 15).toProtoDate()
            endExclusive = LocalDate.of(2021, 3, 16).toProtoDate()
          }
        frequencySpecs +=
          SyntheticEventGroupSpecKt.frequencySpec {
            frequency = 1
            vidRangeSpecs +=
              SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                vidRange = vidRange {
                  start = 1
                  endExclusive = start + 1000
                }
                nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                  doubleValue = 0.25
                }
              }
          }
        frequencySpecs +=
          SyntheticEventGroupSpecKt.frequencySpec {
            frequency = 2
            vidRangeSpecs +=
              SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                vidRange = vidRange {
                  start = 1001
                  endExclusive = start + 1000
                }
                nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                  doubleValue = 0.5
                }
              }
          }
        frequencySpecs +=
          SyntheticEventGroupSpecKt.frequencySpec {
            frequency = 1
            vidRangeSpecs +=
              SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                vidRange = vidRange {
                  start = 17_038_163
                  endExclusive = start + 1000
                }
                nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                  doubleValue = 0.25
                }
              }
          }
        frequencySpecs +=
          SyntheticEventGroupSpecKt.frequencySpec {
            frequency = 2
            vidRangeSpecs +=
              SyntheticEventGroupSpecKt.FrequencySpecKt.vidRangeSpec {
                vidRange = vidRange {
                  start = 17_039_163
                  endExclusive = start + 1000
                }
                nonPopulationFieldValues["video_ad.viewed_fraction"] = fieldValue {
                  doubleValue = 0.5
                }
              }
          }
      }
  }
}
