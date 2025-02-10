/*
 * Copyright 2024 The Cross-Media Measurement Authors
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

import org.wfanet.measurement.api.v2alpha.PopulationSpec
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.subPopulation
import org.wfanet.measurement.api.v2alpha.PopulationSpecKt.vidRange
import org.wfanet.measurement.api.v2alpha.event_group_metadata.testing.SyntheticPopulationSpec
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.person
import org.wfanet.measurement.api.v2alpha.populationSpec
import org.wfanet.measurement.common.pack

fun SyntheticPopulationSpec.toPopulationSpec(): PopulationSpec {
  return populationSpec {
    subpopulations +=
      subPopulationsList.map { it ->
        subPopulation {
          vidRanges += vidRange {
            startVid = it.vidSubRange.start
            endVidInclusive = (it.vidSubRange.endExclusive - 1)
          }
        }
      }
  }
}

/** Adds the attribute field for TestEvents. */
fun SyntheticPopulationSpec.toPopulationSpecWithAttributes(): PopulationSpec {
  return populationSpec {
    subpopulations +=
      subPopulationsList.map { it ->
        subPopulation {
          vidRanges += vidRange {
            startVid = it.vidSubRange.start
            endVidInclusive = (it.vidSubRange.endExclusive - 1)
          }
          attributes +=
            person {
                gender =
                  Person.Gender.forNumber(
                    checkNotNull(it.populationFieldsValuesMap["person.gender"]).enumValue
                  )
                ageGroup =
                  Person.AgeGroup.forNumber(
                    checkNotNull(it.populationFieldsValuesMap["person.age_group"]).enumValue
                  )
                socialGradeGroup =
                  Person.SocialGradeGroup.forNumber(
                    checkNotNull(it.populationFieldsValuesMap["person.social_grade_group"])
                      .enumValue
                  )
              }
              .pack()
        }
      }
  }
}
