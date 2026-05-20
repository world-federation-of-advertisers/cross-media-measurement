/*
 * Copyright 2025 The Cross-Media Measurement Authors
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

package org.wfanet.measurement.reporting.service.internal

import com.google.common.truth.Truth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResult
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt

@RunWith(JUnit4::class)
class GroupingDimensionsTest {
  @Test
  fun `has all possible groupings`() {
    val groupingDimensions = GroupingDimensions(EVENT_MESSAGE_DESCRIPTOR)

    assertThat(groupingDimensions.groupingByFingerprint.values.distinct())
      .containsExactlyElementsIn(ALL_GROUPINGS)
  }

  @Test
  fun `has fingerprint for each version`() {
    val groupingDimensions = GroupingDimensions(EVENT_MESSAGE_DESCRIPTOR)

    assertThat(groupingDimensions.groupingByFingerprint.keys)
      .containsExactlyElementsIn(
        VERSION_1_GROUPINGS.map { Normalization.computeFingerprint(1, it) }
      )
  }

  companion object {
    private val EVENT_MESSAGE_DESCRIPTOR = TestEvent.getDescriptor()
    private val ALL_GROUPINGS =
      listOf(
        ReportingSetResult.Dimension.Grouping.getDefaultInstance(),
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.MALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
        ReportingSetResultKt.DimensionKt.grouping {
          valueByPath["person.age_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_55_PLUS.name }
          valueByPath["person.gender"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          valueByPath["person.social_grade_group"] =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.C2_D_E.name }
        },
      )
    private val VERSION_1_GROUPINGS = ALL_GROUPINGS
  }
}
