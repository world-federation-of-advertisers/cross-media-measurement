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

import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField

@RunWith(JUnit4::class)
class NormalizationTest {
  @Test
  fun `sortGroupings sorts groupings`() {
    val groupings =
      listOf(
        eventTemplateField {
          path = "person.gender"
          value = EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        },
        eventTemplateField {
          path = "person.social_grade_group"
          value =
            EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        },
        eventTemplateField {
          path = "person.age_group"
          value =
            EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
        },
      )

    assertThat(Normalization.sortGroupings(groupings))
      .containsExactly(groupings[2], groupings[0], groupings[1])
      .inOrder()
  }

  @Test
  fun `normalize sorts filters and terms`() {
    val filters =
      listOf(
        eventFilter {
          terms += eventTemplateField {
            path = "person.gender"
            value = EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          }
        },
        eventFilter {
          terms += eventTemplateField {
            path = "person.gender"
            value = EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          }
          terms += eventTemplateField {
            path = "person.age_group"
            value =
              EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          }
        },
      )

    assertThat(Normalization.normalizeEventFilters(filters))
      .containsExactly(
        eventFilter {
          terms += filters[1].termsList[1]
          terms += filters[1].termsList[0]
        },
        eventFilter { terms += filters[0].termsList[0] },
      )
      .inOrder()
  }
}
