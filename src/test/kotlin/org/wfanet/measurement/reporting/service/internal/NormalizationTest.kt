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
import com.google.common.truth.extensions.proto.ProtoTruth.assertThat
import com.google.type.DayOfWeek
import java.util.logging.ConsoleHandler
import java.util.logging.Level
import java.util.logging.Logger
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.EventTemplates
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.internal.reporting.v2.EventTemplateFieldKt
import org.wfanet.measurement.internal.reporting.v2.ReportingSetResultKt
import org.wfanet.measurement.internal.reporting.v2.copy
import org.wfanet.measurement.internal.reporting.v2.eventFilter
import org.wfanet.measurement.internal.reporting.v2.eventTemplateField
import org.wfanet.measurement.internal.reporting.v2.metricFrequencySpec

@RunWith(JUnit4::class)
class NormalizationTest {
  @Test
  fun `normalizeEventFilters sorts filters and terms`() {
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

  @Test
  fun `computeFingerprint returns same value for equivalent MetricFrequencySpecs`() {
    val metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
    val otherMetricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }

    assertThat(Normalization.computeFingerprint(metricFrequencySpec))
      .isEqualTo(Normalization.computeFingerprint(otherMetricFrequencySpec))
  }

  @Test
  fun `computeFingerprint returns different values for inequivalent MetricFrequencySpecs`() {
    val metricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.MONDAY }
    val otherMetricFrequencySpec = metricFrequencySpec { total = true }
    val yetAnotherMetricFrequencySpec = metricFrequencySpec { weekly = DayOfWeek.TUESDAY }

    val fingerprint: Long = Normalization.computeFingerprint(metricFrequencySpec)

    assertThat(fingerprint).isNotEqualTo(Normalization.computeFingerprint(otherMetricFrequencySpec))
    assertThat(fingerprint)
      .isNotEqualTo(Normalization.computeFingerprint(yetAnotherMetricFrequencySpec))
  }

  @Test
  fun `computeFingerprint returns same value for equivalent grouping`() {
    val eventMessageVersion =
      EventTemplates.getEventDescriptor(TestEvent.getDescriptor()).currentVersion
    val grouping =
      ReportingSetResultKt.DimensionKt.grouping {
        valueByPath["person.gender"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        valueByPath["person.social_grade_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
        valueByPath["person.age_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
      }
    val otherGrouping =
      grouping.copy {
        val entries = valueByPath.entries.toList()
        valueByPath.clear()
        valueByPath[entries[2].key] = entries[2].value
        valueByPath[entries[0].key] = entries[0].value
        valueByPath[entries[1].key] = entries[1].value
      }

    assertThat(Normalization.computeFingerprint(eventMessageVersion, grouping))
      .isEqualTo(Normalization.computeFingerprint(eventMessageVersion, otherGrouping))
  }

  @Test
  fun `computeFingerprint returns different values for different eventMessageVersion`() {
    val eventMessageVersion =
      EventTemplates.getEventDescriptor(TestEvent.getDescriptor()).currentVersion
    val grouping =
      ReportingSetResultKt.DimensionKt.grouping {
        valueByPath["person.age_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
        valueByPath["person.gender"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        valueByPath["person.social_grade_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
      }

    assertThat(Normalization.computeFingerprint(eventMessageVersion, grouping))
      .isNotEqualTo(Normalization.computeFingerprint(eventMessageVersion + 1, grouping))
  }

  @Test
  fun `computeFingerprint returns different values for inequivalent groupings`() {
    val eventMessageVersion =
      EventTemplates.getEventDescriptor(TestEvent.getDescriptor()).currentVersion
    val grouping =
      ReportingSetResultKt.DimensionKt.grouping {
        valueByPath["person.age_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
        valueByPath["person.gender"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
        valueByPath["person.social_grade_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.SocialGradeGroup.A_B_C1.name }
      }
    val otherGrouping =
      grouping.copy {
        valueByPath["person.age_group"] =
          EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_35_TO_54.name }
      }

    assertThat(Normalization.computeFingerprint(eventMessageVersion, grouping))
      .isNotEqualTo(Normalization.computeFingerprint(eventMessageVersion, otherGrouping))
  }

  @Test
  fun `computeFingerprint returns same value for equivalent event filters`() {
    val filters =
      listOf(
        eventFilter {
          terms += eventTemplateField {
            path = "foo.bar"
            value = EventTemplateFieldKt.fieldValue { floatValue = 1.1f }
          }
          terms += eventTemplateField {
            path = "foo.bar"
            value = EventTemplateFieldKt.fieldValue { floatValue = 2.2f }
          }
          terms += eventTemplateField {
            path = "person.gender"
            value = EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          }
        },
        eventFilter {
          terms += eventTemplateField {
            path = "person.age_group"
            value =
              EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          }
        },
      )
    val otherFilters = listOf(filters[0], filters[1])

    assertThat(Normalization.computeFingerprint(filters))
      .isEqualTo(Normalization.computeFingerprint(otherFilters))
  }

  @Test
  fun `computeFingerprint returns different values for inequivalent event filters`() {
    val filters =
      listOf(
        eventFilter {
          terms += eventTemplateField {
            path = "foo.bar"
            value = EventTemplateFieldKt.fieldValue { floatValue = 1.1f }
          }
          terms += eventTemplateField {
            path = "foo.bar"
            value = EventTemplateFieldKt.fieldValue { floatValue = 2.2f }
          }
          terms += eventTemplateField {
            path = "person.gender"
            value = EventTemplateFieldKt.fieldValue { enumValue = Person.Gender.FEMALE.name }
          }
        },
        eventFilter {
          terms += eventTemplateField {
            path = "person.age_group"
            value =
              EventTemplateFieldKt.fieldValue { enumValue = Person.AgeGroup.YEARS_18_TO_34.name }
          }
        },
      )
    val otherFilters =
      listOf(
        filters[0].copy { terms[0] = terms[0].copy { value = value.copy { floatValue = 1.5f } } },
        filters[1],
      )

    assertThat(Normalization.computeFingerprint(filters))
      .isNotEqualTo(Normalization.computeFingerprint(otherFilters))
  }

  init {
    Logger.getLogger(this::class.java.`package`.name).level = Level.FINE

    val rootLogger = Logger.getLogger("")
    for (handler in rootLogger.handlers) {
      if (handler is ConsoleHandler) {
        handler.level = Level.FINE
      }
    }
  }
}
