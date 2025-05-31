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

package org.wfanet.measurement.privacybudgetmanager

import com.google.common.truth.Truth.assertThat
import com.google.type.date
import java.time.LocalDate
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeKt.DimensionKt.fieldValue
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeKt.dimension
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeMappingKt.DimensionMappingKt.fieldValueMapping
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeMappingKt.dimensionMapping

@RunWith(JUnit4::class)
class LandscapeUtilsTest {
  @Test
  fun `getBuckets works as expected`() {

    val desctiptor = TestEvent.getDescriptor()
    val landscape = privacyLandscape {
      landscapeIdentifier = "landsape1"
      eventTemplateName = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      dimensions += dimension {
        order = 1
        fieldPath = "person.gender"
        fieldValues += fieldValue { enumValue = "MALE" }
        fieldValues += fieldValue { enumValue = "FEMALE" }
      }
      dimensions += dimension {
        order = 2
        fieldPath = "person.age_group"
        fieldValues += fieldValue { enumValue = "YEARS_18_TO_34" }
        fieldValues += fieldValue { enumValue = "YEARS_35_TO_54" }
        fieldValues += fieldValue { enumValue = "YEARS_55_PLUS" }
      }
    }

    val landscapeMask = eventGroupLandscapeMask {
      eventGroupId = "eg1"
      eventFilter = "person.age_group == 1"
      dateRange = dateRange {
        start = date {
          year = 2025
          month = 1
          day = 15
        }
        endExclusive = date {
          year = 2025
          month = 1
          day = 16
        }
      }
      vidSampleStart = 0.0f
      vidSampleWidth = 0.01f
    }

    val landscapeMasks = listOf(landscapeMask)

    val result = LandscapeUtils.getBuckets("mcid", landscapeMasks, landscape, desctiptor)

    // [18_34, MALE] has the population index of 0
    // [18_34, FEMALE] has the population index of 3
    // 0.0f - 0.01f covers the first 3 vid intervals.
    // end date is exclusive
    val expectedResult =
      listOf(
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 0,
          vidIntervalIndex = 0,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 0,
          vidIntervalIndex = 1,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 0,
          vidIntervalIndex = 2,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 3,
          vidIntervalIndex = 0,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 3,
          vidIntervalIndex = 1,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 3,
          vidIntervalIndex = 2,
        ),
      )

    assertThat(result).isEqualTo(expectedResult)
  }

  @Test
  fun `mapBuckets works for multiple fanouts in a single dimension`() {

    val fomLandscape = privacyLandscape {
      landscapeIdentifier = "landsape1"
      eventTemplateName = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      dimensions += dimension {
        order = 1
        fieldPath = "person.gender"
        fieldValues += fieldValue { enumValue = "MALE" }
        fieldValues += fieldValue { enumValue = "FEMALE" }
      }
      dimensions += dimension {
        order = 2
        fieldPath = "person.age_group"
        fieldValues += fieldValue { enumValue = "YEARS_18_TO_34" }
        fieldValues += fieldValue { enumValue = "YEARS_35_TO_54" }
        fieldValues += fieldValue { enumValue = "YEARS_55_PLUS" }
      }
    }

    val toLandscape = privacyLandscape {
      landscapeIdentifier = "landsape1"
      eventTemplateName = "wfa.measurement.api.v2alpha.event_templates.testing.TestEvent"
      dimensions += dimension {
        order = 1
        fieldPath = "person.gender"
        fieldValues += fieldValue { enumValue = "MALE" }
        fieldValues += fieldValue { enumValue = "FEMALE" }
      }
      dimensions += dimension {
        order = 2
        fieldPath = "person.age_group"
        fieldValues += fieldValue { enumValue = "YEARS_18_TO_34" }
        fieldValues += fieldValue { enumValue = "YEARS_35_TO_44" }
        fieldValues += fieldValue { enumValue = "YEARS_45_TO_54" }
        fieldValues += fieldValue { enumValue = "YEARS_55_TO_64" }
        fieldValues += fieldValue { enumValue = "YEARS_65_PLUS" }
      }
    }
    val mapping = privacyLandscapeMapping {
      mappings += dimensionMapping {
        fromDimensionFieldPath = "person.gender"
        toDimensionFieldPath = "person.gender"
        fieldValueMappings += fieldValueMapping {
          fromFieldValue = fieldValue { enumValue = "MALE" }
          toFieldValues += fieldValue { enumValue = "MALE" }
        }
        fieldValueMappings += fieldValueMapping {
          fromFieldValue = fieldValue { enumValue = "FEMALE" }
          toFieldValues += fieldValue { enumValue = "FEMALE" }
        }
      }
      mappings += dimensionMapping {
        fromDimensionFieldPath = "person.age_group"
        toDimensionFieldPath = "person.age_group"
        fieldValueMappings += fieldValueMapping {
          fromFieldValue = fieldValue { enumValue = "YEARS_18_TO_34" }
          toFieldValues += fieldValue { enumValue = "YEARS_18_TO_34" }
        }
        fieldValueMappings += fieldValueMapping {
          fromFieldValue = fieldValue { enumValue = "YEARS_35_TO_54" }
          toFieldValues += fieldValue { enumValue = "YEARS_35_TO_44" }
          toFieldValues += fieldValue { enumValue = "YEARS_45_TO_54" }
        }
        fieldValueMappings += fieldValueMapping {
          fromFieldValue = fieldValue { enumValue = "YEARS_55_PLUS" }
          toFieldValues += fieldValue { enumValue = "YEARS_55_TO_64" }
          toFieldValues += fieldValue { enumValue = "YEARS_65_PLUS" }
        }
      }
    }
    val buckets =
      listOf(
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 0,
          vidIntervalIndex = 0,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 1,
          vidIntervalIndex = 1,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 5,
          vidIntervalIndex = 2,
        ),
      )

    val result = LandscapeUtils.mapBuckets(buckets, mapping, fomLandscape, toLandscape)

    // Population index 0 is mapped to 0 in the new landscape
    //  This corresponds to (MALE, YEARS_18_TO_34) being mapped to  (MALE, YEARS_18_TO_34)
    // Population index 1 is mapped to 1 and 2 in the new landscape
    //  This corresponds to (MALE, 35_TO_54) being mapped to  (MALE, YEARS_35_TO_44) and (MALE,
    // YEARS_45_TO_54)
    // Population index 2 is mapped to 1 and 2 in the new landscape
    //  This corresponds to (MALE, 35_TO_54) being mapped to  (MALE, YEARS_35_TO_44) and (MALE,
    // YEARS_45_TO_54)
    // Population index 5 is mapped to 8 and 9 in the new landscape
    //  This corresponds to (FEMALE, YEARS_55_PLUS) being mapped to  (FEMALE, YEARS_55_TO_64) and
    // (FEMALE, YEARS_65_PLUS)
    val expectedBuckets =
      listOf(
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 0,
          vidIntervalIndex = 0,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 1,
          vidIntervalIndex = 1,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 2,
          vidIntervalIndex = 1,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 8,
          vidIntervalIndex = 2,
        ),
        PrivacyBucket(
          rowKey =
            LedgerRowKey(
              measurementConsumerId = "mcid",
              eventGroupId = "eg1",
              date = LocalDate.parse("2025-01-15"),
            ),
          populationIndex = 9,
          vidIntervalIndex = 2,
        ),
      )

    assertThat(result).isEqualTo(expectedBuckets)
  }
}
