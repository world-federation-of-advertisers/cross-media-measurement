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
}
