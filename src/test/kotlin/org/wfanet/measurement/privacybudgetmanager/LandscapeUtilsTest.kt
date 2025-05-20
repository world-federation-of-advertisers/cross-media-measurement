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

import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import com.google.type.date
import org.wfanet.measurement.api.v2alpha.event_templates.testing.Person
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeKt.DimensionKt.fieldValue
import org.wfanet.measurement.privacybudgetmanager.PrivacyLandscapeKt.dimension

@RunWith(JUnit4::class)
class LandscapeUtilsTest {
  @Test
  fun `getBuckets works as expected`() {

    val desctiptors = listOf(TestEvent.getDescriptor().file, Person.getDescriptor().file)
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
      eventFilter = "person.age > 25 && video.length > 10"
      dateRange = dateRange {
        start = date {
          year = 2025
          month = 1
          day = 15
        }
        endExclusive = date {
          year = 2025
          month = 2
          day = 28
        }
      }
      vidSampleStart = 0.1f
      vidSampleWidth = 0.8f
    }

    val landscapeMasks = listOf(landscapeMask)

    LandscapeUtils.getBuckets("mcid", landscapeMasks, landscape, desctiptors)
  }
}
