/**
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 * ```
 *      http://www.apache.org/licenses/LICENSE-2.0
 * ```
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import java.time.LocalDate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.ageRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate

enum class AgeGroup(val string: String) {
  RANGE_18_34("18_34"),
  RANGE_35_54("35_54"),
  ABOVE_54("55+")
}

enum class Gender(val string: String) {
  MALE("M"),
  FEMALE("F")
}

/** A set of users whose privacy budget usage is being tracked as a unit. */
data class PrivacyBucketGroup(
  val measurementConsumerId: String,
  val startingDate: LocalDate,
  val endingDate: LocalDate,
  val ageGroup: AgeGroup,
  val gender: Gender,
  val vidSampleStart: Float,
  val vidSampleWidth: Float
)

// 300*200*3*2 = 60.000

/**
 * Converts [PrivacyBucketGroup] to a [TestEvent] message to be filered by the CEL expression for
 * that message.
 *
 * TODO(@uakyol) : Update this to [Event] message when actual templates are registered.
 */
fun PrivacyBucketGroup.toEventProto(): TestEvent {
  val privacyBucketGroupGender = this.gender
  return testEvent {
    privacyBudget = testPrivacyBudgetTemplate {
      age =
        if (ageGroup == AgeGroup.RANGE_18_34) ageRange { value = AgeRange.Value.AGE_18_TO_24 }
        else
          (if (ageGroup == AgeGroup.RANGE_35_54) ageRange { value = AgeRange.Value.AGE_35_TO_54 }
          else ageRange { value = AgeRange.Value.AGE_OVER_54 })

      gender =
        if (privacyBucketGroupGender == Gender.MALE)
          TestPrivacyBudgetTemplateKt.gender {
            value = TestPrivacyBudgetTemplate.Gender.Value.GENDER_MALE
          }
        else
          TestPrivacyBudgetTemplateKt.gender {
            value = TestPrivacyBudgetTemplate.Gender.Value.GENDER_FEMALE
          }
    }
  }
}
