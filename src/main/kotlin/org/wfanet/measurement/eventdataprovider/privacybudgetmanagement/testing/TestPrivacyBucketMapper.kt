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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing

import com.google.protobuf.Message
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.ageRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketMapper
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType

class TestPrivacyBucketMapper : PrivacyBucketMapper {
  override fun toPrivacyFilterProgram(filterExpression: String): Program =
    try {
      EventFilters.compileProgram(
        filterExpression,
        testEvent {},
        setOf("privacy_budget.age.value", "privacy_budget.gender.value")
      )
    } catch (e: EventFilterValidationException) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER,
        emptyList()
      )
    }

  override fun toEventMessage(privacyBucketGroup: PrivacyBucketGroup): Message {
    return testEvent {
      privacyBudget = testPrivacyBudgetTemplate {
        when (privacyBucketGroup.ageGroup) {
          AgeGroup.RANGE_18_34 -> age = ageRange { value = AgeRange.Value.AGE_18_TO_34 }
          AgeGroup.RANGE_35_54 -> age = ageRange { value = AgeRange.Value.AGE_35_TO_54 }
          AgeGroup.ABOVE_54 -> age = ageRange { value = AgeRange.Value.AGE_OVER_54 }
        }
        when (privacyBucketGroup.gender) {
          Gender.MALE ->
            gender =
              TestPrivacyBudgetTemplateKt.gender {
                value = TestPrivacyBudgetTemplate.Gender.Value.GENDER_MALE
              }
          Gender.FEMALE ->
            gender =
              TestPrivacyBudgetTemplateKt.gender {
                value = TestPrivacyBudgetTemplate.Gender.Value.GENDER_FEMALE
              }
        }
      }
    }
  }
}
