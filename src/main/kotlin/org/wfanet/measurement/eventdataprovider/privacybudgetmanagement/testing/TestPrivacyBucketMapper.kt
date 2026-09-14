/*
 * Copyright 2022 The Cross-Media Measurement Authors
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not use this file except
 * in compliance with the License. You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software distributed under the License
 * is distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express
 * or implied. See the License for the specific language governing permissions and limitations under
 * the License.
 */
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing

import com.google.protobuf.Message
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.Common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.TestEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.common
import org.wfanet.measurement.api.v2alpha.event_templates.testing.v1.testEvent
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters.compileProgram
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.AgeGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.Gender
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketGroup
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBucketMapper
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerException
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.PrivacyBudgetManagerExceptionType

/** [PrivacyBucketMapper] for [TestEvent] instances. */
class TestPrivacyBucketMapper : PrivacyBucketMapper {

  override val operativeFields = setOf("common.age_group", "common.gender")

  override fun toPrivacyFilterProgram(filterExpression: String): Program =
    try {
      compileProgram(TestEvent.getDescriptor(), filterExpression, operativeFields)
    } catch (e: EventFilterValidationException) {
      throw PrivacyBudgetManagerException(
        PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER,
        e,
      )
    }

  override fun toEventMessage(privacyBucketGroup: PrivacyBucketGroup): Message {
    return testEvent {
      common = common {
        ageGroup =
          when (privacyBucketGroup.ageGroup) {
            AgeGroup.RANGE_18_34 -> Common.AgeGroup.YEARS_18_TO_34
            AgeGroup.RANGE_35_54 -> Common.AgeGroup.YEARS_35_TO_54
            AgeGroup.ABOVE_54 -> Common.AgeGroup.YEARS_55_PLUS
          }
        gender =
          when (privacyBucketGroup.gender) {
            Gender.MALE -> Common.Gender.MALE
            Gender.FEMALE -> Common.Gender.FEMALE
          }
      }
    }
  }
}
