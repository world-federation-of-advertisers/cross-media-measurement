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

import com.google.common.truth.Truth.assertThat
import com.google.protobuf.Message
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.projectnessie.cel.Program
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplate.AgeRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt
import org.wfanet.measurement.api.v2alpha.event_templates.testing.TestPrivacyBudgetTemplateKt.ageRange
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testEvent
import org.wfanet.measurement.api.v2alpha.event_templates.testing.testPrivacyBudgetTemplate
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.eventfiltration.EventFilters
import org.wfanet.measurement.eventdataprovider.eventfiltration.validation.EventFilterValidationException

private const val MEASUREMENT_CONSUMER_ID = "ACME"

private val MEASUREMENT_SPEC = measurementSpec {
  reachAndFrequency = reachAndFrequency {
    vidSamplingInterval = vidSamplingInterval {
      start = 0.0f
      width = 0.01f
    }
  }
}

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
          AgeGroup.RANGE_18_34 -> age = ageRange { value = AgeRange.Value.AGE_18_TO_24 }
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

@RunWith(JUnit4::class)
class PrivacyBucketFilterTest {

  private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())

  @Test
  fun `Mapper fails for invalid filter expression`() {

    val requisitionSpec = requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter { expression = "privacy_budget.age.value" }
          }
      }
    }

    assertFailsWith<PrivacyBudgetManagerException> {
      privacyBucketFilter.getPrivacyBucketGroups(
        MEASUREMENT_CONSUMER_ID,
        MEASUREMENT_SPEC,
        requisitionSpec
      )
    }
  }

  @Test
  fun `Mapper succeeds for filter expression with only privacy budget Fields`() {

    val requisitionSpec = requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter {
              expression = "privacy_budget.age.value in [0] && privacy_budget.gender.value == 1"
            }
          }
      }
    }

    assertThat(
        privacyBucketFilter.getPrivacyBucketGroups(
          MEASUREMENT_CONSUMER_ID,
          MEASUREMENT_SPEC,
          requisitionSpec
        )
      )
      .containsExactly(
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now(),
          LocalDate.now(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          0.01f
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now().minusDays(1),
          LocalDate.now().minusDays(1),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          0.01f
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now(),
          LocalDate.now(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.01f,
          0.01f
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now().minusDays(1),
          LocalDate.now().minusDays(1),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.01f,
          0.01f
        ),
      )
  }

  @Test
  fun `Mapper succeeds with privacy budget Field and non Privacy budget fields`() {

    val requisitionSpec = requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter {
              expression =
                "privacy_budget.age.value in [0] && privacy_budget.gender.value == 1 && " +
                  "banner_ad.gender.value == 1"
            }
          }
      }
    }

    assertThat(
        privacyBucketFilter.getPrivacyBucketGroups(
          MEASUREMENT_CONSUMER_ID,
          MEASUREMENT_SPEC,
          requisitionSpec
        )
      )
      .containsExactly(
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now(),
          LocalDate.now(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          0.01f
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now().minusDays(1),
          LocalDate.now().minusDays(1),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.0f,
          0.01f
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now(),
          LocalDate.now(),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.01f,
          0.01f
        ),
        PrivacyBucketGroup(
          MEASUREMENT_CONSUMER_ID,
          LocalDate.now().minusDays(1),
          LocalDate.now().minusDays(1),
          AgeGroup.RANGE_18_34,
          Gender.FEMALE,
          0.01f,
          0.01f
        ),
      )
  }

  @Test
  fun `Non Privacy Budget Fields may charge more buckets than necessary`() {

    val requisitionSpec = requisitionSpec {
      eventGroups += eventGroupEntry {
        key = "eventGroups/someEventGroup"
        value =
          RequisitionSpecKt.EventGroupEntryKt.value {
            collectionInterval = timeInterval {
              startTime =
                LocalDate.now().minusDays(1).atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
              endTime = LocalDate.now().atStartOfDay().toInstant(ZoneOffset.UTC).toProtoTime()
            }
            filter = eventFilter {
              expression =
                "privacy_budget.age.value in [0] && privacy_budget.gender.value == 1 || " +
                  "banner_ad.gender.value == 1"
            }
          }
      }
    }
    val result =
      privacyBucketFilter.getPrivacyBucketGroups(
        MEASUREMENT_CONSUMER_ID,
        MEASUREMENT_SPEC,
        requisitionSpec
      )
    assertThat(result).hasSize(24)
  }
}
