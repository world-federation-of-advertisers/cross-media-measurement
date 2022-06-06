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
import java.time.LocalDate
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.api.v2alpha.MeasurementSpec
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.impression
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.reachAndFrequency
import org.wfanet.measurement.api.v2alpha.MeasurementSpecKt.vidSamplingInterval
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventFilter
import org.wfanet.measurement.api.v2alpha.RequisitionSpecKt.eventGroupEntry
import org.wfanet.measurement.api.v2alpha.differentialPrivacyParams
import org.wfanet.measurement.api.v2alpha.measurementSpec
import org.wfanet.measurement.api.v2alpha.requisitionSpec
import org.wfanet.measurement.api.v2alpha.timeInterval
import org.wfanet.measurement.common.toProtoTime
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper

private const val MEASUREMENT_CONSUMER_ID = "ACME"

private val REQUISITION_SPEC = requisitionSpec {
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
            "privacy_budget.gender.value==0 && privacy_budget.age.value==0 && " +
              "banner_ad.gender.value == 1"
        }
      }
  }
}

private val REACH_AND_FREQ_MEASUREMENT_SPEC = measurementSpec {
  reachAndFrequency = reachAndFrequency {
    reachPrivacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }

    frequencyPrivacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.01
    }
    vidSamplingInterval = vidSamplingInterval {
      start = 0.01f
      width = 0.02f
    }
  }
}

private val IMPRESSION_MEASUREMENT_SPEC = measurementSpec {
  impression = impression {
    privacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.02
    }
  }
}

private val DURATION_MEASUREMENT_SPEC = measurementSpec {
  impression = impression {
    privacyParams = differentialPrivacyParams {
      epsilon = 0.3
      delta = 0.02
    }
  }
}

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {
  private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())

  private fun createReference(id: Int, isRefund: Boolean = false) =
    Reference(MEASUREMENT_CONSUMER_ID, "RequisitioId$id", isRefund)

  private fun PrivacyBudgetManager.assertChargeExceedsPrivacyBudget(
    privacyReference: Reference,
    measurementSpec: MeasurementSpec
  ) {
    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        charge(privacyReference, REQUISITION_SPEC, measurementSpec)
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  private fun PrivacyBudgetManager.assertCheckExceedsPrivacyBudget(
    privacyReference: Reference,
    measurementSpec: MeasurementSpec
  ) {
    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        check(privacyReference, REQUISITION_SPEC, measurementSpec)
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `charge throws PRIVACY_BUDGET_EXCEEDED when given a large single charge`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 1.0f, 0.01f)
    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        pbm.charge(createReference(1), REQUISITION_SPEC, REACH_AND_FREQ_MEASUREMENT_SPEC)
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `charge throws INVALID_PRIVACY_BUCKET_FILTER when given wrong event filter`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

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

    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        pbm.charge(createReference(1), requisitionSpec, REACH_AND_FREQ_MEASUREMENT_SPEC)
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER)
  }

  @Test
  fun `checks  privacy budget can be charged empty pbm`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The check succeeds, charges would have filled the Privacy Budget.
    pbm.check(createReference(1), REQUISITION_SPEC, REACH_AND_FREQ_MEASUREMENT_SPEC)
    // It succeeds again because charges are not applied with check
    pbm.check(createReference(2), REQUISITION_SPEC, REACH_AND_FREQ_MEASUREMENT_SPEC)
  }

  @Test
  fun `charges privacy budget for reach and frequency measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.charge(createReference(1), REQUISITION_SPEC, REACH_AND_FREQ_MEASUREMENT_SPEC)

    // Second charge would have exceeded the budget.
    pbm.assertChargeExceedsPrivacyBudget(createReference(2), REACH_AND_FREQ_MEASUREMENT_SPEC)
  }

  @Test
  fun `checks exceeded privacy budget for reach and frequency measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.charge(createReference(1), REQUISITION_SPEC, REACH_AND_FREQ_MEASUREMENT_SPEC)

    // Check fail because charges would have exceeded the budget.
    pbm.assertCheckExceedsPrivacyBudget(createReference(2), REACH_AND_FREQ_MEASUREMENT_SPEC)
  }

  @Test
  fun `charges privacy budget for impression measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.charge(createReference(1), REQUISITION_SPEC, IMPRESSION_MEASUREMENT_SPEC)

    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(createReference(2), IMPRESSION_MEASUREMENT_SPEC)
  }

  @Test
  fun `checks exceeded privacy budget for impression measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.charge(createReference(1), REQUISITION_SPEC, IMPRESSION_MEASUREMENT_SPEC)

    // Check fail because charges would have exceeded the budget.
    pbm.assertCheckExceedsPrivacyBudget(createReference(2), IMPRESSION_MEASUREMENT_SPEC)
  }

  @Test
  fun `charges privacy budget for duration measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.charge(createReference(1), REQUISITION_SPEC, DURATION_MEASUREMENT_SPEC)

    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(createReference(2), DURATION_MEASUREMENT_SPEC)
  }

  @Test
  fun `checks exceeded privacy budget for duration measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.charge(createReference(1), REQUISITION_SPEC, DURATION_MEASUREMENT_SPEC)

    // Check fail because charges would have exceeded the budget.
    pbm.assertCheckExceedsPrivacyBudget(createReference(2), DURATION_MEASUREMENT_SPEC)
  }
}
