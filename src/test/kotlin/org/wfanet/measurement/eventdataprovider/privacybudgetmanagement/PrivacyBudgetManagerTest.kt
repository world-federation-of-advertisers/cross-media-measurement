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

  private fun PrivacyBudgetManager.assertChargeExceedsPrivacyBudget(
    measurementSpec: MeasurementSpec
  ) {
    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        chargePrivacyBudget(MEASUREMENT_CONSUMER_ID, REQUISITION_SPEC, measurementSpec)
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `chargePrivacyBudget throws PRIVACY_BUDGET_EXCEEDED when given a large single charge`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(backingStore, 1.0f, 0.01f)
    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        pbm.chargePrivacyBudget(
          MEASUREMENT_CONSUMER_ID,
          REQUISITION_SPEC,
          REACH_AND_FREQ_MEASUREMENT_SPEC
        )
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `chargePrivacyBudget throws INVALID_PRIVACY_BUCKET_FILTER when given wrong event filter`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(backingStore, 10.0f, 0.02f)

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
        pbm.chargePrivacyBudget(
          MEASUREMENT_CONSUMER_ID,
          requisitionSpec,
          REACH_AND_FREQ_MEASUREMENT_SPEC
        )
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER)
  }

  @Test
  fun `charges privacy budget for reach and frequency measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(
      MEASUREMENT_CONSUMER_ID,
      REQUISITION_SPEC,
      REACH_AND_FREQ_MEASUREMENT_SPEC
    )
    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(REACH_AND_FREQ_MEASUREMENT_SPEC)
  }

  @Test
  fun `charges privacy budget for imprression measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(MEASUREMENT_CONSUMER_ID, REQUISITION_SPEC, IMPRESSION_MEASUREMENT_SPEC)

    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(IMPRESSION_MEASUREMENT_SPEC)
  }

  @Test
  fun `charges privacy budget for duration measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(MEASUREMENT_CONSUMER_ID, REQUISITION_SPEC, DURATION_MEASUREMENT_SPEC)

    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(DURATION_MEASUREMENT_SPEC)
  }
}
