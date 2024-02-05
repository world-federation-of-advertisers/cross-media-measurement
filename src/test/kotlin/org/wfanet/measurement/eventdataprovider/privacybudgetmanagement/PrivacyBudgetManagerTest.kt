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
package org.wfanet.measurement.eventdataprovider.privacybudgetmanagement

import com.google.common.truth.Truth.assertThat
import java.time.LocalDate
import java.time.LocalDateTime
import java.time.ZoneOffset
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.common.OpenEndTimeRange
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {

  @Test
  fun `chargePrivacyBudgetInAcdp throws PRIVACY_BUDGET_EXCEEDED when given a large single acdpCharge`() =
    runBlocking {
      val backingStore = InMemoryBackingStore()
      // Set maximumTotalDelta to small value
      val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-8f)

      // With AcdpCharge(rho = 0.04, theta = 5.0E-6), the converted total delta for this charge
      // (with total epsilon = 3.0) is 1.0542768461593835E-4. See unit test in CompositionTest.kt
      val exception =
        assertFailsWith<PrivacyBudgetManagerException> {
          pbm.chargePrivacyBudgetInAcdp(createAcdpQuery("referenceId1"))
        }
      assertThat(exception.errorType)
        .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
    }

  @Test
  fun `chargePrivacyBudgetInAcdp throws INVALID_PRIVACY_BUCKET_FILTER when given wrong event filter`() =
    runBlocking {
      val backingStore = InMemoryBackingStore()
      val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-4f)

      val exception =
        assertFailsWith<PrivacyBudgetManagerException> {
          pbm.chargePrivacyBudgetInAcdp(createAcdpQuery("referenceId", "person.age_group"))
        }
      assertThat(exception.errorType)
        .isEqualTo(PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER)
    }

  @Test
  fun `chargePrivacyBudgetInAcdp checks exceeding acdp budget and will not charge`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-4f)

    // The acdpCharge succeeds and fills the ACDP budget.
    pbm.chargePrivacyBudgetInAcdp(createAcdpQuery("referenceId1"))

    // Second acdpCharge should exceed the budget with computed delta = 2.1085539E-4f. See unit
    // test in
    // CompositionTest.kt
    pbm.assertChargeExceedsPrivacyBudgetInAcdp(
      AcdpQuery(
        Reference(MEASUREMENT_CONSUMER_ID, "referenceId2", false),
        LandscapeMask(listOf(EventGroupSpec(FILTER_EXPRESSION, timeRange)), 0.01f, 0.02f),
        AcdpCharge(0.06, 5.0E-6),
      )
    )
  }

  @Test
  fun `checks empty pbm and returns acdp budget won't be exceeded with chargingWillExceedPrivacyBudgetInAcdp`() =
    runBlocking {
      val backingStore = InMemoryBackingStore()
      val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-4f)

      // Check returns false because charges would not have exceeded the ACDP budget.
      assertThat(pbm.chargingWillExceedPrivacyBudgetInAcdp(createAcdpQuery("referenceId1")))
        .isFalse()
    }

  @Test
  fun `chargingWillExceedPrivacyBudgetInAcdp checks exceeding acdp budget`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-4f)

    // The acdpCharge succeeds and fills the ACDP Budget.
    pbm.chargePrivacyBudgetInAcdp(createAcdpQuery("referenceId1"))

    // Check returns true because charges would have exceeded the ACDP budget.
    assertThat(pbm.chargingWillExceedPrivacyBudgetInAcdp(createAcdpQuery("referenceId2"))).isTrue()
  }

  @Test
  fun `referenceWillBeProcessed returns true for different reference that will be processed for acdp`() =
    runBlocking {
      val backingStore = InMemoryBackingStore()
      val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-4f)

      // The acdpCharge succeeds and fills the ACDP budget.
      pbm.chargePrivacyBudgetInAcdp(createAcdpQuery("referenceId1"))

      // Check returns true because this reference was not processed before.
      assertThat(
          pbm.referenceWillBeProcessed(Reference(MEASUREMENT_CONSUMER_ID, "referenceId2", false))
        )
        .isTrue()
    }

  @Test
  fun `referenceWillBeProcessed returns false for the same reference that will not be processed for acdp`() =
    runBlocking {
      val backingStore = InMemoryBackingStore()
      val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 3.0f, 2E-4f)

      // The acdpCharge succeeds and fills the ACDP budget.
      pbm.chargePrivacyBudgetInAcdp(createAcdpQuery("referenceId1"))

      // Check returns false because this reference was processed before.
      assertThat(
          pbm.referenceWillBeProcessed(Reference(MEASUREMENT_CONSUMER_ID, "referenceId1", false))
        )
        .isFalse()
    }

  companion object {
    private const val MEASUREMENT_CONSUMER_ID = "ACME"
    private const val FILTER_EXPRESSION = "person.gender==1 && person.age_group==1"
    private val today: LocalDateTime = LocalDate.now().atTime(4, 20)
    private val yesterday: LocalDateTime = today.minusDays(1)
    private val startOfTomorrow: LocalDateTime = today.plusDays(1).toLocalDate().atStartOfDay()
    private val timeRange =
      OpenEndTimeRange(
        yesterday.toInstant(ZoneOffset.UTC),
        startOfTomorrow.toInstant(ZoneOffset.UTC),
      )
    private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())

    private fun createAcdpQuery(referenceId: String, expression: String = FILTER_EXPRESSION) =
      AcdpQuery(
        Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
        LandscapeMask(listOf(EventGroupSpec(expression, timeRange)), 0.01f, 0.02f),
        AcdpCharge(0.04, 5.0E-6),
      )

    private suspend fun PrivacyBudgetManager.assertChargeExceedsPrivacyBudgetInAcdp(
      acdpQuery: AcdpQuery
    ) {
      val exception =
        assertFailsWith<PrivacyBudgetManagerException> { chargePrivacyBudgetInAcdp(acdpQuery) }
      assertThat(exception.errorType)
        .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
    }
  }
}
