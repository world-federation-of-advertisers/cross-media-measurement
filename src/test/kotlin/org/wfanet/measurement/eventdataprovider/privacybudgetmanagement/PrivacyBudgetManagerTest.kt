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

private const val MEASUREMENT_CONSUMER_ID = "ACME"
private const val FILTER_EXPRESSION = "person.gender.value==1 && person.age_group.value==1"

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {
  private val today: LocalDateTime = LocalDate.now().atTime(4, 20)
  private val yesterday: LocalDateTime = today.minusDays(1)
  private val startOfTomorrow: LocalDateTime = today.plusDays(1).toLocalDate().atStartOfDay()
  private val timeRange =
    OpenEndTimeRange(yesterday.toInstant(ZoneOffset.UTC), startOfTomorrow.toInstant(ZoneOffset.UTC))

  private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())

  private fun createQuery(referenceId: String, expression: String = FILTER_EXPRESSION) =
    Query(
      Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
      LandscapeMask(listOf(EventGroupSpec(expression, timeRange)), 0.01f, 0.02f),
      Charge(0.6f, 0.02f)
    )

  private suspend fun PrivacyBudgetManager.assertChargeExceedsPrivacyBudget(
    query: Query,
  ) {
    val exception = assertFailsWith<PrivacyBudgetManagerException> { chargePrivacyBudget(query) }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `charge throws PRIVACY_BUDGET_EXCEEDED when given a large single charge`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 1.0f, 0.01f)
    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        pbm.chargePrivacyBudget(createQuery("referenceId1"))
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `charge throws INVALID_PRIVACY_BUCKET_FILTER when given wrong event filter`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        pbm.chargePrivacyBudget(createQuery("referenceId", "person.age_group.value"))
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER)
  }

  @Test
  fun `charges privacy budget for measurement`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(createQuery("referenceId1"))

    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(createQuery("referenceId2"))
  }

  @Test
  fun `checks empty privacy budget manager and returns budget wont be exceeded`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)
    // Check returns false because charges would not have exceeded the budget.
    assertThat(pbm.chargingWillExceedPrivacyBudget(createQuery("referenceId1"))).isFalse()
  }

  @Test
  fun `checks exceeded privacy budget for measurement`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(createQuery("referenceId1"))

    // Check returns true because charges would have exceeded the budget.
    assertThat(pbm.chargingWillExceedPrivacyBudget(createQuery("referenceId2"))).isTrue()
  }

  @Test
  fun `returns true for different reference that will be processed`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(createQuery("referenceId1"))

    // Check returns true because this reference was not processed before.
    assertThat(
        pbm.referenceWillBeProcessed(Reference(MEASUREMENT_CONSUMER_ID, "referenceId2", false))
      )
      .isTrue()
  }
  @Test
  fun `returns false for same reference that will not be processed`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(createQuery("referenceId1"))

    // Check returns false because this reference was processed before.
    assertThat(
        pbm.referenceWillBeProcessed(Reference(MEASUREMENT_CONSUMER_ID, "referenceId1", false))
      )
      .isFalse()
  }
}
