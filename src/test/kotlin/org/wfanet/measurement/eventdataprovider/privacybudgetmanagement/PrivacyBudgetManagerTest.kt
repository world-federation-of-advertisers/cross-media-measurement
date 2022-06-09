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
import kotlin.test.assertFailsWith
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.privacybudgetmanagement.testing.TestPrivacyBucketMapper

private const val MEASUREMENT_CONSUMER_ID = "ACME"

private const val FILTER_EXPRESSION =
  "privacy_budget.gender.value==0 && privacy_budget.age.value==0 && " +
    "banner_ad.gender.value == 1"

@RunWith(JUnit4::class)
class PrivacyBudgetManagerTest {
  private val privacyBucketFilter = PrivacyBucketFilter(TestPrivacyBucketMapper())

  private fun createQuery(referenceId: String, expression: String = FILTER_EXPRESSION) =
    Query(
      Reference(MEASUREMENT_CONSUMER_ID, referenceId, false),
      LandscapeMask(
        listOf(EventGroupSpec(expression, LocalDate.now().minusDays(1), LocalDate.now())),
        0.01f,
        0.02f
      ),
      Charge(0.6f, 0.02f)
    )

  private fun PrivacyBudgetManager.assertChargeExceedsPrivacyBudget(
    query: Query,
  ) {
    val exception = assertFailsWith<PrivacyBudgetManagerException> { chargePrivacyBudget(query) }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.PRIVACY_BUDGET_EXCEEDED)
  }

  @Test
  fun `chargePrivacyBudget throws PRIVACY_BUDGET_EXCEEDED when given a large single charge`() {
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
  fun `chargePrivacyBudget throws INVALID_PRIVACY_BUCKET_FILTER when given wrong event filter`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    val exception =
      assertFailsWith<PrivacyBudgetManagerException> {
        pbm.chargePrivacyBudget(createQuery("referenceId", "privacy_budget.age.value"))
      }
    assertThat(exception.errorType)
      .isEqualTo(PrivacyBudgetManagerExceptionType.INVALID_PRIVACY_BUCKET_FILTER)
  }

  @Test
  fun `charges privacy budget for measurement`() {
    val backingStore = InMemoryBackingStore()
    val pbm = PrivacyBudgetManager(privacyBucketFilter, backingStore, 10.0f, 0.02f)

    // The charge succeeds and fills the Privacy Budget.
    pbm.chargePrivacyBudget(createQuery("referenceId1"))

    // Second charge should exceed the budget.
    pbm.assertChargeExceedsPrivacyBudget(createQuery("referenceId2"))
  }
}
