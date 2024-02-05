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

import java.time.LocalDate
import kotlin.test.assertFailsWith
import kotlinx.coroutines.runBlocking
import org.junit.Test
import org.junit.runner.RunWith
import org.junit.runners.JUnit4
import org.wfanet.measurement.eventdataprovider.noiser.DpParams

@RunWith(JUnit4::class)
class PrivacyBudgetLedgerTest {

  @Test
  fun `chargeInAcdp works when privacyBucketGroups are empty`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 2E-4))
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      // chargeInAcdp is no op when privacyBucketGroups is empty
      ledger.chargeInAcdp(createReference(0), setOf(), setOf(acdpCharge))
      // The acdpCharges succeed and fills the Privacy Budget.
      ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(acdpCharge))

      // The next acdpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(2), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      }
    }

  @Test
  fun `chargeInAcdp works when acdpCharge list is empty`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 2E-4))
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      // The acdpCharges succeed and doesn't charge anything.
      ledger.chargeInAcdp(createReference(0), setOf(bucket), setOf())
      // The acdpCharges succeed and fills the Privacy Budget.
      ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(acdpCharge))

      // Next acdpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(2), setOf(bucket), setOf(acdpCharge))
      }
    }

  @Test
  fun `chargeInAcdp the same value repeatedly and works`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 2E-3))
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)
      // The acdpCharges succeed and fills the Privacy Budget.
      for (i in 1..9) {
        ledger.chargeInAcdp(createReference(i), setOf(bucket), setOf(acdpCharge))
      }

      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(10), setOf(bucket), setOf(acdpCharge))
      }
    }

  @Test
  fun `chargeInAcdp with the same reference key are no op`() = runBlocking {
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 2E-4))
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f,
      )
    val acdpCharge = AcdpCharge(0.04, 5.0E-6)
    // Only the first acdpCharge would fill the budget, rest are not processed by the ledger
    for (i in 1..100) {
      ledger.chargeInAcdp(createReference(0), setOf(bucket), setOf(acdpCharge))
    }
  }

  @Test
  fun `refund of the acdp charge works`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 2E-4))
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      // The acdpCharge succeed and fills the Privacy Budget.
      ledger.chargeInAcdp(createReference(0), setOf(bucket), setOf(acdpCharge))

      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(acdpCharge))
      }

      // The refund opens up Privacy Budget.
      ledger.chargeInAcdp(createReference(2, true), setOf(bucket), setOf(acdpCharge))
      // Thus, this acdpCharge succeeds and fills the budget.
      ledger.chargeInAcdp(createReference(3), setOf(bucket), setOf(acdpCharge))

      // Then this acdpCharge fails.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(4), setOf(bucket), setOf(acdpCharge))
      }
    }

  @Test
  fun `chargeInAcdp with different values and acdp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 5E-4))
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )

      // The acdpCharges succeed and fills the privacy budget.
      ledger.chargeInAcdp(createReference(0), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(AcdpCharge(0.05, 5.0E-6)))
      ledger.chargeInAcdp(createReference(2), setOf(bucket), setOf(AcdpCharge(0.06, 5.0E-6)))
      ledger.chargeInAcdp(createReference(3), setOf(bucket), setOf(AcdpCharge(0.07, 5.0E-6)))

      // ACDP budget used so far: delta = 4.298047710730021E-4. The next acdpCharge should exceed
      // the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(4), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      }
    }

  @Test
  fun `chargeInAcdp multiple buckets and acdp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 2.5E-4))
      val bucket1 =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )
      val bucket2 =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.FEMALE,
          0.3f,
          0.1f,
        )

      val acdpCharge = AcdpCharge(0.04, 5.0E-6)
      // The acdpCharges succeed and fills the Privacy Budget.
      ledger.chargeInAcdp(createReference(0), setOf(bucket1), setOf(acdpCharge))
      ledger.chargeInAcdp(createReference(1), setOf(bucket2), setOf(acdpCharge))
      ledger.chargeInAcdp(createReference(2), setOf(bucket1), setOf(acdpCharge))
      ledger.chargeInAcdp(createReference(3), setOf(bucket2), setOf(acdpCharge))

      // acdpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(4), setOf(bucket1), setOf(acdpCharge))
      }

      // acdpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(5), setOf(bucket2), setOf(acdpCharge))
      }
    }

  @Test
  fun `chargeInAcdp list of values and acdp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, DpParams(3.0, 5E-4))
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f,
        )

      val acdpChargesList =
        setOf(
          AcdpCharge(0.04, 5.0E-6),
          AcdpCharge(0.05, 5.0E-6),
          AcdpCharge(0.06, 5.0E-6),
          AcdpCharge(0.07, 5.0E-6),
        )

      // The acdpCharges succeed and fills the Privacy Budget.
      ledger.chargeInAcdp(createReference(0), setOf(bucket), acdpChargesList)

      // Next acdpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      }
    }

  companion object {
    private fun createReference(id: Int, isRefund: Boolean = false) =
      Reference("MC1", "RequisitionId$id", isRefund)
  }
}
