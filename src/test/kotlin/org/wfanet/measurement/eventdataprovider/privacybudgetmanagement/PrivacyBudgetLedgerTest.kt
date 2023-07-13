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

@RunWith(JUnit4::class)
class PrivacyBudgetLedgerTest {

  @Test
  fun `charge works when privacy bucket groups are empty`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 1.0f, 0.01f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )
      val dpCharge = DpCharge(1.0f, 0.01f)

      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(0), setOf<PrivacyBucketGroup>(), setOf(dpCharge))
      ledger.charge(createReference(1), setOf(bucket), setOf(dpCharge))

      // DpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(2), setOf(bucket), setOf(DpCharge(0.0001f, 0.0001f)))
      }
    }

  @Test
  fun `charge works when dpCharge list is empty`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 1.0f, 0.01f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )
      val dpCharge = DpCharge(1.0f, 0.01f)

      // The charges succeed and doesn't charge anything.
      ledger.charge(createReference(0), setOf(bucket), setOf())
      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(1), setOf(bucket), setOf(dpCharge))

      // DpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(2), setOf(bucket), setOf(DpCharge(0.0001f, 0.0001f)))
      }
    }

  @Test
  fun `charge same value repeatedly`() =
    runBlocking<Unit> {
      // See the second test in AdvancedCompositionTest`advanced composition`
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 21.5f, 0.06f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )
      val dpCharge = DpCharge(1.0f, 0.001f)
      // The charges succeed and fills the Privacy Budget.
      for (i in 1..29) {
        ledger.charge(createReference(i), setOf(bucket), setOf(dpCharge))
      }
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(30), setOf(bucket), setOf(dpCharge))
      }
    }

  @Test
  fun `charges with the same reference key are no op`() = runBlocking {
    // See the second test in AdvancedCompositionTest`advanced composition`
    val backingStore = InMemoryBackingStore()
    val ledger = PrivacyBudgetLedger(backingStore, 21.5f, 0.06f)
    val bucket =
      PrivacyBucketGroup(
        "ACME",
        LocalDate.parse("2021-07-01"),
        LocalDate.parse("2021-07-01"),
        AgeGroup.RANGE_35_54,
        Gender.MALE,
        0.3f,
        0.1f
      )
    val dpCharge = DpCharge(1.0f, 0.001f)
    // Only the first charge would fill the budget, rest are not processed by the ledger
    for (i in 1..100) {
      ledger.charge(createReference(0), setOf(bucket), setOf(dpCharge))
    }
  }

  @Test
  fun `refund of the charge works`() =
    runBlocking<Unit> {
      // See the second test in AdvancedCompositionTest`advanced composition`
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 21.5f, 0.06f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )
      val dpCharge = DpCharge(1.0f, 0.001f)
      // The charges succeed and fills the Privacy Budget.
      for (i in 1..29) {
        ledger.charge(createReference(i), setOf(bucket), setOf(dpCharge))
      }
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(30), setOf(bucket), setOf(dpCharge))
      }

      // The refund opens up Privacy Budget.
      ledger.charge(createReference(31, true), setOf(bucket), setOf(dpCharge))
      // Thus, this charge succeeds and fills the budget.
      ledger.charge(createReference(32), setOf(bucket), setOf(dpCharge))

      // Then this charge fails.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(33), setOf(bucket), setOf(dpCharge))
      }
    }

  @Test
  fun `charge different values and dp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 1.0001f, 0.01f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )

      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(0), setOf(bucket), setOf(DpCharge(0.4f, 0.001f)))
      ledger.charge(createReference(1), setOf(bucket), setOf(DpCharge(0.3f, 0.001f)))
      ledger.charge(createReference(2), setOf(bucket), setOf(DpCharge(0.2f, 0.001f)))
      ledger.charge(createReference(3), setOf(bucket), setOf(DpCharge(0.1f, 0.001f)))

      // DpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(4), setOf(bucket), setOf(DpCharge(0.1f, 0.001f)))
      }
    }

  @Test
  fun `charge multiple buckets and dp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 1.0001f, 0.01f)
      val bucket1 =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )
      val bucket2 =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.FEMALE,
          0.3f,
          0.1f
        )

      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(0), setOf(bucket1), setOf(DpCharge(0.5f, 0.001f)))
      ledger.charge(createReference(1), setOf(bucket2), setOf(DpCharge(0.5f, 0.001f)))
      ledger.charge(createReference(2), setOf(bucket1), setOf(DpCharge(0.5f, 0.001f)))
      ledger.charge(createReference(3), setOf(bucket2), setOf(DpCharge(0.5f, 0.001f)))

      // DpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(4), setOf(bucket1), setOf(DpCharge(0.5f, 0.001f)))
      }

      // DpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(5), setOf(bucket2), setOf(DpCharge(0.5f, 0.001f)))
      }
    }

  @Test
  fun `charge list of values and dp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 1.0001f, 0.01f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )

      val chargeList = setOf(DpCharge(0.5f, 0.001f), DpCharge(0.3f, 0.001f), DpCharge(0.2f, 0.001f))

      // The charges succeed and fills the Privacy Budget.
      ledger.charge(createReference(0), setOf(bucket), chargeList)

      // DpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.charge(createReference(1), setOf(bucket), setOf(DpCharge(0.1f, 0.001f)))
      }
    }

  @Test
  fun `chargeInAcdp works when privacyBucketGroups are empty`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 3.0f, 2E-4f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )
      val acdpCharge = AcdpCharge(0.04, 5.0E-6)

      // chargeInAcdp is no op when privacyBucketGroups is empty
      ledger.chargeInAcdp(createReference(0), setOf<PrivacyBucketGroup>(), setOf(acdpCharge))
      // The acdp charges succeed and fills the Privacy Budget.
      ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(acdpCharge))

      // The next AcdpCharge should exceed the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(2), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      }
    }

  @Test
  fun `chargeInAcdp with different values and acdp budget is exceeded`() =
    runBlocking<Unit> {
      val backingStore = InMemoryBackingStore()
      val ledger = PrivacyBudgetLedger(backingStore, 3.0f, 5E-4f)
      val bucket =
        PrivacyBucketGroup(
          "ACME",
          LocalDate.parse("2021-07-01"),
          LocalDate.parse("2021-07-01"),
          AgeGroup.RANGE_35_54,
          Gender.MALE,
          0.3f,
          0.1f
        )

      // The charges succeed and fills the privacy budget.
      ledger.chargeInAcdp(createReference(0), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      ledger.chargeInAcdp(createReference(1), setOf(bucket), setOf(AcdpCharge(0.05, 5.0E-6)))
      ledger.chargeInAcdp(createReference(2), setOf(bucket), setOf(AcdpCharge(0.06, 5.0E-6)))
      ledger.chargeInAcdp(createReference(3), setOf(bucket), setOf(AcdpCharge(0.07, 5.0E-6)))

      // ACDP budget used so far: delta = 4.298047710730021E-4. The next AcdpCharge should exceed
      // the budget.
      assertFailsWith<PrivacyBudgetManagerException> {
        ledger.chargeInAcdp(createReference(4), setOf(bucket), setOf(AcdpCharge(0.04, 5.0E-6)))
      }
    }

  companion object {
    private fun createReference(id: Int, isRefund: Boolean = false) =
      Reference("MC1", "RequisitioId$id", isRefund)
  }
}
